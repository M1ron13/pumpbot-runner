"""Pump Detector Bot — мониторинг USDT-перпетуалов Binance Futures / Bybit linear.

Ищет резкий аномальный рост цены (памп) и шлёт алерт в Telegram.
Бот НЕ торгует и НЕ использует API-ключи: только публичные REST-эндпоинты.

Все пороги и параметры — в config.json.
"""

import argparse
import asyncio
import gzip
import json
import logging
import math
import os
import signal
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Iterable, List, Optional, Tuple

CONFIG_DEFAULT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")

log = logging.getLogger("pumpbot")


def now() -> float:
    """Единственный источник времени в боте.

    Тесты подменяют эту функцию (pump_bot.now = fake_clock), чтобы
    прогонять историю быстрее реального времени.
    """
    return time.time()


# --------------------------------------------------------------------------- #
# конфиг и форматирование
# --------------------------------------------------------------------------- #

def load_config(path: str = CONFIG_DEFAULT_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    return apply_env_overrides(cfg)


def apply_env_overrides(cfg: dict) -> dict:
    """Секреты и пути можно задать переменными окружения.

    Так один и тот же образ работает и локально, и в облаке: токен приходит
    из GitHub/Fly secrets, а в репозитории лежит config.example.json без секретов.
    """
    overrides = (
        ("PUMPBOT_TG_TOKEN", "telegram", "bot_token", str),
        ("PUMPBOT_TG_CHAT_ID", "telegram", "chat_id", str),
        ("PUMPBOT_LOG_FILE", "logging", "file", str),
        ("PUMPBOT_LOG_LEVEL", "logging", "level", str),
        ("PUMPBOT_STATE_FILE", "runtime", "state_file", str),
        ("PUMPBOT_STARTUP_PING", "runtime", "startup_ping", lambda v: v.strip().lower() in ("1", "true", "yes", "on")),
        ("PUMPBOT_MAX_RUNTIME_SEC", "runtime", "max_runtime_sec", float),
        ("PUMPBOT_STATE_SAVE_SEC", "runtime", "state_save_sec", float),
    )
    for env_name, section, key, cast in overrides:
        raw = os.environ.get(env_name)
        if raw is None or raw == "":
            continue
        cfg.setdefault(section, {})[key] = cast(raw)
    return cfg


def setup_logging(cfg: dict) -> None:
    log_cfg = cfg["logging"]
    root = logging.getLogger()
    root.setLevel(getattr(logging, log_cfg["level"]))
    fmt = logging.Formatter(log_cfg["format"])
    for handler in list(root.handlers):
        root.removeHandler(handler)
    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    root.addHandler(stream)
    file_handler = logging.FileHandler(log_cfg["file"], encoding="utf-8")
    file_handler.setFormatter(fmt)
    root.addHandler(file_handler)


def fmt_volume(value: Optional[float], decimals: int) -> str:
    """$X.XB / $X.XM / $XK."""
    if value is None:
        return "n/a"
    if value >= 1e9:
        return f"${value / 1e9:.{decimals}f}B"
    if value >= 1e6:
        return f"${value / 1e6:.{decimals}f}M"
    return f"${value / 1e3:.0f}K"


def fmt_price(price: float, significant: int) -> str:
    """Цена без потери мелких знаков у дешёвых монет."""
    if price <= 0:
        return "0"
    exponent = math.floor(math.log10(abs(price)))
    decimals = max(0, significant - 1 - exponent)
    return f"{price:.{decimals}f}".rstrip("0").rstrip(".") or "0"


def fmt_funding(rate: Optional[float], decimals: int) -> str:
    if rate is None:
        return "n/a"
    return f"{rate * 100:.{decimals}f}%"


def mean(values: Iterable[float]) -> float:
    values = list(values)
    return sum(values) / len(values)


def stdev(values: Iterable[float], mu: float) -> float:
    values = list(values)
    if len(values) < 2:
        return 0.0
    variance = sum((v - mu) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(max(0.0, variance))


# --------------------------------------------------------------------------- #
# данные
# --------------------------------------------------------------------------- #

@dataclass
class Snapshot:
    """Снимок тикера с биржи на момент опроса."""
    exchange: str
    symbol: str
    price: float
    quote_volume_24h: float
    funding_rate: Optional[float] = None


@dataclass
class SymbolState:
    """История по одному ключу EXCHANGE:SYMBOL (in-memory)."""
    first_seen: float
    ticks: deque = field(default_factory=deque)      # maxlen задаётся в create() из конфига
    moves_15m: deque = field(default_factory=deque)
    vols_15m: deque = field(default_factory=deque)
    funding_rate: Optional[float] = None

    @classmethod
    def create(cls, ts: float, ticks_maxlen: int, history_maxlen: int) -> "SymbolState":
        return cls(
            first_seen=ts,
            ticks=deque(maxlen=ticks_maxlen),
            moves_15m=deque(maxlen=history_maxlen),
            vols_15m=deque(maxlen=history_maxlen),
        )

    def add_tick(self, ts: float, price: float, cum_quote_volume_24h: float) -> None:
        self.ticks.append((ts, price, cum_quote_volume_24h))

    @property
    def last(self) -> Optional[Tuple[float, float, float]]:
        return self.ticks[-1] if self.ticks else None

    def tick_at_or_before(self, target_ts: float) -> Optional[Tuple[float, float, float]]:
        """Ближайший тик не позже target_ts; None если истории не хватает."""
        for tick in reversed(self.ticks):
            if tick[0] <= target_ts:
                return tick
        return None

    def move_pct(self, window_sec: float) -> Optional[float]:
        """Доходность за окно в процентных пунктах."""
        last = self.last
        if last is None:
            return None
        past = self.tick_at_or_before(last[0] - window_sec)
        if past is None or past[1] <= 0 or past is last:
            return None
        return (last[1] / past[1] - 1.0) * 100.0

    def interval_volume(self, window_sec: float) -> Optional[float]:
        """Объём за окно как дельта кумулятивного 24h-объёма.

        None, если истории нет или дельта отрицательная (24h-окно уже
        скользнуло и «съело» часть объёма) — такой замер пропускаем.
        """
        last = self.last
        if last is None:
            return None
        past = self.tick_at_or_before(last[0] - window_sec)
        if past is None or past is last:
            return None
        delta = last[2] - past[2]
        if delta < 0:
            return None
        return delta


@dataclass
class Signal:
    """Сработавший триггер по одной бирже (до дедупа)."""
    exchange: str
    symbol: str
    triggers: List[str]
    move_15m: Optional[float]
    move_5m: Optional[float]
    zscore: float
    vol_mult: float
    price: float
    volume_24h: float
    funding_rate: Optional[float]
    ts: float


@dataclass
class Alert:
    """Готовый к отправке алерт (после дедупа между биржами)."""
    symbol: str
    exchanges: List[str]
    primary: Signal
    ts: float

    @property
    def is_fast(self) -> bool:
        return "FAST" in self.primary.triggers


# --------------------------------------------------------------------------- #
# детектор
# --------------------------------------------------------------------------- #

class Detector:
    """Чистая (без сети) логика детекции — её же гоняют тесты."""

    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.det = cfg["detection"]
        self.uni = cfg["universe"]
        self.btc = cfg["btc_filter"]
        self.al = cfg["alerts"]
        self.states: Dict[str, SymbolState] = {}
        self.last_alert_ts: Dict[str, float] = {}
        self.alert_history: deque = deque()
        self.binance_funding: Dict[str, float] = {}

    # -- состояние ---------------------------------------------------------- #

    @staticmethod
    def key(exchange: str, symbol: str) -> str:
        return f"{exchange}:{symbol}"

    def ingest(self, snapshots: Iterable[Snapshot], ts: Optional[float] = None) -> None:
        ts = now() if ts is None else ts
        suffix = self.uni["quote_suffix"]
        for snap in snapshots:
            if not snap.symbol.endswith(suffix):
                continue
            if snap.price <= 0:
                continue
            key = self.key(snap.exchange, snap.symbol)
            state = self.states.get(key)
            if state is None:
                state = SymbolState.create(ts, self.det["ticks_maxlen"], self.det["history_maxlen"])
                self.states[key] = state
            state.add_tick(ts, snap.price, snap.quote_volume_24h)
            funding = snap.funding_rate
            if funding is None:
                funding = self.binance_funding.get(snap.symbol) if snap.exchange == "BINANCE" else None
            if funding is not None:
                state.funding_rate = funding

    # -- снимок состояния на диск ------------------------------------------ #

    def dump_state(self, history_keep: int) -> dict:
        """Компактный снимок состояния для восстановления после рестарта.

        Тики НЕ сохраняются (их объём не оправдан): после старта 15-минутное
        окно набирается заново за 15 минут. Главное — статистика (`moves_15m`,
        `vols_15m`) и `first_seen`, без которых бот молчал бы 120 минут.
        Cooldown'ы и суточный счётчик тоже едут в снимке, иначе после рестарта
        тот же памп прилетит в канал повторно.
        """
        states = {}
        for key, state in self.states.items():
            if not state.moves_15m and not state.vols_15m:
                continue
            states[key] = {
                "first_seen": round(state.first_seen, 3),
                "funding_rate": state.funding_rate,
                "moves": [round(v, 4) for v in list(state.moves_15m)[-history_keep:]],
                "vols": [round(v, 2) for v in list(state.vols_15m)[-history_keep:]],
            }
        return {
            "version": 1,
            "saved_at": now(),
            "states": states,
            "last_alert_ts": self.last_alert_ts,
            "alert_history": list(self.alert_history),
            "binance_funding": self.binance_funding,
        }

    def load_state(self, data: dict, max_age_sec: float) -> int:
        """Восстановить состояние из снимка. Возвращает число поднятых символов.

        Слишком старый снимок игнорируется целиком: по устаревшей статистике
        считать z-score опаснее, чем набрать её заново.
        """
        if not isinstance(data, dict) or data.get("version") != 1:
            log.warning("снимок состояния незнакомого формата — старт с чистого листа")
            return 0
        age = now() - float(data.get("saved_at", 0.0))
        if age > max_age_sec:
            log.warning("снимок состояния устарел (%.0f ч) — старт с чистого листа", age / 3600.0)
            return 0

        ticks_maxlen = self.det["ticks_maxlen"]
        history_maxlen = self.det["history_maxlen"]
        restored = 0
        for key, blob in (data.get("states") or {}).items():
            try:
                state = SymbolState.create(float(blob["first_seen"]), ticks_maxlen, history_maxlen)
                state.funding_rate = blob.get("funding_rate")
                state.moves_15m.extend(float(v) for v in blob.get("moves") or [])
                state.vols_15m.extend(float(v) for v in blob.get("vols") or [])
            except (KeyError, TypeError, ValueError):
                continue
            self.states[key] = state
            restored += 1

        self.last_alert_ts.update({k: float(v) for k, v in (data.get("last_alert_ts") or {}).items()})
        self.alert_history.extend(float(v) for v in (data.get("alert_history") or []))
        self.binance_funding.update({k: float(v) for k, v in (data.get("binance_funding") or {}).items()})
        log.info("состояние восстановлено: %s символов, снимку %.0f сек", restored, age)
        return restored

    def set_binance_funding(self, rates: Dict[str, float]) -> None:
        self.binance_funding.update(rates)
        for symbol, rate in rates.items():
            state = self.states.get(self.key("BINANCE", symbol))
            if state is not None:
                state.funding_rate = rate

    # -- рыночный режим ----------------------------------------------------- #

    def btc_multiplier(self) -> float:
        """1.0 в спокойном рынке, threshold_multiplier если BTC сам летит вверх."""
        window = self.det["window_main_sec"]
        moves = []
        for exchange in ("BINANCE", "BYBIT"):
            state = self.states.get(self.key(exchange, self.btc["btc_symbol"]))
            if state is None:
                continue
            move = state.move_pct(window)
            if move is not None:
                moves.append(move)
        if moves and max(moves) >= self.btc["btc_move_15m_pct"]:
            return float(self.btc["threshold_multiplier"])
        return 1.0

    # -- основной проход ---------------------------------------------------- #

    def step(self, snapshots: Iterable[Snapshot], ts: Optional[float] = None) -> List[Alert]:
        """Один проход детектора: принять снимки, обновить статистику, выдать алерты."""
        ts = now() if ts is None else ts
        self.ingest(snapshots, ts)

        window_main = self.det["window_main_sec"]
        window_fast = self.det["window_fast_sec"]
        min_obs = self.det["min_observations"]
        min_history_sec = self.uni["min_history_minutes"] * 60.0
        excluded = set(self.uni["exclude_symbols"])

        btc_mult = self.btc_multiplier()
        signals: List[Signal] = []

        for key, state in self.states.items():
            exchange, symbol = key.split(":", 1)
            last = state.last
            if last is None or last[0] < ts:
                continue  # на этом проходе биржа не ответила по символу

            move_15m = state.move_pct(window_main)
            vol_15m = state.interval_volume(window_main)

            # статистика считается по истории ДО текущего наблюдения,
            # иначе сам памп поднимает mu/sigma и глушит свой же z-score
            hist_moves = list(state.moves_15m)
            hist_vols = list(state.vols_15m)
            if move_15m is not None:
                state.moves_15m.append(move_15m)
            if vol_15m is not None:
                state.vols_15m.append(vol_15m)

            if symbol in excluded:
                continue
            if last[2] < self.uni["min_24h_volume_usd"]:
                continue
            if ts - state.first_seen < min_history_sec:
                continue
            if move_15m is None or vol_15m is None:
                continue
            if len(hist_moves) < min_obs or len(hist_vols) < min_obs:
                continue

            mu = mean(hist_moves)
            sigma = max(stdev(hist_moves, mu), self.det["sigma_floor_pct"])
            vol_baseline = mean(hist_vols)
            if vol_baseline <= 0:
                continue

            zscore = (move_15m - mu) / sigma
            vol_mult = vol_15m / vol_baseline
            move_5m = state.move_pct(window_fast)
            vol_5m = state.interval_volume(window_fast)

            triggers: List[str] = []
            if (
                zscore >= self.det["zscore_threshold"] * btc_mult
                and move_15m >= self.det["min_abs_move_main_pct"] * btc_mult
                and vol_15m >= self.det["volume_mult_main"] * vol_baseline
            ):
                triggers.append("MAIN")

            fast_baseline = vol_baseline * (window_fast / window_main)
            if (
                move_5m is not None
                and vol_5m is not None
                and move_5m >= self.det["fast_move_pct"] * btc_mult
                and vol_5m >= self.det["volume_mult_fast"] * fast_baseline
            ):
                triggers.append("FAST")

            if not triggers:
                continue

            signals.append(Signal(
                exchange=exchange,
                symbol=symbol,
                triggers=triggers,
                move_15m=move_15m,
                move_5m=move_5m,
                zscore=zscore,
                vol_mult=vol_mult,
                price=last[1],
                volume_24h=last[2],
                funding_rate=state.funding_rate,
                ts=ts,
            ))

        return self._dedupe_and_throttle(signals, ts)

    # -- анти-спам ---------------------------------------------------------- #

    def _dedupe_and_throttle(self, signals: List[Signal], ts: float) -> List[Alert]:
        by_symbol: Dict[str, List[Signal]] = {}
        for sig in signals:
            by_symbol.setdefault(sig.symbol, []).append(sig)

        merged: List[Alert] = []
        for symbol, group in by_symbol.items():
            primary = max(group, key=lambda s: s.zscore)
            triggers = [t for t in ("MAIN", "FAST") if any(t in s.triggers for s in group)]
            primary.triggers = triggers
            merged.append(Alert(
                symbol=symbol,
                exchanges=sorted({s.exchange for s in group}),
                primary=primary,
                ts=ts,
            ))

        merged.sort(key=lambda a: a.primary.zscore, reverse=True)

        cooldown_sec = self.al["cooldown_min"] * 60.0
        day_window = self.al["day_window_sec"]
        while self.alert_history and ts - self.alert_history[0] > day_window:
            self.alert_history.popleft()

        accepted: List[Alert] = []
        for alert in merged:
            last_ts = self.last_alert_ts.get(alert.symbol)
            if last_ts is not None and ts - last_ts < cooldown_sec:
                continue
            if len(self.alert_history) >= self.al["max_per_day"]:
                log.warning("дневной лимит алертов исчерпан (%s), %s подавлен",
                            self.al["max_per_day"], alert.symbol)
                continue
            self.last_alert_ts[alert.symbol] = ts
            self.alert_history.append(ts)
            accepted.append(alert)
        return accepted


# --------------------------------------------------------------------------- #
# сообщение
# --------------------------------------------------------------------------- #

def render_alert(alert: Alert, cfg: dict) -> str:
    al = cfg["alerts"]
    sig = alert.primary
    base = sig.symbol[: -len(cfg["universe"]["quote_suffix"])] or sig.symbol
    quote = cfg["universe"]["quote_suffix"]
    exchanges = " + ".join(alert.exchanges)
    chart = al["chart_url_template"].format(exchange=sig.exchange, symbol=sig.symbol)
    clock = datetime.fromtimestamp(alert.ts, tz=timezone.utc).strftime("%H:%M:%S")

    lines = [f"🔴 <b>PUMP: {base}/{quote}</b> [{exchanges}]"]
    if alert.is_fast and sig.move_5m is not None:
        lines.append(f"⚡ <b>FAST +{sig.move_5m:.1f}%/5м — вертикаль</b>")
    move_line = f"📈 +{sig.move_15m:.1f}% / 15м"
    if sig.move_5m is not None:
        move_line += f"  (+{sig.move_5m:.1f}% / 5м)"
    lines.append(move_line)
    lines.append(f"⚡ z-score: {sig.zscore:.1f} | Vol: {sig.vol_mult:.1f}x avg")
    lines.append(
        f"💰 Цена: {fmt_price(sig.price, al['price_significant_digits'])}"
        f" | 24h Vol: {fmt_volume(sig.volume_24h, al['volume_decimals'])}"
    )
    lines.append(f"💸 Funding: {fmt_funding(sig.funding_rate, al['funding_decimals'])}")
    lines.append(f"🕐 {clock} UTC")
    lines.append(f"📊 График (ссылка: {chart})")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# сеть
# --------------------------------------------------------------------------- #

class RateLimitError(Exception):
    """HTTP 429/418 — биржа просит притормозить."""


class Feed:
    """Тонкая обёртка над aiohttp: таймауты, backoff, парсинг тикеров."""

    def __init__(self, cfg: dict, session):
        self.cfg = cfg
        self.net = cfg["network"]
        self.session = session
        self.backoff_until: Dict[str, float] = {}
        self.backoff_sec: Dict[str, float] = {}

    def throttled(self, name: str) -> bool:
        until = self.backoff_until.get(name, 0.0)
        return now() < until

    def _register_rate_limit(self, name: str) -> None:
        current = self.backoff_sec.get(name, 0.0)
        nxt = self.net["backoff_start_sec"] if current <= 0 else min(current * 2, self.net["backoff_max_sec"])
        self.backoff_sec[name] = nxt
        self.backoff_until[name] = now() + nxt
        log.warning("%s: rate limit, backoff %.0f сек", name, nxt)

    def _reset_backoff(self, name: str) -> None:
        self.backoff_sec[name] = 0.0
        self.backoff_until[name] = 0.0

    async def get_json(self, name: str, url: str) -> Optional[dict]:
        if self.throttled(name):
            return None
        import aiohttp  # локальный импорт: тесты работают без aiohttp
        timeout = aiohttp.ClientTimeout(total=self.net["request_timeout_sec"])
        try:
            async with self.session.get(url, timeout=timeout) as resp:
                if resp.status in self.net["backoff_http_codes"]:
                    self._register_rate_limit(name)
                    return None
                if resp.status != 200:
                    log.warning("%s: HTTP %s", name, resp.status)
                    return None
                data = await resp.json(content_type=None)
                self._reset_backoff(name)
                return data
        except asyncio.TimeoutError:
            log.warning("%s: таймаут %s сек", name, self.net["request_timeout_sec"])
        except Exception as exc:  # сеть/DNS/JSON — цикл не роняем
            log.warning("%s: ошибка запроса: %s", name, exc)
        return None

    async def binance_tickers(self) -> List[Snapshot]:
        data = await self.get_json("binance/tickers", self.net["binance_tickers_url"])
        out: List[Snapshot] = []
        if not isinstance(data, list):
            return out
        for item in data:
            try:
                out.append(Snapshot(
                    exchange="BINANCE",
                    symbol=item["symbol"],
                    price=float(item["lastPrice"]),
                    quote_volume_24h=float(item["quoteVolume"]),
                ))
            except (KeyError, TypeError, ValueError):
                continue
        return out

    async def bybit_tickers(self) -> List[Snapshot]:
        data = await self.get_json("bybit/tickers", self.net["bybit_tickers_url"])
        out: List[Snapshot] = []
        if not isinstance(data, dict):
            return out
        for item in (data.get("result") or {}).get("list") or []:
            try:
                funding = item.get("fundingRate")
                out.append(Snapshot(
                    exchange="BYBIT",
                    symbol=item["symbol"],
                    price=float(item["lastPrice"]),
                    quote_volume_24h=float(item["turnover24h"]),
                    funding_rate=float(funding) if funding not in (None, "") else None,
                ))
            except (KeyError, TypeError, ValueError):
                continue
        return out

    async def binance_funding(self) -> Dict[str, float]:
        data = await self.get_json("binance/premiumIndex", self.net["binance_premium_index_url"])
        out: Dict[str, float] = {}
        if not isinstance(data, list):
            return out
        for item in data:
            try:
                out[item["symbol"]] = float(item["lastFundingRate"])
            except (KeyError, TypeError, ValueError):
                continue
        return out


class Telegram:
    def __init__(self, cfg: dict, session):
        self.cfg = cfg["telegram"]
        self.net = cfg["network"]
        self.session = session

    @property
    def configured(self) -> bool:
        return not self.cfg["bot_token"].startswith("ВСТАВЬ") and not str(self.cfg["chat_id"]).startswith("ВСТАВЬ")

    async def send(self, text: str) -> bool:
        if not self.configured:
            log.error("Telegram не настроен: заполни bot_token/chat_id в config.json")
            return False
        import aiohttp
        url = f"{self.cfg['api_base']}/bot{self.cfg['bot_token']}/sendMessage"
        payload = {
            "chat_id": self.cfg["chat_id"],
            "text": text,
            "parse_mode": self.cfg["parse_mode"],
            "disable_web_page_preview": self.cfg["disable_web_page_preview"],
        }
        timeout = aiohttp.ClientTimeout(total=self.net["request_timeout_sec"])
        try:
            async with self.session.post(url, json=payload, timeout=timeout) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    log.error("Telegram sendMessage HTTP %s: %s", resp.status, body[:300])
                    return False
                return True
        except Exception as exc:
            log.error("Telegram sendMessage не удался: %s", exc)
            return False


# --------------------------------------------------------------------------- #
# снимок состояния на диске
# --------------------------------------------------------------------------- #

def read_state_file(path: str) -> Optional[dict]:
    """Прочитать state.json.gz. Битый или отсутствующий файл — не ошибка."""
    if not path or not os.path.exists(path):
        return None
    try:
        with gzip.open(path, "rt", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        log.warning("снимок состояния не прочитан (%s): %s", path, exc)
        return None


def write_state_file(path: str, data: dict) -> bool:
    """Атомарная запись: сначала во временный файл, потом переименование."""
    if not path:
        return False
    tmp = f"{path}.tmp"
    try:
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        with gzip.open(tmp, "wt", encoding="utf-8") as fh:
            json.dump(data, fh, separators=(",", ":"))
        os.replace(tmp, path)
        return True
    except Exception as exc:
        log.error("снимок состояния не сохранён (%s): %s", path, exc)
        try:
            os.path.exists(tmp) and os.remove(tmp)
        except OSError:
            pass
        return False


# --------------------------------------------------------------------------- #
# бот
# --------------------------------------------------------------------------- #

class PumpBot:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.net = cfg["network"]
        self.rt = cfg.get("runtime", {})
        self.detector = Detector(cfg)
        self.stopping = False
        self.stop_reason = "сигнал"
        self._last_funding_ts = 0.0
        self._last_state_save = 0.0
        self._consecutive_failures = 0
        self._restored = 0

    def request_stop(self, reason: str = "сигнал") -> None:
        if not self.stopping:
            self.stop_reason = reason
            log.info("получен запрос на остановку (%s)", reason)
        self.stopping = True

    # -- состояние ---------------------------------------------------------- #

    @property
    def state_path(self) -> str:
        return self.rt.get("state_file") or ""

    def load_state(self) -> None:
        data = read_state_file(self.state_path)
        if not data:
            log.info("снимка состояния нет — холодный старт, прогрев %s мин",
                     self.cfg["universe"]["min_history_minutes"])
            return
        self._restored = self.detector.load_state(data, self.rt.get("state_max_age_sec", 43200))
        if self._restored:
            log.info("прогрев пропущен: статистика поднята по %s символам, "
                     "15-минутное окно наберётся за 15 мин", self._restored)

    def save_state(self, reason: str) -> None:
        if not self.state_path:
            return
        data = self.detector.dump_state(self.rt.get("state_history_keep", 360))
        if write_state_file(self.state_path, data):
            self._last_state_save = now()
            size_kb = os.path.getsize(self.state_path) / 1024.0
            log.info("состояние сохранено (%s): %s символов, %.0f КБ",
                     reason, len(data["states"]), size_kb)

    # -- главный цикл -------------------------------------------------------- #

    async def run(self) -> None:
        import aiohttp
        min_history = self.cfg["universe"]["min_history_minutes"]
        max_runtime = float(self.rt.get("max_runtime_sec") or 0)
        started_at = now()
        self.load_state()
        log.info(
            "старт: прогрев %s мин, опрос каждые %s сек, порог z=%s / move=%s%% / vol=%sx%s",
            min_history, self.net["poll_interval_sec"],
            self.cfg["detection"]["zscore_threshold"],
            self.cfg["detection"]["min_abs_move_main_pct"],
            self.cfg["detection"]["volume_mult_main"],
            f", лимит рана {max_runtime / 3600.0:.2f} ч" if max_runtime else "",
        )
        state_save_sec = float(self.rt.get("state_save_sec") or 0)
        watchdog_limit = int(self.rt.get("watchdog_max_consecutive_failures") or 0)

        async with aiohttp.ClientSession() as session:
            feed = Feed(self.cfg, session)
            telegram = Telegram(self.cfg, session)
            if self.rt.get("startup_ping"):
                await telegram.send(self.startup_message())
            while not self.stopping:
                started = now()
                try:
                    await self.iteration(feed, telegram)
                except Exception as exc:
                    log.exception("необработанная ошибка в итерации: %s", exc)
                    self._consecutive_failures += 1
                    await asyncio.sleep(self.net["error_sleep_sec"])
                    continue

                if watchdog_limit and self._consecutive_failures >= watchdog_limit:
                    log.critical("watchdog: %s итераций подряд без данных — выхожу на рестарт",
                                 self._consecutive_failures)
                    self.request_stop("watchdog")
                    break
                if state_save_sec and now() - self._last_state_save >= state_save_sec:
                    self.save_state("по таймеру")
                if max_runtime and now() - started_at >= max_runtime:
                    self.request_stop("лимит времени рана")
                    break

                elapsed = now() - started
                await asyncio.sleep(max(self.net["min_sleep_sec"], self.net["poll_interval_sec"] - elapsed))

        self.save_state(f"выход: {self.stop_reason}")
        log.info("остановлен (%s)", self.stop_reason)

    def startup_message(self) -> str:
        cfg = self.cfg
        where = os.environ.get("PUMPBOT_RUNTIME_LABEL", "локально")
        if self._restored:
            history = f"статистика поднята из снимка по {self._restored} символам, прогрев пропущен"
        else:
            history = f"холодный старт, прогрев {cfg['universe']['min_history_minutes']} мин"
        return (
            "🟢 <b>Pump Detector</b> — запущен\n"
            f"Где: {where} · опрос Binance + Bybit каждые {self.net['poll_interval_sec']} сек\n"
            f"{history}\n"
            f"Пороги: z ≥ {cfg['detection']['zscore_threshold']} · рост ≥ "
            f"{cfg['detection']['min_abs_move_main_pct']}% / 15м · объём ≥ "
            f"{cfg['detection']['volume_mult_main']}× среднего"
        )

    async def iteration(self, feed: Feed, telegram: Telegram) -> None:
        binance, bybit = await asyncio.gather(feed.binance_tickers(), feed.bybit_tickers())

        if now() - self._last_funding_ts >= self.net["funding_refresh_sec"]:
            rates = await feed.binance_funding()
            if rates:
                self.detector.set_binance_funding(rates)
            self._last_funding_ts = now()

        snapshots = list(binance) + list(bybit)
        if not snapshots:
            self._consecutive_failures += 1
            log.warning("пустой снимок рынка, итерация пропущена (подряд: %s)", self._consecutive_failures)
            return
        self._consecutive_failures = 0

        alerts = self.detector.step(snapshots)
        for alert in alerts:
            sig = alert.primary
            log.info(
                "АЛЕРТ %s [%s] %s: %.2f%%/15м z=%.2f vol=%.2fx",
                sig.symbol, "+".join(alert.exchanges), "+".join(sig.triggers),
                sig.move_15m, sig.zscore, sig.vol_mult,
            )
            await telegram.send(render_alert(alert, self.cfg))


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Pump Detector Bot")
    parser.add_argument("--config", default=CONFIG_DEFAULT_PATH, help="путь к config.json")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    setup_logging(cfg)
    bot = PumpBot(cfg)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    for sig_name in ("SIGINT", "SIGTERM"):
        sig_num = getattr(signal, sig_name, None)
        if sig_num is None:
            continue
        try:
            loop.add_signal_handler(sig_num, bot.request_stop)
        except (NotImplementedError, RuntimeError):
            pass  # Windows: ловим KeyboardInterrupt ниже
    try:
        loop.run_until_complete(bot.run())
    except KeyboardInterrupt:
        log.info("Ctrl+C — завершаюсь")
        bot.request_stop("Ctrl+C")
        bot.save_state("выход по Ctrl+C")
    finally:
        loop.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

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
import sqlite3
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
class Candle:
    """Минутная свеча: время открытия (сек), закрытие, quote volume."""
    ts: float
    close: float
    quote_volume: float


@dataclass
class SymbolState:
    """История по одному ключу EXCHANGE:SYMBOL (in-memory)."""
    first_seen: float
    ticks: deque = field(default_factory=deque)      # maxlen задаётся в create() из конфига
    moves_15m: deque = field(default_factory=deque)
    vols_15m: deque = field(default_factory=deque)
    funding_rate: Optional[float] = None
    backfilled: bool = False   # история поднята из свечей → 120-минутный прогрев не нужен

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


def history_from_candles(
    candles: List[Candle],
    window_main_sec: float,
    ticks_window_sec: float,
    current_volume_24h: Optional[float] = None,
) -> Optional[dict]:
    """Восстановить `moves_15m`, `vols_15m` и хвост тиков из минутных свечей.

    - moves: для каждой минуты t доходность close[t] / close[t-15] в процентах;
    - vols: сумма quote volume по скользящему окну из 15 свечей (то же окно);
    - ticks: последние ~30 минут как (время, close, накопительный объём), чтобы
      расчёты по окнам работали сразу, без ожидания живых тиков.

    Накопительный объём подгоняется так, чтобы на последней свече он совпал
    с реальным 24h-объёмом из тикера: тик[2] служит и фильтром ликвидности,
    и базой для дельт, поэтому уровень должен быть настоящим.
    """
    span = int(round(window_main_sec / 60.0))
    if len(candles) < span + 1:
        return None
    candles = sorted(candles, key=lambda c: c.ts)

    moves: List[float] = []
    vols: List[float] = []
    for i in range(span, len(candles)):
        past_close = candles[i - span].close
        if past_close <= 0:
            continue
        moves.append((candles[i].close / past_close - 1.0) * 100.0)
        vols.append(sum(c.quote_volume for c in candles[i - span + 1:i + 1]))

    tick_count = max(2, int(round(ticks_window_sec / 60.0)) + 1)
    tail = candles[-tick_count:]
    tail_volume = sum(c.quote_volume for c in tail)
    base = (current_volume_24h - tail_volume) if current_volume_24h is not None else 0.0
    if base < 0:
        base = 0.0

    ticks: List[Tuple[float, float, float]] = []
    running = base
    for candle in tail:
        running += candle.quote_volume
        ticks.append((candle.ts + 60.0, candle.close, running))

    if not moves or not vols or len(ticks) < 2:
        return None
    return {"moves": moves, "vols": vols, "ticks": ticks, "first_ts": candles[0].ts}


# --------------------------------------------------------------------------- #
# слой 2: чтение открытого интереса
# --------------------------------------------------------------------------- #

def read_open_interest(oi_now: Optional[float], oi_past: Optional[float],
                       threshold_pct: float) -> Tuple[str, Optional[float]]:
    """Цена вверх — но на чьи деньги.

    OI растёт вместе с ценой → в позицию заходят новые деньги, шортить опасно.
    OI падает → это закрытие шортов (сквиз), импульс выдохнется сам.
    """
    if not oi_now or not oi_past or oi_past <= 0:
        return "FLAT", None
    change = (oi_now - oi_past) / oi_past * 100.0
    if change > threshold_pct:
        return "NEW_MONEY", change
    if change < -threshold_pct:
        return "SQUEEZE", change
    return "FLAT", change


OI_VERDICTS = {
    "NEW_MONEY": "OI↑ новые деньги — шорт опасен",
    "SQUEEZE": "OI↓ шорт-сквиз — импульс выдохнется",
    "FLAT": "OI≈ без выраженного потока",
}


# --------------------------------------------------------------------------- #
# слой 3: детектор истощения (второй алерт — момент входа в fade)
# --------------------------------------------------------------------------- #

def detect_exhaustion(candles: List[Candle], cfg: dict) -> Tuple[bool, str]:
    """Новый максимум на затухающем объёме после импульса.

    Работает по закрытым свечам: последняя свеча биржи ещё формируется,
    её объём заведомо неполный и дал бы ложное «истощение» на каждом проходе.
    """
    ex = cfg["exhaustion"]
    lookback = int(ex["lookback_candles"])
    if len(candles) < lookback + 2:
        return False, ""
    window = candles[-(lookback + 1):-1]          # без незакрытой свечи
    if len(window) < 3:
        return False, ""
    last = window[-1]
    earlier = window[:-1]
    impulse = max(earlier, key=lambda c: c.quote_volume)
    if impulse.quote_volume <= 0:
        return False, ""
    prev_high = max(c.close for c in earlier)
    new_high = last.close >= prev_high * float(ex["new_high_tolerance"])
    decay = last.quote_volume / impulse.quote_volume
    if new_high and decay < float(ex["volume_decay"]):
        return True, f"новый хай на объёме {decay:.0%} от импульсного"
    return False, ""


# --------------------------------------------------------------------------- #
# слой 1: контекст события (почему растёт)
# --------------------------------------------------------------------------- #

def base_coin(symbol: str, quote_suffix: str) -> str:
    coin = symbol[: -len(quote_suffix)] if symbol.endswith(quote_suffix) else symbol
    for prefix in ("1000000", "10000", "1000"):
        if coin.startswith(prefix):
            coin = coin[len(prefix):]
            break
    return coin


def normalize_slug(slug: str) -> str:
    """Слаг протокола → предполагаемый тикер: «frax-finance» → FRAX."""
    cleaned = slug
    for tail in ("-finance", "-network", "-protocol", "-labs", "-dao"):
        cleaned = cleaned.replace(tail, "")
    return cleaned.replace("-", "").upper()


def build_slug_map(slugs: Optional[list], overrides: dict) -> Dict[str, str]:
    """Карта «тикер → слаг»: сначала ручные соответствия, потом нормализация."""
    mapping: Dict[str, str] = {}
    for slug in slugs or []:
        if not isinstance(slug, str):
            continue
        ticker = overrides.get(slug) or normalize_slug(slug)
        mapping.setdefault(ticker, slug)
    for slug, ticker in overrides.items():
        mapping[ticker] = slug
    return mapping


def unlock_context(dataset: Optional[dict], window_days: float, now_ts: float) -> Optional[str]:
    """Ближайший анлок из датасета DefiLlama: срок, доля supply, получатель.

    Формат — датасет-хост `defillama-datasets.llama.fi/emissions/{слаг}`:
    свободный доступ, тогда как api.llama.fi/emissions с 2026 отвечает HTTP 402.
    """
    if not isinstance(dataset, dict):
        return None
    meta = dataset.get("metadata") or {}
    max_supply = (dataset.get("supplyMetrics") or {}).get("maxSupply") or meta.get("total")
    best = None
    for event in meta.get("events") or []:
        if not isinstance(event, dict):
            continue
        ts = event.get("timestamp")
        if not ts:
            continue
        try:
            days = (float(ts) - now_ts) / 86400.0
        except (TypeError, ValueError):
            continue
        if abs(days) > window_days:
            continue
        if best is None or abs(days) < abs(best[0]):
            best = (days, event)
    if best is None:
        return None

    days, event = best
    parts = [f"анлок {'через' if days > 0 else 'был'} {abs(days):.1f} дн"]
    tokens = [t for t in (event.get("noOfTokens") or []) if isinstance(t, (int, float))]
    if tokens and max_supply:
        try:
            share = sum(tokens) / float(max_supply) * 100.0
            parts.append(f"{share:.2f}% supply")
        except (TypeError, ValueError, ZeroDivisionError):
            pass
    recipient = event.get("category")
    if recipient:
        parts.append(str(recipient))
    return " · ".join(parts)


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
    oi_read: str = "FLAT"
    oi_change_pct: Optional[float] = None
    event_context: str = ""
    note: str = ""


@dataclass
class Alert:
    """Готовый к отправке алерт (после дедупа между биржами)."""
    symbol: str
    exchanges: List[str]
    primary: Signal
    ts: float
    at_startup: bool = False   # найден на первом проходе после backfill
    kind: str = "PUMP"         # PUMP — рост, EXHAUST — истощение импульса

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
        self.startup_pass = False   # True на проходе сразу после backfill: алерт помечается
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

    # -- подсев истории из свечей ------------------------------------------- #

    def seed_from_candles(
        self,
        exchange: str,
        symbol: str,
        candles: List[Candle],
        current_volume_24h: Optional[float] = None,
        ticks_window_sec: float = 1800.0,
    ) -> bool:
        """Заполнить историю символа из свечей. True — если получилось.

        Работает и как первичный backfill, и как инкрементальный после разрыва:
        новые наблюдения добавляются только за минуты, которых ещё нет
        (по времени последнего тика), поэтому дублей в `moves_15m` не будет.
        """
        history = history_from_candles(
            candles, self.det["window_main_sec"], ticks_window_sec, current_volume_24h
        )
        if history is None:
            return False

        key = self.key(exchange, symbol)
        state = self.states.get(key)
        if state is None:
            state = SymbolState.create(history["first_ts"], self.det["ticks_maxlen"], self.det["history_maxlen"])
            self.states[key] = state
            state.moves_15m.extend(history["moves"])
            state.vols_15m.extend(history["vols"])
            for tick in history["ticks"]:
                state.add_tick(*tick)
        else:
            last_ts = state.ticks[-1][0] if state.ticks else 0.0
            fresh_ticks = [t for t in history["ticks"] if t[0] > last_ts]
            if not fresh_ticks and state.moves_15m:
                return False  # разрыва фактически не было, дублировать нечего
            minutes_missing = len(fresh_ticks)
            if minutes_missing:
                # столько же новых наблюдений статистики, сколько пропущенных минут
                state.moves_15m.extend(history["moves"][-minutes_missing:])
                state.vols_15m.extend(history["vols"][-minutes_missing:])
                for tick in fresh_ticks:
                    state.add_tick(*tick)
            state.first_seen = min(state.first_seen, history["first_ts"])
        state.backfilled = True
        return True

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
            if ts - state.first_seen < min_history_sec and not state.backfilled:
                continue  # прогрев обязателен только там, где backfill не удался
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
                at_startup=self.startup_pass,
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

    icon = "🎯" if alert.kind == "EXHAUST" else "🔴"
    lines = [f"{icon} <b>{alert.kind}: {base}/{quote}</b> [{exchanges}]"]
    if alert.kind == "EXHAUST":
        lines.append(f"⚡ <b>истощение импульса — момент для fade</b>")
    if alert.at_startup:
        lines.append("⏱ обнаружен при старте")
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
    oi_line = OI_VERDICTS.get(sig.oi_read, OI_VERDICTS["FLAT"])
    if sig.oi_change_pct is not None:
        oi_line += f" ({sig.oi_change_pct:+.1f}%)"
    lines.append(f"🧭 {oi_line}")
    if sig.event_context:
        lines.append(f"📰 {sig.event_context}")
    if sig.note:
        lines.append(f"📝 {sig.note}")
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
        self.used_weight = 0.0   # X-MBX-USED-WEIGHT-1M последнего ответа Binance

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
                weight = resp.headers.get(self.net["binance_weight_header"])
                if weight is not None:
                    try:
                        self.used_weight = float(weight)
                    except ValueError:
                        pass
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

    async def binance_klines(self, symbol: str, limit: int) -> Optional[List[Candle]]:
        url = self.net["binance_klines_url"].format(symbol=symbol, limit=limit)
        data = await self.get_json("binance/klines", url)
        if not isinstance(data, list):
            return None
        out: List[Candle] = []
        for item in data:
            try:  # [openTime, o, h, l, c, volume, closeTime, quoteAssetVolume, ...]
                out.append(Candle(ts=float(item[0]) / 1000.0, close=float(item[4]),
                                  quote_volume=float(item[7])))
            except (IndexError, TypeError, ValueError):
                continue
        return out or None

    async def bybit_klines(self, symbol: str, limit: int) -> Optional[List[Candle]]:
        url = self.net["bybit_klines_url"].format(symbol=symbol, limit=limit)
        data = await self.get_json("bybit/klines", url)
        if not isinstance(data, dict):
            return None
        rows = (data.get("result") or {}).get("list") or []
        out: List[Candle] = []
        for item in rows:  # [startMs, o, h, l, c, volume, turnover] — приходят от новых к старым
            try:
                out.append(Candle(ts=float(item[0]) / 1000.0, close=float(item[4]),
                                  quote_volume=float(item[6])))
            except (IndexError, TypeError, ValueError):
                continue
        out.sort(key=lambda c: c.ts)
        return out or None

    async def candles_15m(self, exchange: str, symbol: str, limit: int) -> Optional[List[Candle]]:
        """15-минутные свечи — для детектора истощения."""
        if exchange == "BINANCE":
            url = self.net["binance_klines_15m_url"].format(symbol=symbol, limit=limit)
            data = await self.get_json("binance/klines15m", url)
            if not isinstance(data, list):
                return None
            out = []
            for item in data:
                try:
                    out.append(Candle(float(item[0]) / 1000.0, float(item[4]), float(item[7])))
                except (IndexError, TypeError, ValueError):
                    continue
            return out or None
        url = self.net["bybit_klines_15m_url"].format(symbol=symbol, limit=limit)
        data = await self.get_json("bybit/klines15m", url)
        rows = (data or {}).get("result", {}).get("list") if isinstance(data, dict) else None
        if not rows:
            return None
        out = []
        for item in rows:
            try:
                out.append(Candle(float(item[0]) / 1000.0, float(item[4]), float(item[6])))
            except (IndexError, TypeError, ValueError):
                continue
        out.sort(key=lambda c: c.ts)
        return out or None

    async def candles(self, exchange: str, symbol: str, limit: int) -> Optional[List[Candle]]:
        if exchange == "BINANCE":
            return await self.binance_klines(symbol, min(limit, self.net["binance_klines_max"]))
        if exchange == "BYBIT":
            return await self.bybit_klines(symbol, min(limit, self.net["bybit_klines_max"]))
        return None

    async def binance_open_interest(self, symbol: str, period: str, limit: int
                                    ) -> Tuple[Optional[float], Optional[float]]:
        """Текущий OI и OI в начале окна — из истории, одним запросом."""
        url = self.net["binance_oi_hist_url"].format(symbol=symbol, period=period, limit=limit)
        data = await self.get_json("binance/openInterestHist", url)
        if not isinstance(data, list) or not data:
            return None, None
        try:
            rows = sorted(data, key=lambda item: float(item["timestamp"]))
            return float(rows[-1]["sumOpenInterest"]), float(rows[0]["sumOpenInterest"])
        except (KeyError, TypeError, ValueError):
            return None, None

    async def bybit_open_interest(self, symbol: str, interval: str, limit: int
                                  ) -> Tuple[Optional[float], Optional[float]]:
        url = self.net["bybit_oi_hist_url"].format(symbol=symbol, interval=interval, limit=limit)
        data = await self.get_json("bybit/open-interest", url)
        rows = (data or {}).get("result", {}).get("list") if isinstance(data, dict) else None
        if not rows:
            return None, None
        try:
            ordered = sorted(rows, key=lambda item: float(item["timestamp"]))
            return float(ordered[-1]["openInterest"]), float(ordered[0]["openInterest"])
        except (KeyError, TypeError, ValueError):
            return None, None

    async def open_interest(self, exchange: str, symbol: str, cfg: dict
                            ) -> Tuple[Optional[float], Optional[float]]:
        oi_cfg = cfg["open_interest"]
        if exchange == "BINANCE":
            return await self.binance_open_interest(symbol, oi_cfg["binance_period"], oi_cfg["points"])
        if exchange == "BYBIT":
            return await self.bybit_open_interest(symbol, oi_cfg["bybit_interval"], oi_cfg["points"])
        return None, None

    async def price(self, exchange: str, symbol: str) -> Optional[float]:
        """Текущая цена одного символа — для дозаписи исходов сигналов."""
        if exchange == "BINANCE":
            data = await self.get_json("binance/price", self.net["binance_price_url"].format(symbol=symbol))
            try:
                return float(data["price"])
            except (KeyError, TypeError, ValueError):
                return None
        data = await self.get_json("bybit/price", self.net["bybit_price_url"].format(symbol=symbol))
        try:
            return float(data["result"]["list"][0]["lastPrice"])
        except (KeyError, IndexError, TypeError, ValueError):
            return None

    async def llama_protocols(self) -> Optional[list]:
        data = await self.get_json("llama/protocols", self.net["llama_protocols_url"])
        return data if isinstance(data, list) else None

    async def llama_unlock_dataset(self, slug: str) -> Optional[dict]:
        url = self.net["llama_dataset_url"].format(slug=slug)
        data = await self.get_json(f"llama/{slug}", url)
        return data if isinstance(data, dict) else None

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


class Backfill:
    """Восстановление истории из свечей: батчи, контроль веса, устойчивость к отказам.

    Fetcher передаётся снаружи (`async fetch(exchange, symbol, limit)`), поэтому
    тесты гоняют backfill на заглушке без сети.
    """

    def __init__(self, cfg: dict, fetch_candles, weight_source=None, sleeper=None):
        self.cfg = cfg
        self.bf = cfg["backfill"]
        self.uni = cfg["universe"]
        self.det = cfg["detection"]
        self.fetch_candles = fetch_candles
        self.weight_source = weight_source or (lambda: 0.0)
        self.sleeper = sleeper or asyncio.sleep

    def targets(self, snapshots: Iterable[Snapshot]) -> List[Tuple[str, str, float]]:
        """Только ликвидная часть вселенной + BTC (нужен для фильтра режима рынка)."""
        btc = self.cfg["btc_filter"]["btc_symbol"]
        suffix = self.uni["quote_suffix"]
        out = []
        for snap in snapshots:
            if not snap.symbol.endswith(suffix):
                continue
            if snap.quote_volume_24h < self.uni["min_24h_volume_usd"] and snap.symbol != btc:
                continue
            out.append((snap.exchange, snap.symbol, snap.quote_volume_24h))
        return out

    async def run(self, detector: Detector, snapshots: Iterable[Snapshot],
                  minutes: Optional[float] = None, label: str = "старт") -> dict:
        targets = self.targets(snapshots)
        span_min = int(round(self.det["window_main_sec"] / 60.0))
        if minutes is None:
            need = int(self.bf["lookback_minutes"])
        else:
            need = int(minutes) + span_min + int(self.bf["gap_margin_minutes"])
        limit = max(span_min + 2, need)

        batch_size = int(self.bf["batch_size"])
        ok = failed = 0
        for start in range(0, len(targets), batch_size):
            batch = targets[start:start + batch_size]
            await self._respect_weight()
            results = await asyncio.gather(
                *(self.fetch_candles(ex, sym, limit) for ex, sym, _ in batch),
                return_exceptions=True,
            )
            for (exchange, symbol, vol24h), candles in zip(batch, results):
                if isinstance(candles, Exception) or not candles:
                    failed += 1
                    continue
                seeded = detector.seed_from_candles(
                    exchange, symbol, candles, current_volume_24h=vol24h,
                    ticks_window_sec=self.bf["ticks_window_sec"],
                )
                ok += 1 if seeded else 0
            log.info("backfill (%s) %s/%s символов, вес %.0f/%s, отказов %s",
                     label, min(start + batch_size, len(targets)), len(targets),
                     self.weight_source(), self.bf["binance_weight_limit"], failed)
            if start + batch_size < len(targets):
                await self.sleeper(self.bf["batch_pause_sec"])
        log.info("backfill (%s) завершён: поднято %s символов, отказов %s", label, ok, failed)
        return {"seeded": ok, "failed": failed, "targets": len(targets)}

    async def _respect_weight(self) -> None:
        """Binance считает вес запросов в минуту — у порога ждём начала новой минуты."""
        if self.weight_source() <= self.bf["binance_weight_limit"]:
            return
        pause = 60.0 - (now() % 60.0)
        log.warning("вес Binance %.0f выше порога %s — пауза %.0f сек",
                    self.weight_source(), self.bf["binance_weight_limit"], pause)
        await self.sleeper(pause)


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
# слой 5: журнал сигналов и исходов (+1ч / +4ч / +24ч)
# --------------------------------------------------------------------------- #

class SignalJournal:
    """SQLite-журнал: каждый алерт и что с ценой стало через 1, 4 и 24 часа.

    Каждый горизонт замеряется в своё время (по достижении возраста), а не
    задним числом одним значением — иначе накопленный датасет описывает не
    исход сигнала, а момент, когда до него дошли руки.
    """

    SCHEMA = """
        CREATE TABLE IF NOT EXISTS signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts REAL NOT NULL,
            iso TEXT NOT NULL,
            kind TEXT NOT NULL,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            price REAL NOT NULL,
            move_15m REAL, move_5m REAL, zscore REAL, vol_mult REAL,
            funding REAL, volume_24h REAL,
            oi_read TEXT, oi_change_pct REAL,
            event_context TEXT, triggers TEXT, note TEXT,
            out_1h REAL, out_4h REAL, out_24h REAL
        )
    """
    HORIZONS = (("out_1h", 3600.0), ("out_4h", 14400.0), ("out_24h", 86400.0))

    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(path)
        self.conn.execute(self.SCHEMA)
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def record(self, alert: "Alert") -> int:
        sig = alert.primary
        cur = self.conn.execute(
            """INSERT INTO signals (ts, iso, kind, exchange, symbol, price, move_15m,
                    move_5m, zscore, vol_mult, funding, volume_24h, oi_read,
                    oi_change_pct, event_context, triggers, note)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (alert.ts, datetime.fromtimestamp(alert.ts, tz=timezone.utc).isoformat(),
             alert.kind, sig.exchange, sig.symbol, sig.price, sig.move_15m, sig.move_5m,
             sig.zscore, sig.vol_mult, sig.funding_rate, sig.volume_24h, sig.oi_read,
             sig.oi_change_pct, sig.event_context, "+".join(sig.triggers), sig.note),
        )
        self.conn.commit()
        return int(cur.lastrowid)

    def pending(self, ts: float) -> List[Tuple[int, str, str, float, str]]:
        """Сигналы, у которых подошёл срок замера очередного горизонта."""
        rows = self.conn.execute(
            "SELECT id, exchange, symbol, price, ts, out_1h, out_4h, out_24h "
            "FROM signals WHERE out_24h IS NULL"
        ).fetchall()
        due = []
        for rid, exchange, symbol, price, signal_ts, o1, o4, o24 in rows:
            age = ts - signal_ts
            for (column, horizon), value in zip(self.HORIZONS, (o1, o4, o24)):
                if value is None and age >= horizon:
                    due.append((rid, exchange, symbol, price, column))
        return due

    def fill(self, row_id: int, column: str, price_now: float, price_then: float) -> None:
        if not price_then:
            return
        change = (price_now - price_then) / price_then * 100.0
        self.conn.execute(f"UPDATE signals SET {column} = ? WHERE id = ?", (change, row_id))
        self.conn.commit()

    def report(self) -> str:
        rows = self.conn.execute(
            """SELECT kind, COALESCE(oi_read, 'FLAT'), COUNT(*),
                      AVG(out_1h), AVG(out_4h), AVG(out_24h),
                      SUM(CASE WHEN out_24h < 0 THEN 1 ELSE 0 END)
               FROM signals GROUP BY kind, oi_read ORDER BY kind, oi_read"""
        ).fetchall()
        lines = [f"{'ТИП':9}{'OI':11}{'N':>4}{'ср.1ч%':>9}{'ср.4ч%':>9}{'ср.24ч%':>9}{'ушло вниз':>11}"]
        for kind, oi, count, a1, a4, a24, down in rows:
            def num(v):
                return f"{v:>9.2f}" if v is not None else f"{'—':>9}"
            share = f"{(down or 0) / count:.0%}" if count else "—"
            lines.append(f"{kind:9}{oi:11}{count:>4}{num(a1)}{num(a4)}{num(a24)}{share:>11}")
        return chr(10).join(lines)


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
        self._last_data_ts = 0.0
        self._backfill_stats = {}
        self._slug_map: Dict[str, str] = {}
        self._unlock_cache: Dict[str, Tuple[float, Optional[dict]]] = {}
        self._emissions_ts = 0.0
        self._watching: Dict[str, dict] = {}   # символы под наблюдением на истощение
        self._last_outcome_ts = 0.0
        self.journal: Optional[SignalJournal] = None
        journal_path = cfg.get("journal", {}).get("db_file")
        if journal_path:
            try:
                self.journal = SignalJournal(journal_path)
            except Exception as exc:
                log.error("журнал сигналов недоступен (%s): %s", journal_path, exc)

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
            backfill = Backfill(self.cfg, feed.candles, weight_source=lambda: feed.used_weight)
            if self.cfg["backfill"]["enabled"]:
                await self.initial_backfill(feed, backfill)
            if self.rt.get("startup_ping"):
                await telegram.send(self.startup_message())
            while not self.stopping:
                started = now()
                try:
                    await self.iteration(feed, telegram, backfill)
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

    async def enrich(self, alert: Alert, feed: Feed) -> None:
        """Слои 1-2: чем дышит импульс (OI) и есть ли у него причина (событие)."""
        sig = alert.primary
        if self.cfg["open_interest"]["enabled"]:
            try:
                oi_now, oi_past = await feed.open_interest(sig.exchange, sig.symbol, self.cfg)
                sig.oi_read, sig.oi_change_pct = read_open_interest(
                    oi_now, oi_past, self.cfg["open_interest"]["threshold_pct"])
            except Exception as exc:
                log.warning("OI по %s не получен: %s", sig.symbol, exc)
        if self.cfg["event_context"]["enabled"]:
            try:
                sig.event_context = await self.event_context(sig.symbol, feed) or ""
            except Exception as exc:
                log.warning("контекст события по %s не получен: %s", sig.symbol, exc)

    async def event_context(self, symbol: str, feed: Feed) -> Optional[str]:
        """Анлоки DefiLlama по монете, которая сработала.

        Список протоколов тянется раз в час на весь бот; тяжёлый датасет (под
        мегабайт) — только по монете из алерта и с кэшем, а не по всей вселенной.
        """
        ctx_cfg = self.cfg["event_context"]
        if now() - self._emissions_ts >= ctx_cfg["refresh_sec"]:
            slugs = await feed.llama_protocols()
            if slugs:
                self._slug_map = build_slug_map(slugs, ctx_cfg.get("slug_overrides") or {})
                self._emissions_ts = now()

        coin = base_coin(symbol, self.cfg["universe"]["quote_suffix"])
        slug = (self._slug_map or {}).get(coin)
        if not slug:
            return None

        cached = self._unlock_cache.get(coin)
        if cached and now() - cached[0] < ctx_cfg["refresh_sec"]:
            dataset = cached[1]
        else:
            dataset = await feed.llama_unlock_dataset(slug)
            self._unlock_cache[coin] = (now(), dataset)
        return unlock_context(dataset, ctx_cfg["unlock_window_days"], now())

    async def check_exhaustion(self, feed: Feed) -> List[Alert]:
        """Слой 3: второй алерт — импульс выдохся, вот момент для fade."""
        ex_cfg = self.cfg["exhaustion"]
        if not ex_cfg["enabled"] or not self._watching:
            return []
        out: List[Alert] = []
        window_sec = ex_cfg["watch_hours"] * 3600.0
        for key, watch in list(self._watching.items()):
            if now() - watch["since"] > window_sec:
                del self._watching[key]
                continue
            if watch["alerted"] or now() - watch["since"] < ex_cfg["min_wait_sec"]:
                continue
            candles = await feed.candles_15m(watch["exchange"], watch["symbol"],
                                             ex_cfg["lookback_candles"] + 2)
            if not candles:
                continue
            ok, note = detect_exhaustion(candles, self.cfg)
            if not ok:
                continue
            state = self.detector.states.get(key)
            last = state.last if state else None
            move_15m = state.move_pct(self.cfg["detection"]["window_main_sec"]) if state else None
            signal = Signal(
                exchange=watch["exchange"], symbol=watch["symbol"], triggers=["EXHAUST"],
                move_15m=move_15m if move_15m is not None else 0.0,
                move_5m=state.move_pct(self.cfg["detection"]["window_fast_sec"]) if state else None,
                zscore=0.0, vol_mult=0.0,
                price=last[1] if last else candles[-1].close,
                volume_24h=last[2] if last else 0.0,
                funding_rate=state.funding_rate if state else None,
                ts=now(), note=note,
            )
            alert = Alert(symbol=watch["symbol"], exchanges=[watch["exchange"]],
                          primary=signal, ts=now(), kind="EXHAUST")
            await self.enrich(alert, feed)
            watch["alerted"] = True
            out.append(alert)
            log.info("ИСТОЩЕНИЕ %s [%s]: %s", watch["symbol"], watch["exchange"], note)
        return out

    async def update_outcomes(self, feed: Feed) -> None:
        """Слой 5: дозапись исходов, каждый горизонт замеряется в своё время."""
        if self.journal is None:
            return
        if now() - self._last_outcome_ts < self.cfg["journal"]["outcome_check_sec"]:
            return
        self._last_outcome_ts = now()
        try:
            due = self.journal.pending(now())
        except Exception as exc:
            log.error("журнал не опрошен: %s", exc)
            return
        for row_id, exchange, symbol, price_then, column in due:
            price_now = await feed.price(exchange, symbol)
            if price_now is None:
                continue
            self.journal.fill(row_id, column, price_now, price_then)
            log.info("исход %s %s: %s = %+.2f%%", exchange, symbol, column,
                     (price_now - price_then) / price_then * 100.0)

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

    async def initial_backfill(self, feed: Feed, backfill: Backfill) -> None:
        """История из свечей при старте: слепой зоны нет, прогрев не нужен."""
        binance, bybit = await asyncio.gather(feed.binance_tickers(), feed.bybit_tickers())
        snapshots = list(binance) + list(bybit)
        if not snapshots:
            log.warning("backfill пропущен: тикеры недоступны — включается обычный прогрев")
            return
        try:
            self._backfill_stats = await backfill.run(self.detector, snapshots, label="старт")
        except Exception as exc:
            log.exception("backfill не удался (%s) — включается обычный прогрев", exc)
            return
        if self._backfill_stats.get("seeded"):
            self.detector.startup_pass = True   # алерты этого прохода помечаются «при старте»

    async def iteration(self, feed: Feed, telegram: Telegram, backfill: Optional[Backfill] = None) -> None:
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

        gap = now() - self._last_data_ts if self._last_data_ts else 0.0
        if backfill is not None and self.cfg["backfill"]["enabled"] and                 gap > self.cfg["backfill"]["gap_trigger_sec"]:
            log.warning("разрыв потока данных %.0f сек — инкрементальный backfill", gap)
            try:
                await backfill.run(self.detector, snapshots, minutes=gap / 60.0, label="разрыв")
                self.detector.startup_pass = True
            except Exception as exc:
                log.exception("инкрементальный backfill не удался: %s", exc)
        self._last_data_ts = now()

        alerts = self.detector.step(snapshots)
        self.detector.startup_pass = False
        for alert in alerts:
            await self.enrich(alert, feed)
            key = self.detector.key(alert.primary.exchange, alert.primary.symbol)
            self._watching[key] = {"since": now(), "symbol": alert.primary.symbol,
                                   "exchange": alert.primary.exchange, "alerted": False}
        alerts.extend(await self.check_exhaustion(feed))
        await self.update_outcomes(feed)
        for alert in alerts:
            sig = alert.primary
            log.info(
                "АЛЕРТ %s [%s] %s: %.2f%%/15м z=%.2f vol=%.2fx",
                sig.symbol, "+".join(alert.exchanges), "+".join(sig.triggers),
                sig.move_15m, sig.zscore, sig.vol_mult,
            )
            await telegram.send(render_alert(alert, self.cfg))
            if self.journal is not None:
                try:
                    self.journal.record(alert)
                except Exception as exc:
                    log.error("сигнал не записан в журнал: %s", exc)


async def run_outcomes_once(cfg: dict) -> None:
    """Разовая дозапись исходов — можно вешать в планировщик отдельно от бота."""
    import aiohttp
    bot = PumpBot(cfg)
    if bot.journal is None:
        log.error("журнал не настроен: заполни journal.db_file в config.json")
        return
    async with aiohttp.ClientSession() as session:
        bot._last_outcome_ts = 0.0
        await bot.update_outcomes(Feed(cfg, session))
    bot.journal.close()


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Pump Detector Bot")
    parser.add_argument("mode", nargs="?", default="run", choices=("run", "outcomes", "report"),
                        help="run — бот; outcomes — дозаписать исходы; report — сводка по журналу")
    parser.add_argument("--config", default=CONFIG_DEFAULT_PATH, help="путь к config.json")
    args = parser.parse_args(argv)

    cfg = load_config(args.config)
    setup_logging(cfg)

    if args.mode == "report":
        journal = SignalJournal(cfg["journal"]["db_file"])
        print(journal.report())
        journal.close()
        return 0
    if args.mode == "outcomes":
        asyncio.run(run_outcomes_once(cfg))
        return 0

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

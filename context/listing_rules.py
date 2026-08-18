#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LISTING RULES — фильтры и матрица решений для листинговых событий.

Появился после инцидента: в канал ушло ~20 сообщений, из которых 19 были перпами
на акции (TradFi), 32 из 43 — без распознанного тикера, а листинги монет, годами
торгующихся на Binance, подавались как события. Правила ниже существуют, чтобы
такое не повторилось, и потому вынесены в отдельный модуль — их можно тестировать
построчно, без сети и без Telegram.

Три рубежа:
1. `classify_instrument_kind` — перпы на акции и токенизированные акции отсекаются
   при парсинге, ДО записи в кэш событий: это вообще не события класса LISTING.
2. `extract_tickers` — без тикера событие непригодно: нельзя проверить, где монета
   уже торгуется, значит и отправлять нечего.
3. `decide` — матрица «новая аудитория» из ТЗ: в канал идут только STRONG_BULLISH,
   BEARISH_SETUP и OPERATIONAL. Нет снепшота инструментов — событие ждёт, а не
   уходит «на всякий случай».
"""

import json
import os
import re
import sqlite3
from typing import Dict, List, Optional

HERE = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.dirname(HERE)
CONFIG_PATH = os.path.join(BOT_DIR, "context_config.json")
CONFIG_SECTION = "listing"

# классы решений
STRONG_BULLISH = "STRONG_BULLISH"
WEAK = "WEAK"
WEAK_BEARISH = "WEAK_BEARISH"
OUT_OF_UNIVERSE = "OUT_OF_UNIVERSE"
BEARISH_SETUP = "BEARISH_SETUP"
OPERATIONAL = "OPERATIONAL"
HOLD = "HOLD"
REJECTED = "REJECTED"

DEFAULTS = {
    "tradfi_markers": [
        "tradfi", "tradifi", "equity x-perp", "equity perp", "x-perps", "equities",
        "tokenized stock", "tokenized equit", "pre-ipo", "pre-market",
        "stock perpetual", "us stock", "xstock",
    ],
    "post_classes": [STRONG_BULLISH, BEARISH_SETUP, OPERATIONAL],
    "major_exchanges": ["BINANCE", "UPBIT", "COINBASE"],
    # где МЫ торгуем: только делистинг здесь операционно важен
    "tradable_exchanges": ["BINANCE", "BYBIT"],
    # за чьими анонсами следим: событие оттуда — информация, но не наша операционка
    "monitored_exchanges": ["BINANCE", "BYBIT", "UPBIT", "OKX", "KUCOIN"],
    "parse_failed_warn_share": 0.3,
    "instruments_db": os.path.join(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))), "context_cache.db"),
    "quote_assets": ["USDT", "USDC", "KRW"],
    "not_tickers": ["USDT", "USDC", "BUSD", "USD", "KRW", "BTC", "ETH", "NFT",
                    "API", "CEO", "USA", "SEC", "ETF", "DEX", "CEX", "AMA", "APR",
                    "APY", "TVL", "P2P", "AND", "THE", "FOR", "NEW", "WILL", "SPOT"],
}

SPOT_MARKERS = ("for spot trading", "spot trading", "spot to list", "spot market",
                "거래지원", "krw market", "spot listing")
PERP_MARKERS = ("perpetual", "usdt contract", "futures will launch", "perp",
                "usdⓢ-margined", "usds-margined", "contract will launch")

TICKER_BRACKETS = re.compile(r"\(\$?([A-Z0-9]{2,12})\)")
TICKER_PAIR = re.compile(r"\b([A-Z0-9]{2,12})\s*/\s*(?:USDT|USDC|KRW|USD)\b")
TICKER_SUFFIX = re.compile(r"\b([A-Z0-9]{2,12})(?:USDT|USDC)\b")
UPPER_WORD = re.compile(r"\b([A-Z][A-Z0-9]{1,11})\b")


def load_config(path: str = CONFIG_PATH) -> dict:
    cfg = dict(DEFAULTS)
    try:
        with open(path, "r", encoding="utf-8") as fh:
            loaded = json.load(fh)
        # правила лежат отдельной секцией общего конфига слоя
        cfg.update(loaded.get(CONFIG_SECTION, loaded))
    except FileNotFoundError:
        print(f"WARNING фильтры: {path} не найден — работают значения по умолчанию")
    except json.JSONDecodeError as exc:
        # молчаливый откат на дефолты уже один раз скрыл, что конфиг не читается
        print(f"WARNING фильтры: {path} невалиден ({exc}) — работают значения по умолчанию")
    return cfg


# --------------------------------------------------------------------------- #
# рубеж 1: что это вообще за инструмент
# --------------------------------------------------------------------------- #

def classify_instrument_kind(title: str, cfg: dict, structured_type: str = None) -> str:
    """'crypto' | 'tradfi'. Структурный тип источника — первый рубеж, заголовок — второй."""
    # Binance в своём API пишет contractType как TRADIFI_PERPETUAL — через «tradifi»,
    # а Bybit в заголовках «TradFi»; проверяем оба написания
    if structured_type and any(marker in str(structured_type).lower()
                               for marker in ("equity", "tradfi", "tradifi", "stock")):
        return "tradfi"
    text = (title or "").lower()
    for marker in cfg["tradfi_markers"]:
        if marker in text:
            return "tradfi"
    return "crypto"


def market_kind(title: str) -> Optional[str]:
    """'spot' | 'perp' | None. При неоднозначности честно None — не угадываем."""
    text = (title or "").lower()
    spot = any(marker in text for marker in SPOT_MARKERS)
    perp = any(marker in text for marker in PERP_MARKERS)
    if spot and perp:
        return None
    if spot:
        return "spot"
    if perp:
        return "perp"
    return None


# --------------------------------------------------------------------------- #
# рубеж 2: тикеры
# --------------------------------------------------------------------------- #

def extract_tickers(title: str, cfg: dict) -> List[str]:
    """Все тикеры из заголовка. Пустой список = событие непригодно для отправки."""
    text = title or ""
    banned = set(cfg["not_tickers"])
    found: List[str] = []

    def add(candidate: str) -> None:
        candidate = candidate.upper()
        if candidate and not candidate.isdigit() and candidate not in banned and candidate not in found:
            found.append(candidate)

    for pattern in (TICKER_BRACKETS, TICKER_SUFFIX):
        for match in pattern.findall(text):
            add(match)
    for match in TICKER_PAIR.findall(text):
        add(match)

    if not found:
        # «Binance Will Delist ACX, HFT, PIVX, PYR» — перечисление после глагола.
        # Регистр глагола любой (IGNORECASE), сами тикеры берём только заглавными.
        listed = re.search(
            r"(?:delist|list|remove|add)\w*\s+((?:[A-Z0-9]{2,12}"
            r"(?:\s*,\s*|\s+&\s+|\s+and\s+))+[A-Z0-9]{2,12})",
            text, re.I)
        if listed:
            for part in re.split(r"\s*,\s*|\s+&\s+|\s+and\s+", listed.group(1)):
                part = part.strip()
                if part.isupper():
                    add(part)
    return found


# --------------------------------------------------------------------------- #
# где монета уже торгуется
# --------------------------------------------------------------------------- #

def listed_places(ticker: str, cfg: dict, conn=None) -> Optional[List[dict]]:
    """Снепшоты инструментов из кэша контекст-слоя.

    None означает «снепшота нет» — это не то же самое, что «нигде не торгуется»,
    и решение в таком случае откладывается.
    """
    own = conn is None
    if own:
        path = cfg["instruments_db"]
        if not os.path.exists(path):
            return None
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
        conn.row_factory = sqlite3.Row
    try:
        try:
            total = conn.execute("SELECT COUNT(*) AS n FROM instruments WHERE alive = 1").fetchone()["n"]
        except sqlite3.Error:
            return None
        if not total:
            return None
        variants = [ticker] + [f"{ticker}{quote}" for quote in cfg["quote_assets"]]
        placeholders = ",".join("?" * len(variants))
        rows = conn.execute(
            f"SELECT exchange, market, symbol FROM instruments"
            f" WHERE alive = 1 AND symbol IN ({placeholders})", variants).fetchall()
        return [dict(r) for r in rows]
    finally:
        if own:
            conn.close()


# --------------------------------------------------------------------------- #
# рубеж 3: матрица решений
# --------------------------------------------------------------------------- #

PRODUCT_ONLY_MARKERS = ("margin and loan", "margin & loan", "crypto loans", "savings",
                        "simple earn", "flexible loan", "vip loan", "dual investment",
                        "copy trading", "convert")


def is_product_only(title: str) -> bool:
    """Изменение в продукте биржи, а не в торгах инструментом."""
    text = (title or "").lower()
    return any(marker in text for marker in PRODUCT_ONLY_MARKERS)


def decide(*, event_type: str, exchange: str, ticker: Optional[str], market: Optional[str],
           places: Optional[List[dict]], cfg: dict, product_only: bool = False) -> dict:
    """Матрица из ТЗ, строка в строку. Возвращает {'class', 'post', 'reason'}."""
    event_type = (event_type or "").upper()
    exchange = (exchange or "").upper()
    majors = {m.upper() for m in cfg["major_exchanges"]}
    monitored = {m.upper() for m in cfg["monitored_exchanges"]}
    tradable = {m.upper() for m in cfg.get("tradable_exchanges") or ["BINANCE", "BYBIT"]}
    post_classes = set(cfg["post_classes"])

    def result(cls: str, reason: str) -> dict:
        return {"class": cls, "post": cls in post_classes, "reason": reason}

    if not ticker:
        return {"class": REJECTED, "post": False,
                "reason": "тикер не распознан — событие непригодно для проверки"}

    # монета, которой нет на биржах, где мы торгуем, нам недоступна — постить нечего.
    # Исключение: листинг на нашей бирже как раз и вводит монету в universe.
    if places is not None and not places and exchange not in tradable:
        return {"class": OUT_OF_UNIVERSE, "post": False,
                "reason": f"монеты нет на {', '.join(sorted(tradable))} — вне нашей вселенной"}

    if event_type in ("DELISTING", "DELISTED"):
        if product_only:
            # «Margin And Loan Will Delist BTTC» — сворачивают кредитный продукт,
            # сами торги не прекращаются: к торговле перпами это не относится
            return {"class": WEAK, "post": False,
                    "reason": "делистинг продукта (маржа/кредиты/сбережения), торги продолжаются"}
        if exchange in tradable:
            return result(OPERATIONAL, f"делистинг на {exchange} — торгуем там, операционно важно")
        if exchange in monitored:
            # уход монеты с Upbit/OKX/KuCoin — медвежий фон, но не наша операционка:
            # позиция там не держится, действий не требуется
            return {"class": WEAK_BEARISH, "post": False,
                    "reason": f"делистинг на {exchange} — там мы не торгуем, только фон"}
        return {"class": WEAK, "post": False, "reason": f"делистинг на немониторимой бирже {exchange}"}

    if event_type not in ("LISTING", "NEW_INSTRUMENT", "PERP_LAUNCH", "FUTURES"):
        return {"class": WEAK, "post": False, "reason": f"тип {event_type} не листинговый"}

    if places is None:
        return {"class": HOLD, "post": False,
                "reason": "снепшота инструментов нет — решение отложено, отправка запрещена"}

    on_majors = {p["exchange"].upper() for p in places if p["exchange"].upper() in majors}
    markets = {p["market"] for p in places}

    if event_type in ("PERP_LAUNCH", "FUTURES") or market == "perp":
        if markets and markets == {"spot"}:
            return result(BEARISH_SETUP, "первый перп у монеты, которая была только на споте — "
                                         "появилась возможность шортить")
        if "perp" in markets:
            return {"class": WEAK, "post": False,
                    "reason": "перп у монеты уже существует — новой возможности не появилось"}

    if exchange in majors and not on_majors:
        return result(STRONG_BULLISH, f"монеты не было на крупных площадках, листинг на {exchange} — "
                                      f"новая аудитория")
    if on_majors:
        return {"class": WEAK, "post": False,
                "reason": f"уже торгуется на {', '.join(sorted(on_majors))} — "
                          f"новой аудитории листинг не даёт"}
    return {"class": WEAK, "post": False,
            "reason": f"листинг на {exchange}: не крупная площадка, эффекта аудитории нет"}


# --------------------------------------------------------------------------- #
# сообщение
# --------------------------------------------------------------------------- #

PATTERN_NOTE = ("⚠️ Типовой паттерн: рост до листинга, дамп на открытии торгов. "
                "Анонсы систематически утекают инсайдерам — часть движения до анонса "
                "означает, что ранние уже в позиции.")

ICONS = {STRONG_BULLISH: "🟢", BEARISH_SETUP: "🩳", OPERATIONAL: "🔴",
         WEAK_BEARISH: "📉", OUT_OF_UNIVERSE: "⬜", WEAK: "ℹ️"}


def render(event: dict, verdict: dict, places: Optional[List[dict]]) -> str:
    """Сообщение без торговых рекомендаций: факт, где торгуется, риск-пометка."""
    ticker = event.get("ticker") or "?"
    exchange = (event.get("source") or "").upper()
    market = event.get("market")
    market_text = {"spot": "спот", "perp": "перп"}.get(market, "тип: не определён")

    # «Will Delist ACX, HFT, PIVX, PYR, VANRY, VIC» — в канал должны попасть все
    # монеты события, иначе сообщение говорит про одну, а касается шести
    tickers = event.get("tickers") or [ticker]
    head = ", ".join(tickers[:8]) + (f" и ещё {len(tickers) - 8}" if len(tickers) > 8 else "")
    lines = [f"{ICONS.get(verdict['class'], 'ℹ️')} <b>{event.get('etype')}: {head} "
             f"→ {exchange}</b> ({market_text})",
             f"📰 {(event.get('title') or '')[:200]}"]
    if places:
        where = ", ".join(sorted({f"{p['exchange']} {p['market']}" for p in places}))
        lines.append(f"📍 Уже торгуется: {where}")
    elif places is not None:
        lines.append("📍 На отслеживаемых площадках не найдена")
    lines.append(f"🧭 Класс: {verdict['class']} — {verdict['reason']}")
    if verdict["class"] == STRONG_BULLISH:
        lines.append(PATTERN_NOTE)
    if event.get("url"):
        lines.append(f"🔗 {event['url']}")
    return "\n".join(lines)

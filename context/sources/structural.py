"""Структурные источники контура A: диффы инструментов, анлоки, календарь.

Дифф инструментов ловит ФАКТИЧЕСКИЙ листинг — символ появился в торгах, даже если
анонса не было или он прошёл мимо парсеров. Первый прогон событием не считается:
иначе «появился» весь рынок сразу.

Анлоки берутся тем же кодом, что использует боевой бот (импорт, не копия), с
бесплатного датасет-хоста DefiLlama — `api.llama.fi/emissions` закрыт (HTTP 402).
"""

import logging
import time
from typing import Dict, List

log = logging.getLogger("context.structural")

MARKETS = (
    ("BINANCE", "perp", "binance_exchange_info"),
    ("BINANCE", "spot", "binance_spot_info"),
    ("BYBIT", "perp", "bybit_instruments_linear"),
    ("BYBIT", "spot", "bybit_instruments_spot"),
)


async def _json(session, url: str, timeout_ms: int):
    import aiohttp
    timeout = aiohttp.ClientTimeout(total=timeout_ms / 1000.0)
    async with session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        return await resp.json(content_type=None)


async def fetch_symbols(session, cfg: dict, exchange: str, market: str) -> List[str]:
    src = cfg["sources"]
    budget = cfg["budget"]["per_source_ms"]
    if exchange == "BINANCE":
        url = src["binance_exchange_info"] if market == "perp" else src["binance_spot_info"]
        data = await _json(session, url, budget)
        out = []
        for s in data.get("symbols", []):
            if s.get("status") not in ("TRADING", None):
                continue
            if market == "perp" and s.get("contractType") != "PERPETUAL":
                continue
            if s.get("underlyingType") == "EQUITY":
                continue        # перпы на акции к крипто-контексту не относятся
            out.append(s["symbol"])
        return out

    category = "linear" if market == "perp" else "spot"
    data = await _json(session, src["bybit_instruments"].format(category=category), budget)
    rows = (data.get("result") or {}).get("list") or []
    return [r["symbol"] for r in rows if r.get("status") in ("Trading", None)]


async def instruments_diff(session, cfg: dict, cache) -> List[dict]:
    """Появившиеся и исчезнувшие инструменты → события NEW_INSTRUMENT / DELISTED."""
    events = []
    for exchange, market, _ in MARKETS:
        try:
            symbols = await fetch_symbols(session, cfg, exchange, market)
        except Exception as exc:
            log.warning("%s/%s: список инструментов не получен: %s", exchange, market, exc)
            continue
        diff = cache.apply_instruments(exchange, market, symbols)
        if diff["first_run"]:
            log.info("%s/%s: первый снимок, %s инструментов (событий не создаём)",
                     exchange, market, diff["total"])
            continue
        for symbol in diff["added"]:
            events.append({"source": f"{exchange}:{market}", "raw_type": "instruments_diff",
                           "event_type": "NEW_INSTRUMENT", "title": f"{symbol} появился на {exchange} ({market})",
                           "url": "", "ts": time.time(),
                           "payload": {"exchange": exchange, "market": market, "symbol": symbol}})
        for symbol in diff["removed"]:
            events.append({"source": f"{exchange}:{market}", "raw_type": "instruments_diff",
                           "event_type": "DELISTED", "title": f"{symbol} исчез с {exchange} ({market})",
                           "url": "", "ts": time.time(),
                           "payload": {"exchange": exchange, "market": market, "symbol": symbol}})
    return events


async def unlocks(session, cfg: dict, bot_cfg: dict, tickers: List[str]) -> Dict[str, str]:
    """Контекст анлоков по монетам. Логика берётся из боевого модуля, не копируется."""
    import pump_bot

    ctx_cfg = bot_cfg["event_context"]
    net = bot_cfg["network"]
    out: Dict[str, str] = {}
    try:
        feed = pump_bot.Feed(bot_cfg, session)
        slugs = await feed.llama_protocols()
        slug_map = pump_bot.build_slug_map(slugs, ctx_cfg.get("slug_overrides") or {})
    except Exception as exc:
        log.warning("список протоколов DefiLlama не получен: %s", exc)
        return out

    for ticker in tickers:
        slug = slug_map.get(ticker)
        if not slug:
            continue
        try:
            dataset = await feed.llama_unlock_dataset(slug)
            text = pump_bot.unlock_context(dataset, ctx_cfg["unlock_window_days"], time.time())
        except Exception as exc:
            log.warning("анлоки по %s не получены: %s", ticker, exc)
            continue
        if text:
            out[ticker] = text
    return out


async def coinmarketcal(session, cfg: dict) -> List[dict]:
    """Календарь событий. Без ключа источник выключен в конфиге."""
    key = cfg["keys"].get("coinmarketcal")
    if not key:
        return []
    import aiohttp
    from datetime import datetime, timezone
    timeout = aiohttp.ClientTimeout(total=cfg["budget"]["per_source_ms"] / 1000.0)
    params = {"max": 50, "dateRangeStart": datetime.now(timezone.utc).strftime("%Y-%m-%d")}
    async with session.get(cfg["sources"]["coinmarketcal_events"], params=params, timeout=timeout,
                           headers={"x-api-key": key, "Accept": "application/json"}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        data = await resp.json(content_type=None)
    out = []
    for event in (data.get("body") or []):
        coins = [c.get("symbol", "").upper() for c in (event.get("coins") or [])]
        out.append({"source": "COINMARKETCAL", "raw_type": "calendar", "event_type": "CALENDAR",
                    "title": (event.get("title") or {}).get("en") or "",
                    "url": event.get("source") or "https://coinmarketcal.com/",
                    "ts": time.time(), "payload": {"coins": coins, "date": event.get("date_event")},
                    "tickers": coins})
    return out

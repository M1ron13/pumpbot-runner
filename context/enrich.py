"""Контур B: обогащение по факту алерта. Бюджет жёсткий.

Гарантия неблокирования устроена двумя рубежами:
1. у каждого саб-запроса свой таймаут (per_source_ms);
2. весь слой обёрнут во внешний `wait_for` (total_ms) и try/except.

Не успел — в контекст идёт то, что собралось, остальное опускается. Алерт не ждёт
слой никогда: исключение внутри превращается в пометку «контекст не проверен».
"""

import asyncio
import logging
import time
from typing import List, Optional

from context import config as ctx_config
from context import cross_listing, dedup, render, signals
from context.cache import Cache
from context.classify import Classifier
from context.matcher import TickerMatcher
from context.sources import market, news

log = logging.getLogger("context.enrich")

CATALYST_EVENT_TYPES = ("LISTING", "NEW_INSTRUMENT", "PERP_LAUNCH", "CALENDAR")
BEARISH_EVENT_TYPES = ("DELISTING", "DELISTED", "UNLOCK", "HACK")


async def _guard(name: str, coro, timeout_ms: float, ok: list, failed: list):
    """Саб-запрос со своим таймаутом: падение одного не рушит обогащение."""
    try:
        result = await asyncio.wait_for(coro, timeout=timeout_ms / 1000.0)
        ok.append(name)
        return result
    except asyncio.TimeoutError:
        failed.append(f"{name}: таймаут")
    except Exception as exc:
        failed.append(f"{name}: {type(exc).__name__}")
        log.debug("%s: %s", name, exc)
    return None



async def basis_context(session, cfg: dict, symbol: str, perp_price: float) -> dict:
    """Basis: цена перпа уже известна из алерта, поэтому нужен один запрос спота."""
    spot = await market.spot_price(session, cfg, symbol)
    basis = signals.basis_pct(perp_price, spot)
    verdict = signals.basis_verdict(basis, cfg)
    if basis is not None and verdict is None and not signals.basis_is_sane(basis, cfg):
        log.warning("basis по %s = %.1f%% — цены несопоставимы, строка не печатается",
                    symbol, basis)
    return {"basis_pct": basis, "basis_verdict": verdict, "spot_price": spot}


async def krw_context(session, cfg: dict, cache, ticker: str, usd_price: float) -> dict:
    """Корейская премия к фону рынка: премия монеты минус медиана мажоров."""
    krw_cfg = cfg["signals"]["krw"]
    majors = list(krw_cfg["background_tickers"])

    rate = cache.get_state("usd_krw")
    rate_ts = float(cache.get_state("usd_krw_ts") or 0)
    if not rate or time.time() - rate_ts > float(krw_cfg["fx_cache_sec"]):
        fetched = await market.usd_krw_rate(session, cfg)
        if fetched:
            rate = fetched
            cache.set_state("usd_krw", rate)
            cache.set_state("usd_krw_ts", time.time())
    if not rate:
        return {}

    krw_prices = await market.upbit_krw_prices(session, cfg, [ticker] + majors)
    if ticker not in krw_prices:
        return {}          # монета не торгуется на Upbit — строки просто нет
    usd_prices = await market.spot_prices(session, cfg, [f"{m}USDT" for m in majors])

    background = signals.market_background({
        m: signals.krw_premium_pct(krw_prices.get(m), rate, usd_prices.get(f"{m}USDT"))
        for m in majors})
    premium = signals.krw_premium_pct(krw_prices.get(ticker), rate, usd_price)
    return {"krw_premium": premium, "krw_background": background,
            "krw_excess": signals.excess_premium(premium, background)}


async def collect(session, cfg: dict, cache: Cache, *, ticker: str, symbol: str,
                  now_ts: float, unlock_text: str = None, price: float = None) -> dict:
    """Собрать контекст в пределах бюджета. Возвращает словарь для render.render()."""
    ok: List[str] = []
    failed: List[str] = []
    context = {"symbol": symbol, "ticker": ticker, "sources_ok": ok, "sources_failed": failed}
    window = cfg["context_window"]

    # 0. кэш контура A — бесплатно и мгновенно
    events = []
    try:
        events = cache.events_for(ticker, float(window["listing_within_sec"]), now_ts=now_ts)
        ok.append("кэш событий")
    except Exception as exc:
        failed.append(f"кэш событий: {type(exc).__name__}")
        log.warning("кэш событий недоступен: %s", exc)

    tasks = {}
    per_source = float(cfg["budget"]["per_source_ms"])
    if ctx_config.enabled(cfg, "derivatives"):
        tasks["деривативы"] = _guard("деривативы", market.derivatives(session, cfg, symbol),
                                     per_source, ok, failed)
    if ctx_config.enabled(cfg, "dexscreener"):
        # один символ носят десятки монет — отдаём все адреса-кандидаты,
        # выбор настоящего проекта делается по глубине стакана
        addresses = []
        try:
            import json as _json_mod
            for coin in cache.coins_by_symbol(ticker):
                for address in (_json_mod.loads(coin.get("platforms") or "{}") or {}).values():
                    if address and address not in addresses:
                        addresses.append(address)
        except Exception:
            addresses = []
        tasks["DEX"] = _guard("DEX", market.dex(session, cfg, ticker, addresses),
                              per_source, ok, failed)
    if cfg["signals"]["basis"]["enabled"] and price:
        tasks["basis"] = _guard("basis", basis_context(session, cfg, symbol, price),
                                per_source, ok, failed)
    if cfg["signals"]["krw"]["enabled"] and price:
        tasks["KRW"] = _guard("KRW", krw_context(session, cfg, cache, ticker, price),
                              per_source, ok, failed)
    if ctx_config.enabled(cfg, "tavily"):
        tasks["tavily"] = _guard("tavily", news.tavily(session, cfg, ticker), per_source, ok, failed)
    if ctx_config.enabled(cfg, "cryptopanic"):
        tasks["cryptopanic"] = _guard("cryptopanic", news.cryptopanic(session, cfg, ticker),
                                      per_source, ok, failed)

    results = {}
    if tasks:
        gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)
        results = {name: (None if isinstance(value, BaseException) else value)
                   for name, value in zip(tasks.keys(), gathered)}

    context["derivatives"] = results.get("деривативы") or {}
    context.update(results.get("basis") or {})
    context.update(results.get("KRW") or {})
    # состояние фандинга читается из кэша бесплатно — его собирает контур A
    funding_state = cache.signal_state(symbol, "funding")
    if funding_state:
        sustained_sec = float(cfg["signals"]["funding"]["sustained_hours"]) * 3600.0
        context["funding_state"] = {
            "state": funding_state["state"], "value": funding_state["value"],
            "sustained": (funding_state["state"] == signals.FUNDING_SHORT_EXTREME
                          and now_ts - float(funding_state["since_ts"] or now_ts) >= sustained_sec)}
    context["dex"] = results.get("DEX") or {}
    if unlock_text:
        context["unlock"] = unlock_text

    # свободный текст → дешёвые фильтры → дедуп → один вызов LLM
    raw_news = (results.get("tavily") or []) + (results.get("cryptopanic") or [])
    verdicts = {"catalysts": [], "bearish": []}
    if raw_news:
        filtered = news.cheap_filters(raw_news, cfg, now_ts)
        matcher = TickerMatcher(cfg, cache)
        matched = [item for item in filtered
                   if matcher.match(item.get("title", ""), item.get("source", ""),
                                    hinted_ticker=ticker)]
        clustered = dedup.cluster(matched, cfg)
        classifier = Classifier(cfg, session, cache)
        outcome = await classifier.classify(symbol, clustered)
        verdicts = outcome["verdicts"]
        context["llm_status"] = outcome["status"]
        if outcome["status"].startswith("классификация не удалась"):
            failed.append("классификация")
        for item in verdicts["catalysts"]:
            idx = int(item.get("headline_id", 0)) - 1
            if 0 <= idx < len(clustered):
                item.setdefault("url", clustered[idx].get("url"))
                item.setdefault("ts", clustered[idx].get("ts", now_ts))
                item.setdefault("source", clustered[idx].get("domain"))

    # вердикт: событие биржи важнее новостного текста — его тип точный, не выведенный
    catalyst_event = next((e for e in events
                           if (e.get("event_type") or "").upper() in CATALYST_EVENT_TYPES
                           and now_ts - float(e["ts"]) <= float(window["listing_within_sec"])), None)
    bearish_event = next((e for e in events
                          if (e.get("event_type") or "").upper() in BEARISH_EVENT_TYPES), None)

    if catalyst_event:
        context["verdict"] = render.VERDICT_CATALYST
        context["catalyst"] = catalyst_event
        listed_on = cache.where_listed(ticker)
        verdict = cross_listing.classify(
            {**catalyst_event, "ticker": ticker,
             "market": (catalyst_event.get("payload") or "")}, listed_on, cfg)
        context["cross_listing"] = verdict
    elif verdicts["catalysts"]:
        item = verdicts["catalysts"][0]
        context["verdict"] = render.VERDICT_CATALYST
        context["catalyst"] = {"event_type": item.get("event_type", "OTHER"),
                               "source": item.get("source") or "новости",
                               "ts": item.get("ts", now_ts), "url": item.get("url", "")}
    elif bearish_event or verdicts["bearish"]:
        context["verdict"] = render.VERDICT_BEARISH
        context["bearish"] = bearish_event or {
            "event_type": verdicts["bearish"][0].get("event_type", "OTHER")}
    elif failed and not ok:
        context["verdict"] = render.VERDICT_UNKNOWN
    else:
        context["verdict"] = render.VERDICT_CLEAN
    return context


async def enrich_alert(alert, bot_cfg: dict, session=None, cfg: dict = None,
                       cache: Cache = None) -> Optional[str]:
    """Точка входа для бота. Возвращает готовый блок текста либо None.

    Никогда не поднимает исключение и никогда не превышает бюджет: и то, и другое
    означало бы задержку алерта.
    """
    started = time.time()
    own_session = own_cache = None
    try:
        cfg = cfg or ctx_config.load()
        if not cfg.get("enabled"):
            return None

        budget_ms = float(cfg["budget"]["total_ms"])
        if session is None:
            import aiohttp
            own_session = aiohttp.ClientSession()
            session = own_session
        if cache is None:
            own_cache = Cache(ctx_config.cache_path(cfg))
            cache = own_cache

        sig = alert.primary
        quote = bot_cfg["universe"]["quote_suffix"]
        import pump_bot
        ticker = pump_bot.base_coin(sig.symbol, quote)
        now_ts = float(alert.ts)

        context = await asyncio.wait_for(
            collect(session, cfg, cache, ticker=ticker, symbol=sig.symbol,
                    now_ts=now_ts, unlock_text=sig.event_context, price=sig.price),
            timeout=budget_ms / 1000.0)
        block = render.render(context, cfg, now_ts)
        cache.log_enrichment(symbol=sig.symbol, verdict=context.get("verdict"), block=block,
                             budget_ms=int(budget_ms), elapsed_ms=int((time.time() - started) * 1000),
                             sources_ok=context.get("sources_ok"),
                             sources_failed=context.get("sources_failed"))
        return block
    except asyncio.TimeoutError:
        log.warning("контекст не собран в бюджет — алерт уходит без него")
        return render.render({"verdict": render.VERDICT_UNKNOWN,
                              "sources_failed": ["не уложились в бюджет"]},
                             cfg or {"derivatives_flags": {"high_long_short_ratio": 2.5}},
                             time.time())
    except Exception as exc:
        log.warning("контекст-слой отключился на этом алерте: %s", exc)
        return None
    finally:
        if own_session is not None:
            await own_session.close()
        if own_cache is not None:
            own_cache.close()

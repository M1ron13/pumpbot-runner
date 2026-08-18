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
from context import cross_listing, dedup, render
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


async def collect(session, cfg: dict, cache: Cache, *, ticker: str, symbol: str,
                  now_ts: float, unlock_text: str = None) -> dict:
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
                    now_ts=now_ts, unlock_text=sig.event_context),
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

"""Контур A: фоновый мониторинг. Отдельный процесс, не задача внутри бота.

Причина отдельного процесса: фоновый опрос бирж не должен конкурировать за цикл
детектора — у того есть свой бюджет на итерацию, и любая просадка в нём означает
пропущенные минуты рынка.

Запуск: python -m context.monitor            (постоянный процесс)
        python -m context.monitor --once     (один проход всех источников)
"""

import argparse
import asyncio
import logging
import sys
import time

from context import config as ctx_config
from context.cache import Cache
from context.matcher import TickerMatcher
from context.sources import announcements, structural

log = logging.getLogger("context.monitor")


def setup_logging(level="INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
        handlers=[logging.StreamHandler(sys.stdout),
                  logging.FileHandler("context_monitor.log", encoding="utf-8")])


async def refresh_coins(session, cfg: dict, cache: Cache) -> int:
    """Справочник CoinGecko: имя ↔ тикер, без него короткие тикеры матчить нельзя."""
    import aiohttp
    age = cache.coins_age_sec()
    if age is not None and age < float(cfg["monitor"]["coins_dictionary_sec"]):
        return 0
    timeout = aiohttp.ClientTimeout(total=60)
    async with session.get(cfg["sources"]["coingecko_coins"], timeout=timeout,
                           headers={"User-Agent": "Mozilla/5.0"}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        data = await resp.json(content_type=None)
    coins = [{"coin_id": c["id"], "symbol": c["symbol"], "name": c["name"],
              "platforms": {k: v for k, v in (c.get("platforms") or {}).items() if v}}
             for c in data if c.get("id") and c.get("symbol") and c.get("name")]
    count = cache.upsert_coins(coins)
    log.info("справочник монет обновлён: %s записей", count)
    return count


async def poll_announcements(session, cfg: dict, cache: Cache, source: str) -> int:
    """Один источник объявлений → события в кэш. Тикер ищется правилами матчинга."""
    fetcher = announcements.FETCHERS[source]
    items = await fetcher(session, cfg)
    matcher = TickerMatcher(cfg, cache)
    ttl = float(cfg["monitor"]["event_ttl_sec"])
    added = 0
    for item in items:
        match = matcher.match(item["title"], item["source"])
        added += int(cache.add_event(
            ts=item["ts"], source=item["source"], event_type=item["event_type"],
            raw_type=item.get("raw_type"), ticker=(match or {}).get("ticker"),
            symbol=None, title=item["title"], url=item.get("url"),
            payload={"tags": item.get("tags") or []}, ttl_sec=ttl,
            matched_rule=(match or {}).get("rule")))
    log.info("%s: получено %s, новых %s", source, len(items), added)
    return added


async def poll_instruments(session, cfg: dict, cache: Cache) -> int:
    events = await structural.instruments_diff(session, cfg, cache)
    matcher = TickerMatcher(cfg, cache)
    ttl = float(cfg["monitor"]["event_ttl_sec"])
    added = 0
    for event in events:
        symbol = (event.get("payload") or {}).get("symbol", "")
        ticker = symbol[:-4] if symbol.endswith("USDT") else symbol
        added += int(cache.add_event(
            ts=event["ts"], source=event["source"], event_type=event["event_type"],
            raw_type=event.get("raw_type"), ticker=ticker or None, symbol=symbol,
            title=event["title"], url=event.get("url"), payload=event.get("payload"),
            ttl_sec=ttl, matched_rule="символ инструмента"))
    if added:
        log.info("диффы инструментов: новых событий %s", added)
    return added


async def loop_source(name: str, interval_sec: float, coro_factory) -> None:
    """Один источник — один цикл. Ошибка в источнике не роняет остальные."""
    while True:
        started = time.time()
        try:
            await coro_factory()
        except Exception as exc:
            log.warning("%s: %s", name, exc)
        elapsed = time.time() - started
        await asyncio.sleep(max(1.0, interval_sec - elapsed))


async def run_once(session, cfg: dict, cache: Cache) -> dict:
    stats = {}
    try:
        stats["coins"] = await refresh_coins(session, cfg, cache)
    except Exception as exc:
        log.warning("справочник монет: %s", exc)
    for source in ("bybit_announcements", "binance_announcements",
                   "upbit_announcements", "okx_announcements"):
        if not ctx_config.enabled(cfg, source):
            continue
        try:
            stats[source] = await poll_announcements(session, cfg, cache, source)
        except Exception as exc:
            log.warning("%s: %s", source, exc)
    if ctx_config.enabled(cfg, "instruments_diff"):
        try:
            stats["instruments"] = await poll_instruments(session, cfg, cache)
        except Exception as exc:
            log.warning("диффы инструментов: %s", exc)
    if ctx_config.enabled(cfg, "coinmarketcal"):
        try:
            events = await structural.coinmarketcal(session, cfg)
            ttl = float(cfg["monitor"]["event_ttl_sec"])
            added = 0
            for event in events:
                for ticker in (event.get("tickers") or [None]):
                    added += int(cache.add_event(
                        ts=event["ts"], source=event["source"], event_type=event["event_type"],
                        raw_type=event.get("raw_type"), ticker=ticker, title=event["title"],
                        url=event.get("url"), payload=event.get("payload"), ttl_sec=ttl,
                        matched_rule="календарь"))
            stats["coinmarketcal"] = added
        except Exception as exc:
            log.warning("coinmarketcal: %s", exc)
    cache.purge_expired()
    return stats


async def main_async(once: bool) -> int:
    import aiohttp
    cfg = ctx_config.load()
    cache = Cache(ctx_config.cache_path(cfg))
    log.info("контур A: включены источники %s",
             [k for k, v in cfg["enabled_sources"].items() if v])
    async with aiohttp.ClientSession() as session:
        if once:
            stats = await run_once(session, cfg, cache)
            log.info("проход завершён: %s", stats)
            cache.close()
            return 0

        monitor = cfg["monitor"]
        tasks = [
            loop_source("справочник монет", monitor["coins_dictionary_sec"],
                        lambda: refresh_coins(session, cfg, cache)),
        ]
        for source, key in (("bybit_announcements", "bybit_sec"),
                            ("binance_announcements", "binance_sec"),
                            ("upbit_announcements", "upbit_sec"),
                            ("okx_announcements", "okx_sec")):
            if ctx_config.enabled(cfg, source):
                tasks.append(loop_source(source, monitor[key],
                                         lambda s=source: poll_announcements(session, cfg, cache, s)))
        if ctx_config.enabled(cfg, "instruments_diff"):
            tasks.append(loop_source("диффы инструментов", monitor["instruments_sec"],
                                     lambda: poll_instruments(session, cfg, cache)))
        try:
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            pass
    cache.close()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Контур A: фоновый мониторинг контекста")
    parser.add_argument("--once", action="store_true", help="один проход и выход")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args(argv)
    setup_logging(args.log_level)
    try:
        return asyncio.run(main_async(args.once))
    except KeyboardInterrupt:
        log.info("остановлен")
        return 0


if __name__ == "__main__":
    sys.exit(main())

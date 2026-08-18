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
import os
import sys
import time

from context import config as ctx_config
from context.cache import Cache
from context.matcher import TickerMatcher
from context import dedup, heartbeat, signals
from context.classify import Classifier, normalize_category
from context.publisher import Publisher
from context.sources import announcements, market, rss, structural

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
    """Один источник объявлений → события в кэш, через единый входной фильтр.

    TradFi-инструменты и объявления без тикера в кэш не попадают: иначе они дойдут
    до вердикта КАТАЛИЗАТОР в алертах пампа или до канала.
    """
    fetcher = announcements.FETCHERS[source]
    items = await fetcher(session, cfg)
    ttl = float(cfg["monitor"]["event_ttl_sec"])
    added = skipped = failed = 0
    for item in items:
        screened = announcements.screen(item, cfg)
        if not screened["keep"]:
            if screened["stage"] == "filtered_tradfi":
                skipped += 1
                log.info("[filtered_tradfi] %s | %s", item["source"], item["title"][:110])
            else:
                failed += 1
                log.info("[parse_failed] %s | %s | %s", item["source"],
                         item["title"][:110], item.get("url", ""))
            continue
        added += int(cache.add_event(
            ts=item["ts"], source=item["source"], event_type=item["event_type"],
            raw_type=item.get("raw_type"), ticker=screened["tickers"][0],
            symbol=None, title=item["title"], url=item.get("url"),
            payload={"tags": item.get("tags") or [], "tickers": screened["tickers"],
                     "market": screened.get("market")},
            ttl_sec=ttl, matched_rule="фильтр листингов"))
    total = len(items)
    if total >= 10 and failed / total > 0.3:
        log.warning("%s: тикер не извлекается у %.0f%% объявлений (%s/%s) — парсер деградировал",
                    source, failed / total * 100, failed, total)
    log.info("%s: получено %s, новых %s, отфильтровано TradFi %s, без тикера %s",
             source, total, added, skipped, failed)
    return added


async def poll_funding(session, cfg: dict, cache: Cache) -> int:
    """Фандинг по всем символам → состояния с гистерезисом → внутренние события.

    События FUNDING_EXTREME_* сознательно не входят в белый список публикатора: они
    существуют только как строки внутри алерта пампа, самостоятельными сообщениями
    не становятся.
    """
    if not cfg["signals"]["funding"]["enabled"]:
        return 0
    snapshot = await market.funding_snapshot(session, cfg)
    if not snapshot:
        return 0
    now_ts = time.time()
    ttl = float(cfg["monitor"]["event_ttl_sec"])
    changes = 0
    counts = {signals.FUNDING_LONG_EXTREME: 0, signals.FUNDING_SHORT_EXTREME: 0}
    for symbol, data in snapshot.items():
        funding_8h = signals.to_8h(data["rate"], data.get("interval_hours"))
        previous = cache.signal_state(symbol, "funding")
        state = signals.funding_transition(previous, funding_8h, cfg, now_ts)
        cache.record_signal(symbol, "funding", state["state"], state["value"],
                            state["since_ts"], sustained=state["sustained"],
                            changed=state["changed"], now_ts=now_ts)
        if state["state"] in counts:
            counts[state["state"]] += 1
        if state["changed"] and state["state"] != signals.FUNDING_OFF:
            changes += 1
            ticker = symbol[:-4] if symbol.endswith("USDT") else symbol
            cache.add_event(
                ts=now_ts, source="SIGNALS", event_type=f"FUNDING_EXTREME_"
                f"{'LONG' if state['state'] == signals.FUNDING_LONG_EXTREME else 'SHORT'}",
                raw_type="funding", ticker=ticker, symbol=symbol,
                title=f"{symbol}: фандинг {state['value'] * 100:+.3f}%/8ч",
                url="", payload={"value_8h": state["value"], "state": state["state"]},
                ttl_sec=ttl, matched_rule="внутренний сигнал")
    log.info("фандинг: символов %s, перегрев лонгов %s, перегрев шортов %s, переходов %s",
             len(snapshot), counts[signals.FUNDING_LONG_EXTREME],
             counts[signals.FUNDING_SHORT_EXTREME], changes)
    return changes


async def poll_rss(session, cfg: dict, cache: Cache) -> int:
    """RSS первоисточников → кандидаты новостей с распознанной монетой.

    Матчинг здесь тот же, что для анонсов: короткие тикеры требуют имени проекта в
    тексте. Для SEC это особенно важно — в исках тикеров нет вообще, только названия.
    """
    if not cfg["rss"]["enabled"]:
        return 0
    items = await rss.poll(session, cfg, cache)
    if not items:
        return 0
    matcher = TickerMatcher(cfg, cache)
    added = unmatched = 0
    clustered = dedup.cluster(items, cfg)
    for item in clustered:
        text = f"{item['title']} {item.get('summary') or ''}"
        match = matcher.match(text, item["source"])
        if not match:
            unmatched += 1
            continue
        # ключ события: по нему считаются независимые подтверждения из разных фидов
        event_key = f"{match['ticker']}:{dedup.normalize(item['title'])[:6]}"
        added += int(cache.add_news_candidate(
            ts=item["ts"], source=item["source"], feed=item.get("feed"),
            source_tier=item.get("source_tier"), ticker=match["ticker"],
            title=item["title"], summary=item.get("summary"), url=item.get("url"),
            event_key=" ".join(event_key.split()),
            confirmations=1 + int(item.get("duplicates") or 0)))
    log.info("RSS: элементов %s, после дедупа %s, новых кандидатов %s, без монеты %s",
             len(items), len(clustered), added, unmatched)
    return added


async def classify_news(session, cfg: dict, cache: Cache) -> int:
    """Кандидаты → LLM → решение по Типу 4. Без ключа классификация не запускается."""
    pending = cache.unclassified_news()
    if not pending:
        return 0
    classifier = Classifier(cfg, session, cache,
                            timeout_ms=cfg["classification"]["background_timeout_ms"])
    if not classifier.provider:
        log.info("классификация пропущена: нет ключа LLM (%s кандидатов ждут)", len(pending))
        return 0

    outcome = await classifier.classify("НОВОСТИ", pending)
    if outcome["status"] != "ок":
        log.warning("классификация: %s", outcome["status"])
        return 0

    published = 0
    publisher = Publisher(cfg, cache, await make_sender(session, cfg))
    for item in outcome["items"]:
        index = int(item.get("headline_id", 0)) - 1
        if not (0 <= index < len(pending)):
            continue
        candidate = pending[index]
        news = {
            "ticker": candidate["ticker"],
            "category": normalize_category(item.get("event_type")),
            "is_fact": bool(item.get("is_fact")), "source_tier": min(
                int(candidate["source_tier"] or 3), int(item.get("source_tier") or 3)),
            "confidence": float(item.get("confidence") or 0.0),
            "confirmations": cache.count_confirmations(candidate["event_key"]),
            "summary": candidate["title"], "source_name": candidate["feed"],
            "url": candidate["url"], "ts": candidate["ts"],
            "event_key": candidate["event_key"],
        }
        message, label = publisher.message_for_news(news, time.time())
        if message is None:
            cache.save_classification(candidate["id"], news["category"], news["is_fact"],
                                      news["confidence"], f"лог: {label}")
            continue
        sent, label = await publisher.publish(message, time.time())
        cache.save_classification(candidate["id"], news["category"], news["is_fact"],
                                  news["confidence"],
                                  ("канал: " if sent else "лог: ") + label)
        published += int(sent)
    log.info("классификация: разобрано %s, отправлено %s", len(outcome["items"]), published)
    return published


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


async def loop_source(name: str, interval_sec: float, coro_factory, cache=None) -> None:
    """Один источник — один цикл. Ошибка в источнике не роняет остальные.

    Отметка прогресса ставится после итерации — включая неудачную по сети: важно, что
    цикл жив и вернулся, а не что данные пришли. Зависший запрос отметку не обновит,
    и следующий инстанс это увидит.
    """
    while True:
        started = time.time()
        try:
            await coro_factory()
        except Exception as exc:
            log.warning("%s: %s", name, exc)
        if cache is not None:
            heartbeat.mark_loop(cache, name, interval_sec)
            heartbeat.mark_owner(cache)
        elapsed = time.time() - started
        await asyncio.sleep(max(1.0, interval_sec - elapsed))


async def make_sender(session, cfg: dict):
    """Отправка через боевой Telegram-клиент бота — один клиент, один формат, один канал."""
    import sys
    bot_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if bot_dir not in sys.path:
        sys.path.insert(0, bot_dir)
    import pump_bot
    bot_cfg_path = os.path.join(bot_dir, "config.local.json")
    if not os.path.exists(bot_cfg_path):
        bot_cfg_path = os.path.join(bot_dir, "config.json")
    telegram = pump_bot.Telegram(pump_bot.load_config(bot_cfg_path), session)

    async def send(text: str) -> bool:
        return await telegram.send(text)

    return send


async def run_once(session, cfg: dict, cache: Cache) -> dict:
    stats = {}
    try:
        stats["coins"] = await refresh_coins(session, cfg, cache)
    except Exception as exc:
        log.warning("справочник монет: %s", exc)
    for source in ("bybit_announcements", "binance_announcements", "upbit_announcements",
                   "kucoin_announcements", "okx_announcements"):
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
    if cfg["rss"]["enabled"]:
        try:
            stats["rss"] = await poll_rss(session, cfg, cache)
            stats["классификация"] = await classify_news(session, cfg, cache)
        except Exception as exc:
            log.warning("RSS: %s", exc)
    if cfg["signals"]["funding"]["enabled"]:
        try:
            stats["фандинг"] = await poll_funding(session, cfg, cache)
        except Exception as exc:
            log.warning("фандинг: %s", exc)
    if cfg["publisher"]["enabled"]:
        try:
            telegram = await make_sender(session, cfg)
            stats["публикатор"] = await Publisher(cfg, cache, telegram).run_once()
        except Exception as exc:
            log.warning("публикатор: %s", exc)
    cache.purge_expired(ledger_sec=float(cfg['monitor']['publish_ledger_sec']))
    return stats


async def main_async(once: bool) -> int:
    import aiohttp
    cfg = ctx_config.load()
    cache = Cache(ctx_config.cache_path(cfg))
    log.info("контур A: включены источники %s",
             [k for k, v in cfg["enabled_sources"].items() if v])
    async with aiohttp.ClientSession() as session:
        if once:
            # разовый прогон тоже обязан уступать: два пишущих инстанса — это то,
            # от чего защищает механизм прав, и «database is locked» тому симптом
            decision = heartbeat.claim_leadership(cache, cfg)
            if not decision["claim"]:
                log.info("разовый прогон отменён: %s", decision["reason"])
                cache.close()
                return 0
            stats = await run_once(session, cfg, cache)
            log.info("проход завершён: %s", stats)
            cache.close()
            return 0

        monitor = cfg["monitor"]
        decision = heartbeat.claim_leadership(cache, cfg)
        log.info("права на работу: %s (%s)",
                 "заняты" if decision["claim"] else "уступлены", decision["reason"])
        if not decision["claim"]:
            cache.close()
            return 0
        heartbeat.mark_owner(cache)

        tasks = [
            loop_source("справочник монет", monitor["coins_dictionary_sec"],
                        lambda: refresh_coins(session, cfg, cache), cache),
        ]
        for source, key in (("bybit_announcements", "bybit_sec"),
                            ("binance_announcements", "binance_sec"),
                            ("upbit_announcements", "upbit_sec"),
                            ("okx_announcements", "okx_sec")):
            if ctx_config.enabled(cfg, source):
                tasks.append(loop_source(source, monitor[key],
                                         lambda s=source: poll_announcements(session, cfg, cache, s),
                                         cache))
        if ctx_config.enabled(cfg, "instruments_diff"):
            tasks.append(loop_source("диффы инструментов", monitor["instruments_sec"],
                                     lambda: poll_instruments(session, cfg, cache), cache))
        if cfg["rss"]["enabled"]:
            tasks.append(loop_source("rss", monitor["rss_sec"],
                                     lambda: poll_rss(session, cfg, cache), cache))
            tasks.append(loop_source("классификация новостей", monitor["rss_sec"],
                                     lambda: classify_news(session, cfg, cache), cache))
        if cfg["signals"]["funding"]["enabled"]:
            tasks.append(loop_source("фандинг", monitor["funding_sec"],
                                     lambda: poll_funding(session, cfg, cache), cache))
        if cfg["publisher"]["enabled"]:
            sender = await make_sender(session, cfg)
            publisher = Publisher(cfg, cache, sender)
            tasks.append(loop_source("публикатор", monitor["publisher_sec"],
                                     publisher.run_once, cache))
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

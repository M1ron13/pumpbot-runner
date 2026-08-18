"""Ретро-прогон: что решит публикатор по свежим и уже накопленным объявлениям.

Ничего не отправляет и кэш не меняет. Нужен как приёмка перед включением канала:
таблица «заголовок → тикер → фильтр/класс → решение» показывает не намерение
правил, а их фактический вывод на реальных данных.

Запуск: python -m context.retro [--limit 100]
"""

import argparse
import asyncio
import os
import sys
import time

from context import config as ctx_config
from context import listing_rules as rules
from context.cache import Cache
from context.publisher import Publisher
from context.sources import announcements


def cut(text, width):
    text = (text or "").replace("\n", " ")
    return text if len(text) <= width else text[: width - 1] + "…"


async def collect_live(cfg, limit):
    """Свежие объявления из всех включённых источников."""
    import aiohttp
    items = []
    async with aiohttp.ClientSession() as session:
        for source, fetcher in announcements.FETCHERS.items():
            if not ctx_config.enabled(cfg, source):
                continue
            try:
                got = await fetcher(session, cfg)
            except Exception as exc:
                print(f"{source}: недоступен ({type(exc).__name__})")
                continue
            print(f"{source}: получено {len(got)}")
            items.extend(got[:limit])
    return items


def from_cache(cache, limit):
    """Уже собранные события — включая те, что раньше уходили в канал."""
    rows = cache.conn.execute(
        "SELECT source, event_type, ticker, title, url FROM events ORDER BY id DESC LIMIT ?",
        (limit,)).fetchall()
    return [{"source": r["source"], "event_type": r["event_type"], "ticker": r["ticker"],
             "title": r["title"], "url": r["url"], "raw_type": None} for r in rows]


def decide_row(item, cfg, cache, publisher, now_ts):
    """Полный путь одного объявления: входной фильтр → тикер → матрица → решение."""
    screened = announcements.screen(item, cfg)
    if not screened["keep"]:
        return {"stage": screened["stage"], "ticker": "—", "class": "—", "post": False,
                "reason": screened["reason"]}
    ticker = item.get("ticker") or screened["tickers"][0]
    event = {**item, "ticker": ticker}
    outcome = publisher.evaluate(event, now_ts)
    verdict = outcome["verdict"]
    return {"stage": "оценено", "ticker": ticker, "class": verdict["class"],
            "post": bool(outcome["blocked"] is None), "reason": verdict["reason"],
            "blocked": outcome["blocked"]}


async def main_async(limit: int, use_cache: bool) -> int:
    cfg = ctx_config.load()
    cfg["publisher"] = {**cfg["publisher"], "enabled": True}   # приёмка считает как при включённом канале
    cache = Cache(ctx_config.cache_path(cfg))
    publisher = Publisher(cfg, cache, sender=None)
    now_ts = time.time()

    items = await collect_live(cfg, limit)
    if use_cache:
        items += from_cache(cache, limit)
    if not items:
        print("данных нет")
        return 1

    rows = []
    for item in items[: limit * 2]:
        rows.append({**decide_row(item, cfg, cache, publisher, now_ts),
                     "source": item.get("source"), "title": item.get("title")})

    print(f"\n{'ИСТОЧНИК':9}{'ЗАГОЛОВОК':58}{'ТИКЕР':10}{'КЛАСС/ЭТАП':18}{'РЕШЕНИЕ':8}")
    print("─" * 103)
    for row in rows:
        stage = row["class"] if row["stage"] == "оценено" else row["stage"]
        print(f"{cut(row['source'], 8):9}{cut(row['title'], 57):58}{cut(row['ticker'], 9):10}"
              f"{cut(stage, 17):18}{'КАНАЛ' if row['post'] else 'лог':8}")

    print("─" * 103)
    print(f"всего: {len(rows)}")
    buckets = {}
    for row in rows:
        key = row["class"] if row["stage"] == "оценено" else row["stage"]
        buckets[key] = buckets.get(key, 0) + 1
    for key, count in sorted(buckets.items(), key=lambda kv: -kv[1]):
        print(f"   {key:18} {count}")

    to_channel = [r for r in rows if r["post"]]
    print(f"\nПРОХОДЯТ В КАНАЛ: {len(to_channel)}")
    for row in to_channel:
        print(f"   [{row['source']}] {row['class']} {row['ticker']} — {row['reason']}")
        print(f"      {cut(row['title'], 92)}")
    cache.close()
    return 0


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Ретро-прогон решений публикатора")
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--no-cache", action="store_true", help="только свежие объявления")
    args = parser.parse_args(argv)
    return asyncio.run(main_async(args.limit, not args.no_cache))


if __name__ == "__main__":
    sys.exit(main())

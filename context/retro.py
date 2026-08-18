"""Ретро-прогон: что публикатор решил бы по объявлениям, ничего не отправляя.

Приёмка перед включением канала. Таблица «заголовок → тип → метка → решение»
показывает не намерение правил, а их фактический вывод на реальных данных.
Отправки не происходит: сообщения только собираются и проходят guard-цепочку до
последнего шага.

Запуск: python -m context.retro [--limit 100] [--no-cache]
"""

import argparse
import asyncio
import sys
import time

from context import config as ctx_config
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


def decide_row(item, cfg, publisher, now_ts):
    """Путь объявления: входной фильтр → тикер → белый список → guard-цепочка.

    До самой отправки дело не доходит: собранное сообщение и метка причины —
    это и есть ответ на вопрос «что ушло бы в канал».
    """
    screened = announcements.screen(item, cfg)
    if not screened["keep"]:
        return {"type": "—", "ticker": "—", "label": screened["stage"], "post": False,
                "text": None}

    ticker = item.get("ticker") or screened["tickers"][0]
    message, label = publisher.message_for({**item, "ticker": ticker}, now_ts)
    if message is None:
        return {"type": "—", "ticker": ticker, "label": label, "post": False, "text": None}

    # сообщение собрано — проверяем остаток guard-цепочки, кроме собственно отправки
    if message.requires_universe and not publisher.in_universe(message.ticker)[0]:
        return {"type": message.type, "ticker": ticker, "label": "out_of_universe",
                "post": False, "text": None}
    if publisher.shadow(message.type):
        return {"type": message.type, "ticker": ticker, "label": "would_send (тень)",
                "post": False, "text": message.text}
    return {"type": message.type, "ticker": ticker, "label": "would_send",
            "post": True, "text": message.text}


async def main_async(limit: int, use_cache: bool) -> int:
    cfg = ctx_config.load()
    cfg["publisher"] = {**cfg["publisher"], "enabled": True}   # считаем как при включённом канале
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
        rows.append({**decide_row(item, cfg, publisher, now_ts),
                     "source": item.get("source"), "title": item.get("title")})

    print(f"\n{'ИСТОЧНИК':9}{'ЗАГОЛОВОК':56}{'ТИКЕР':9}{'ТИП':15}{'МЕТКА':20}{'РЕШЕНИЕ':8}")
    print("─" * 110)
    for row in rows:
        print(f"{cut(row['source'], 8):9}{cut(row['title'], 55):56}{cut(row['ticker'], 8):9}"
              f"{cut(row['type'], 14):15}{cut(row['label'], 19):20}"
              f"{'КАНАЛ' if row['post'] else 'лог':8}")

    print("─" * 110)
    print(f"всего: {len(rows)}")
    buckets = {}
    for row in rows:
        buckets[row["label"]] = buckets.get(row["label"], 0) + 1
    for key, count in sorted(buckets.items(), key=lambda kv: -kv[1]):
        print(f"   {key:22} {count}")

    to_channel = [r for r in rows if r["post"]]
    shadowed = [r for r in rows if str(r["label"]).startswith("would_send (тень)")]
    print(f"\nУШЛО БЫ В КАНАЛ: {len(to_channel)} | ПРИДЕРЖАНО ТЕНЬЮ: {len(shadowed)}")
    for row in to_channel + shadowed:
        print(f"\n   [{row['source']}] {row['type']} {row['ticker']} — {row['label']}")
        print(f"   заголовок источника: {cut(row['title'], 84)}")
        for line in (row.get("text") or "").splitlines():
            print(f"   | {line}")
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

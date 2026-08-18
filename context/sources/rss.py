"""RSS первоисточников: SEC Litigation, CoinDesk, The Block, Cointelegraph.

Зависимостей не добавляем: разбор идёт через `xml.etree` из stdlib. `feedparser` брать
не стал сознательно — у контекст-слоя правило «только aiohttp плюс stdlib», а RSS/Atom
нужен нам ровно в объёме «заголовок, ссылка, дата». Устойчивость к экзотике фидов,
за которую ценят feedparser, здесь закрывается тестами на четырёх реальных лентах.

Экономия трафика: ETag и Last-Modified запоминаются, при `304 Not Modified` фид не
разбирается и LLM не вызывается вовсе.

SEC-фид особый: в исках нет тикеров, поэтому монета ищется по названию проекта или
компании — этим занимается матчинг тикеров с правилами коротких тикеров.
"""

import logging
import re
import time
from email.utils import parsedate_to_datetime
from typing import List, Optional
from xml.etree import ElementTree

log = logging.getLogger("context.rss")

NAMESPACES = {"atom": "http://www.w3.org/2005/Atom", "dc": "http://purl.org/dc/elements/1.1/"}
TAG_CLEAN = re.compile(r"<[^>]+>")


def parse_date(value: str) -> Optional[float]:
    """RFC 822 (RSS) или ISO 8601 (Atom). Не разобрали — None, время не выдумываем."""
    if not value:
        return None
    try:
        return parsedate_to_datetime(value).timestamp()
    except (TypeError, ValueError):
        pass
    try:
        from datetime import datetime
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def clean(text: str) -> str:
    return TAG_CLEAN.sub(" ", (text or "")).replace("&nbsp;", " ").strip()


def parse_feed(xml_text: str, feed_name: str, tier: int) -> List[dict]:
    """RSS 2.0 и Atom одним разбором: нас интересуют title, link, description, дата."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        log.warning("%s: XML не разобран (%s)", feed_name, exc)
        return []

    items = root.findall(".//item") or root.findall(".//atom:entry", NAMESPACES)
    out = []
    for item in items:
        title = item.findtext("title") or item.findtext("atom:title", default="", namespaces=NAMESPACES)
        link = item.findtext("link") or ""
        if not link:
            link_el = item.find("atom:link", NAMESPACES)
            link = (link_el.get("href") if link_el is not None else "") or ""
        summary = (item.findtext("description")
                   or item.findtext("atom:summary", default="", namespaces=NAMESPACES)
                   or item.findtext("atom:content", default="", namespaces=NAMESPACES) or "")
        published = (item.findtext("pubDate")
                     or item.findtext("atom:published", default="", namespaces=NAMESPACES)
                     or item.findtext("atom:updated", default="", namespaces=NAMESPACES)
                     or item.findtext("dc:date", default="", namespaces=NAMESPACES))
        title = clean(title)
        if not title:
            continue
        out.append({
            "source": f"rss:{feed_name}",
            "feed": feed_name,
            "source_tier": tier,
            "title": title,
            "summary": clean(summary)[:500],
            "url": link.strip(),
            "ts": parse_date(published) or time.time(),
        })
    return out


async def fetch_feed(session, cfg: dict, cache, feed: dict) -> List[dict]:
    """Один фид с учётом ETag/Last-Modified. При 304 возвращает пустой список."""
    import aiohttp
    name = feed["name"]
    state = cache.get_state(f"rss:{name}") or {}
    headers = {"User-Agent": cfg["rss"]["user_agent"]}
    if state.get("etag"):
        headers["If-None-Match"] = state["etag"]
    if state.get("last_modified"):
        headers["If-Modified-Since"] = state["last_modified"]

    timeout = aiohttp.ClientTimeout(total=cfg["budget"]["per_source_ms"] / 1000.0)
    async with session.get(feed["url"], headers=headers, timeout=timeout) as resp:
        if resp.status == 304:
            log.debug("%s: без изменений (304)", name)
            return []
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        text = await resp.text()
        cache.set_state(f"rss:{name}", {
            "etag": resp.headers.get("ETag") or state.get("etag"),
            "last_modified": resp.headers.get("Last-Modified") or state.get("last_modified"),
            "fetched_ts": time.time()})
    return parse_feed(text, name, int(feed.get("tier", 3)))


async def poll(session, cfg: dict, cache) -> List[dict]:
    """Все включённые фиды. Падение одного не мешает остальным."""
    items = []
    for feed in cfg["rss"]["feeds"]:
        if not feed.get("enabled", True):
            continue
        try:
            got = await fetch_feed(session, cfg, cache, feed)
        except Exception as exc:
            log.warning("%s: %s", feed["name"], exc)
            continue
        if got:
            log.info("%s: элементов %s", feed["name"], len(got))
        items.extend(got)
    return items

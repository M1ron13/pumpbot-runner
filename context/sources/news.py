"""Контур B: свободный поиск новостей (Tavily) и крипто-новости по тикеру (CryptoPanic).

Оба источника требуют собственных ключей: MCP-коннекторы доступны только в сессии
Claude Code, а круглосуточному боту — нет. Без ключа функция возвращает пустой
список, и слой честно пишет «публичных новостей не найдено», а не выдумывает.

Здесь только сбор и дешёвые фильтры (свежесть, whitelist домена). Классификация —
в classify.py, одним батч-вызовом.
"""

import logging
import time
from typing import List
from urllib.parse import urlparse

log = logging.getLogger("context.news")


def domain_of(url: str) -> str:
    try:
        return (urlparse(url).hostname or "").lower().replace("www.", "")
    except ValueError:
        return ""


def source_tier(domain: str) -> int:
    tier1 = ("sec.gov", "binance.com", "bybit.com", "upbit.com", "okx.com")
    tier2 = ("reuters.com", "bloomberg.com", "apnews.com", "wsj.com", "ft.com")
    if any(domain.endswith(d) for d in tier1):
        return 1
    if any(domain.endswith(d) for d in tier2):
        return 2
    return 3


def cheap_filters(items: List[dict], cfg: dict, now_ts: float = None) -> List[dict]:
    """Свежесть и whitelist домена — до всякой модели: дешёвое отсекает основную массу."""
    now_ts = now_ts if now_ts is not None else time.time()
    window = float(cfg["matching"]["news_freshness_sec"])
    whitelist = [d.lower() for d in cfg["matching"]["source_whitelist"]]
    out = []
    for item in items:
        ts = float(item.get("ts") or now_ts)
        if now_ts - ts > window:
            continue
        domain = item.get("domain") or domain_of(item.get("url", ""))
        if whitelist and not any(domain.endswith(d) for d in whitelist):
            continue
        out.append({**item, "domain": domain, "source_tier": source_tier(domain)})
    return out


async def tavily(session, cfg: dict, ticker: str, project_name: str = "") -> List[dict]:
    key = cfg["keys"].get("tavily")
    if not key:
        return []
    import aiohttp
    query = f"{project_name} {ticker} crypto news".strip()
    payload = {"api_key": key, "query": query, "search_depth": "basic",
               "max_results": 8, "days": 1, "topic": "news"}
    timeout = aiohttp.ClientTimeout(total=cfg["budget"]["per_source_ms"] / 1000.0)
    async with session.post(cfg["sources"]["tavily_search"], json=payload, timeout=timeout) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        data = await resp.json(content_type=None)
    out = []
    for item in (data.get("results") or []):
        out.append({"source": "TAVILY", "title": item.get("title") or "",
                    "url": item.get("url") or "", "ts": time.time(),
                    "snippet": (item.get("content") or "")[:300]})
    return out


async def cryptopanic(session, cfg: dict, ticker: str) -> List[dict]:
    token = cfg["keys"].get("cryptopanic")
    if not token:
        return []
    import aiohttp
    url = cfg["sources"]["cryptopanic_posts"].format(token=token, ticker=ticker)
    timeout = aiohttp.ClientTimeout(total=cfg["budget"]["per_source_ms"] / 1000.0)
    async with session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        data = await resp.json(content_type=None)
    out = []
    for item in (data.get("results") or []):
        published = item.get("published_at") or ""
        ts = time.time()
        if published:
            try:
                from datetime import datetime
                ts = datetime.fromisoformat(published.replace("Z", "+00:00")).timestamp()
            except ValueError:
                pass
        votes = item.get("votes") or {}
        out.append({"source": "CRYPTOPANIC", "title": item.get("title") or "",
                    "url": (item.get("source") or {}).get("domain") or item.get("url") or "",
                    "ts": ts, "votes_positive": votes.get("positive", 0),
                    "votes_negative": votes.get("negative", 0)})
    return out

"""Объявления бирж (контур A). Тип события берётся как отдал источник.

Bybit и Upbit — официальные эндпоинты. Binance CMS неофициальный: при смене схемы
логируется WARNING и включается fallback на страницу анонсов, но падения быть не должно.
OKX выключен в конфиге — корп-сеть сбрасывает соединение с okx.com по SNI.
"""

import logging
import re
import time
from typing import List

from context import listing_rules as rules

log = logging.getLogger("context.announcements")

# Тип берём из данных источника, не переклассифицируем.
BYBIT_TYPE_MAP = {
    "new_crypto": "LISTING",
    "latest_activities": "OTHER",
    "delistings": "DELISTING",
    "maintenance_updates": "OPERATIONAL",
    "product_updates": "OTHER",
    "new_fiat_listings": "LISTING",
    "other": "OTHER",
}

UPBIT_LISTING_MARKERS = ("거래지원", "상장", "마켓 추가", "신규")
UPBIT_DELIST_MARKERS = ("거래지원 종료", "상장폐지")
BINANCE_PERP_RE = re.compile(r"perpetual", re.I)
BINANCE_DELIST_RE = re.compile(r"\bdelist|will remove", re.I)


async def _json(session, url: str, timeout_ms: int, headers=None):
    import aiohttp
    timeout = aiohttp.ClientTimeout(total=timeout_ms / 1000.0)
    async with session.get(url, timeout=timeout, headers=headers or {}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        return await resp.json(content_type=None)


async def bybit(session, cfg: dict) -> List[dict]:
    data = await _json(session, cfg["sources"]["bybit_announcements"],
                       cfg["budget"]["per_source_ms"])
    out = []
    for item in ((data.get("result") or {}).get("list") or []):
        raw_type = ((item.get("type") or {}).get("key") or "other").lower()
        ts_ms = item.get("dateTimestamp") or item.get("startDateTimestamp") or 0
        out.append({
            "source": "BYBIT",
            "raw_type": raw_type,
            "event_type": BYBIT_TYPE_MAP.get(raw_type, "OTHER"),
            "title": item.get("title") or "",
            "url": item.get("url") or "",
            "ts": float(ts_ms) / 1000.0 if ts_ms else time.time(),
            "tags": item.get("tags") or [],
        })
    return out


async def binance(session, cfg: dict) -> List[dict]:
    """CMS-эндпоинт неофициальный: схема может измениться в любой момент.

    Источник формы: используется страницей www.binance.com/en/support/announcement.
    При неожиданной структуре — WARNING и пустой список, а не исключение наружу.
    """
    out = []
    for catalog in cfg["sources"]["binance_cms_catalogs"]:
        url = cfg["sources"]["binance_cms"].format(catalog=catalog)
        try:
            data = await _json(session, url, cfg["budget"]["per_source_ms"],
                               headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
        except Exception as exc:
            log.warning("binance CMS каталог %s недоступен: %s", catalog, exc)
            continue
        articles = ((data.get("data") or {}).get("articles")
                    or (data.get("data") or {}).get("catalogs") or [])
        if not isinstance(articles, list):
            log.warning("binance CMS: неожиданная структура ответа (каталог %s) — "
                        "нужен пересмотр эндпоинта", catalog)
            continue
        for item in articles:
            if not isinstance(item, dict):
                continue
            title = item.get("title") or ""
            release = item.get("releaseDate") or item.get("publishDate") or 0
            event_type = "LISTING"
            if BINANCE_DELIST_RE.search(title):
                event_type = "DELISTING"
            elif BINANCE_PERP_RE.search(title):
                event_type = "PERP_LAUNCH"
            out.append({
                "source": "BINANCE",
                "raw_type": f"catalog:{catalog}",
                "event_type": event_type,
                "title": title,
                "url": f"https://www.binance.com/en/support/announcement/{item.get('code', '')}",
                "ts": float(release) / 1000.0 if release else time.time(),
                "tags": [],
            })
    return out


async def upbit(session, cfg: dict) -> List[dict]:
    """Корейские листинги дают сильнейшие пампы. Без category=all эндпоинт даёт HTTP 400."""
    data = await _json(session, cfg["sources"]["upbit_announcements"],
                       cfg["budget"]["per_source_ms"],
                       headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    notices = ((data.get("data") or {}).get("notices")
               or (data.get("data") or {}).get("list") or [])
    out = []
    for item in notices:
        title = item.get("title") or ""
        event_type = "OTHER"
        if any(marker in title for marker in UPBIT_DELIST_MARKERS):
            event_type = "DELISTING"
        elif any(marker in title for marker in UPBIT_LISTING_MARKERS):
            event_type = "LISTING"
        listed_at = item.get("listed_at") or item.get("created_at") or ""
        ts = time.time()
        if isinstance(listed_at, str) and listed_at[:4].isdigit():
            try:
                from datetime import datetime, timezone
                ts = datetime.fromisoformat(listed_at.replace("Z", "+00:00")).replace(
                    tzinfo=timezone.utc).timestamp()
            except ValueError:
                pass
        out.append({
            "source": "UPBIT",
            "raw_type": str(item.get("category") or "notice"),
            "event_type": event_type,
            "title": title,
            "url": f"https://upbit.com/service_center/notice?id={item.get('id')}",
            "ts": ts,
            "tags": [],
        })
    return out


async def okx(session, cfg: dict) -> List[dict]:
    data = await _json(session, cfg["sources"]["okx_announcements"],
                       cfg["budget"]["per_source_ms"],
                       headers={"User-Agent": "Mozilla/5.0"})
    out = []
    for group in (data.get("data") or []):
        for item in (group.get("details") or []):
            ts_ms = item.get("pTime") or 0
            out.append({
                "source": "OKX",
                "raw_type": str(item.get("annType") or "other"),
                "event_type": "LISTING" if "listing" in str(item.get("annType", "")).lower() else "OTHER",
                "title": item.get("title") or "",
                "url": item.get("url") or "",
                "ts": float(ts_ms) / 1000.0 if ts_ms else time.time(),
                "tags": [],
            })
    return out


async def kucoin(session, cfg: dict) -> List[dict]:
    """KuCoin: единственный источник, который раньше был только в старом пайплайне."""
    data = await _json(session, cfg["sources"]["kucoin_announcements"],
                       cfg["budget"]["per_source_ms"],
                       headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"})
    items = ((data.get("items") or (data.get("data") or {}).get("items"))
             if isinstance(data, dict) else None) or []
    out = []
    for item in items:
        if not isinstance(item, dict):
            continue
        title = item.get("title") or ""
        event_type = "DELISTING" if BINANCE_DELIST_RE.search(title) else "LISTING"
        published = item.get("publish_ts") or item.get("createdAt") or 0
        try:
            ts = float(published) / 1000.0 if float(published) > 1e11 else float(published)
        except (TypeError, ValueError):
            ts = time.time()
        out.append({"source": "KUCOIN", "raw_type": "cms", "event_type": event_type,
                    "title": title, "url": item.get("path") or item.get("url") or "",
                    "ts": ts or time.time(), "tags": []})
    return out


def screen(item: dict, cfg: dict) -> dict:
    """Единый входной фильтр для любого источника анонсов.

    Инструменты на акции и объявления без тикера дальше кэша не идут: именно их
    отсутствие в одном из пайплайнов и дало инцидент со спамом.
    """
    listing_cfg = rules.load_config()
    title = item.get("title") or ""
    kind = rules.classify_instrument_kind(title, listing_cfg, item.get("raw_type"))
    if kind == "tradfi":
        return {"keep": False, "stage": "filtered_tradfi", "tickers": [],
                "reason": "инструмент на акции — не крипто-событие"}
    tickers = rules.extract_tickers(title, listing_cfg)
    if not tickers:
        return {"keep": False, "stage": "parse_failed", "tickers": [],
                "reason": "тикер не извлечён"}
    return {"keep": True, "stage": "ок", "tickers": tickers,
            "market": rules.market_kind(title), "reason": ""}


FETCHERS = {
    "bybit_announcements": bybit,
    "binance_announcements": binance,
    "upbit_announcements": upbit,
    "okx_announcements": okx,
    "kucoin_announcements": kucoin,
}

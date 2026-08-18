"""Единственная точка отправки событий в канал.

После инцидента (~20 мусорных сообщений за час) отправка сведена в один модуль с
четырьмя ограничителями, каждый из которых работает независимо:

1. **Гейт классов** — в канал идут только классы из конфига (STRONG_BULLISH,
   BEARISH_SETUP, OPERATIONAL); всё прочее пишется в лог.
2. **Guard по тикеру** — событие без распознанного тикера не отправляется никогда:
   без него нельзя проверить, где монета уже торгуется.
3. **Лимит частоты** — не больше N сообщений в час и M в сутки. Даже если правила
   ошибутся, канал не превратится в поток: лишнее уходит в лог с пометкой.
4. **Выключатель и режим тени** — `enabled: false` глушит отправку целиком;
   источник в `shadow_sources` собирается и решается, но не постит. Любой новый
   источник обязан пройти сутки в тени до включения.

Публикатор помечает события в кэше (`published_ts`), поэтому повторной отправки
одного события не бывает даже при рестарте процесса.
"""

import logging
import time
from typing import List, Optional

from context import listing_rules as rules

log = logging.getLogger("context.publisher")

REASON_DISABLED = "публикатор выключен"
REASON_SHADOW = "источник в режиме тени"
REASON_TICKER = "нет распознанного тикера"
REASON_CLASS = "класс не входит в разрешённые"
REASON_RATE = "лимит частоты"
REASON_TYPE = "тип события не публикуется"
REASON_DUPLICATE = "такое сообщение уже отправлялось"


class Publisher:
    def __init__(self, cfg: dict, cache, sender=None):
        self.cfg = cfg
        self.pub = cfg["publisher"]
        self.rules_cfg = rules.load_config()
        self.cache = cache
        self.sender = sender          # async callable(text) -> bool

    # -- ограничители ------------------------------------------------------- #

    def allowed(self, event: dict, verdict: dict, now_ts: float) -> Optional[str]:
        """None — можно отправлять; строка — причина, по которой нельзя."""
        if not self.pub.get("enabled"):
            return REASON_DISABLED
        source = (event.get("source") or "").split(":")[0].upper()
        if source in {s.upper() for s in self.pub.get("shadow_sources") or []}:
            return REASON_SHADOW
        if (event.get("event_type") or "").upper() not in {
                t.upper() for t in self.pub["post_event_types"]}:
            return REASON_TYPE
        if not event.get("ticker"):
            return REASON_TICKER
        if not verdict.get("post"):
            return REASON_CLASS
        hourly, daily = self.counters(now_ts)
        if hourly >= int(self.pub["max_per_hour"]):
            return f"{REASON_RATE}: {hourly} за час"
        if daily >= int(self.pub["max_per_day"]):
            return f"{REASON_RATE}: {daily} за сутки"
        return None

    def counters(self, now_ts: float) -> tuple:
        # Считаем ТОЛЬКО реально отправленное. Решения «в лог» тоже помечают событие
        # обработанным, и если считать их, лимит начинает глушить настоящие события —
        # тихий отказ вместо защиты.
        row = self.cache.conn.execute(
            "SELECT"
            " SUM(CASE WHEN published_ts >= ? THEN 1 ELSE 0 END) AS hourly,"
            " SUM(CASE WHEN published_ts >= ? THEN 1 ELSE 0 END) AS daily"
            " FROM events WHERE published_ts IS NOT NULL"
            "   AND publish_decision LIKE 'канал:%'",
            (now_ts - 3600, now_ts - 86400)).fetchone()
        return int(row["hourly"] or 0), int(row["daily"] or 0)

    # -- основной проход ---------------------------------------------------- #

    def pending(self, limit: int = 50) -> List[dict]:
        rows = self.cache.conn.execute(
            "SELECT * FROM events WHERE published_ts IS NULL"
            " ORDER BY ts DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    def mark(self, event_id: int, ts: float, decision: str) -> None:
        self.cache.conn.execute(
            "UPDATE events SET published_ts = ?, publish_decision = ? WHERE id = ?",
            (ts, decision, event_id))
        self.cache.conn.commit()

    def evaluate(self, event: dict, now_ts: float) -> dict:
        """Решение по событию: класс, можно ли отправлять, текст сообщения."""
        ticker = event.get("ticker")
        title = event.get("title") or ""
        places = rules.listed_places(ticker, self.rules_cfg, self.cache.conn) if ticker else None
        market = rules.market_kind(title)
        verdict = rules.decide(event_type=event.get("event_type"),
                               exchange=(event.get("source") or ""),
                               ticker=ticker, market=market, places=places,
                               cfg=self.rules_cfg,
                               product_only=rules.is_product_only(title))
        blocked = self.allowed(event, verdict, now_ts)
        payload = {"etype": event.get("event_type"), "ticker": ticker,
                   "tickers": [ticker] if ticker else [], "source": event.get("source"),
                   "title": title, "url": event.get("url"), "market": market}
        return {"verdict": verdict, "blocked": blocked, "market": market, "places": places,
                "text": rules.render(payload, verdict, places) if ticker else None}

    async def run_once(self, now_ts: float = None) -> dict:
        now_ts = now_ts if now_ts is not None else time.time()
        stats = {"проверено": 0, "отправлено": 0, "в лог": 0}
        for event in self.pending():
            stats["проверено"] += 1
            outcome = self.evaluate(event, now_ts)
            verdict, blocked = outcome["verdict"], outcome["blocked"]
            if blocked:
                log.info("не отправлено [%s] %s %s — %s (%s)", event.get("source"),
                         event.get("event_type"), event.get("ticker") or "?",
                         blocked, verdict.get("reason"))
                self.mark(event["id"], now_ts, f"лог: {blocked}")
                stats["в лог"] += 1
                continue
            # идемпотентность по тексту: второй отправитель, второй процесс или
            # рестарт посреди отправки не должны дать второе сообщение
            if not self.cache.remember_sent(outcome["text"], symbol=event.get("ticker"),
                                            sender="publisher", now_ts=now_ts):
                log.info("не отправлено [%s] %s %s — %s", event.get("source"),
                         event.get("event_type"), event.get("ticker"), REASON_DUPLICATE)
                self.mark(event["id"], now_ts, f"лог: {REASON_DUPLICATE}")
                stats["в лог"] += 1
                continue
            sent = True
            if self.sender is not None:
                sent = await self.sender(outcome["text"])
            if sent:
                log.info("отправлено %s %s [%s] — %s", event.get("event_type"),
                         event.get("ticker"), verdict["class"], verdict["reason"])
                self.mark(event["id"], now_ts, f"канал: {verdict['class']}")
                stats["отправлено"] += 1
            else:
                log.warning("отправка не удалась, событие останется в очереди: %s",
                            event.get("ticker"))
        return stats

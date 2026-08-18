"""Единственная точка отправки. Работает по БЕЛОМУ списку.

Два инцидента подряд имели один корень: мусор улетал в канал, после чего дописывался
фильтр под конкретный симптом. Чёрные списки проигрывают всегда — класс мусора,
которого в них нет, найдётся. Здесь наоборот: отправить можно **только** то, что
перечислено в `WHITELIST`, и ветки кода к отправке чего-либо ещё не существует.

Четыре типа сообщений:

* `PUMP_ALERT` — алерт детектора с блоком контекста (шлёт сам бот, Тип 1 ТЗ);
* `NEW_LISTING` — монета, которой не было ни на одной крупной площадке, листится
  на Binance/Upbit/Coinbase;
* `UNIVERSE_EVENT` — событие по монете из нашего торгового universe (перп на
  Binance или Bybit): делистинг оттуда, инцидент безопасности, первый перп для
  спот-монеты;
* `MAJOR_NEWS` — отдельное сообщение по важной новости. Самый спамоопасный тип,
  поэтому закрытый список категорий, жёсткие условия и режим тени по умолчанию.

ПРАВИЛО РЕЖИМА ТЕНИ: любой новый тип сообщения и любой новый источник добавляются
с `shadow_mode = true`. Снимается только вручную и только после того, как владелец
посмотрел ретро-таблицу. Автоматически «дозреть» до боевого режима ничто не может.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from context import listing_rules as rules

log = logging.getLogger("context.publisher")

PUMP_ALERT = "PUMP_ALERT"
NEW_LISTING = "NEW_LISTING"
UNIVERSE_EVENT = "UNIVERSE_EVENT"
MAJOR_NEWS = "MAJOR_NEWS"
WHITELIST = (PUMP_ALERT, NEW_LISTING, UNIVERSE_EVENT, MAJOR_NEWS)

# Метки причин — по ним читаются ретро-таблица и лог.
LABEL_KILL = "kill_switch"
LABEL_NOT_WHITELISTED = "not_whitelisted"
LABEL_PARSE_FAILED = "parse_failed"
LABEL_TRADFI = "filtered_tradfi"
LABEL_OUT_OF_UNIVERSE = "out_of_universe"
LABEL_DUP = "dup"
LABEL_RATE = "rate_limited"
LABEL_SHADOW = "would_send"
LABEL_SENT = "sent"
LABEL_SEND_FAILED = "send_failed"
LABEL_WEAK = "weak_not_whitelisted"
LABEL_PRODUCT = "product_only"
LABEL_NEWS_RULES = "major_news_rules"

# Закрытый список категорий Типа 4. Партнёрств, интеграций, апдейтов и маркетинга
# здесь нет сознательно: их много, влияние на цену слабое, и именно они превращают
# канал в новостную ленту. Такие новости живут только в блоке контекста алертов.
MAJOR_NEWS_CATEGORIES = {
    "LEGAL": "Судебное дело",
    "LEADERSHIP_CRIMINAL": "Уголовное дело или арест",
    "REGULATORY": "Регуляторное решение",
    "POLITICAL_MENTION": "Упоминание политиком",
    "SECURITY": "Инцидент безопасности",
    "PROJECT_CRITICAL": "Критическое событие проекта",
    "SCHEDULED": "Запланированное событие",
}

ISO_DATE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")
SHORT_DATE = re.compile(r"\b(\d{1,2})/(\d{1,2})\s+(\d{1,2}:\d{2})\b")
MONTHS_RU = ("янв", "фев", "мар", "апр", "мая", "июн", "июл", "авг", "сен", "окт", "ноя", "дек")


@dataclass
class Message:
    """Готовое к отправке сообщение из белого списка."""
    type: str
    ticker: Optional[str]
    source: str
    external_id: str
    text: str
    is_tradfi: bool = False
    requires_universe: bool = True
    payload: dict = field(default_factory=dict)


# --------------------------------------------------------------------------- #
# оформление: сырой заголовок источника в канал не идёт
# --------------------------------------------------------------------------- #

def effective_date(title: str) -> Optional[str]:
    """Дата вступления в силу из заголовка. Не нашли — None, выдумывать нельзя."""
    iso = ISO_DATE.search(title or "")
    if iso:
        try:
            date = datetime.strptime(iso.group(1), "%Y-%m-%d")
            return f"{date.day} {MONTHS_RU[date.month - 1]}"
        except ValueError:
            return None
    short = SHORT_DATE.search(title or "")
    if short:
        month, day, clock = short.groups()
        try:
            return f"{int(day)} {MONTHS_RU[int(month) - 1]}, {clock}"
        except (ValueError, IndexError):
            return None
    return None


def where_traded(places: Optional[List[dict]]) -> str:
    if not places:
        return "—"
    return ", ".join(sorted({f"{p['exchange']} {p['market']}" for p in places}))


def render_listing(ticker, exchange, market, url, effective) -> str:
    market_text = {"spot": "спот", "perp": "перп"}.get(market, "тип не определён")
    lines = [f"🟢 <b>НОВЫЙ ЛИСТИНГ: {ticker}</b>", f"Биржа: {exchange} ({market_text})"]
    if effective:
        lines.append(f"Старт торгов: {effective}")
    lines.append("Ранее на крупных площадках не торговалась")
    lines.append("Типовой паттерн: рост до листинга, дамп на открытии торгов. Анонсы "
                 "утекают инсайдерам — часть движения могла случиться до анонса.")
    if url:
        lines.append(f"🔗 {url}")
    return "\n".join(lines)


def render_universe_event(kind, ticker, exchange, market, places, url, effective, note="") -> str:
    icons = {"DELISTING": "🔴", "SECURITY": "⚠️", "BEARISH_SETUP": "🩳"}
    titles = {"DELISTING": "ДЕЛИСТИНГ", "SECURITY": "ИНЦИДЕНТ", "BEARISH_SETUP": "ПЕРВЫЙ ПЕРП"}
    market_text = {"spot": "спот", "perp": "перп"}.get(market, "тип не определён")
    lines = [f"{icons.get(kind, 'ℹ️')} <b>{titles.get(kind, kind)}: {ticker}</b>",
             f"Биржа: {exchange} ({market_text})"]
    if effective:
        lines.append(f"Вступает в силу: {effective}")
    lines.append(f"У нас торгуется: {where_traded(places)}")
    if note:
        lines.append(note)
    if url:
        lines.append(f"🔗 {url}")
    return "\n".join(lines)


def render_major_news(news: dict, places: Optional[List[dict]], now_ts: float) -> str:
    """Только факт, источник и время: без оценок «бычье/медвежье» и рекомендаций."""
    category = MAJOR_NEWS_CATEGORIES.get(str(news.get("category") or "").upper(), "Событие")
    ts = float(news.get("ts") or now_ts)
    minutes = max(0.0, (now_ts - ts) / 60.0)
    tier = int(news.get("source_tier") or 3)
    confirmations = int(news.get("confirmations") or 1)
    source_line = f"Источник: {news.get('source_name') or '?'} (tier {tier})"
    if confirmations > 1:
        source_line += f" + подтверждений: {confirmations - 1}"
    lines = [f"📰 <b>НОВОСТЬ: {news.get('ticker')}</b>",
             f"Категория: {category}",
             f"Суть: {(news.get('summary') or '')[:200]}",
             source_line,
             f"Время: {datetime.fromtimestamp(ts, tz=timezone.utc).strftime('%H:%M')} UTC "
             f"({minutes:.0f} мин назад)",
             f"Торгуется у нас: {where_traded(places)}"]
    if news.get("url"):
        lines.append(f"🔗 {news['url']}")
    return "\n".join(lines)


# --------------------------------------------------------------------------- #
# правила Типа 4
# --------------------------------------------------------------------------- #

def major_news_allowed(news: dict, cfg: dict, sent_today: int) -> Tuple[bool, str]:
    """Все условия Типа 4 разом: (можно, причина отказа)."""
    news_cfg = cfg["publisher"]["major_news"]
    category = str(news.get("category") or "").upper()
    if category not in MAJOR_NEWS_CATEGORIES:
        return False, f"категория {category or '—'} вне закрытого списка"
    if not news.get("is_fact"):
        return False, "слух или план, а не свершившийся факт"
    try:
        tier = int(news.get("source_tier") or 3)
        confidence = float(news.get("confidence") or 0.0)
        confirmations = int(news.get("confirmations") or 1)
    except (TypeError, ValueError):
        return False, "некорректные поля классификации"
    if tier > int(news_cfg["max_source_tier"]):
        return False, f"источник tier {tier} — недостаточно"
    if confidence < float(news_cfg["min_confidence"]):
        return False, f"уверенность {confidence:.2f} ниже порога"
    if confirmations < int(news_cfg["min_confirmations"]) and tier != 1:
        return False, "нет второго независимого подтверждения"
    if sent_today >= int(news_cfg["max_per_day"]):
        if confidence < float(news_cfg["over_limit_min_confidence"]) or tier != 1:
            return False, (f"суточный лимит {news_cfg['max_per_day']} исчерпан: нужны tier 1 "
                           f"и уверенность ≥ {news_cfg['over_limit_min_confidence']}")
    return True, ""


# --------------------------------------------------------------------------- #
# публикатор
# --------------------------------------------------------------------------- #

class Publisher:
    def __init__(self, cfg: dict, cache, sender=None):
        self.cfg = cfg
        self.pub = cfg["publisher"]
        self.rules_cfg = rules.load_config()
        self.cache = cache
        self.sender = sender

    def shadow(self, message_type: str) -> bool:
        return message_type.upper() in {t.upper() for t in self.pub.get("shadow_types") or []}

    def counters(self, now_ts: float) -> Tuple[int, int]:
        return (self.cache.published_count(now_ts - 3600),
                self.cache.published_count(now_ts - 86400))

    def in_universe(self, ticker: str) -> Tuple[bool, Optional[List[dict]]]:
        """Есть ли монета там, где мы торгуем. Нет снепшота — считаем неподтверждённой."""
        places = rules.listed_places(ticker, self.rules_cfg, self.cache.conn) if ticker else None
        if not places:
            return False, places
        tradable = {e.upper() for e in self.rules_cfg.get("tradable_exchanges") or []}
        return any(p["exchange"].upper() in tradable for p in places), places

    # -- guard-цепочка: единственный путь к отправке ------------------------ #

    async def publish(self, message: Message, now_ts: float = None) -> Tuple[bool, str]:
        now_ts = now_ts if now_ts is not None else time.time()

        if not self.pub.get("enabled"):
            return self._refuse(message, LABEL_KILL, now_ts)
        if message.type not in WHITELIST:
            return self._refuse(message, LABEL_NOT_WHITELISTED, now_ts)
        if not message.ticker:
            return self._refuse(message, LABEL_PARSE_FAILED, now_ts)
        if message.is_tradfi:
            return self._refuse(message, LABEL_TRADFI, now_ts)
        if message.requires_universe and not self.in_universe(message.ticker)[0]:
            return self._refuse(message, LABEL_OUT_OF_UNIVERSE, now_ts)
        if not self.cache.reserve_published(message.source, message.external_id,
                                            message.type, message.ticker, now_ts):
            return self._refuse(message, LABEL_DUP, now_ts)

        hourly, daily = self.counters(now_ts)
        if hourly >= int(self.pub["max_per_hour"]) or daily >= int(self.pub["max_per_day"]):
            self.cache.release_published(message.source, message.external_id)
            return self._refuse(message, f"{LABEL_RATE}: {hourly}/час, {daily}/сутки", now_ts)

        if self.shadow(message.type):
            self.cache.release_published(message.source, message.external_id)
            log.info("WOULD SEND [%s] %s: %s", message.type, message.ticker,
                     message.text.replace("\n", " | ")[:160])
            self._journal(message, LABEL_SHADOW, now_ts)
            return False, LABEL_SHADOW

        sent = True
        if self.sender is not None:
            sent = await self.sender(message.text)
        if not sent:
            self.cache.release_published(message.source, message.external_id)
            return self._refuse(message, LABEL_SEND_FAILED, now_ts)

        self.cache.mark_published_sent(message.source, message.external_id, now_ts)
        self._journal(message, LABEL_SENT, now_ts)
        log.info("отправлено [%s] %s", message.type, message.ticker)
        return True, LABEL_SENT

    def _refuse(self, message: Message, label: str, now_ts: float) -> Tuple[bool, str]:
        log.info("не отправлено [%s] %s — %s", message.type, message.ticker or "?", label)
        self._journal(message, label, now_ts)
        return False, label

    def _journal(self, message: Message, label: str, now_ts: float) -> None:
        try:
            self.cache.log_publish_decision(
                ts=now_ts, message_type=message.type, ticker=message.ticker,
                source=message.source, external_id=message.external_id,
                label=label, text=message.text)
        except Exception:
            pass    # журнал решений не имеет права мешать работе

    # -- сборка сообщений ---------------------------------------------------- #

    def message_for(self, event: dict, now_ts: float) -> Tuple[Optional[Message], str]:
        """Событие кэша → сообщение белого списка либо (None, метка причины)."""
        title = event.get("title") or ""
        ticker = event.get("ticker")
        source = (event.get("source") or "").split(":")[0].upper()
        external_id = event.get("url") or f"{source}:{title[:80]}"
        event_type = (event.get("event_type") or "").upper()

        if rules.classify_instrument_kind(title, self.rules_cfg, event.get("raw_type")) == "tradfi":
            return None, LABEL_TRADFI
        if not ticker:
            return None, LABEL_PARSE_FAILED

        market = rules.market_kind(title)
        confirmed, places = self.in_universe(ticker)
        majors = {m.upper() for m in self.rules_cfg["major_exchanges"]}
        tradable = {m.upper() for m in self.rules_cfg.get("tradable_exchanges") or []}
        on_majors = {p["exchange"].upper() for p in (places or [])} & majors
        effective = effective_date(title)

        if event_type in ("LISTING", "NEW_INSTRUMENT", "PERP_LAUNCH"):
            if source not in majors:
                return None, LABEL_WEAK
            if not on_majors:
                return Message(type=NEW_LISTING, ticker=ticker, source=source,
                               external_id=external_id, requires_universe=False,
                               text=render_listing(ticker, source, market,
                                                   event.get("url") or "", effective)), ""
            if market == "perp" and {p["market"] for p in (places or [])} == {"spot"}:
                return Message(type=UNIVERSE_EVENT, ticker=ticker, source=source,
                               external_id=external_id,
                               text=render_universe_event(
                                   "BEARISH_SETUP", ticker, source, market, places,
                                   event.get("url") or "", effective,
                                   "Монета была только на споте — появилась возможность шортить")), ""
            return None, LABEL_WEAK

        if event_type in ("DELISTING", "DELISTED"):
            if rules.is_product_only(title):
                return None, LABEL_PRODUCT
            if source not in tradable or not confirmed:
                return None, LABEL_OUT_OF_UNIVERSE
            return Message(type=UNIVERSE_EVENT, ticker=ticker, source=source,
                           external_id=external_id,
                           text=render_universe_event("DELISTING", ticker, source, market, places,
                                                      event.get("url") or "", effective)), ""

        if event_type in ("HACK", "SECURITY"):
            if not confirmed:
                return None, LABEL_OUT_OF_UNIVERSE
            return Message(type=UNIVERSE_EVENT, ticker=ticker, source=source,
                           external_id=external_id,
                           text=render_universe_event("SECURITY", ticker, source, market, places,
                                                      event.get("url") or "", effective)), ""

        return None, LABEL_NOT_WHITELISTED

    def message_for_news(self, news: dict, now_ts: float) -> Tuple[Optional[Message], str]:
        """Новость → сообщение Типа 4, если проходит все условия закрытого списка."""
        ticker = news.get("ticker")
        if not ticker:
            return None, LABEL_PARSE_FAILED
        confirmed, places = self.in_universe(ticker)
        if not confirmed:
            return None, LABEL_OUT_OF_UNIVERSE
        allowed, reason = major_news_allowed(
            news, self.cfg, self.cache.published_count(now_ts - 86400, message_type=MAJOR_NEWS))
        if not allowed:
            return None, f"{LABEL_NEWS_RULES}: {reason}"
        return Message(type=MAJOR_NEWS, ticker=ticker,
                       source=str(news.get("source_name") or "news").upper(),
                       external_id=str(news.get("event_key") or news.get("url") or ""),
                       text=render_major_news(news, places, now_ts), payload=news), ""

    # -- проход по очереди --------------------------------------------------- #

    def pending(self, limit: int = 50) -> List[dict]:
        rows = self.cache.conn.execute(
            "SELECT * FROM events WHERE published_ts IS NULL ORDER BY ts DESC LIMIT ?",
            (limit,)).fetchall()
        return [dict(r) for r in rows]

    def mark_event(self, event_id: int, ts: float, label: str) -> None:
        self.cache.conn.execute(
            "UPDATE events SET published_ts = ?, publish_decision = ? WHERE id = ?",
            (ts, label, event_id))
        self.cache.conn.commit()

    async def run_once(self, now_ts: float = None) -> dict:
        now_ts = now_ts if now_ts is not None else time.time()
        stats = {"проверено": 0, "отправлено": 0, "в лог": 0, "в тени": 0}
        for event in self.pending():
            stats["проверено"] += 1
            message, label = self.message_for(event, now_ts)
            if message is None:
                self.mark_event(event["id"], now_ts, f"лог: {label}")
                stats["в лог"] += 1
                log.info("не отправлено [%s] %s %s — %s", event.get("source"),
                         event.get("event_type"), event.get("ticker") or "?", label)
                continue
            sent, label = await self.publish(message, now_ts)
            self.mark_event(event["id"], now_ts,
                            ("канал: " if sent else "лог: ") + f"{message.type}/{label}")
            if sent:
                stats["отправлено"] += 1
            elif label == LABEL_SHADOW:
                stats["в тени"] += 1
            else:
                stats["в лог"] += 1
        return stats

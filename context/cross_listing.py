"""CROSS_LISTING: листинг как самостоятельный сигнал, а не пометка.

Классификация идёт по логике «новая аудитория», а не «новая биржа»: монета, которой
не было на крупных площадках, получает доступ к новым деньгам; монета, уже
торгуемая на Binance, от листинга на второстепенной бирже не меняется.

Отдельный случай — анонс ПЕРПА для монеты, у которой был только спот: появляется
физическая возможность шортить, и это медвежий сетап, а не бычья новость.

Никаких «покупай»: величина листинг-эффекта после 2024 сжалась и различается по
биржам, поэтому до бэктеста сигнал информационный.
"""

from typing import List, Optional

STRONG_BULLISH = "STRONG_BULLISH"
WEAK = "WEAK"
BEARISH_SETUP = "BEARISH_SETUP"
OPERATIONAL = "OPERATIONAL"


def classify(event: dict, listed_on: List[dict], cfg: dict) -> dict:
    """event: {'event_type', 'source', 'ticker', ...}; listed_on: строки из instruments."""
    majors = {m.upper() for m in cfg["cross_listing"]["major_exchanges"]}
    event_type = (event.get("event_type") or "").upper()
    exchange = (event.get("source") or "").split(":")[0].upper()

    already = {(row["exchange"].upper(), row["market"]) for row in listed_on}
    on_majors = {ex for ex, _ in already if ex in majors}
    has_spot_only = bool({m for _, m in already} == {"spot"})

    if event_type in ("DELISTING", "DELISTED"):
        return {"class": OPERATIONAL, "post": True,
                "reason": f"делистинг на {exchange or 'бирже'} — операционно важно"}

    if event_type in ("PERP_LAUNCH", "NEW_INSTRUMENT") and event.get("market") == "perp" and has_spot_only:
        return {"class": BEARISH_SETUP, "post": True,
                "reason": "появился перп у монеты, которая была только на споте — "
                          "появилась возможность шортить"}

    if event_type in ("LISTING", "NEW_INSTRUMENT"):
        if exchange in majors and not on_majors:
            return {"class": STRONG_BULLISH, "post": True,
                    "reason": f"монеты не было на крупных площадках, листинг на {exchange} — "
                              f"новая аудитория"}
        if on_majors:
            return {"class": WEAK, "post": bool(cfg["cross_listing"]["post_weak_to_channel"]),
                    "reason": f"уже торгуется на {', '.join(sorted(on_majors))} — "
                              f"новой аудитории листинг не добавляет"}
        return {"class": WEAK, "post": bool(cfg["cross_listing"]["post_weak_to_channel"]),
                "reason": f"листинг на {exchange or 'второстепенной бирже'}"}

    return {"class": WEAK, "post": False, "reason": "тип события не листинговый"}


def render_listing_play(event: dict, verdict: dict, listed_on: List[dict],
                        price_change_pct: Optional[float] = None,
                        minutes_ago: Optional[float] = None,
                        trading_starts: str = None) -> str:
    """Сообщение LISTING PLAY. Без рекомендаций — паттерн и риск, а не сигнал на вход."""
    ticker = event.get("ticker") or "?"
    exchange = (event.get("source") or "").split(":")[0].upper()
    market = event.get("market") or "спот"
    lines = [f"🟢 <b>LISTING: {ticker} → {exchange} ({market})</b>"]

    when = []
    if minutes_ago is not None:
        when.append(f"анонс {minutes_ago:.0f} мин назад")
    if trading_starts:
        when.append(f"торги: {trading_starts}")
    if when:
        lines.append("🕐 " + " | ".join(when))

    if listed_on:
        places = ", ".join(sorted({f"{r['exchange']} {r['market']}" for r in listed_on}))
        lines.append(f"📍 Уже торгуется: {places}")
    else:
        lines.append("📍 На отслеживаемых площадках раньше не торговалась")

    if price_change_pct is not None:
        lines.append(f"📈 Реакция цены с анонса: {price_change_pct:+.1f}%")

    lines.append(f"🧭 Класс: {verdict['class']} — {verdict['reason']}")
    lines.append("⚠️ Типовой паттерн: рост до листинга, дамп на открытии торгов. "
                 "Анонсы систематически утекают инсайдерам — часть движения до анонса "
                 "означает, что ранние уже в позиции.")
    return "\n".join(lines)

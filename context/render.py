"""Блок 🧭 КОНТЕКСТ — дополняет сообщение алерта, не заменяет его.

Правила формулировок:
- «публичных новостей не найдено» вместо «новостей нет»: инсайдерские пампы
  случаются ДО публикации, отсутствие новости не означает отсутствие катализатора;
- «n/a» не печатается — строка без данных просто опускается;
- никаких торговых рекомендаций: факт + пометка риска.
"""

from typing import Optional

from context import signals

VERDICT_CATALYST = "КАТАЛИЗАТОР"
VERDICT_BEARISH = "МЕДВЕЖИЙ_ФОН"
VERDICT_CLEAN = "ЧИСТО"
VERDICT_UNKNOWN = "НЕ_ПРОВЕРЕН"

EVENT_TITLES = {
    "LISTING": "листинг",
    "DELISTING": "делистинг",
    "PERP_LAUNCH": "запуск перпа",
    "NEW_INSTRUMENT": "новый инструмент",
    "DELISTED": "инструмент снят",
    "REGULATORY": "регуляторное событие",
    "LEGAL": "юридическое событие",
    "LEADERSHIP": "смена руководства",
    "POLITICAL_MENTION": "упоминание политиком",
    "PARTNERSHIP": "партнёрство",
    "HACK": "взлом",
    "UNLOCK": "анлок",
    "CALENDAR": "событие календаря",
    "OTHER": "событие",
}


def _minutes_ago(now_ts: float, ts: float) -> str:
    minutes = max(0.0, (now_ts - ts) / 60.0)
    if minutes < 60:
        return f"{minutes:.0f} мин назад"
    hours = minutes / 60.0
    if hours < 48:
        return f"{hours:.1f} ч назад"
    return f"{hours / 24:.0f} дн назад"


def render(context: dict, cfg: dict, now_ts: float) -> Optional[str]:
    """Собрать блок. None — если печатать нечего."""
    lines = ["───────────────", "🧭 <b>КОНТЕКСТ</b>"]
    verdict = context.get("verdict")

    if verdict == VERDICT_CATALYST:
        event = context.get("catalyst") or {}
        title = EVENT_TITLES.get((event.get("event_type") or "").upper(), "событие")
        where = event.get("source") or ""
        when = _minutes_ago(now_ts, float(event.get("ts") or now_ts))
        url = event.get("url") or ""
        source = f" [{where}]" if where else ""
        lines.append(f"⚠️ <b>КАТАЛИЗАТОР: {title} {where} ({when})</b>{'' if url else source}")
        if url:
            lines.append(f"   {url}")
        lines.append("   Шорт против новости — высокий риск")
    elif verdict == VERDICT_BEARISH:
        event = context.get("bearish") or {}
        title = EVENT_TITLES.get((event.get("event_type") or "").upper(), "событие")
        lines.append(f"📉 Медвежий фон: {title}")
    elif verdict == VERDICT_UNKNOWN:
        failed = ", ".join(context.get("sources_failed") or []) or "источники не ответили"
        lines.append(f"❔ Контекст не проверен ({failed})")
    else:
        lines.append("✅ Публичных новостей не найдено (инсайдерский катализатор не исключён)")

    # риск сквоза идёт выше цифр: это про то, чем закончится шорт, а не про фон
    squeeze = signals.squeeze_line(context.get("funding_state"), cfg)
    if squeeze:
        lines.append(squeeze)

    basis = signals.basis_line(context.get("basis_pct"), context.get("basis_verdict"))
    if basis:
        lines.append(basis)

    krw = signals.krw_line(context.get("krw_premium"), context.get("krw_background"), cfg)
    if krw:
        lines.append(krw)

    derivatives = context.get("derivatives") or {}
    if derivatives:
        parts = []
        if derivatives.get("oi_change_pct") is not None:
            window = derivatives.get("oi_window_min") or 30
            parts.append(f"OI {derivatives['oi_change_pct']:+.1f}%/{window}м")
        ratio = derivatives.get("long_short_ratio")
        if ratio is not None:
            crowd = " (толпа в лонгах)" if ratio >= cfg["derivatives_flags"]["high_long_short_ratio"] else ""
            parts.append(f"L/S ratio {ratio:.2f}{crowd}")
        taker = derivatives.get("taker_buy_sell_ratio")
        if taker is not None:
            parts.append(f"тейкер buy/sell {taker:.2f}")
        if parts:
            lines.append("📊 Деривативы: " + " | ".join(parts))

    dex = context.get("dex") or {}
    if dex:
        # монета, найденная только по тикеру, может оказаться тёзкой на другой сети:
        # честнее пометить, чем выдать чужие цифры за свои
        unverified = " · по тикеру, адресом не подтверждено" if dex.get("matched_by") == "тикер" else ""
        lines.append(
            f"🔄 DEX ({dex.get('dex')}/{dex.get('chain')}): объём 24ч ${dex['volume_h24'] / 1e6:.1f}M"
            f" | цена 1ч {dex['price_change_h1']:+.1f}% / 24ч {dex['price_change_h24']:+.1f}%"
            f"{unverified}")

    unlock = context.get("unlock")
    if unlock:
        lines.append(f"🔓 {unlock}")

    liquidations = context.get("liquidations")
    if liquidations:
        lines.append(f"💥 Ликвидации: {liquidations}")

    listing_play = context.get("listing_play")
    if listing_play:
        lines.append(f"🟢 {listing_play}")

    return "\n".join(lines) if len(lines) > 2 else None

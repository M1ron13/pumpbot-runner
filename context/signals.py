"""Математика внутренних сигналов: фандинг, basis, корейская премия.

Это не новые источники — это новые выводы из данных, которые уже текут в систему.
Все функции чистые: без сети и без кэша, чтобы каждое правило проверялось построчно.

Четыре решения, без которых сигналы врут:

1. **Фандинг приводится к 8 часам.** У части символов интервал 4 часа, и сырое
   сравнение с порогом завышало бы их вдвое. Сравнивается только 8-часовой эквивалент.
2. **Гистерезис.** Событие снимается не по тому же порогу, по которому включилось,
   иначе состояние мигает вокруг границы.
3. **Basis двусторонний.** Перп дороже спота — перегрев деривативной толпы. Перп
   ДЕШЕВЛЕ спота — в перпах сидят шорты, и это подтверждение риска сквиза, а не
   «перегрев с минусом». Разный смысл, разные строки.
4. **Корейская премия считается к фону.** Кимчи-премия существует почти всегда как
   общий уровень рынка: при фоне +3.5% порог +3% сработает на любой монете. Значение
   имеет только превышение над медианой BTC/ETH в тот же момент.
"""

from statistics import median
from typing import Dict, List, Optional

# состояния фандинга
FUNDING_OFF = "OFF"
FUNDING_LONG_EXTREME = "LONG_EXTREME"
FUNDING_SHORT_EXTREME = "SHORT_EXTREME"

BASIS_HOT = "HOT"
BASIS_COLD = "COLD"


# --------------------------------------------------------------------------- #
# фандинг
# --------------------------------------------------------------------------- #

def to_8h(rate: float, interval_hours: Optional[float]) -> float:
    """Ставка к 8-часовому эквиваленту. Интервал неизвестен — считаем, что 8 часов."""
    try:
        rate = float(rate)
    except (TypeError, ValueError):
        return 0.0
    try:
        hours = float(interval_hours or 8.0)
    except (TypeError, ValueError):
        hours = 8.0
    if hours <= 0:
        hours = 8.0
    return rate * (8.0 / hours)


def funding_transition(prev: Optional[dict], funding_8h: float, cfg: dict,
                       now_ts: float) -> dict:
    """Новое состояние фандинга с гистерезисом и отметкой устойчивости.

    prev — предыдущая запись состояния (`state`, `since_ts`) либо None.
    Возвращает {'state', 'since_ts', 'sustained', 'changed', 'value'}.
    """
    signals = cfg["signals"]["funding"]
    pos, pos_release = float(signals["extreme_pos"]), float(signals["release_pos"])
    neg, neg_release = float(signals["extreme_neg"]), float(signals["release_neg"])
    sustained_sec = float(signals["sustained_hours"]) * 3600.0

    prev_state = (prev or {}).get("state") or FUNDING_OFF
    since_ts = float((prev or {}).get("since_ts") or now_ts)

    if prev_state == FUNDING_LONG_EXTREME:
        # выходим только за release-порогом: у входа и выхода разные границы
        state = FUNDING_LONG_EXTREME if funding_8h > pos_release else FUNDING_OFF
    elif prev_state == FUNDING_SHORT_EXTREME:
        state = FUNDING_SHORT_EXTREME if funding_8h < neg_release else FUNDING_OFF
    else:
        if funding_8h >= pos:
            state = FUNDING_LONG_EXTREME
        elif funding_8h <= neg:
            state = FUNDING_SHORT_EXTREME
        else:
            state = FUNDING_OFF

    changed = state != prev_state
    if changed:
        since_ts = now_ts
    sustained = (state == FUNDING_SHORT_EXTREME and now_ts - since_ts >= sustained_sec)
    return {"state": state, "since_ts": since_ts, "sustained": sustained,
            "changed": changed, "value": funding_8h}


def squeeze_line(state: dict, cfg: dict) -> Optional[str]:
    """Строка риска сквиза. Только для устойчиво отрицательного фандинга."""
    if not state or state.get("state") != FUNDING_SHORT_EXTREME or not state.get("sustained"):
        return None
    hours = int(cfg["signals"]["funding"]["sustained_hours"])
    return (f"⚠️ РИСК СКВИЗА: funding отрицательный {hours}ч+, шорты перегружены "
            f"({state['value'] * 100:+.3f}%/8ч)")


# --------------------------------------------------------------------------- #
# basis спот-перп
# --------------------------------------------------------------------------- #

def basis_pct(perp_price: float, spot_price: float) -> Optional[float]:
    try:
        perp, spot = float(perp_price), float(spot_price)
    except (TypeError, ValueError):
        return None
    if spot <= 0 or perp <= 0:
        return None
    return (perp / spot - 1.0) * 100.0


def basis_is_sane(basis: Optional[float], cfg: dict) -> bool:
    """Basis в десятки процентов означает сравнение разных инструментов, а не рынок.

    Такое бывает при несовпадении спот-пары (другая монета с тем же тикером, обёртка,
    ошибка в символе). Печатать такое число в канал нельзя — сначала не верим.
    """
    if basis is None:
        return False
    return abs(basis) <= float(cfg["signals"]["basis"]["max_abs_pct"])


def basis_verdict(basis: Optional[float], cfg: dict) -> Optional[str]:
    if basis is None or not basis_is_sane(basis, cfg):
        return None
    limits = cfg["signals"]["basis"]
    if basis >= float(limits["hot_pct"]):
        return BASIS_HOT
    if basis <= float(limits["cold_pct"]):
        return BASIS_COLD
    return None


def basis_line(basis: Optional[float], verdict: Optional[str]) -> Optional[str]:
    """Знак числа не заменяет смысл: у перегрева и у навеса шортов разные выводы."""
    if basis is None or verdict is None:
        return None
    if verdict == BASIS_HOT:
        return f"📐 Basis: {basis:+.1f}% (перп дороже спота — деривативная толпа перегрета)"
    return (f"📐 Basis: {basis:+.1f}% (перп дешевле спота — в перпах сидят шорты, "
            f"подтверждение риска сквиза)")


# --------------------------------------------------------------------------- #
# корейская премия
# --------------------------------------------------------------------------- #

def krw_premium_pct(krw_price: float, usd_krw_rate: float, usd_price: float) -> Optional[float]:
    try:
        krw, rate, usd = float(krw_price), float(usd_krw_rate), float(usd_price)
    except (TypeError, ValueError):
        return None
    if rate <= 0 or usd <= 0 or krw <= 0:
        return None
    return ((krw / rate) / usd - 1.0) * 100.0


def market_background(premiums: Dict[str, float]) -> Optional[float]:
    """Фон рынка: медиана премий мажоров в тот же момент."""
    values = [v for v in (premiums or {}).values() if v is not None]
    return median(values) if values else None


def excess_premium(coin_premium: Optional[float],
                   background: Optional[float]) -> Optional[float]:
    if coin_premium is None:
        return None
    return coin_premium - (background or 0.0)


def krw_line(coin_premium: Optional[float], background: Optional[float],
             cfg: dict) -> Optional[str]:
    """Строка появляется только на превышении фона, а не на самом факте премии."""
    excess = excess_premium(coin_premium, background)
    if excess is None:
        return None
    if excess < float(cfg["signals"]["krw"]["excess_pct"]):
        return None
    background_text = f", фон рынка {background:+.1f}%" if background is not None else ""
    return (f"🇰🇷 KRW-премия: {coin_premium:+.1f}% (превышение фона {excess:+.1f}%"
            f"{background_text} — корейский ретейл заходит, типично финальная фаза пампа)")

"""Контур B: деривативы Binance и DEX-активность.

Деривативы показывают механику пампа: растёт ли OI вместе с ценой (свежие лонги —
топливо будущего дампа), перекошено ли позиционирование, кто агрессор по тейкеру.
DexScreener отвечает на другой вопрос: не сделала ли монета движение на DEX ещё
до перпов — тогда картина не «памп начинается», а «памп уже созрел».
"""

import logging
from typing import Optional

log = logging.getLogger("context.market")


async def _json(session, url: str, timeout_ms: int):
    import aiohttp
    timeout = aiohttp.ClientTimeout(total=timeout_ms / 1000.0)
    async with session.get(url, timeout=timeout, headers={"User-Agent": "Mozilla/5.0"}) as resp:
        if resp.status != 200:
            raise RuntimeError(f"HTTP {resp.status}")
        return await resp.json(content_type=None)


async def derivatives(session, cfg: dict, symbol: str) -> dict:
    """long/short ratio, динамика OI за окно, агрессия тейкеров."""
    src = cfg["sources"]
    budget = cfg["budget"]["per_source_ms"]
    out = {}

    try:
        rows = await _json(session, src["long_short_ratio"].format(symbol=symbol), budget)
        if rows:
            out["long_short_ratio"] = float(rows[-1]["longShortRatio"])
    except Exception as exc:
        log.debug("long/short по %s: %s", symbol, exc)

    try:
        rows = await _json(session, src["open_interest_hist"].format(symbol=symbol), budget)
        if rows and len(rows) >= 2:
            first = float(rows[0]["sumOpenInterest"])
            last = float(rows[-1]["sumOpenInterest"])
            minutes = int((float(rows[-1]["timestamp"]) - float(rows[0]["timestamp"])) / 60000)
            if first > 0:
                out["oi_change_pct"] = (last - first) / first * 100.0
                out["oi_window_min"] = minutes
    except Exception as exc:
        log.debug("OI history по %s: %s", symbol, exc)

    try:
        rows = await _json(session, src["taker_ratio"].format(symbol=symbol), budget)
        if rows:
            out["taker_buy_sell_ratio"] = float(rows[-1]["buySellRatio"])
    except Exception as exc:
        log.debug("taker ratio по %s: %s", symbol, exc)

    if not out:
        raise RuntimeError("ни один эндпоинт деривативов не ответил")
    return out


async def dex(session, cfg: dict, ticker: str, addresses=None) -> Optional[dict]:
    """Сводка по самой ликвидной паре монеты на DEX.

    Тикер не идентифицирует токен: по «ARB» приходят чужие токены с тем же символом
    на других сетях, а в справочнике CoinGecko один символ носят десятки монет.
    Поэтому пробуем все адреса-кандидаты, собираем пары со всех успешных ответов и
    выбираем самую ликвидную: у настоящего проекта стакан глубже, чем у тёзок.
    Пары ниже порогов ликвидности и объёма отбрасываются — иначе слой печатает пыль.
    """
    limits = cfg.get("dex_filters") or {}
    min_liquidity = float(limits.get("min_liquidity_usd", 0))
    min_volume = float(limits.get("min_volume_h24_usd", 0))
    max_addresses = int(limits.get("max_addresses", 4))

    def liquidity(pair):
        return float((pair.get("liquidity") or {}).get("usd") or 0.0)

    def usable(pairs):
        # достаточно одного признака живого рынка: у части настоящих пар DexScreener
        # отдаёт нулевой суточный объём при миллиардной ликвидности, и требование
        # «и то, и другое» выбрасывало именно настоящие токены
        return [p for p in pairs
                if liquidity(p) >= min_liquidity
                or float((p.get("volume") or {}).get("h24") or 0.0) >= min_volume]

    collected, matched_by = [], None
    for address in list(addresses or [])[:max_addresses]:
        try:
            data = await _json(session, cfg["sources"]["dexscreener_tokens"].format(address=address),
                               cfg["budget"]["per_source_ms"])
        except Exception as exc:
            log.debug("DEX по адресу %s: %s", address, exc)
            continue
        found = usable(data.get("pairs") or [])
        if found:
            collected.extend(found)
            matched_by = "адрес контракта"

    if not collected:
        try:
            data = await _json(session, cfg["sources"]["dexscreener_search"].format(query=ticker),
                               cfg["budget"]["per_source_ms"])
        except Exception:
            return None
        by_symbol = [p for p in (data.get("pairs") or [])
                     if (p.get("baseToken") or {}).get("symbol", "").upper() == ticker.upper()]
        collected = usable(by_symbol)
        matched_by = "тикер" if collected else None

    if not collected:
        return None

    best = max(collected, key=liquidity)
    volume = best.get("volume") or {}
    change = best.get("priceChange") or {}
    return {
        "pair": f"{(best.get('baseToken') or {}).get('symbol')}/"
                f"{(best.get('quoteToken') or {}).get('symbol')}",
        "dex": best.get("dexId"),
        "chain": best.get("chainId"),
        "liquidity_usd": liquidity(best),
        "volume_h1": float(volume.get("h1") or 0.0),
        "volume_h24": float(volume.get("h24") or 0.0),
        "price_change_h1": float(change.get("h1") or 0.0),
        "price_change_h24": float(change.get("h24") or 0.0),
        "pairs_found": len(collected),
        "matched_by": matched_by,
    }


# --------------------------------------------------------------------------- #
# данные для внутренних сигналов (пункты 1-3 пакета v2)
# --------------------------------------------------------------------------- #

async def spot_price(session, cfg: dict, symbol: str) -> Optional[float]:
    """Спот-цена: Binance, при отказе — Bybit. Цена перпа берётся из самого алерта."""
    try:
        data = await _json(session, cfg["sources"]["binance_spot_price"].format(symbol=symbol),
                           cfg["budget"]["per_source_ms"])
        return float(data["price"])
    except Exception:
        pass
    try:
        data = await _json(session, cfg["sources"]["bybit_spot_price"].format(symbol=symbol),
                           cfg["budget"]["per_source_ms"])
        return float((data["result"]["list"] or [{}])[0]["lastPrice"])
    except Exception:
        return None


async def spot_prices(session, cfg: dict, symbols) -> dict:
    """Спот-цены нескольких символов одним запросом — для фона корейской премии."""
    quoted = ",".join(f'"{s}"' for s in symbols)
    try:
        data = await _json(session, cfg["sources"]["binance_spot_prices"].format(symbols=quoted),
                           cfg["budget"]["per_source_ms"])
    except Exception:
        return {}
    out = {}
    for item in data if isinstance(data, list) else []:
        try:
            out[item["symbol"]] = float(item["price"])
        except (KeyError, TypeError, ValueError):
            continue
    return out


async def upbit_krw_prices(session, cfg: dict, tickers) -> dict:
    """Цены в KRW по нескольким монетам одним запросом. Нет KRW-пары — нет ключа."""
    markets = ",".join(f"KRW-{t}" for t in tickers)
    try:
        data = await _json(session, cfg["sources"]["upbit_ticker"].format(markets=markets),
                           cfg["budget"]["per_source_ms"])
    except Exception:
        return {}
    out = {}
    for item in data if isinstance(data, list) else []:
        market = str(item.get("market") or "")
        if market.startswith("KRW-"):
            try:
                out[market[4:]] = float(item["trade_price"])
            except (KeyError, TypeError, ValueError):
                continue
    return out


async def usd_krw_rate(session, cfg: dict) -> Optional[float]:
    try:
        data = await _json(session, cfg["sources"]["usd_krw"], cfg["budget"]["per_source_ms"])
        return float((data.get("rates") or {}).get("KRW"))
    except Exception:
        return None


async def funding_snapshot(session, cfg: dict) -> dict:
    """Фандинг по всем символам + интервал начисления, чтобы привести к 8 часам."""
    out = {}
    try:
        rows = await _json(session, cfg["sources"]["binance_premium_index_all"],
                           cfg["budget"]["per_source_ms"])
        for item in rows if isinstance(rows, list) else []:
            try:
                out[item["symbol"]] = {"rate": float(item["lastFundingRate"]),
                                       "interval_hours": None, "exchange": "BINANCE"}
            except (KeyError, TypeError, ValueError):
                continue
    except Exception as exc:
        log.warning("premiumIndex недоступен: %s", exc)

    # интервалы: у части символов 4 часа вместо 8, и это меняет сравнение с порогом
    try:
        info = await _json(session, cfg["sources"]["binance_funding_info"],
                           cfg["budget"]["per_source_ms"])
        for item in info if isinstance(info, list) else []:
            symbol = item.get("symbol")
            if symbol in out:
                try:
                    out[symbol]["interval_hours"] = float(item["fundingIntervalHours"])
                except (KeyError, TypeError, ValueError):
                    pass
    except Exception as exc:
        log.debug("fundingInfo недоступен: %s", exc)
    return out

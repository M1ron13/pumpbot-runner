"""Сопоставление текста новости с монетой — главный источник ошибок слоя.

«Sun выступил с заявлением» не должно матчиться на монету SUN, «ID card reform» —
на ID, «One thing about...» — на ONE. Поэтому:

- тикер длиной ≥ 5 или уникальное имя проекта → матч по тикеру допустим;
- тикер ≤ 4 символов → только по полному имени проекта либо по паре «имя + тикер»
  в одном тексте;
- границы слов и регистр обязательны: `\\bSUN\\b`, а не подстрока.

Каждое решение (в том числе отказ) пишется в `match_log` — иначе точность
матчинга невозможно проверить постфактум.
"""

import re
import time
from typing import List, Optional

TICKER_IN_BRACKETS = re.compile(r"\(([A-Z0-9]{2,12})\)")
UPPER_WORD = re.compile(r"\b([A-Z0-9]{2,12})\b")

# Кандидатами не могут быть числа («000» из «$100,000») и котируемые активы:
# такие «тикеры» только засоряют журнал матчинга и создают риск ложных матчей.
NOT_TICKERS = {"USDT", "USDC", "BUSD", "USD", "EUR", "NFT", "API", "CEO",
               "USA", "SEC", "ETF", "DEX", "CEX", "AMA", "APR", "APY", "TVL", "P2P"}


def normalize(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "")).strip()


def word_present(text: str, word: str, case_sensitive: bool = True) -> bool:
    if not word:
        return False
    flags = 0 if case_sensitive else re.IGNORECASE
    return re.search(rf"\b{re.escape(word)}\b", text or "", flags) is not None


class TickerMatcher:
    def __init__(self, cfg: dict, cache=None):
        rules = cfg["matching"]
        self.short_max = int(rules["short_ticker_max_len"])
        self.require_name = bool(rules["require_name_for_short_ticker"])
        self.min_name_len = int(rules["min_project_name_len"])
        self.cache = cache

    # -- основной вход -------------------------------------------------------- #

    def match(self, text: str, source: str = "", candidates: Optional[List[dict]] = None,
              hinted_ticker: str = None) -> Optional[dict]:
        """Вернуть {'ticker', 'coin_id', 'rule'} либо None, если уверенности нет."""
        text = normalize(text)
        if not text:
            return None

        # тикер в скобках — сильнейший сигнал: «Sun (SUN) announces...»
        if hinted_ticker and word_present(text, hinted_ticker):
            return self._accept(text, source, hinted_ticker, None, "подсказка источника")
        bracketed = TICKER_IN_BRACKETS.findall(text)
        for ticker in bracketed:
            coins = self._coins(ticker, candidates)
            if coins:
                return self._accept(text, source, ticker, coins[0].get("coin_id"), "тикер в скобках")
            return self._accept(text, source, ticker, None, "тикер в скобках без справочника")

        for ticker in dict.fromkeys(UPPER_WORD.findall(text)):
            if ticker.isdigit() or ticker in NOT_TICKERS:
                continue
            coins = self._coins(ticker, candidates)
            if len(ticker) > self.short_max:
                if coins:
                    return self._accept(text, source, ticker, coins[0].get("coin_id"), "длинный тикер")
                continue
            # короткий тикер: нужно имя проекта в том же тексте
            for coin in coins:
                name = (coin.get("name") or "").strip()
                if len(name) >= self.min_name_len and word_present(text, name, case_sensitive=False):
                    return self._accept(text, source, ticker, coin.get("coin_id"),
                                        "короткий тикер + имя проекта")
            if coins and self.require_name:
                self._reject(text, source, ticker, "короткий тикер без имени проекта")
        return None

    # -- вспомогательное ------------------------------------------------------ #

    def _coins(self, ticker: str, candidates: Optional[List[dict]]) -> List[dict]:
        if candidates is not None:
            return [c for c in candidates if (c.get("symbol") or "").upper() == ticker.upper()]
        if self.cache is not None:
            return self.cache.coins_by_symbol(ticker)
        return []

    def _accept(self, text, source, ticker, coin_id, rule) -> dict:
        self._log(text, source, ticker, coin_id, rule, "матч")
        return {"ticker": ticker.upper(), "coin_id": coin_id, "rule": rule}

    def _reject(self, text, source, ticker, rule) -> None:
        self._log(text, source, ticker, None, rule, "отказ")

    def _log(self, text, source, ticker, coin_id, rule, decision) -> None:
        if self.cache is None:
            return
        try:
            self.cache.log_match(ts=time.time(), source=source, text=text, ticker=ticker,
                                 coin_id=coin_id, rule=rule, decision=decision)
        except Exception:
            pass   # журнал матчинга не имеет права ломать обработку события

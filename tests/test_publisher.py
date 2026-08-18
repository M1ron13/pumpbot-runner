"""Тесты вокруг публикатора, не покрытые белым списком.

Сценарии guard-цепочки живут в `test_whitelist.py`. Здесь остаётся то, что к самой
цепочке не относится: входной фильтр анонсов, правила матрицы листингов и поведение
кэша (журнал отправок против повторов и чистка, которая его не трогает).
"""

import asyncio
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from context import config as ctx_config          # noqa: E402
from context import listing_rules as rules        # noqa: E402
from context import publisher as pub              # noqa: E402
from context.cache import Cache                   # noqa: E402
from context.sources import announcements         # noqa: E402

NOW = 1_700_000_000.0


class Sender:
    def __init__(self, ok=True):
        self.sent = []
        self.ok = ok

    async def __call__(self, text):
        self.sent.append(text)
        return self.ok


class CacheBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pub-test-")
        self.cfg = ctx_config.load()
        self.cache = Cache(os.path.join(self.tmpdir, "cache.db"))
        self.sender = Sender()
        self.cache.apply_instruments("BINANCE", "perp", ["XYZUSDT"], now_ts=NOW - 7200)
        self.cache.apply_instruments("BINANCE", "perp", ["XYZUSDT"], now_ts=NOW - 60)

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def publisher(self, **overrides):
        base = {**self.cfg["publisher"], "enabled": True, "shadow_types": []}
        self.cfg["publisher"] = {**base, **overrides}
        return pub.Publisher(self.cfg, self.cache, self.sender)


class ScreenTestCase(unittest.TestCase):
    """Входной фильтр анонсов: TradFi и объявления без тикера в кэш не попадают."""

    def setUp(self):
        self.cfg = ctx_config.load()

    def test_stages(self):
        cases = [
            ("New listing: ISRGUSDT TradFi Perpetual Contract", "filtered_tradfi"),
            ("KuCoin Spot to List Tokenized Stocks: CRCLX, TSLAX", "filtered_tradfi"),
            ("Notice of Removal of Spot Trading Pairs - 2026-08-14", "parse_failed"),
            ("Upbit will list Monad (MON) in KRW market", "ок"),
        ]
        for title, expected in cases:
            result = announcements.screen({"title": title, "source": "TEST"}, self.cfg)
            self.assertEqual(result["stage"], expected, title)


class ListingMatrixTestCase(unittest.TestCase):
    """Правки после замечаний по проду: где мы торгуем ≠ за чем следим."""

    def setUp(self):
        self.rules_cfg = rules.load_config()
        self.binance_perp = [{"exchange": "BINANCE", "market": "perp", "symbol": "XYZUSDT"}]

    def test_upbit_delisting_is_weak_bearish(self):
        verdict = rules.decide(event_type="DELISTING", exchange="UPBIT", ticker="STORJ",
                               market="spot", places=self.binance_perp, cfg=self.rules_cfg)
        self.assertEqual(verdict["class"], rules.WEAK_BEARISH)
        self.assertFalse(verdict["post"])

    def test_binance_delisting_is_operational(self):
        verdict = rules.decide(event_type="DELISTING", exchange="BINANCE", ticker="AERGO",
                               market="perp", places=self.binance_perp, cfg=self.rules_cfg)
        self.assertEqual(verdict["class"], rules.OPERATIONAL)
        self.assertTrue(verdict["post"])

    def test_delisting_of_coin_outside_universe(self):
        verdict = rules.decide(event_type="DELISTING", exchange="UPBIT", ticker="TT",
                               market="spot", places=[], cfg=self.rules_cfg)
        self.assertEqual(verdict["class"], rules.OUT_OF_UNIVERSE)
        self.assertFalse(verdict["post"])

    def test_listing_is_not_blocked_by_universe_gate(self):
        """Листинг монеты, которой нигде не было, — это и есть сигнал «новая аудитория»."""
        verdict = rules.decide(event_type="LISTING", exchange="UPBIT", ticker="NEWCOIN",
                               market="spot", places=[], cfg=self.rules_cfg)
        self.assertEqual(verdict["class"], rules.STRONG_BULLISH)


class PublishLedgerTestCase(CacheBase):
    """Журнал отправок: он и есть защита от повторов, чистка его не трогает."""

    def add_event(self, ticker="NEWCOIN", url="https://ex/1", ts=NOW, event_type="LISTING"):
        self.cache.add_event(ts=ts, source="BINANCE", event_type=event_type, ticker=ticker,
                             title=f"Binance will list Something ({ticker})", url=url)

    def test_counters_count_only_sent(self):
        self.assertTrue(self.cache.reserve_published("BINANCE", "id-1", "NEW_LISTING", "A", NOW))
        self.assertEqual(self.cache.published_count(NOW - 3600), 0,
                         "бронь без отправки в счётчик не идёт")
        self.cache.mark_published_sent("BINANCE", "id-1", NOW)
        self.assertEqual(self.cache.published_count(NOW - 3600), 1)

    def test_reserve_is_exclusive_and_releasable(self):
        self.assertTrue(self.cache.reserve_published("BINANCE", "id-2", "NEW_LISTING", "A", NOW))
        self.assertFalse(self.cache.reserve_published("BINANCE", "id-2", "NEW_LISTING", "A", NOW),
                         "второй отправитель обязан получить отказ")
        self.cache.release_published("BINANCE", "id-2")
        self.assertTrue(self.cache.reserve_published("BINANCE", "id-2", "NEW_LISTING", "A", NOW),
                        "после снятия брони ключ снова свободен")

    def test_published_event_survives_purge(self):
        """TTL считается от момента записи, а обработанные события живут дольше."""
        self.add_event(ts=NOW - 30 * 86400, url="https://ex/old")
        stats = asyncio.run(self.publisher().run_once(now_ts=NOW))
        self.assertEqual(stats["отправлено"], 1)

        self.cache.purge_expired(now_ts=NOW + 10 * 86400, ledger_sec=2592000)
        rows = self.cache.conn.execute(
            "SELECT publish_decision FROM events WHERE publish_decision LIKE 'канал:%'").fetchall()
        self.assertTrue(rows, "отправленное событие не должно исчезать при чистке")

    def test_repeated_ingestion_does_not_resend(self):
        for _ in range(3):
            self.add_event(url="https://ex/same")
        stats = asyncio.run(self.publisher().run_once(now_ts=NOW))
        self.assertEqual(stats["отправлено"], 1)
        self.assertEqual(len(self.sender.sent), 1, "одно событие — одно сообщение")

    def test_logged_decisions_do_not_consume_rate_limit(self):
        for i in range(15):                       # эти уйдут в лог: монета уже на Binance
            self.add_event(ticker="XYZ", url=f"https://ex/weak{i}")
        self.add_event(ticker="NEWCOIN", url="https://ex/strong")
        publisher = self.publisher(max_per_hour=3, max_per_day=10)
        stats = asyncio.run(publisher.run_once(now_ts=NOW))
        self.assertEqual(stats["отправлено"], 1,
                         "15 логов не должны съесть лимит и заглушить настоящее событие")


if __name__ == "__main__":
    unittest.main(verbosity=2)

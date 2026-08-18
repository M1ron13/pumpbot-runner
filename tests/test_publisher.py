"""Тесты единственного отправителя событий.

Каждый тест проверяет один ограничитель. Смысл: даже если правила ошибутся, канал
не должен превратиться в поток — именно это случилось при инциденте.
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
from context.cache import Cache                   # noqa: E402
from context.publisher import (                   # noqa: E402
    Publisher, REASON_CLASS, REASON_DISABLED, REASON_RATE, REASON_SHADOW, REASON_TICKER,
)

NOW = 1_700_000_000.0


class RecordingSender:
    def __init__(self, ok=True):
        self.sent = []
        self.ok = ok

    async def __call__(self, text):
        self.sent.append(text)
        return self.ok


class PublisherTestBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pub-test-")
        self.cfg = ctx_config.load()
        self.cache = Cache(os.path.join(self.tmpdir, "cache.db"))
        self.sender = RecordingSender()
        # снепшот инструментов: без него любое решение — HOLD
        self.cache.apply_instruments("BINANCE", "spot", ["XYZUSDT"], now_ts=NOW - 3600)
        self.cache.apply_instruments("BINANCE", "spot", ["XYZUSDT"], now_ts=NOW - 60)

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def publisher(self, **overrides):
        self.cfg["publisher"] = {**self.cfg["publisher"], **overrides}
        return Publisher(self.cfg, self.cache, self.sender)

    def add_event(self, *, event_type="LISTING", ticker="XYZ", source="UPBIT",
                  title=None, ts=NOW, url="https://example.com/1"):
        title = title if title is not None else f"{source} will list Something ({ticker})"
        self.cache.add_event(ts=ts, source=source, event_type=event_type, ticker=ticker,
                             title=title, url=url)

    def run_publisher(self, publisher):
        return asyncio.run(publisher.run_once(now_ts=NOW))


class GateTestCase(PublisherTestBase):
    def test_strong_bullish_is_published(self):
        self.add_event(ticker="NEWCOIN")     # монеты нет в снепшоте → новая аудитория
        stats = self.run_publisher(self.publisher())
        self.assertEqual(stats["отправлено"], 1)
        self.assertIn("NEWCOIN", self.sender.sent[0])

    def test_weak_listing_never_reaches_channel(self):
        self.add_event(ticker="XYZ", source="BYBIT")   # XYZ уже на Binance
        stats = self.run_publisher(self.publisher())
        self.assertEqual(stats["отправлено"], 0)
        self.assertEqual(stats["в лог"], 1)
        self.assertEqual(self.sender.sent, [])

    def test_event_without_ticker_is_blocked(self):
        self.cache.add_event(ts=NOW, source="OKX", event_type="LISTING", ticker=None,
                             title="OKX to list perpetual futures", url="https://okx.com/1")
        publisher = self.publisher()
        event = publisher.pending()[0]
        outcome = publisher.evaluate(event, NOW)
        self.assertEqual(outcome["blocked"], REASON_TICKER)
        self.run_publisher(publisher)
        self.assertEqual(self.sender.sent, [])

    def test_class_gate_reason(self):
        self.add_event(ticker="XYZ", source="BYBIT")
        publisher = self.publisher()
        outcome = publisher.evaluate(publisher.pending()[0], NOW)
        self.assertEqual(outcome["blocked"], REASON_CLASS)
        self.assertEqual(outcome["verdict"]["class"], rules.WEAK)


class KillSwitchTestCase(PublisherTestBase):
    def test_disabled_publisher_sends_nothing(self):
        self.add_event(ticker="NEWCOIN")
        stats = self.run_publisher(self.publisher(enabled=False))
        self.assertEqual(stats["отправлено"], 0)
        self.assertEqual(self.sender.sent, [])

    def test_shadow_source_is_collected_but_silent(self):
        self.add_event(ticker="NEWCOIN", source="UPBIT")
        publisher = self.publisher(shadow_sources=["UPBIT"])
        outcome = publisher.evaluate(publisher.pending()[0], NOW)
        self.assertEqual(outcome["blocked"], REASON_SHADOW)
        self.assertEqual(self.run_publisher(publisher)["отправлено"], 0)


class RateLimitTestCase(PublisherTestBase):
    def test_hourly_limit_stops_the_flood(self):
        for i in range(10):
            self.add_event(ticker=f"COIN{i}", url=f"https://example.com/{i}")
        stats = self.run_publisher(self.publisher(max_per_hour=3, max_per_day=20))
        self.assertEqual(stats["отправлено"], 3, "лимит в час обязан обрезать поток")
        self.assertEqual(len(self.sender.sent), 3)

    def test_daily_limit(self):
        for i in range(6):
            self.add_event(ticker=f"COIN{i}", url=f"https://example.com/{i}")
        stats = self.run_publisher(self.publisher(max_per_hour=100, max_per_day=2))
        self.assertEqual(stats["отправлено"], 2)

    def test_rate_reason_names_the_limit(self):
        self.add_event(ticker="NEWCOIN")
        publisher = self.publisher(max_per_hour=0)
        outcome = publisher.evaluate(publisher.pending()[0], NOW)
        self.assertIn(REASON_RATE, outcome["blocked"])


class NoRepeatTestCase(PublisherTestBase):
    def test_event_is_published_once(self):
        self.add_event(ticker="NEWCOIN")
        publisher = self.publisher()
        first = self.run_publisher(publisher)
        second = self.run_publisher(publisher)
        self.assertEqual(first["отправлено"], 1)
        self.assertEqual(second["проверено"], 0, "опубликованное событие не берётся повторно")
        self.assertEqual(len(self.sender.sent), 1)

    def test_failed_send_keeps_event_in_queue(self):
        self.add_event(ticker="NEWCOIN")
        self.sender.ok = False
        publisher = self.publisher()
        self.run_publisher(publisher)
        self.assertEqual(len(publisher.pending()), 1,
                         "неудачная отправка не должна терять событие")


class TradFiScreenTestCase(PublisherTestBase):
    """Инструменты на акции не должны даже попадать в кэш."""

    def test_screen_rejects_tradfi_and_untickered(self):
        from context.sources import announcements
        cases = [
            ("New listing: ISRGUSDT TradFi Perpetual Contract", "filtered_tradfi"),
            ("KuCoin Spot to List Tokenized Stocks: CRCLX, TSLAX", "filtered_tradfi"),
            ("Notice of Removal of Spot Trading Pairs - 2026-08-14", "parse_failed"),
            ("Upbit will list Monad (MON) in KRW market", "ок"),
        ]
        for title, expected in cases:
            result = announcements.screen({"title": title, "source": "TEST"}, self.cfg)
            self.assertEqual(result["stage"], expected, title)



class RateLimitCountsOnlySentTestCase(PublisherTestBase):
    """Решения «в лог» не должны расходовать лимит частоты."""

    def test_logged_decisions_do_not_consume_limit(self):
        for i in range(15):                      # эти уйдут в лог: монета уже на Binance
            self.add_event(ticker="XYZ", source="BYBIT", url=f"https://example.com/weak{i}",
                           title=f"Bybit will list Something (XYZ) #{i}")
        self.add_event(ticker="NEWCOIN", url="https://example.com/strong")
        publisher = self.publisher(max_per_hour=3, max_per_day=10)
        stats = self.run_publisher(publisher)
        self.assertEqual(stats["отправлено"], 1,
                         "15 логов не должны съесть лимит и заглушить настоящее событие")
        hourly, daily = publisher.counters(NOW)
        self.assertEqual((hourly, daily), (1, 1), "в счётчике только отправленное")


class NoRepeatAfterPurgeTestCase(PublisherTestBase):
    """Чистка кэша не должна открывать дорогу повторной отправке."""

    def test_published_event_survives_purge(self):
        # объявление со старой датой: TTL считается от записи, а не от даты события
        self.add_event(ticker="NEWCOIN", ts=NOW - 30 * 86400, url="https://example.com/old")
        publisher = self.publisher()
        self.assertEqual(self.run_publisher(publisher)["отправлено"], 1)

        removed = self.cache.purge_expired(now_ts=NOW + 10 * 86400, ledger_sec=2592000)
        rows = self.cache.conn.execute(
            "SELECT publish_decision FROM events WHERE publish_decision LIKE 'канал:%'").fetchall()
        self.assertTrue(rows, f"отправленное событие удалено чисткой (удалено строк: {removed})")

    def test_repeated_ingestion_does_not_resend(self):
        for _ in range(3):
            self.add_event(ticker="NEWCOIN", url="https://example.com/same")
        publisher = self.publisher()
        self.assertEqual(self.run_publisher(publisher)["отправлено"], 1)
        self.assertEqual(len(self.sender.sent), 1, "одно событие — одно сообщение")

if __name__ == "__main__":
    unittest.main(verbosity=2)

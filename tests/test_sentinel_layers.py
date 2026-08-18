"""Тесты слоёв поверх детекции: OI, истощение, контекст события, журнал исходов.

Все — оффлайн: время подменяется через pump_bot.now, сеть не трогается.
"""

import os
import sys
import tempfile
import shutil
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pump_bot  # noqa: E402

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")
START_TS = 1_700_000_000.0


def cfg():
    return pump_bot.load_config(CONFIG_PATH)


class OpenInterestTestCase(unittest.TestCase):
    """Слой 2: на чьи деньги растёт цена."""

    def test_verdicts(self):
        self.assertEqual(pump_bot.read_open_interest(110.0, 100.0, 3.0)[0], "NEW_MONEY")
        self.assertEqual(pump_bot.read_open_interest(90.0, 100.0, 3.0)[0], "SQUEEZE")
        self.assertEqual(pump_bot.read_open_interest(101.0, 100.0, 3.0)[0], "FLAT")

    def test_change_value_and_missing_data(self):
        verdict, change = pump_bot.read_open_interest(120.0, 100.0, 3.0)
        self.assertEqual(verdict, "NEW_MONEY")
        self.assertAlmostEqual(change, 20.0)
        for bad in ((None, 100.0), (100.0, None), (100.0, 0.0)):
            self.assertEqual(pump_bot.read_open_interest(bad[0], bad[1], 3.0), ("FLAT", None))


class ExhaustionTestCase(unittest.TestCase):
    """Слой 3: новый хай на затухающем объёме."""

    def candles(self, closes, volumes):
        return [pump_bot.Candle(START_TS + i * 900, c, v)
                for i, (c, v) in enumerate(zip(closes, volumes))]

    def test_new_high_on_fading_volume(self):
        closes = [1.0, 1.0, 1.0, 1.2, 1.25, 1.3, 1.32, 1.33, 1.36, 1.36]
        volumes = [100, 100, 100, 900, 600, 400, 300, 200, 150, 120]
        ok, note = pump_bot.detect_exhaustion(self.candles(closes, volumes), cfg())
        self.assertTrue(ok, "новый хай на 17% объёма от импульса — это истощение")
        self.assertIn("от импульсного", note)

    def test_no_signal_while_volume_holds(self):
        closes = [1.0, 1.0, 1.0, 1.2, 1.25, 1.3, 1.32, 1.33, 1.40, 1.40]
        volumes = [100, 100, 100, 900, 850, 880, 900, 950, 1000, 900]
        ok, _ = pump_bot.detect_exhaustion(self.candles(closes, volumes), cfg())
        self.assertFalse(ok, "объём не падает — импульс живой, fade рано")

    def test_no_signal_without_new_high(self):
        closes = [1.0, 1.0, 1.0, 1.2, 1.30, 1.28, 1.26, 1.24, 1.20, 1.18]
        volumes = [100, 100, 100, 900, 600, 400, 300, 200, 150, 120]
        ok, _ = pump_bot.detect_exhaustion(self.candles(closes, volumes), cfg())
        self.assertFalse(ok, "цена уже сползает — это не пик, а состоявшийся откат")

    def test_short_history_is_safe(self):
        ok, _ = pump_bot.detect_exhaustion(self.candles([1.0, 1.1], [10, 10]), cfg())
        self.assertFalse(ok)

    def test_unclosed_candle_is_ignored(self):
        """Последняя свеча биржи ещё формируется — её неполный объём не сигнал."""
        closes = [1.0, 1.0, 1.0, 1.2, 1.25, 1.3, 1.32, 1.33, 1.34, 1.45]
        volumes = [100, 100, 100, 900, 850, 880, 900, 950, 980, 5]   # хвост — незакрытая свеча
        ok, _ = pump_bot.detect_exhaustion(self.candles(closes, volumes), cfg())
        self.assertFalse(ok, "незакрытая свеча не должна давать истощение")


class EventContextTestCase(unittest.TestCase):
    """Слой 1: причина пампа."""

    def test_base_coin(self):
        self.assertEqual(pump_bot.base_coin("XYZUSDT", "USDT"), "XYZ")
        self.assertEqual(pump_bot.base_coin("1000PEPEUSDT", "USDT"), "PEPE")
        self.assertEqual(pump_bot.base_coin("1000000BABYDOGEUSDT", "USDT"), "BABYDOGE")

    def dataset(self, days_ahead, tokens=56_000_000, max_supply=10_000_000_000,
                category="insiders"):
        """Форма ответа датасет-хоста DefiLlama (бесплатный, без ключа)."""
        return {
            "metadata": {"events": [{"timestamp": START_TS + days_ahead * 86400,
                                     "noOfTokens": [tokens], "category": category,
                                     "unlockType": "cliff"}]},
            "supplyMetrics": {"maxSupply": max_supply},
        }

    def test_unlock_within_window(self):
        text = pump_bot.unlock_context(self.dataset(2), 7.0, START_TS)
        self.assertIsNotNone(text)
        self.assertIn("через 2.0 дн", text)
        self.assertIn("0.56% supply", text)
        self.assertIn("insiders", text)

    def test_past_unlock_and_window_edges(self):
        self.assertIn("был 3.0 дн", pump_bot.unlock_context(self.dataset(-3), 7.0, START_TS))
        self.assertIsNone(pump_bot.unlock_context(self.dataset(30), 7.0, START_TS),
                          "анлок за окном внимания не упоминается")

    def test_bad_payloads_are_safe(self):
        for payload in (None, {}, {"metadata": {}}, {"metadata": {"events": ["мусор"]}},
                        {"metadata": {"events": [{"noOfTokens": [1]}]}}, ["не словарь"]):
            self.assertIsNone(pump_bot.unlock_context(payload, 7.0, START_TS))

    def test_slug_map_prefers_overrides(self):
        mapping = pump_bot.build_slug_map(
            ["arbitrum", "frax-finance", "gmx"], {"arbitrum": "ARB"})
        self.assertEqual(mapping["ARB"], "arbitrum", "ручное соответствие сильнее нормализации")
        self.assertEqual(mapping["FRAX"], "frax-finance")
        self.assertEqual(mapping["GMX"], "gmx")


class JournalTestCase(unittest.TestCase):
    """Слой 5: журнал сигналов и исходов."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="pumpbot-journal-")
        self.journal = pump_bot.SignalJournal(os.path.join(self.tmpdir, "signals.db"))

    def tearDown(self):
        self.journal.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def alert(self, ts=START_TS, price=100.0):
        signal = pump_bot.Signal(
            exchange="BINANCE", symbol="XYZUSDT", triggers=["MAIN"], move_15m=9.0,
            move_5m=7.0, zscore=8.1, vol_mult=6.0, price=price, volume_24h=51_000_000.0,
            funding_rate=0.0018, ts=ts, oi_read="SQUEEZE", oi_change_pct=-7.5,
            event_context="анлок через 2.0 дн",
        )
        return pump_bot.Alert(symbol="XYZUSDT", exchanges=["BINANCE"], primary=signal, ts=ts)

    def test_each_horizon_is_measured_at_its_own_time(self):
        row_id = self.journal.record(self.alert())

        self.assertEqual(self.journal.pending(START_TS + 600), [], "через 10 минут мерить нечего")

        due = self.journal.pending(START_TS + 3700)
        self.assertEqual([d[4] for d in due], ["out_1h"], "созрел только часовой горизонт")
        self.journal.fill(row_id, "out_1h", price_now=110.0, price_then=100.0)

        due = self.journal.pending(START_TS + 4 * 3600 + 60)
        self.assertEqual([d[4] for d in due], ["out_4h"], "часовой уже заполнен, созрел 4-часовой")
        self.journal.fill(row_id, "out_4h", price_now=95.0, price_then=100.0)

        due = self.journal.pending(START_TS + 25 * 3600)
        self.assertEqual([d[4] for d in due], ["out_24h"])
        self.journal.fill(row_id, "out_24h", price_now=80.0, price_then=100.0)

        row = self.journal.conn.execute(
            "SELECT out_1h, out_4h, out_24h FROM signals WHERE id = ?", (row_id,)).fetchone()
        self.assertAlmostEqual(row[0], 10.0)
        self.assertAlmostEqual(row[1], -5.0)
        self.assertAlmostEqual(row[2], -20.0)
        self.assertEqual(self.journal.pending(START_TS + 48 * 3600), [],
                         "заполненный сигнал больше не опрашивается")

    def test_record_keeps_layers_and_report_runs(self):
        self.journal.record(self.alert())
        row = self.journal.conn.execute(
            "SELECT kind, symbol, oi_read, oi_change_pct, event_context, triggers "
            "FROM signals").fetchone()
        self.assertEqual(row[0], "PUMP")
        self.assertEqual(row[1], "XYZUSDT")
        self.assertEqual(row[2], "SQUEEZE")
        self.assertAlmostEqual(row[3], -7.5)
        self.assertIn("анлок", row[4])
        self.assertEqual(row[5], "MAIN")

        self.journal.fill(1, "out_24h", price_now=80.0, price_then=100.0)
        report = self.journal.report()
        self.assertIn("PUMP", report)
        self.assertIn("SQUEEZE", report)


class AlertRenderTestCase(unittest.TestCase):
    """Сообщение: строки OI, события и тип сигнала."""

    def build(self, kind="PUMP", oi_read="SQUEEZE", note=""):
        signal = pump_bot.Signal(
            exchange="BINANCE", symbol="XYZUSDT", triggers=["MAIN"], move_15m=11.2,
            move_5m=8.2, zscore=7.4, vol_mult=6.3, price=0.0432, volume_24h=38_200_000.0,
            funding_rate=0.0018, ts=START_TS, oi_read=oi_read, oi_change_pct=-7.5,
            event_context="анлок через 2.0 дн", note=note,
        )
        return pump_bot.Alert(symbol="XYZUSDT", exchanges=["BINANCE"], primary=signal,
                              ts=START_TS, kind=kind)

    def test_pump_message_has_layers(self):
        text = pump_bot.render_alert(self.build(), cfg())
        self.assertIn("🔴 <b>PUMP: XYZ/USDT</b>", text)
        self.assertIn("OI↓ шорт-сквиз", text)
        self.assertIn("(-7.5%)", text)
        self.assertIn("📰 анлок через 2.0 дн", text)

    def test_exhaustion_message(self):
        text = pump_bot.render_alert(
            self.build(kind="EXHAUST", oi_read="NEW_MONEY", note="новый хай на объёме 17%"), cfg())
        self.assertIn("🎯 <b>EXHAUST: XYZ/USDT</b>", text)
        self.assertIn("истощение импульса", text)
        self.assertIn("OI↑ новые деньги", text)
        self.assertIn("📝 новый хай на объёме 17%", text)


if __name__ == "__main__":
    unittest.main(verbosity=2)

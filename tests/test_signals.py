"""Тесты внутренних сигналов: фандинг, basis, корейская премия.

Каждый тест закрывает одно из решений, без которых сигнал врёт: нормировка фандинга
к 8 часам, гистерезис, двусторонний basis, премия к фону рынка и история переходов
как материал для бэктеста слоя.
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from context import config as ctx_config      # noqa: E402
from context import render, signals           # noqa: E402
from context.cache import Cache               # noqa: E402

NOW = 1_700_000_000.0
HOUR = 3600.0


class FundingNormalizationTestCase(unittest.TestCase):
    """Правка 3: сравнение с порогом только в 8-часовом эквиваленте."""

    def test_four_hour_interval_is_doubled(self):
        self.assertAlmostEqual(signals.to_8h(0.0005, 4), 0.0010)

    def test_eight_hour_interval_unchanged(self):
        self.assertAlmostEqual(signals.to_8h(0.0012, 8), 0.0012)

    def test_unknown_interval_treated_as_eight(self):
        self.assertAlmostEqual(signals.to_8h(0.0012, None), 0.0012)
        self.assertAlmostEqual(signals.to_8h(0.0012, 0), 0.0012)

    def test_four_hour_symbol_crosses_threshold_only_after_normalization(self):
        cfg = ctx_config.load()
        raw = 0.0009                      # 0.09% за 4 часа
        self.assertLess(raw, cfg["signals"]["funding"]["extreme_pos"],
                        "сырое значение ниже порога")
        self.assertGreaterEqual(signals.to_8h(raw, 4), cfg["signals"]["funding"]["extreme_pos"],
                                "в 8-часовом эквиваленте порог перекрыт")


class FundingHysteresisTestCase(unittest.TestCase):
    """Правка 2 и сценарий из ТЗ: 0.16% → событие, 0.13% → держится, ниже release → снялось."""

    def setUp(self):
        self.cfg = ctx_config.load()

    def test_scenario_from_spec(self):
        state = signals.funding_transition(None, 0.0016, self.cfg, NOW)
        self.assertEqual(state["state"], signals.FUNDING_LONG_EXTREME)
        self.assertTrue(state["changed"])

        held = signals.funding_transition(state, 0.0013, self.cfg, NOW + HOUR)
        self.assertEqual(held["state"], signals.FUNDING_LONG_EXTREME,
                         "0.13% выше release-порога — состояние держится")
        self.assertFalse(held["changed"])
        self.assertEqual(held["since_ts"], state["since_ts"], "начало эпизода не сдвигается")

        released = signals.funding_transition(held, 0.0009, self.cfg, NOW + 2 * HOUR)
        self.assertEqual(released["state"], signals.FUNDING_OFF)
        self.assertTrue(released["changed"])

    def test_short_extreme_and_sustained_flag(self):
        state = signals.funding_transition(None, -0.0012, self.cfg, NOW)
        self.assertEqual(state["state"], signals.FUNDING_SHORT_EXTREME)
        self.assertFalse(state["sustained"], "сразу после входа устойчивости нет")

        later = signals.funding_transition(state, -0.0011, self.cfg, NOW + 25 * HOUR)
        self.assertTrue(later["sustained"], "через 25 часов эпизод считается устойчивым")

    def test_squeeze_line_requires_sustained(self):
        fresh = {"state": signals.FUNDING_SHORT_EXTREME, "value": -0.0012, "sustained": False}
        sustained = {"state": signals.FUNDING_SHORT_EXTREME, "value": -0.0012, "sustained": True}
        self.assertIsNone(signals.squeeze_line(fresh, self.cfg))
        line = signals.squeeze_line(sustained, self.cfg)
        self.assertIn("РИСК СКВИЗА", line)
        self.assertIn("24ч+", line)

    def test_squeeze_line_absent_for_positive_funding(self):
        state = {"state": signals.FUNDING_LONG_EXTREME, "value": 0.0016, "sustained": True}
        self.assertIsNone(signals.squeeze_line(state, self.cfg))


class BasisTestCase(unittest.TestCase):
    """Правка 1: у перегрева и у навеса шортов разный смысл, не знак числа."""

    def setUp(self):
        self.cfg = ctx_config.load()

    def test_hot_basis(self):
        basis = signals.basis_pct(101.2, 100.0)
        self.assertAlmostEqual(basis, 1.2, places=6)
        self.assertEqual(signals.basis_verdict(basis, self.cfg), signals.BASIS_HOT)
        line = signals.basis_line(basis, signals.BASIS_HOT)
        self.assertIn("перп дороже спота", line)
        self.assertIn("перегрета", line)

    def test_small_basis_gives_no_line(self):
        basis = signals.basis_pct(100.3, 100.0)
        self.assertIsNone(signals.basis_verdict(basis, self.cfg))
        self.assertIsNone(signals.basis_line(basis, None))

    def test_cold_basis_is_squeeze_confirmation_not_overheat(self):
        basis = signals.basis_pct(99.2, 100.0)
        self.assertEqual(signals.basis_verdict(basis, self.cfg), signals.BASIS_COLD)
        line = signals.basis_line(basis, signals.BASIS_COLD)
        self.assertIn("перп дешевле спота", line)
        self.assertIn("риска сквиза", line)
        self.assertNotIn("перегрета", line)

    def test_broken_prices_are_safe(self):
        self.assertIsNone(signals.basis_pct(100.0, 0))
        self.assertIsNone(signals.basis_pct(None, 100.0))
        self.assertIsNone(signals.basis_verdict(None, self.cfg))


class KrwPremiumTestCase(unittest.TestCase):
    """Правка 4: значение имеет только превышение над фоном рынка."""

    def setUp(self):
        self.cfg = ctx_config.load()

    def test_premium_math(self):
        # 105 KRW при курсе 1400 = $0.075; цена у нас $0.0714 → премия ≈ +5%
        premium = signals.krw_premium_pct(105.0, 1400.0, 0.0714)
        self.assertAlmostEqual(premium, 5.04, places=1)

    def test_background_is_median_of_majors(self):
        self.assertAlmostEqual(signals.market_background({"BTC": 3.4, "ETH": 3.6}), 3.5)
        self.assertIsNone(signals.market_background({}))

    def test_coin_premium_equal_to_background_is_silent(self):
        """Фон +3.5% и монета +3.5% — сигнала нет, хотя абсолютный порог перекрыт."""
        line = signals.krw_line(3.5, 3.5, self.cfg)
        self.assertIsNone(line, "премия на уровне фона сигналом не является")

    def test_excess_over_background_triggers(self):
        line = signals.krw_line(7.2, 3.5, self.cfg)
        self.assertIsNotNone(line)
        self.assertIn("+7.2%", line)
        self.assertIn("превышение фона +3.7%", line)

    def test_absent_upbit_pair_gives_no_line(self):
        self.assertIsNone(signals.krw_line(None, 3.5, self.cfg))
        self.assertIsNone(signals.excess_premium(None, 3.5))


class SignalHistoryTestCase(unittest.TestCase):
    """Правка 5: история переходов, а не перезапись строки."""

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="sig-test-")
        self.cache = Cache(os.path.join(self.tmpdir, "cache.db"))

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_transitions_are_appended(self):
        self.cache.record_signal("XYZUSDT", "funding", signals.FUNDING_LONG_EXTREME,
                                 0.0016, NOW, changed=True, now_ts=NOW)
        self.cache.record_signal("XYZUSDT", "funding", signals.FUNDING_LONG_EXTREME,
                                 0.0013, NOW, changed=False, now_ts=NOW + HOUR)
        self.cache.record_signal("XYZUSDT", "funding", signals.FUNDING_OFF,
                                 0.0009, NOW + 2 * HOUR, changed=True, now_ts=NOW + 2 * HOUR)

        history = self.cache.signal_history("XYZUSDT", "funding")
        self.assertEqual(len(history), 2, "в историю пишутся переходы, а не каждое измерение")
        self.assertEqual(history[0]["state_to"], signals.FUNDING_OFF)
        self.assertEqual(history[-1]["state_to"], signals.FUNDING_LONG_EXTREME)

        current = self.cache.signal_state("XYZUSDT", "funding")
        self.assertEqual(current["state"], signals.FUNDING_OFF)
        self.assertAlmostEqual(current["value"], 0.0009)

    def test_active_signals_lookup(self):
        self.cache.record_signal("AUSDT", "funding", signals.FUNDING_SHORT_EXTREME,
                                 -0.0012, NOW, changed=True, now_ts=NOW)
        self.cache.record_signal("BUSDT", "funding", signals.FUNDING_OFF,
                                 0.0001, NOW, changed=True, now_ts=NOW)
        active = self.cache.active_signals("funding", signals.FUNDING_SHORT_EXTREME)
        self.assertEqual([row["symbol"] for row in active], ["AUSDT"])


class RenderIntegrationTestCase(unittest.TestCase):
    """Строки сигналов попадают в блок контекста и не появляются без данных."""

    def setUp(self):
        self.cfg = ctx_config.load()

    def test_all_three_lines_present(self):
        context = {
            "verdict": render.VERDICT_CLEAN,
            "funding_state": {"state": signals.FUNDING_SHORT_EXTREME, "value": -0.0013,
                              "sustained": True},
            "basis_pct": 1.4, "basis_verdict": signals.BASIS_HOT,
            "krw_premium": 7.1, "krw_background": 3.4,
            "derivatives": {}, "dex": {},
        }
        block = render.render(context, self.cfg, NOW)
        self.assertIn("РИСК СКВИЗА", block)
        self.assertIn("Basis: +1.4%", block)
        self.assertIn("KRW-премия: +7.1%", block)

    def test_no_lines_without_data(self):
        block = render.render({"verdict": render.VERDICT_CLEAN, "derivatives": {}, "dex": {}},
                              self.cfg, NOW)
        for marker in ("РИСК СКВИЗА", "Basis", "KRW-премия"):
            self.assertNotIn(marker, block)

    def test_internal_signals_are_not_publishable(self):
        """Ни один внутренний сигнал не является типом сообщения белого списка."""
        from context import publisher as pub
        for name in ("FUNDING_EXTREME_LONG", "FUNDING_EXTREME_SHORT", "BASIS_HOT", "KRW_HOT"):
            self.assertNotIn(name, pub.WHITELIST)



class BasisSanityTestCase(unittest.TestCase):
    """Найдено живой проверкой: basis в десятки процентов = разные инструменты."""

    def setUp(self):
        self.cfg = ctx_config.load()

    def test_absurd_basis_gives_no_verdict(self):
        basis = signals.basis_pct(0.35, 0.099)      # ~+253%: сравнили разные монеты
        self.assertGreater(basis, 100)
        self.assertFalse(signals.basis_is_sane(basis, self.cfg))
        self.assertIsNone(signals.basis_verdict(basis, self.cfg),
                          "заведомо несопоставимые цены не должны давать вердикт")
        self.assertIsNone(signals.basis_line(basis, signals.basis_verdict(basis, self.cfg)))

    def test_normal_basis_still_passes_sanity(self):
        for value in (1.2, -0.7, 5.0, -19.9):
            self.assertTrue(signals.basis_is_sane(value, self.cfg), value)

if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Оффлайн-тесты логики детекции на синтетических данных (без сети).

Время в боте берётся из pump_bot.now() — тесты подменяют эту функцию,
поэтому 4 часа рынка прогоняются за доли секунды.

Запуск:  python -m unittest discover -s tests -v
"""

import asyncio
import os
import random
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pump_bot  # noqa: E402

CONFIG_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json")

POLL_SEC = 10.0
START_TS = 1_700_000_000.0
NOISE_PCT = 0.08          # шум цены на тик, ±%
CALM_VOL_INC = 1_000.0    # прирост кумулятивного 24h-объёма на тик, USD
BASE_24H_VOL = 50_000_000.0


def make_cfg(**sections):
    """config.json с точечными правками по секциям."""
    cfg = pump_bot.load_config(CONFIG_PATH)
    for section, values in sections.items():
        cfg[section].update(values)
    return cfg


class Sim:
    """Синтетический рынок: два символа на двух биржах, управляемое время."""

    def __init__(self, cfg, seed=7):
        self.cfg = cfg
        self.detector = pump_bot.Detector(cfg)
        self.t = START_TS
        self.bases = {"XYZUSDT": 0.04, "BTCUSDT": 60_000.0}
        self.cum = {sym: BASE_24H_VOL for sym in self.bases}
        self.drift = {sym: 1.0 for sym in self.bases}
        self.rng = random.Random(seed)
        self.alerts = []

    # -- управление сценарием ------------------------------------------- #

    def set_drift(self, symbol, pct_per_15m):
        """Плавный тренд: pct_per_15m процентов за 15-минутное окно."""
        self.drift[symbol] = (1.0 + pct_per_15m / 100.0) ** (POLL_SEC / 900.0)

    def jump(self, symbol, pct, vol_multiple):
        """Мгновенный скачок цены на pct% с объёмным всплеском vol_multiple x baseline."""
        self.bases[symbol] *= (1.0 + pct / 100.0)
        baseline_15m = CALM_VOL_INC * (900.0 / POLL_SEC)
        extra = baseline_15m * vol_multiple - baseline_15m
        return self.step(extra_vol={symbol: extra})

    def step(self, extra_vol=None, vol_inc=CALM_VOL_INC):
        extra_vol = extra_vol or {}
        snapshots = []
        for symbol in self.bases:
            self.bases[symbol] *= self.drift[symbol]
            self.cum[symbol] += vol_inc + extra_vol.get(symbol, 0.0)
            for exchange in ("BINANCE", "BYBIT"):
                noise = 1.0 + self.rng.uniform(-NOISE_PCT, NOISE_PCT) / 100.0
                snapshots.append(pump_bot.Snapshot(
                    exchange=exchange,
                    symbol=symbol,
                    price=self.bases[symbol] * noise,
                    quote_volume_24h=self.cum[symbol],
                    funding_rate=0.0018 if exchange == "BYBIT" else None,
                ))
        alerts = self.detector.step(snapshots)
        self.alerts.extend((self.t, a) for a in alerts)
        self.t += POLL_SEC
        return alerts

    def advance(self, minutes):
        for _ in range(int(minutes * 60 / POLL_SEC)):
            self.step()

    def alerts_for(self, symbol):
        return [(ts, a) for ts, a in self.alerts if a.symbol == symbol]


class DetectionTestCase(unittest.TestCase):
    def setUp(self):
        self._real_now = pump_bot.now
        self.sim = None
        pump_bot.now = lambda: self.sim.t if self.sim else START_TS

    def tearDown(self):
        pump_bot.now = self._real_now

    # 1 ------------------------------------------------------------------ #
    def test_calm_market_gives_no_signals(self):
        """4 часа спокойного рынка (шум ±0.08%) — ни одного сигнала."""
        self.sim = Sim(make_cfg())
        self.sim.advance(240)
        self.assertEqual(self.sim.alerts, [], "на спокойном рынке сигналов быть не должно")

    # 2 ------------------------------------------------------------------ #
    def test_pump_is_detected(self):
        """Памп +9%/15м с объёмом 6x — сигнал есть, z > 4, обе биржи в одном алерте."""
        self.sim = Sim(make_cfg())
        self.sim.advance(180)
        self.assertEqual(self.sim.alerts, [], "до пампа сигналов нет")

        self.sim.jump("XYZUSDT", 9.0, vol_multiple=6.0)
        found = self.sim.alerts_for("XYZUSDT")
        self.assertEqual(len(found), 1, "ожидался ровно один алерт по XYZUSDT")

        alert = found[0][1]
        sig = alert.primary
        self.assertGreater(sig.zscore, 4.0, "z-score должен превысить порог 4")
        self.assertGreaterEqual(sig.move_15m, 6.0)
        self.assertGreaterEqual(sig.vol_mult, 4.0)
        self.assertEqual(alert.exchanges, ["BINANCE", "BYBIT"], "дедуп: одна монета — один алерт на две биржи")
        self.assertIn("MAIN", sig.triggers)

        message = pump_bot.render_alert(alert, self.sim.cfg)
        self.assertIn("PUMP: XYZ/USDT", message)
        self.assertIn("[BINANCE + BYBIT]", message)
        self.assertIn("Funding: 0.180%", message)
        self.assertIn("24h Vol: $51.5M", message)

    # 3 ------------------------------------------------------------------ #
    def test_btc_filter_raises_thresholds(self):
        """+6.5% проходит при btc_mult=1.0 и отсекается, когда BTC сам летит вверх."""
        def scenario(btc_pumping):
            self.sim = Sim(make_cfg())
            self.sim.advance(180)
            if btc_pumping:
                self.sim.set_drift("BTCUSDT", 2.5)
            self.sim.advance(20)
            self.assertEqual(
                self.sim.detector.btc_multiplier(),
                1.3 if btc_pumping else 1.0,
                "btc_mult рассчитан неверно",
            )
            self.sim.jump("XYZUSDT", 6.5, vol_multiple=6.0)
            return self.sim.alerts_for("XYZUSDT")

        self.assertEqual(len(scenario(btc_pumping=False)), 1, "при спокойном BTC +6.5% должен дать сигнал")
        self.assertEqual(scenario(btc_pumping=True), [], "при пампе BTC порог 6.0*1.3=7.8% отсекает +6.5%")

    # 4 ------------------------------------------------------------------ #
    def test_cooldown_suppresses_second_signal(self):
        """Второй памп той же монеты через 5 минут подавлен cooldown'ом."""
        def scenario(cooldown_min):
            self.sim = Sim(make_cfg(alerts={"cooldown_min": cooldown_min}))
            self.sim.advance(180)
            first = self.sim.jump("XYZUSDT", 9.0, vol_multiple=6.0)
            self.assertEqual(len(first), 1, "первый памп должен дать алерт")
            t_first = self.sim.t - POLL_SEC
            self.sim.advance(5)
            self.sim.jump("XYZUSDT", 9.0, vol_multiple=6.0)
            return t_first, self.sim.alerts_for("XYZUSDT")

        t_first, with_cooldown = scenario(60)
        self.assertEqual(len(with_cooldown), 1, "в пределах cooldown должен остаться один алерт")
        self.assertEqual(with_cooldown[0][0], t_first)

        _, without_cooldown = scenario(0)
        self.assertGreater(len(without_cooldown), 1,
                           "без cooldown повторный памп проходит — значит подавил именно cooldown")

    # 5 ------------------------------------------------------------------ #
    def test_negative_volume_delta_returns_none(self):
        """Отрицательная дельта кумулятивного объёма → интервальный объём None, без падения."""
        state = pump_bot.SymbolState.create(START_TS, ticks_maxlen=100, history_maxlen=100)
        state.add_tick(START_TS, 1.0, 5_000_000.0)
        state.add_tick(START_TS + 900, 1.1, 4_000_000.0)  # 24h-окно скользнуло
        self.assertIsNone(state.interval_volume(900), "отрицательная дельта должна давать None")
        self.assertAlmostEqual(state.move_pct(900), 10.0, places=6)

        # тот же случай внутри полного прохода детектора: пампа нет, исключений нет
        self.sim = Sim(make_cfg())
        self.sim.advance(180)
        before = len(self.sim.alerts)
        self.sim.bases["XYZUSDT"] *= 1.09
        self.sim.step(vol_inc=-2_000_000.0)
        self.assertEqual(len(self.sim.alerts), before,
                         "при None-объёме сигнал не выдаётся и цикл не падает")


class StateSnapshotTestCase(unittest.TestCase):
    """Снимок состояния: восстановление статистики без 120-минутного прогрева."""

    def setUp(self):
        self._real_now = pump_bot.now
        self.sim = None
        pump_bot.now = lambda: self.sim.t if self.sim else START_TS
        self.tmpdir = tempfile.mkdtemp(prefix="pumpbot-state-")

    def tearDown(self):
        pump_bot.now = self._real_now
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def test_state_roundtrip_skips_warmup(self):
        cfg = make_cfg()
        path = os.path.join(self.tmpdir, "state.json.gz")

        # 3 часа рынка → снимок на диск
        self.sim = Sim(cfg)
        self.sim.advance(180)
        saved_at = self.sim.t
        self.assertTrue(pump_bot.write_state_file(path, self.sim.detector.dump_state(360)))
        self.assertTrue(os.path.getsize(path) > 0)

        # новый бот с чистой памятью поднимает снимок и сразу видит пампы
        fresh = Sim(cfg)
        self.sim = fresh                              # часы теста следуют за активным ботом
        fresh.t = saved_at + 120                      # рестарт занял 2 минуты
        restored = fresh.detector.load_state(pump_bot.read_state_file(path), max_age_sec=43200)
        self.assertGreater(restored, 0, "снимок должен поднять хотя бы один символ")

        key = pump_bot.Detector.key("BINANCE", "XYZUSDT")
        state = fresh.detector.states[key]
        self.assertGreaterEqual(len(state.moves_15m), cfg["detection"]["min_observations"])
        self.assertEqual(len(state.ticks), 0, "тики в снимок не пишутся — они не нужны")
        self.assertLess(state.first_seen, fresh.t - cfg["universe"]["min_history_minutes"] * 60,
                        "first_seen из снимка снимает 120-минутный прогрев")

        # 15 минут новых тиков (набор окна) + памп → сигнал есть
        fresh.advance(16)
        self.assertEqual(fresh.alerts, [], "на наборе окна сигналов нет")
        fresh.jump("XYZUSDT", 9.0, vol_multiple=6.0)
        found = fresh.alerts_for("XYZUSDT")
        self.assertEqual(len(found), 1, "после восстановления снимка памп детектируется")
        self.assertGreater(found[0][1].primary.zscore, 4.0)

    def test_stale_and_broken_state_fall_back_to_cold_start(self):
        cfg = make_cfg()
        path = os.path.join(self.tmpdir, "state.json.gz")

        self.sim = Sim(cfg)
        self.sim.advance(180)
        pump_bot.write_state_file(path, self.sim.detector.dump_state(360))

        stale = Sim(cfg)
        stale.t = self.sim.t + 13 * 3600
        self.sim = stale              # снимку 13 часов — старше лимита 12 ч
        self.assertEqual(stale.detector.load_state(pump_bot.read_state_file(path), max_age_sec=43200), 0)

        broken = os.path.join(self.tmpdir, "broken.json.gz")
        with open(broken, "wb") as fh:
            fh.write(b"not a gzip at all")
        self.sim = Sim(cfg)
        self.assertIsNone(pump_bot.read_state_file(broken), "битый файл — не исключение, а None")
        self.assertIsNone(pump_bot.read_state_file(os.path.join(self.tmpdir, "missing.gz")))
        self.assertEqual(self.sim.detector.load_state({"version": 99}, max_age_sec=43200), 0)

    def test_cooldown_survives_restart(self):
        """После рестарта тот же памп не прилетает в канал повторно."""
        cfg = make_cfg()
        path = os.path.join(self.tmpdir, "state.json.gz")

        self.sim = Sim(cfg)
        self.sim.advance(180)
        self.sim.jump("XYZUSDT", 9.0, vol_multiple=6.0)
        self.assertEqual(len(self.sim.alerts_for("XYZUSDT")), 1)
        pump_bot.write_state_file(path, self.sim.detector.dump_state(360))

        fresh = Sim(cfg)
        fresh.t = self.sim.t + 60
        self.sim = fresh
        fresh.detector.load_state(pump_bot.read_state_file(path), max_age_sec=43200)
        self.assertIn("XYZUSDT", fresh.detector.last_alert_ts, "cooldown должен переехать в снимке")
        fresh.bases["XYZUSDT"] *= 1.09
        fresh.advance(16)
        self.assertEqual(fresh.alerts_for("XYZUSDT"), [],
                         "cooldown из снимка гасит повторный алерт после рестарта")


if __name__ == "__main__":
    unittest.main(verbosity=2)


class BackfillTestCase(unittest.TestCase):
    """Backfill из свечей: слепой зоны после старта и разрывов нет."""

    def setUp(self):
        self._real_now = pump_bot.now
        self.t = START_TS
        pump_bot.now = lambda: self.t

    def tearDown(self):
        pump_bot.now = self._real_now

    # -- синтетические свечи --------------------------------------------- #

    def candles(self, minutes=240, pump_pct=0.0, pump_vol_mult=1.0, base=0.04, vol=60_000.0):
        """Минутные свечи: спокойный рынок, при pump_pct — скачок на последней минуте."""
        rng = random.Random(11)
        out = []
        price = base
        start = self.t - minutes * 60
        for i in range(minutes):
            price = base * (1.0 + rng.uniform(-NOISE_PCT, NOISE_PCT) / 100.0)
            quote_vol = vol
            if pump_pct and i == minutes - 1:
                price = base * (1.0 + pump_pct / 100.0)
                quote_vol = vol * pump_vol_mult * 15  # весь всплеск в последнюю минуту окна
            out.append(pump_bot.Candle(ts=start + i * 60, close=price, quote_volume=quote_vol))
        return out

    def snapshots(self, candles, symbol="XYZUSDT"):
        last = candles[-1]
        return [pump_bot.Snapshot(exchange=ex, symbol=symbol, price=last.close,
                                  quote_volume_24h=BASE_24H_VOL, funding_rate=None)
                for ex in ("BINANCE", "BYBIT")]

    def make_backfill(self, cfg, fetcher):
        return pump_bot.Backfill(cfg, fetcher, weight_source=lambda: 100.0,
                                 sleeper=lambda _sec: asyncio.sleep(0))

    # 1 ------------------------------------------------------------------ #
    def test_backfill_detects_pump_at_startup(self):
        """Свечи с пампом +10%/15м и объёмом 6x → алерт сразу, без прогрева."""
        cfg = make_cfg()
        candles = self.candles(pump_pct=10.0, pump_vol_mult=6.0)

        async def fetcher(exchange, symbol, limit):
            return candles

        detector = pump_bot.Detector(cfg)
        stats = asyncio.run(self.make_backfill(cfg, fetcher).run(detector, self.snapshots(candles)))
        self.assertEqual(stats["seeded"], 2, "должны подняться оба ключа биржа:символ")

        detector.startup_pass = True
        self.t += 60
        alerts = detector.step(self.snapshots(candles))
        self.assertEqual(len(alerts), 1, "памп из свечей обнаруживается на первом же проходе")
        alert = alerts[0]
        self.assertTrue(alert.at_startup)
        self.assertGreater(alert.primary.zscore, 4.0)
        self.assertIn("⏱ обнаружен при старте", pump_bot.render_alert(alert, cfg))

    # 2 ------------------------------------------------------------------ #
    def test_backfill_failure_falls_back_to_warmup(self):
        """Пустой ответ и исключение — не падаем, работает обычный прогрев."""
        cfg = make_cfg()
        candles = self.candles()

        async def empty(exchange, symbol, limit):
            return None

        async def boom(exchange, symbol, limit):
            raise RuntimeError("биржа недоступна")

        for fetcher in (empty, boom):
            detector = pump_bot.Detector(cfg)
            stats = asyncio.run(self.make_backfill(cfg, fetcher).run(detector, self.snapshots(candles)))
            self.assertEqual(stats["seeded"], 0)
            self.assertEqual(stats["failed"], 2)
            self.assertEqual(detector.states, {}, "без свечей история не подсевается")

        # прогрев остаётся обязательным там, где backfill не удался
        detector = pump_bot.Detector(cfg)
        detector.step(self.snapshots(candles))
        key = pump_bot.Detector.key("BINANCE", "XYZUSDT")
        self.assertFalse(detector.states[key].backfilled)

    # 3 ------------------------------------------------------------------ #
    def test_backfill_calm_market_no_false_alerts(self):
        """Спокойные свечи → при старте ложных алертов нет."""
        cfg = make_cfg()
        candles = self.candles()

        async def fetcher(exchange, symbol, limit):
            return candles

        detector = pump_bot.Detector(cfg)
        asyncio.run(self.make_backfill(cfg, fetcher).run(detector, self.snapshots(candles)))
        detector.startup_pass = True
        self.t += 60
        self.assertEqual(detector.step(self.snapshots(candles)), [],
                         "на спокойной истории старт не должен давать сигналов")

    # 4 ------------------------------------------------------------------ #
    def test_incremental_backfill_has_no_duplicates(self):
        """Разрыв 5 минут → добираются только пропущенные минуты, дублей нет."""
        cfg = make_cfg()
        first = self.candles(minutes=240)

        async def fetch_first(exchange, symbol, limit):
            return first

        detector = pump_bot.Detector(cfg)
        backfill = self.make_backfill(cfg, fetch_first)
        asyncio.run(backfill.run(detector, self.snapshots(first)))

        key = pump_bot.Detector.key("BINANCE", "XYZUSDT")
        state = detector.states[key]
        moves_before = len(state.moves_15m)
        ticks_before = len(state.ticks)
        last_tick_before = state.ticks[-1][0]

        # прошло 5 минут без данных, свечи продлились на 5 минут вперёд
        self.t += 5 * 60
        second = self.candles(minutes=245)

        async def fetch_second(exchange, symbol, limit):
            return second

        asyncio.run(self.make_backfill(cfg, fetch_second).run(
            detector, self.snapshots(second), minutes=5.0, label="разрыв"))

        state = detector.states[key]
        self.assertEqual(len(state.moves_15m), moves_before + 5,
                         "добавиться должны ровно 5 наблюдений — по числу пропущенных минут")
        self.assertEqual(len(state.ticks), ticks_before + 5)
        self.assertGreater(state.ticks[-1][0], last_tick_before)
        self.assertEqual(len(set(t[0] for t in state.ticks)), len(state.ticks),
                         "дублей по времени в тиках быть не должно")

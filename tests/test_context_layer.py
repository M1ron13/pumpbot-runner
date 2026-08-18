"""Тесты контекст-слоя — все 10 пунктов ТЗ.

Сеть не трогается: источники подменяются заглушками. Проверяется главное — что слой
не врёт про отсутствие новостей, не матчит омонимы, не задерживает алерт и не падает.
"""

import asyncio
import os
import shutil
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pump_bot                                        # noqa: E402
from context import config as ctx_config               # noqa: E402
from context import cross_listing, dedup, enrich, render  # noqa: E402
from context.cache import Cache                        # noqa: E402
from context.classify import Classifier, apply_rules, parse_response  # noqa: E402
from context.matcher import TickerMatcher              # noqa: E402
from context.sources import market, news               # noqa: E402

NOW = 1_700_000_000.0
COINS = [
    {"symbol": "SUN", "name": "Sun Token", "coin_id": "sun-token"},
    {"symbol": "ID", "name": "SPACE ID", "coin_id": "space-id"},
    {"symbol": "ONE", "name": "Harmony", "coin_id": "harmony"},
    {"symbol": "XYZTOKEN", "name": "XyzToken", "coin_id": "xyztoken"},
]


def make_alert(symbol="XYZUSDT", ts=NOW):
    signal = pump_bot.Signal(
        exchange="BINANCE", symbol=symbol, triggers=["MAIN"], move_15m=11.2, move_5m=8.2,
        zscore=7.4, vol_mult=6.3, price=0.0432, volume_24h=38_200_000.0, funding_rate=0.0018,
        ts=ts, oi_read="SQUEEZE", oi_change_pct=-7.5, event_context="анлок через 3.0 дн · 4.2% supply",
    )
    return pump_bot.Alert(symbol=symbol, exchanges=["BINANCE"], primary=signal, ts=ts)


class LayerTestBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="ctx-test-")
        self.cfg = ctx_config.load()
        self.cfg["cache_db"] = os.path.join(self.tmpdir, "cache.db")
        self.cache = Cache(self.cfg["cache_db"])
        self.bot_cfg = pump_bot.load_config(
            os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "config.json"))

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def collect(self, ticker="XYZTOKEN", symbol="XYZUSDT", session=None, unlock=None):
        return asyncio.run(enrich.collect(session, self.cfg, self.cache, ticker=ticker,
                                          symbol=symbol, now_ts=NOW, unlock_text=unlock))


class CatalystTestCase(LayerTestBase):
    """п.1 и п.2: есть листинг в кэше → КАТАЛИЗАТОР; пусто → «новостей не найдено»."""

    def test_fresh_listing_becomes_catalyst(self):
        self.cache.add_event(ts=NOW - 700, source="UPBIT", event_type="LISTING",
                             ticker="XYZTOKEN", title="XyzToken (XYZTOKEN) 거래지원 안내",
                             url="https://upbit.com/notice/1")
        for source in ("derivatives", "dexscreener", "tavily", "cryptopanic"):
            self.cfg["enabled_sources"][source] = False

        context = self.collect()
        self.assertEqual(context["verdict"], render.VERDICT_CATALYST)
        block = render.render(context, self.cfg, NOW)
        self.assertIn("КАТАЛИЗАТОР", block)
        self.assertIn("листинг", block)
        self.assertIn("https://upbit.com/notice/1", block, "источник обязателен в блоке")
        self.assertIn("Шорт против новости", block)

    def test_empty_sources_say_no_public_news(self):
        for source in ("derivatives", "dexscreener", "tavily", "cryptopanic"):
            self.cfg["enabled_sources"][source] = False
        context = self.collect()
        block = render.render(context, self.cfg, NOW)
        self.assertEqual(context["verdict"], render.VERDICT_CLEAN)
        self.assertIn("Публичных новостей не найдено", block)
        self.assertIn("инсайдерский катализатор не исключён", block,
                      "формулировка обязана оставлять место инсайдерскому пампу")


class BudgetTestCase(LayerTestBase):
    """п.3 и п.9: мёртвые и медленные источники не задерживают алерт."""

    class SlowSession:
        def __init__(self, delay):
            self.delay = delay

        def get(self, *a, **kw):
            raise RuntimeError("источник мёртв")

        def post(self, *a, **kw):
            raise RuntimeError("источник мёртв")

    def test_all_sources_dead_alert_still_goes(self):
        self.cfg["enabled_sources"]["derivatives"] = True
        self.cfg["enabled_sources"]["dexscreener"] = True
        started = time.time()
        context = self.collect(session=self.SlowSession(0))
        elapsed = time.time() - started
        self.assertLess(elapsed, 3.5, "слой обязан вернуться в бюджете даже когда всё мертво")
        self.assertTrue(context["sources_failed"], "падения источников должны быть зафиксированы")
        block = render.render(context, self.cfg, NOW)
        self.assertTrue(block)

    def test_slow_source_is_cut_by_budget(self):
        async def slow(*_args, **_kwargs):
            await asyncio.sleep(10)
            return {"long_short_ratio": 9.9}

        original = market.derivatives
        market.derivatives = slow
        self.cfg["budget"]["per_source_ms"] = 300
        self.cfg["budget"]["total_ms"] = 800
        try:
            started = time.time()
            context = self.collect(session=object())
            elapsed = time.time() - started
        finally:
            market.derivatives = original
        self.assertLess(elapsed, 1.5, "задержка источника 10 с не должна держать алерт")
        self.assertFalse(context["derivatives"], "данные медленного источника просто опускаются")

    def test_enrich_alert_never_raises(self):
        """Хук бота получает None/строку, но никогда исключение."""
        broken_cfg = {"enabled": True, "budget": {"total_ms": 100},
                      "derivatives_flags": {"high_long_short_ratio": 2.5}}
        result = asyncio.run(enrich.enrich_alert(make_alert(), self.bot_cfg, session=object(),
                                                cfg=broken_cfg, cache=self.cache))
        self.assertIsNone(result)


class MatchingTestCase(LayerTestBase):
    """п.4: омонимы."""

    def test_homonyms_are_not_matched(self):
        matcher = TickerMatcher(self.cfg, self.cache)
        for text in ("Justin Sun made a statement about markets",
                     "ID card reform passed in parliament",
                     "One thing about the market today",
                     "Up to 10,000 USDT in rewards"):
            self.assertIsNone(matcher.match(text, "test", candidates=COINS),
                              f"ложный матч на тексте: {text}")

    def test_project_name_with_ticker_matches(self):
        matcher = TickerMatcher(self.cfg, self.cache)
        got = matcher.match("Sun Token (SUN) announces staking upgrade", "test", candidates=COINS)
        self.assertIsNotNone(got)
        self.assertEqual(got["ticker"], "SUN")

    def test_every_decision_is_logged(self):
        matcher = TickerMatcher(self.cfg, self.cache)
        matcher.match("Sun Token (SUN) announces staking", "test", candidates=COINS)
        rows = self.cache.conn.execute("SELECT decision, rule FROM match_log").fetchall()
        self.assertTrue(rows, "каждое сопоставление обязано попадать в match_log")


class DedupTestCase(LayerTestBase):
    """п.5: восемь пересказов одного события — один item."""

    def test_eight_retellings_collapse(self):
        base = "Upbit will list XyzToken (XYZTOKEN) in KRW market"
        variants = [
            base,
            "Upbit to list XyzToken (XYZTOKEN) in the KRW market",
            "XyzToken (XYZTOKEN) listing on Upbit KRW market announced",
            "Upbit lists XyzToken XYZTOKEN in KRW market",
            "Upbit announces XyzToken (XYZTOKEN) KRW market listing",
            "XyzToken to be listed on Upbit KRW market",
            "Upbit will list XyzToken XYZTOKEN KRW",
            "Upbit listing: XyzToken (XYZTOKEN), KRW market",
        ]
        items = [{"title": t, "ts": NOW, "source_tier": 3} for t in variants]
        clustered = dedup.cluster(items, self.cfg)
        self.assertEqual(len(clustered), 1, f"ожидался 1 кластер, получено {len(clustered)}")
        self.assertEqual(clustered[0]["duplicates"], 7)

    def test_listing_and_delisting_never_merge(self):
        """Заголовки различаются одним словом, но смысл противоположный."""
        items = [
            {"title": "Upbit will list XyzToken (XYZTOKEN) in KRW market", "ts": NOW, "source_tier": 3},
            {"title": "Upbit will delist XyzToken (XYZTOKEN) from KRW market", "ts": NOW, "source_tier": 3},
        ]
        clustered = dedup.cluster(items, self.cfg)
        self.assertEqual(len(clustered), 2,
                         "листинг и делистинг обязаны остаться разными событиями")

    def test_different_events_stay_separate(self):
        items = [
            {"title": "Upbit will list XyzToken in KRW market", "ts": NOW, "source_tier": 3},
            {"title": "SEC charges exchange over unregistered securities", "ts": NOW, "source_tier": 1},
        ]
        self.assertEqual(len(dedup.cluster(items, self.cfg)), 2)


class ClassificationTestCase(LayerTestBase):
    """п.5 и п.10: правила вердиктов — в коде и из конфига; битый JSON не роняет слой."""

    def test_catalyst_rule_from_config(self):
        rules = self.cfg["classification"]["catalyst"]
        good = [{"headline_id": 1, "direction": "bullish",
                 "confidence": rules["min_confidence"], "is_fact": True, "source_tier": 3}]
        weak = [{"headline_id": 1, "direction": "bullish",
                 "confidence": rules["min_confidence"] - 0.1, "is_fact": True, "source_tier": 3}]
        rumor_tier3 = [{"headline_id": 1, "direction": "bullish",
                        "confidence": 0.9, "is_fact": False, "source_tier": 3}]
        rumor_tier2 = [{"headline_id": 1, "direction": "bullish",
                        "confidence": 0.9, "is_fact": False, "source_tier": 2}]
        self.assertEqual(len(apply_rules(good, self.cfg)["catalysts"]), 1)
        self.assertEqual(len(apply_rules(weak, self.cfg)["catalysts"]), 0,
                         "ниже порога уверенности — не катализатор")
        self.assertEqual(len(apply_rules(rumor_tier3, self.cfg)["catalysts"]), 0,
                         "слух из крипто-медиа катализатором не является")
        self.assertEqual(len(apply_rules(rumor_tier2, self.cfg)["catalysts"]), 1,
                         "слух от Reuters/Bloomberg проходит по правилу source_tier ≤ 2")

    def test_bearish_rule_from_config(self):
        items = [{"headline_id": 1, "direction": "bearish",
                  "confidence": self.cfg["classification"]["bearish"]["min_confidence"]}]
        self.assertEqual(len(apply_rules(items, self.cfg)["bearish"]), 1)

    def test_invalid_json_is_retried_then_gives_up(self):
        calls = {"n": 0}

        class FakeClassifier(Classifier):
            @property
            def provider(self):
                return "groq"

            async def _call(self, symbol, headlines):
                calls["n"] += 1
                return "это не json"

        self.cfg["keys"]["groq"] = "test-key"
        classifier = FakeClassifier(self.cfg, session=object(), cache=self.cache)
        outcome = asyncio.run(classifier.classify("XYZUSDT", [{"title": "что-то"}]))
        self.assertEqual(calls["n"], int(self.cfg["classification"]["retries"]) + 1,
                         "невалидный JSON → один повтор")
        self.assertTrue(outcome["status"].startswith("классификация не удалась"))
        self.assertEqual(outcome["items"], [])

    def test_parse_response_extracts_json_from_noise(self):
        items = parse_response('Вот ответ: {"items": [{"headline_id": 1}]} — всё')
        self.assertEqual(items, [{"headline_id": 1}])
        self.assertIsNone(parse_response("совсем не json"))

    def test_no_provider_means_no_llm(self):
        self.cfg["keys"]["groq"] = ""
        outcome = asyncio.run(Classifier(self.cfg, session=object(), cache=self.cache)
                              .classify("XYZUSDT", [{"title": "что-то"}]))
        self.assertEqual(outcome["status"], "нет провайдера LLM")


class CrossListingTestCase(LayerTestBase):
    """п.7 и п.8: класс листинга и медвежий сетап на запуск перпа."""

    def test_unlisted_coin_on_major_is_strong(self):
        verdict = cross_listing.classify(
            {"event_type": "LISTING", "source": "UPBIT", "ticker": "XYZTOKEN"}, [], self.cfg)
        self.assertEqual(verdict["class"], cross_listing.STRONG_BULLISH)
        self.assertTrue(verdict["post"])

    def test_already_on_binance_is_weak_and_not_posted(self):
        listed = [{"exchange": "BINANCE", "market": "perp", "symbol": "XYZUSDT", "first_seen": NOW}]
        verdict = cross_listing.classify(
            {"event_type": "LISTING", "source": "BYBIT", "ticker": "XYZTOKEN"}, listed, self.cfg)
        self.assertEqual(verdict["class"], cross_listing.WEAK)
        self.assertFalse(verdict["post"], "WEAK-листинги в канал не идут")

    def test_perp_for_spot_only_coin_is_bearish_setup(self):
        listed = [{"exchange": "BINANCE", "market": "spot", "symbol": "XYZUSDT", "first_seen": NOW}]
        verdict = cross_listing.classify(
            {"event_type": "PERP_LAUNCH", "source": "BINANCE", "ticker": "XYZTOKEN",
             "market": "perp"}, listed, self.cfg)
        self.assertEqual(verdict["class"], cross_listing.BEARISH_SETUP)
        self.assertIn("возможность шортить", verdict["reason"])

    def test_delisting_is_operational_and_posted(self):
        verdict = cross_listing.classify(
            {"event_type": "DELISTING", "source": "BINANCE", "ticker": "XYZTOKEN"}, [], self.cfg)
        self.assertEqual(verdict["class"], cross_listing.OPERATIONAL)
        self.assertTrue(verdict["post"])

    def test_listing_play_message_has_no_advice(self):
        verdict = {"class": cross_listing.STRONG_BULLISH, "reason": "новая аудитория"}
        text = cross_listing.render_listing_play(
            {"event_type": "LISTING", "source": "UPBIT", "ticker": "XYZTOKEN", "market": "спот"},
            verdict, [], price_change_pct=18.4, minutes_ago=9, trading_starts="20 авг 10:00 UTC")
        self.assertIn("LISTING: XYZTOKEN → UPBIT", text)
        self.assertIn("+18.4%", text)
        self.assertIn("дамп на открытии торгов", text)
        for advice in ("покупай", "buy", "вход в лонг", "рекомендуем"):
            self.assertNotIn(advice.lower(), text.lower(), "рекомендаций в сообщении быть не должно")


class RenderTestCase(LayerTestBase):
    """п.7: формат блока, отсутствие «n/a», данные деривативов и DEX."""

    def test_no_na_lines(self):
        context = {"verdict": render.VERDICT_CLEAN, "derivatives": {}, "dex": {}}
        block = render.render(context, self.cfg, NOW)
        self.assertNotIn("n/a", block)
        self.assertNotIn("Деривативы", block, "строка без данных опускается целиком")

    def test_derivatives_and_dex_lines(self):
        context = {
            "verdict": render.VERDICT_CLEAN,
            "derivatives": {"oi_change_pct": 6.2, "oi_window_min": 30,
                            "long_short_ratio": 3.4, "taker_buy_sell_ratio": 1.8},
            "dex": {"dex": "raydium", "chain": "solana", "volume_h24": 2_100_000.0,
                    "price_change_h1": 22.0, "price_change_h24": 41.0},
            "unlock": "анлок через 3.0 дн · 4.2% supply",
        }
        block = render.render(context, self.cfg, NOW)
        self.assertIn("OI +6.2%/30м", block)
        self.assertIn("толпа в лонгах", block)
        self.assertIn("DEX", block)
        self.assertIn("🔓", block)

    def test_unknown_verdict_names_failed_sources(self):
        context = {"verdict": render.VERDICT_UNKNOWN, "sources_failed": ["деривативы: таймаут"]}
        block = render.render(context, self.cfg, NOW)
        self.assertIn("Контекст не проверен", block)
        self.assertIn("деривативы: таймаут", block)


class NewsFilterTestCase(LayerTestBase):
    """Дешёвые фильтры до LLM: свежесть и whitelist источников."""

    def test_stale_and_unknown_sources_are_dropped(self):
        items = [
            {"title": "свежая из Reuters", "url": "https://reuters.com/a", "ts": NOW - 60},
            {"title": "старая из Reuters", "url": "https://reuters.com/b", "ts": NOW - 200_000},
            {"title": "свежая с помойки", "url": "https://random-blog.xyz/c", "ts": NOW - 60},
        ]
        kept = news.cheap_filters(items, self.cfg, NOW)
        self.assertEqual([k["title"] for k in kept], ["свежая из Reuters"])
        self.assertEqual(kept[0]["source_tier"], 2, "Reuters — второй тир")


if __name__ == "__main__":
    unittest.main(verbosity=2)

"""Тесты publisher на белом списке — раздел 6 ТЗ.

Логика тестов зеркальна логике модуля: сначала проверяем, что каждый разрешённый тип
проходит, затем — что **каждый** пункт запретного списка не проходит и получает
правильную метку. Плюс grep-тест: отправка Telegram не должна существовать нигде,
кроме publisher и пути алертов бота.
"""

import asyncio
import glob
import io
import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from context import config as ctx_config           # noqa: E402
from context import publisher as pub               # noqa: E402
from context.cache import Cache                    # noqa: E402

NOW = 1_700_000_000.0
BOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Sender:
    def __init__(self, ok=True):
        self.sent = []
        self.ok = ok

    async def __call__(self, text):
        self.sent.append(text)
        return self.ok


class WhitelistBase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="wl-test-")
        self.cfg = ctx_config.load()
        self.cache = Cache(os.path.join(self.tmpdir, "cache.db"))
        self.sender = Sender()
        # монета XYZ есть в нашем universe (перп Binance), NOWHERE — нет нигде
        self.cache.apply_instruments("BINANCE", "perp", ["XYZUSDT"], now_ts=NOW - 7200)
        self.cache.apply_instruments("BINANCE", "perp", ["XYZUSDT"], now_ts=NOW - 60)
        self.cache.apply_instruments("BINANCE", "spot", ["SPOTONLYUSDT"], now_ts=NOW - 7200)
        self.cache.apply_instruments("BINANCE", "spot", ["SPOTONLYUSDT"], now_ts=NOW - 60)

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def publisher(self, **overrides):
        base = {**self.cfg["publisher"], "enabled": True, "shadow_types": []}
        self.cfg["publisher"] = {**base, **overrides}
        return pub.Publisher(self.cfg, self.cache, self.sender)

    def event(self, **kwargs):
        base = {"source": "BINANCE", "event_type": "LISTING", "ticker": "NEWCOIN",
                "title": "Binance will list NewCoin (NEWCOIN)", "url": "https://ex/1",
                "raw_type": None}
        return {**base, **kwargs}

    def news(self, **kwargs):
        base = {"ticker": "XYZ", "category": "REGULATORY", "is_fact": True, "source_tier": 2,
                "confidence": 0.85, "confirmations": 2, "summary": "Регулятор одобрил заявку",
                "source_name": "Reuters", "url": "https://reuters.com/a", "ts": NOW - 480,
                "event_key": "reg-xyz-1"}
        return {**base, **kwargs}

    def publish_event(self, event, publisher=None, now_ts=NOW):
        publisher = publisher or self.publisher()
        message, label = publisher.message_for(event, now_ts)
        if message is None:
            return False, label
        return asyncio.run(publisher.publish(message, now_ts))

    def publish_news(self, news, publisher=None, now_ts=NOW):
        publisher = publisher or self.publisher()
        message, label = publisher.message_for_news(news, now_ts)
        if message is None:
            return False, label
        return asyncio.run(publisher.publish(message, now_ts))


class AllowedTypesTestCase(WhitelistBase):
    """п.6.1: каждый тип белого списка проходит на валидных данных."""

    def test_new_listing_passes(self):
        sent, label = self.publish_event(self.event(ticker="NEWCOIN", source="BINANCE"))
        self.assertTrue(sent, label)
        self.assertIn("НОВЫЙ ЛИСТИНГ: NEWCOIN", self.sender.sent[0])

    def test_new_listing_on_upbit_passes(self):
        """Upbit — крупная площадка: листинг там сигнал, хотя мы на Upbit не торгуем."""
        sent, label = self.publish_event(self.event(
            source="UPBIT", ticker="NEWCOIN", title="Upbit will list NewCoin (NEWCOIN) KRW"))
        self.assertTrue(sent, label)

    def test_universe_delisting_passes(self):
        sent, label = self.publish_event(self.event(
            event_type="DELISTING", ticker="XYZ", source="BINANCE",
            title="Binance Futures Will Delist XYZUSDT Perpetual Contract (2026-09-14)"))
        self.assertTrue(sent, label)
        text = self.sender.sent[0]
        self.assertIn("ДЕЛИСТИНГ: XYZ", text)
        self.assertIn("14 сен", text, "дата вступления в силу должна извлекаться")
        self.assertIn("BINANCE perp", text)

    def test_first_perp_for_spot_only_passes(self):
        sent, label = self.publish_event(self.event(
            event_type="PERP_LAUNCH", ticker="SPOTONLY", source="BINANCE",
            title="Binance Futures Will Launch SPOTONLYUSDT Perpetual Contract"))
        self.assertTrue(sent, label)
        self.assertIn("ПЕРВЫЙ ПЕРП: SPOTONLY", self.sender.sent[0])

    def test_major_news_passes_when_all_conditions_met(self):
        sent, label = self.publish_news(self.news())
        self.assertTrue(sent, label)
        text = self.sender.sent[0]
        self.assertIn("НОВОСТЬ: XYZ", text)
        self.assertIn("Категория: Регуляторное решение", text)
        self.assertIn("tier 2", text)

    def test_message_has_no_advice_or_bias_words(self):
        self.publish_news(self.news())
        text = self.sender.sent[0].lower()
        for word in ("бычье", "медвежье", "покупай", "продавай", "лонг", "шорт"):
            self.assertNotIn(word, text)


class ForbiddenTestCase(WhitelistBase):
    """п.6.2: каждый пункт запретного списка не проходит и получает метку."""

    def test_out_of_universe_event(self):
        sent, label = self.publish_event(self.event(
            event_type="DELISTING", ticker="TT", source="UPBIT",
            title="썬더코어(TT) 거래지원 종료 안내 (9/14 15:00)"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_OUT_OF_UNIVERSE)

    def test_tradfi_instrument(self):
        sent, label = self.publish_event(self.event(
            ticker="ISRG", source="BYBIT",
            title="New listing: ISRGUSDT TradFi Perpetual Contract"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_TRADFI)

    def test_unparsed_ticker(self):
        sent, label = self.publish_event(self.event(
            ticker=None, source="OKX", title="OKX to list perpetual futures"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_PARSE_FAILED)

    def test_weak_listing(self):
        sent, label = self.publish_event(self.event(
            ticker="XYZ", source="BINANCE", title="Binance will list XYZ (XYZ) spot"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_WEAK)

    def test_internal_signals_are_not_message_types(self):
        for event_type in ("FUNDING_EXTREME_LONG", "FUNDING_EXTREME_SHORT", "BASIS_HOT", "KRW_HOT"):
            sent, label = self.publish_event(self.event(event_type=event_type, ticker="XYZ"))
            self.assertFalse(sent, event_type)
            self.assertEqual(label, pub.LABEL_NOT_WHITELISTED, event_type)

    def test_product_only_change(self):
        sent, label = self.publish_event(self.event(
            event_type="DELISTING", ticker="XYZ", source="BINANCE",
            title="Binance Margin And Loan Will Delist XYZ & POWR"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_PRODUCT)

    def test_message_type_outside_whitelist_is_refused(self):
        message = pub.Message(type="SOMETHING_NEW", ticker="XYZ", source="X",
                              external_id="x1", text="текст")
        sent, label = asyncio.run(self.publisher().publish(message, NOW))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_NOT_WHITELISTED)


class MajorNewsRulesTestCase(WhitelistBase):
    """п.6.9: условия Типа 4 по отдельности."""

    def test_partnership_category_is_refused(self):
        sent, label = self.publish_news(self.news(category="PARTNERSHIP"))
        self.assertFalse(sent)
        self.assertIn("вне закрытого списка", label)

    def test_rumor_is_refused_even_with_tier1_and_high_confidence(self):
        sent, label = self.publish_news(self.news(is_fact=False, confidence=0.95, source_tier=1))
        self.assertFalse(sent)
        self.assertIn("слух", label)

    def test_tier3_without_confirmation_is_refused(self):
        sent, label = self.publish_news(self.news(source_tier=3, confirmations=1))
        self.assertFalse(sent)
        self.assertIn("tier 3", label)

    def test_tier2_with_two_confirmations_passes(self):
        sent, label = self.publish_news(self.news(source_tier=2, confirmations=2))
        self.assertTrue(sent, label)

    def test_tier1_single_source_passes(self):
        sent, label = self.publish_news(self.news(source_tier=1, confirmations=1,
                                                 source_name="SEC"))
        self.assertTrue(sent, label)

    def test_low_confidence_is_refused(self):
        sent, label = self.publish_news(self.news(confidence=0.7))
        self.assertFalse(sent)
        self.assertIn("уверенность", label)

    def test_coin_outside_universe_is_refused(self):
        sent, label = self.publish_news(self.news(ticker="NOWHERE"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_OUT_OF_UNIVERSE)

    def test_market_wide_news_without_ticker_is_refused(self):
        sent, label = self.publish_news(self.news(ticker=None,
                                                 summary="ЕС принял регулирование крипты"))
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_PARSE_FAILED)

    def test_daily_limit_then_only_tier1_high_confidence(self):
        publisher = self.publisher()
        for i in range(5):
            sent, label = self.publish_news(
                self.news(source_tier=1, confirmations=1, event_key=f"k{i}"),
                publisher=publisher)
            self.assertTrue(sent, f"{i}: {label}")

        sent, label = self.publish_news(self.news(confidence=0.85, source_tier=2,
                                                 event_key="k-over-1"), publisher=publisher)
        self.assertFalse(sent, "6-е сообщение при 0.85/tier2 не должно проходить")
        self.assertIn("лимит", label)

        sent, label = self.publish_news(self.news(confidence=0.92, source_tier=1, confirmations=1,
                                                 event_key="k-over-2"), publisher=publisher)
        self.assertTrue(sent, f"tier 1 при 0.92 проходит сверх лимита: {label}")

    def test_major_news_is_in_shadow_by_default(self):
        """Тип 4 при первом запуске обязан только логировать WOULD SEND."""
        default_shadow = ctx_config.load()["publisher"]["shadow_types"]
        self.assertIn("MAJOR_NEWS", default_shadow,
                      "по умолчанию Тип 4 должен быть в режиме тени")
        publisher = self.publisher(shadow_types=["MAJOR_NEWS"])
        sent, label = self.publish_news(self.news(), publisher=publisher)
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_SHADOW)
        self.assertEqual(self.sender.sent, [])


class GuardChainTestCase(WhitelistBase):
    """п.6.3-6.6: дедуп, лимиты, kill-switch, режим тени."""

    def test_dedup_by_source_and_external_id(self):
        publisher = self.publisher()
        first = self.publish_event(self.event(), publisher=publisher)
        second = self.publish_event(self.event(), publisher=publisher)
        self.assertTrue(first[0])
        self.assertEqual(second[1], pub.LABEL_DUP)
        self.assertEqual(len(self.sender.sent), 1)

    def test_hourly_limit(self):
        publisher = self.publisher(max_per_hour=6)
        for i in range(6):
            sent, label = self.publish_event(
                self.event(ticker=f"COIN{i}", url=f"https://ex/{i}"), publisher=publisher)
            self.assertTrue(sent, f"{i}: {label}")
        sent, label = self.publish_event(
            self.event(ticker="COIN7", url="https://ex/7"), publisher=publisher)
        self.assertFalse(sent, "седьмое сообщение за час должно быть отклонено")
        self.assertIn(pub.LABEL_RATE, label)

    def test_kill_switch_blocks_everything(self):
        publisher = self.publisher(enabled=False)
        for event in (self.event(), self.event(event_type="DELISTING", ticker="XYZ")):
            sent, label = self.publish_event(event, publisher=publisher)
            self.assertFalse(sent)
        self.assertEqual(self.sender.sent, [])

    def test_shadow_mode_logs_would_send(self):
        publisher = self.publisher(shadow_types=["NEW_LISTING"])
        sent, label = self.publish_event(self.event(), publisher=publisher)
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_SHADOW)
        self.assertEqual(self.sender.sent, [])

    def test_shadow_does_not_consume_dedup_key(self):
        """Событие, придержанное тенью, должно уйти после включения типа."""
        shadowed = self.publisher(shadow_types=["NEW_LISTING"])
        self.publish_event(self.event(), publisher=shadowed)
        live = self.publisher(shadow_types=[])
        sent, label = self.publish_event(self.event(), publisher=live)
        self.assertTrue(sent, label)

    def test_failed_send_releases_dedup_key(self):
        self.sender.ok = False
        publisher = self.publisher()
        sent, label = self.publish_event(self.event(), publisher=publisher)
        self.assertFalse(sent)
        self.assertEqual(label, pub.LABEL_SEND_FAILED)
        self.sender.ok = True
        sent, label = self.publish_event(self.event(), publisher=publisher)
        self.assertTrue(sent, "после сетевой ошибки повторная попытка обязана пройти")

    def test_every_decision_is_journaled(self):
        publisher = self.publisher()
        self.publish_event(self.event(), publisher=publisher)
        self.publish_event(self.event(ticker=None, url="https://ex/none"), publisher=publisher)
        labels = {row["label"] for row in self.cache.publish_log_tail(10)}
        self.assertIn(pub.LABEL_SENT, labels)


class IncidentReplayTestCase(WhitelistBase):
    """п.6.8: реальные события инцидентов — все отклонены с корректными метками."""

    def test_incident_events_are_all_refused(self):
        cases = [
            (self.event(event_type="DELISTING", ticker="STORJ", source="UPBIT",
                        title="스토리지(STORJ) 거래지원 종료 안내 (9/14 15:00)"),
             pub.LABEL_OUT_OF_UNIVERSE),
            (self.event(event_type="DELISTING", ticker="JASMY", source="UPBIT",
                        title="재스미코인(JASMY) 거래지원 종료 안내(9/14 15:00)"),
             pub.LABEL_OUT_OF_UNIVERSE),
            (self.event(event_type="DELISTING", ticker="TT", source="UPBIT",
                        title="썬더코어(TT) 거래지원 종료 안내 (9/14 15:00)"),
             pub.LABEL_OUT_OF_UNIVERSE),
            (self.event(ticker="ISRG", source="BYBIT",
                        title="New listing: ISRGUSDT TradFi Perpetual Contract"), pub.LABEL_TRADFI),
            (self.event(ticker="MNST", source="BYBIT",
                        title="New listing: MNSTUSDT TradFi Perpetual Contract"), pub.LABEL_TRADFI),
            (self.event(ticker="DDOG", source="BYBIT",
                        title="New listing: DDOGUSDT TradFi Perpetual Contract"), pub.LABEL_TRADFI),
            (self.event(ticker=None, source="OKX",
                        title="OKX to list perpetual futures for CXMT equity"), pub.LABEL_TRADFI),
        ]
        publisher = self.publisher()
        for event, expected in cases:
            sent, label = self.publish_event(event, publisher=publisher)
            self.assertFalse(sent, f"{event['title'][:40]} не должно уходить")
            self.assertEqual(label, expected, event["title"][:40])
        self.assertEqual(self.sender.sent, [])


class SinglePointOfSendTestCase(unittest.TestCase):
    """п.6.7: отправки Telegram нет нигде, кроме publisher и пути алертов бота."""

    ALLOWED = {"context/publisher.py", "pump_bot.py"}

    def test_no_telegram_calls_outside_publisher(self):
        offenders = []
        for path in glob.glob(os.path.join(BOT_DIR, "**", "*.py"), recursive=True):
            rel = os.path.relpath(path, BOT_DIR).replace("\\", "/")
            if rel.startswith("tests/") or rel in self.ALLOWED:
                continue
            text = io.open(path, encoding="utf-8").read()
            if "api.telegram.org" in text or "sendMessage" in text:
                offenders.append(rel)
        self.assertEqual(offenders, [],
                         "отправка Telegram допустима только в publisher и в пути алертов бота")

    def test_publisher_is_the_only_event_sender(self):
        text = io.open(os.path.join(BOT_DIR, "context", "publisher.py"), encoding="utf-8").read()
        self.assertIn("async def publish", text)
        for label in ("LABEL_KILL", "LABEL_DUP", "LABEL_RATE", "LABEL_SHADOW",
                      "LABEL_OUT_OF_UNIVERSE", "LABEL_TRADFI", "LABEL_PARSE_FAILED"):
            self.assertIn(label, text, f"в guard-цепочке нет метки {label}")



class CategoryAlignmentTestCase(WhitelistBase):
    """Найдено живым прогоном: схема LLM и закрытый список должны совпадать."""

    def test_llm_categories_map_into_closed_list(self):
        from context.classify import CATEGORY_ALIASES, normalize_category
        for raw, expected in CATEGORY_ALIASES.items():
            self.assertEqual(normalize_category(raw), expected)
            self.assertIn(expected, pub.MAJOR_NEWS_CATEGORIES,
                          f"{raw} должен приводиться к категории закрытого списка")

    def test_hack_is_publishable_as_security(self):
        """Инцидент безопасности по монете из universe обязан проходить."""
        from context.classify import normalize_category
        news = self.news(category=normalize_category("HACK"), source_tier=1,
                         confirmations=1, summary="Эксплойт моста, выведено $30M")
        sent, label = self.publish_news(news)
        self.assertTrue(sent, label)
        self.assertIn("Инцидент безопасности", self.sender.sent[0])

    def test_schema_lists_only_closed_list_categories(self):
        from context import classify
        for name in pub.MAJOR_NEWS_CATEGORIES:
            self.assertIn(name, classify.SYSTEM_PROMPT,
                          f"категория {name} должна быть в схеме промпта")

if __name__ == "__main__":
    unittest.main(verbosity=2)

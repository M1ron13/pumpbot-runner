"""LLM-классификация свободного текста новостей.

Только для текста, прошедшего дешёвые фильтры (матчинг тикера, whitelist, свежесть,
дедуп). Структурные события бирж через LLM не гоняются — их тип уже в данных.

Один батч-вызов на алерт, строгий JSON, один повтор при невалидном ответе.
Пороги вердиктов («катализатор», «медвежий фон») применяются **в коде**, а не
внутри промпта: классификация по ключевым словам запрещена, но и решение отдавать
модели нельзя — иначе правила невозможно протестировать.

Провайдер выбирается конфигом; без ключа слой работает без LLM (свободный текст
уходит только в лог).
"""

import asyncio
import json
import re
import time
from typing import List, Optional

# Категории в схеме должны совпадать с закрытым списком публикатора: живой прогон
# показал, что модель отдавала HACK, а список ждал SECURITY — реальный инцидент
# отклонялся по неверной причине. Ниже — точные имена плюс карта старых значений.
CATEGORY_ALIASES = {
    "HACK": "SECURITY",
    "LEADERSHIP": "LEADERSHIP_CRIMINAL",
    "UNLOCK": "SCHEDULED",
    "BANKRUPTCY": "PROJECT_CRITICAL",
}


def normalize_category(value: str) -> str:
    name = str(value or "").upper()
    return CATEGORY_ALIASES.get(name, name)


SYSTEM_PROMPT = (
    "Ты классифицируешь крипто-новости для риск-контекста. Отвечай ТОЛЬКО валидным JSON "
    "по схеме {\"items\":[{\"headline_id\":int,\"event_type\":\"LEGAL|LEADERSHIP_CRIMINAL|"
    "REGULATORY|POLITICAL_MENTION|SECURITY|PROJECT_CRITICAL|SCHEDULED|LISTING|PARTNERSHIP|"
    "OTHER\",\"direction\":\"bullish|"
    "bearish|unclear\",\"is_fact\":bool,\"source_tier\":1|2|3,\"confidence\":0..1,"
    "\"reasoning\":\"одно предложение\"}]}. "
    "is_fact=false, если это слух, план или пересказ («sources say», «reportedly», «планирует»). "
    "SECURITY — хак, эксплойт, кража, остановка выводов. LEADERSHIP_CRIMINAL — арест, "
    "уголовное дело, смерть основателя. SCHEDULED — событие с датой в будущем: анлок, "
    "суд, mainnet. PROJECT_CRITICAL — банкротство, прекращение проекта, rug pull. "
    "source_tier: 1 — первоисточник (регулятор, ведомство, официальный аккаунт), "
    "2 — Reuters/Bloomberg/AP, 3 — крипто-медиа. "
    "Опровержение слуха не является бычьей новостью."
)

JSON_BLOCK = re.compile(r"\{.*\}", re.S)


def build_user_prompt(symbol: str, headlines: List[dict]) -> str:
    lines = [f"Монета: {symbol}", "Заголовки:"]
    for i, item in enumerate(headlines, start=1):
        source = item.get("source") or item.get("domain") or "?"
        lines.append(f"{i}. [{source}] {item.get('title', '')[:300]}")
    return "\n".join(lines)


def parse_response(text: str) -> Optional[List[dict]]:
    if not text:
        return None
    match = JSON_BLOCK.search(text)
    if not match:
        return None
    try:
        data = json.loads(match.group(0))
    except json.JSONDecodeError:
        return None
    items = data.get("items")
    return items if isinstance(items, list) else None


def apply_rules(items: List[dict], cfg: dict) -> dict:
    """Пороги из конфига — код, а не промпт: только так правила тестируемы."""
    rules = cfg["classification"]
    catalyst_rule = rules["catalyst"]
    bearish_rule = rules["bearish"]

    catalysts, bearish, ignored = [], [], []
    for item in items or []:
        direction = str(item.get("direction", "")).lower()
        try:
            confidence = float(item.get("confidence", 0.0))
        except (TypeError, ValueError):
            confidence = 0.0
        is_fact = bool(item.get("is_fact"))
        try:
            tier = int(item.get("source_tier", 3))
        except (TypeError, ValueError):
            tier = 3

        if (direction == catalyst_rule["direction"]
                and confidence >= float(catalyst_rule["min_confidence"])
                and (is_fact or tier <= int(catalyst_rule["require_fact_or_tier"]))):
            catalysts.append(item)
        elif (direction == bearish_rule["direction"]
              and confidence >= float(bearish_rule["min_confidence"])):
            bearish.append(item)
        else:
            ignored.append(item)
    return {"catalysts": catalysts, "bearish": bearish, "ignored": ignored}


class Classifier:
    def __init__(self, cfg: dict, session=None, cache=None, timeout_ms: float = None):
        self.cfg = cfg
        self.rules = cfg["classification"]
        self.session = session
        self.cache = cache
        # у пути алерта бюджет 2 секунды, у фоновой классификации новостей его нет:
        # там важнее разобрать батч, чем успеть к отправке сообщения
        self.timeout_ms = float(timeout_ms or self.rules["timeout_ms"])

    @property
    def provider(self) -> Optional[str]:
        name = self.rules.get("provider", "groq")
        return name if self.cfg["keys"].get(name) else None

    async def classify(self, symbol: str, headlines: List[dict]) -> dict:
        """Вернуть {'items', 'verdicts', 'status'}; без ключа — статус 'нет провайдера'."""
        headlines = headlines[: int(self.rules["max_headlines"])]
        if not headlines:
            return {"items": [], "verdicts": apply_rules([], self.cfg), "status": "нет заголовков"}
        if not self.provider or self.session is None:
            return {"items": [], "verdicts": apply_rules([], self.cfg),
                    "status": "нет провайдера LLM"}

        attempts = int(self.rules["retries"]) + 1
        last_error = None
        for attempt in range(attempts):
            started = time.time()
            try:
                text = await self._call(symbol, headlines)
                items = parse_response(text)
                latency = int((time.time() - started) * 1000)
                if items is None:
                    last_error = "невалидный JSON"
                    self._log(symbol, len(headlines), 0, False, latency, last_error)
                    continue
                self._log(symbol, len(headlines), len(items), True, latency, None)
                return {"items": items, "verdicts": apply_rules(items, self.cfg), "status": "ок"}
            except asyncio.TimeoutError:
                last_error = "таймаут"
                self._log(symbol, len(headlines), 0, False,
                          int((time.time() - started) * 1000), last_error)
            except Exception as exc:
                last_error = str(exc)[:200]
                self._log(symbol, len(headlines), 0, False,
                          int((time.time() - started) * 1000), last_error)
        return {"items": [], "verdicts": apply_rules([], self.cfg),
                "status": f"классификация не удалась ({last_error})"}

    async def _call(self, symbol: str, headlines: List[dict]) -> str:
        import aiohttp
        provider = self.provider
        timeout = aiohttp.ClientTimeout(total=self.timeout_ms / 1000.0)
        user = build_user_prompt(symbol, headlines)

        if provider == "groq":
            payload = {
                "model": self.rules["groq_model"],
                "messages": [{"role": "system", "content": SYSTEM_PROMPT},
                             {"role": "user", "content": user}],
                "temperature": 0,
                "response_format": {"type": "json_object"},
            }
            headers = {"Authorization": f"Bearer {self.cfg['keys']['groq']}"}
            async with self.session.post(self.rules["groq_url"], json=payload,
                                         headers=headers, timeout=timeout) as resp:
                body = await resp.json(content_type=None)
            return (body.get("choices") or [{}])[0].get("message", {}).get("content", "")

        if provider == "anthropic":
            payload = {
                "model": self.rules["anthropic_model"],
                "max_tokens": 1024,
                "system": SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user}],
            }
            headers = {"x-api-key": self.cfg["keys"]["anthropic"],
                       "anthropic-version": "2023-06-01"}
            async with self.session.post(self.rules["anthropic_url"], json=payload,
                                         headers=headers, timeout=timeout) as resp:
                body = await resp.json(content_type=None)
            blocks = body.get("content") or []
            return "".join(b.get("text", "") for b in blocks if isinstance(b, dict))

        raise RuntimeError(f"неизвестный провайдер: {provider}")

    def _log(self, symbol, items_in, items_out, ok, latency_ms, error) -> None:
        if self.cache is None:
            return
        try:
            self.cache.log_llm(symbol=symbol, provider=self.provider,
                               model=self.rules.get(f"{self.provider}_model"),
                               items_in=items_in, items_out=items_out, ok=ok,
                               latency_ms=latency_ms, error=error)
        except Exception:
            pass

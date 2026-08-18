"""Загрузка context_config.json и ключей из окружения.

Ключи только из окружения: в конфиге, который лежит в репозитории, секретов быть
не должно.
"""

import json
import os

HERE = os.path.dirname(os.path.abspath(__file__))
BOT_DIR = os.path.dirname(HERE)
DEFAULT_PATH = os.path.join(BOT_DIR, "context_config.json")

ENV_KEYS = {
    "tavily": "TAVILY_API_KEY",
    "cryptopanic": "CRYPTOPANIC_TOKEN",
    "coinmarketcal": "COINMARKETCAL_KEY",
    "groq": "GROQ_API_KEY",
    "anthropic": "ANTHROPIC_API_KEY",
    "gemini": "GEMINI_API_KEY",
}


def load(path: str = DEFAULT_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    cfg["_path"] = path
    cfg["keys"] = {name: os.environ.get(env, "") for name, env in ENV_KEYS.items()}
    # источник без ключа считается выключенным, даже если в конфиге стоит true
    for source in ("tavily", "cryptopanic", "coinmarketcal"):
        if not cfg["keys"].get(source):
            cfg["enabled_sources"][source] = False
    return cfg


def cache_path(cfg: dict) -> str:
    path = cfg.get("cache_db") or "context_cache.db"
    return path if os.path.isabs(path) else os.path.join(BOT_DIR, path)


def enabled(cfg: dict, source: str) -> bool:
    return bool(cfg.get("enabled_sources", {}).get(source))

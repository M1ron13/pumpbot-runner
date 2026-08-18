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


def load_env_file(path: str = None) -> dict:
    """Ключи из pumpbot/.env — чтобы не искать, куда их вписывать.

    Переменные окружения приоритетнее файла: в планировщике удобнее файл, в разовом
    запуске — окружение.
    """
    path = path or os.path.join(BOT_DIR, ".env")
    values = {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                name, _, value = line.partition("=")
                value = value.strip().strip('"').strip("'")
                if value:
                    values[name.strip()] = value
    except FileNotFoundError:
        pass
    return values


def load(path: str = DEFAULT_PATH) -> dict:
    with open(path, "r", encoding="utf-8") as fh:
        cfg = json.load(fh)
    cfg["_path"] = path
    from_file = load_env_file()
    cfg["keys"] = {name: (os.environ.get(env) or from_file.get(env, ""))
                   for name, env in ENV_KEYS.items()}
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

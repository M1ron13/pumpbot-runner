"""Чтение Telegram-каналов первоисточников (Telethon, юзер-сессия).

Зачем: каналы вроде Wu Blockchain и Tree of Alpha публикуют события раньше агрегаторов.
Бот-токен здесь не годится — боты не могут читать чужие каналы, поэтому нужна
юзер-сессия. Она создаётся один раз интерактивно (код из Telegram, при включённой
двухфакторке ещё пароль) и дальше живёт в файле `telegram_session.session`,
который в git не попадает.

Правила:

* читаем ТОЛЬКО каналы из whitelist в конфиге. Самостоятельно список не расширяем;
* каскад перепечаток (одна новость в пяти каналах за минуту) схлопывает существующий
  дедуп по событию — тот же, что для RSS;
* tier источника: официальное зеркало биржи — 1, остальные каналы — 3;
* Telethon слушает updates, а не опрашивает: rate-limit не задевается.

Запуск авторизации:  python -m context.sources.telegram_feed login
Проверка доступа:    python -m context.sources.telegram_feed check
"""

import asyncio
import logging
import os
import sys
import time
from typing import List, Optional

log = logging.getLogger("context.telegram")

SESSION_NAME = "telegram_session"


def session_path(cfg: dict) -> str:
    bot_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(bot_dir, cfg["telegram_feed"].get("session_file") or SESSION_NAME)


def credentials(cfg: dict) -> tuple:
    """api_id и api_hash из ключей окружения/.env — в конфиге репозитория их нет."""
    api_id = cfg["keys"].get("telegram_api_id") or ""
    api_hash = cfg["keys"].get("telegram_api_hash") or ""
    try:
        api_id = int(api_id)
    except (TypeError, ValueError):
        api_id = 0
    return api_id, api_hash


def build_client(cfg: dict):
    from telethon import TelegramClient
    api_id, api_hash = credentials(cfg)
    if not api_id or not api_hash:
        raise RuntimeError("нет TELEGRAM_API_ID / TELEGRAM_API_HASH в pumpbot/.env")
    return TelegramClient(session_path(cfg), api_id, api_hash)


async def fetch_recent(cfg: dict, cache, limit_per_channel: int = 20) -> List[dict]:
    """Свежие сообщения из каналов whitelist. Без сессии возвращает пустой список."""
    feed_cfg = cfg["telegram_feed"]
    if not feed_cfg.get("enabled"):
        return []
    if not os.path.exists(session_path(cfg) + ".session"):
        log.info("сессия Telegram не создана — источник ждёт авторизации")
        return []

    client = build_client(cfg)
    out: List[dict] = []
    await client.connect()
    try:
        if not await client.is_user_authorized():
            log.warning("сессия Telegram есть, но не авторизована — нужен повторный login")
            return []
        since_ts = time.time() - float(feed_cfg["lookback_sec"])
        for channel in feed_cfg["channels"]:
            if not channel.get("enabled", True):
                continue
            name = channel["name"]
            try:
                async for message in client.iter_messages(name, limit=limit_per_channel):
                    text = (message.message or "").strip()
                    if not text:
                        continue
                    ts = message.date.timestamp() if message.date else time.time()
                    if ts < since_ts:
                        break
                    title = text.splitlines()[0][:300]
                    out.append({
                        "source": f"tg:{name.lstrip('@')}",
                        "feed": f"tg:{name.lstrip('@')}",
                        "source_tier": int(channel.get("tier", 3)),
                        "title": title,
                        "summary": text[:500],
                        "url": f"https://t.me/{name.lstrip('@')}/{message.id}",
                        "ts": ts,
                    })
            except Exception as exc:
                log.warning("канал %s: %s", name, exc)
    finally:
        await client.disconnect()
    return out


# --------------------------------------------------------------------------- #
# интерактивные команды
# --------------------------------------------------------------------------- #

async def login(cfg: dict) -> int:
    """Одноразовая авторизация. Код приходит в Telegram, ввод — руками владельца."""
    client = build_client(cfg)
    phone = cfg["telegram_feed"].get("phone") or input("номер телефона (в формате +7...): ").strip()
    await client.start(phone=lambda: phone)
    me = await client.get_me()
    print(f"авторизовано: {me.first_name} (@{me.username}) id={me.id}")
    print(f"сессия сохранена: {session_path(cfg)}.session")
    await client.disconnect()
    return 0


async def check(cfg: dict) -> int:
    """Доступность каналов whitelist и свежесть их сообщений."""
    client = build_client(cfg)
    await client.connect()
    if not await client.is_user_authorized():
        print("сессия не авторизована — сначала login")
        await client.disconnect()
        return 1
    for channel in cfg["telegram_feed"]["channels"]:
        name = channel["name"]
        try:
            entity = await client.get_entity(name)
            last = None
            async for message in client.iter_messages(entity, limit=1):
                last = message
            age = (time.time() - last.date.timestamp()) / 60 if last and last.date else None
            print(f"   ок   {name:26} tier {channel.get('tier', 3)} | последнее сообщение "
                  f"{age:.0f} мин назад" if age is not None else f"   ок   {name}")
        except Exception as exc:
            print(f"   нет  {name:26} {type(exc).__name__}: {exc}")
    await client.disconnect()
    return 0


def main(argv=None) -> int:
    from context import config as ctx_config
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    argv = argv if argv is not None else sys.argv[1:]
    command = argv[0] if argv else "check"
    cfg = ctx_config.load()
    if command == "login":
        return asyncio.run(login(cfg))
    if command == "check":
        return asyncio.run(check(cfg))
    print("команды: login | check")
    return 2


if __name__ == "__main__":
    sys.exit(main())

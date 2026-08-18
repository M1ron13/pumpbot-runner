"""Кэш контекст-слоя (sqlite, только stdlib).

Контур A пишет сюда события, снимки инструментов и справочник монет; контур B
читает при алерте — поэтому чтение обязано быть дешёвым и никогда не блокировать
отправку. Отдельная таблица `match_log` существует ради проверки точности
матчинга тикеров постфактум: без неё ложные сопоставления невидимы.
"""

import hashlib
import json
import os
import sqlite3
import time
from typing import Iterable, List, Optional

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    source TEXT NOT NULL,
    raw_type TEXT,
    event_type TEXT NOT NULL,
    symbol TEXT,
    ticker TEXT,
    title TEXT,
    url TEXT,
    payload TEXT,
    ttl_ts REAL NOT NULL,
    matched_rule TEXT,
    published_ts REAL,
    publish_decision TEXT,
    UNIQUE(source, url, title)
);
CREATE INDEX IF NOT EXISTS idx_events_ticker_ts ON events(ticker, ts);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events(event_type, ts);

CREATE TABLE IF NOT EXISTS instruments (
    exchange TEXT NOT NULL,
    market TEXT NOT NULL,
    symbol TEXT NOT NULL,
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL,
    alive INTEGER NOT NULL DEFAULT 1,
    PRIMARY KEY (exchange, market, symbol)
);

CREATE TABLE IF NOT EXISTS coins (
    coin_id TEXT PRIMARY KEY,
    symbol TEXT NOT NULL,
    name TEXT NOT NULL,
    aliases TEXT,
    platforms TEXT,
    updated_ts REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_coins_symbol ON coins(symbol);

CREATE TABLE IF NOT EXISTS match_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    source TEXT,
    text TEXT,
    ticker TEXT,
    coin_id TEXT,
    rule TEXT,
    decision TEXT
);

CREATE TABLE IF NOT EXISTS llm_calls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    symbol TEXT,
    provider TEXT,
    model TEXT,
    items_in INTEGER,
    items_out INTEGER,
    ok INTEGER,
    latency_ms INTEGER,
    error TEXT
);

CREATE TABLE IF NOT EXISTS enrichments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    symbol TEXT,
    verdict TEXT,
    block TEXT,
    budget_ms INTEGER,
    elapsed_ms INTEGER,
    sources_ok TEXT,
    sources_failed TEXT
);

CREATE TABLE IF NOT EXISTS published (
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    message_type TEXT,
    ticker TEXT,
    reserved_at REAL,
    sent_at REAL,
    PRIMARY KEY (source, external_id)
);

CREATE TABLE IF NOT EXISTS publish_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    message_type TEXT,
    ticker TEXT,
    source TEXT,
    external_id TEXT,
    label TEXT,
    text TEXT
);
CREATE INDEX IF NOT EXISTS idx_publish_log_ts ON publish_log(ts);

CREATE TABLE IF NOT EXISTS sent_messages (
    hash TEXT PRIMARY KEY,
    ts REAL NOT NULL,
    symbol TEXT,
    sender TEXT
);

CREATE TABLE IF NOT EXISTS monitor_state (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_ts REAL
);
"""


class Cache:
    def __init__(self, path: str):
        self.path = path
        directory = os.path.dirname(os.path.abspath(path))
        if directory:
            os.makedirs(directory, exist_ok=True)
        self.conn = sqlite3.connect(path, timeout=10.0, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.executescript(SCHEMA)
        # миграция баз, созданных до появления адресов контрактов
        columns = {r[1] for r in self.conn.execute("PRAGMA table_info(coins)")}
        if "platforms" not in columns:
            self.conn.execute("ALTER TABLE coins ADD COLUMN platforms TEXT")
        event_columns = {r[1] for r in self.conn.execute("PRAGMA table_info(events)")}
        for column, ddl in (("published_ts", "REAL"), ("publish_decision", "TEXT")):
            if column not in event_columns:
                self.conn.execute(f"ALTER TABLE events ADD COLUMN {column} {ddl}")
        self.conn.commit()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    # -- события ------------------------------------------------------------ #

    def add_event(self, *, ts: float, source: str, event_type: str, raw_type: str = None,
                  symbol: str = None, ticker: str = None, title: str = None, url: str = None,
                  payload: dict = None, ttl_sec: float = 172800.0, matched_rule: str = None) -> bool:
        """True — событие новое. Дубли гасятся уникальным (source, url, title)."""
        # TTL считается от МОМЕНТА ЗАПИСИ, а не от даты объявления: иначе старое по
        # дате событие удаляется сразу после обработки, на следующем проходе
        # добавляется заново и отправляется повторно — механика спама
        try:
            self.conn.execute(
                "INSERT INTO events (ts, source, raw_type, event_type, symbol, ticker, title, url,"
                " payload, ttl_ts, matched_rule) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (ts, source, raw_type, event_type, symbol, ticker, title, url,
                 json.dumps(payload or {}, ensure_ascii=False), time.time() + ttl_sec, matched_rule))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def events_for(self, ticker: str, within_sec: float, types: Optional[Iterable[str]] = None,
                   now_ts: float = None) -> List[dict]:
        now_ts = now_ts if now_ts is not None else time.time()
        sql = "SELECT * FROM events WHERE ticker = ? AND ts >= ? AND ttl_ts >= ?"
        params = [ticker, now_ts - within_sec, now_ts]
        if types:
            types = list(types)
            sql += f" AND event_type IN ({','.join('?' * len(types))})"
            params += types
        sql += " ORDER BY ts DESC"
        return [dict(r) for r in self.conn.execute(sql, params)]

    def purge_expired(self, now_ts: float = None, ledger_sec: float = 2592000.0) -> int:
        """Чистка кэша. Обработанные события хранятся дольше — это журнал против повторов.

        Удалить строку с отметкой публикации значит разрешить отправить это событие
        второй раз, когда источник снова его отдаст.
        """
        now_ts = now_ts if now_ts is not None else time.time()
        cur = self.conn.execute(
            "DELETE FROM events WHERE ttl_ts < ?"
            " AND (published_ts IS NULL OR published_ts < ?)",
            (now_ts, now_ts - ledger_sec))
        self.conn.commit()
        return cur.rowcount

    # -- инструменты (для диффа) -------------------------------------------- #

    def instruments_snapshot(self, exchange: str, market: str) -> set:
        rows = self.conn.execute(
            "SELECT symbol FROM instruments WHERE exchange = ? AND market = ? AND alive = 1",
            (exchange, market))
        return {r["symbol"] for r in rows}

    def apply_instruments(self, exchange: str, market: str, symbols: Iterable[str],
                          now_ts: float = None) -> dict:
        """Записать снимок и вернуть дифф: что появилось и что исчезло."""
        now_ts = now_ts if now_ts is not None else time.time()
        symbols = set(symbols)
        known = self.instruments_snapshot(exchange, market)
        first_run = not known and not self.conn.execute(
            "SELECT 1 FROM instruments WHERE exchange = ? AND market = ? LIMIT 1",
            (exchange, market)).fetchone()

        added = sorted(symbols - known)
        removed = sorted(known - symbols)
        for symbol in symbols:
            self.conn.execute(
                "INSERT INTO instruments (exchange, market, symbol, first_seen, last_seen, alive)"
                " VALUES (?,?,?,?,?,1) ON CONFLICT(exchange, market, symbol)"
                " DO UPDATE SET last_seen = excluded.last_seen, alive = 1",
                (exchange, market, symbol, now_ts, now_ts))
        for symbol in removed:
            self.conn.execute(
                "UPDATE instruments SET alive = 0, last_seen = ? WHERE exchange = ? AND market = ?"
                " AND symbol = ?", (now_ts, exchange, market, symbol))
        self.conn.commit()
        # первый прогон — не событие: весь рынок «появился» только с точки зрения пустой базы
        return {"added": [] if first_run else added, "removed": [] if first_run else removed,
                "first_run": first_run, "total": len(symbols)}

    def where_listed(self, ticker: str, quote: str = "USDT") -> List[dict]:
        rows = self.conn.execute(
            "SELECT exchange, market, symbol, first_seen FROM instruments"
            " WHERE alive = 1 AND (symbol = ? OR symbol = ?)",
            (f"{ticker}{quote}", ticker))
        return [dict(r) for r in rows]

    # -- справочник монет ---------------------------------------------------- #

    def upsert_coins(self, coins: Iterable[dict], now_ts: float = None) -> int:
        now_ts = now_ts if now_ts is not None else time.time()
        count = 0
        for coin in coins:
            self.conn.execute(
                "INSERT INTO coins (coin_id, symbol, name, aliases, platforms, updated_ts)"
                " VALUES (?,?,?,?,?,?)"
                " ON CONFLICT(coin_id) DO UPDATE SET symbol = excluded.symbol,"
                " name = excluded.name, aliases = excluded.aliases,"
                " platforms = excluded.platforms, updated_ts = excluded.updated_ts",
                (coin["coin_id"], coin["symbol"].upper(), coin["name"],
                 json.dumps(coin.get("aliases") or [], ensure_ascii=False),
                 json.dumps(coin.get("platforms") or {}, ensure_ascii=False), now_ts))
            count += 1
        self.conn.commit()
        return count

    def coins_by_symbol(self, symbol: str) -> List[dict]:
        rows = self.conn.execute("SELECT * FROM coins WHERE symbol = ?", (symbol.upper(),))
        return [dict(r) for r in rows]

    def coins_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) AS n FROM coins").fetchone()["n"]

    def coins_age_sec(self, now_ts: float = None) -> Optional[float]:
        row = self.conn.execute("SELECT MAX(updated_ts) AS t FROM coins").fetchone()
        if not row or row["t"] is None:
            return None
        return (now_ts if now_ts is not None else time.time()) - row["t"]

    # -- журналы ------------------------------------------------------------- #

    def log_match(self, *, ts: float, source: str, text: str, ticker: str,
                  coin_id: str, rule: str, decision: str) -> None:
        self.conn.execute(
            "INSERT INTO match_log (ts, source, text, ticker, coin_id, rule, decision)"
            " VALUES (?,?,?,?,?,?,?)", (ts, source, (text or "")[:300], ticker, coin_id, rule, decision))
        self.conn.commit()

    def log_llm(self, **kwargs) -> None:
        self.conn.execute(
            "INSERT INTO llm_calls (ts, symbol, provider, model, items_in, items_out, ok,"
            " latency_ms, error) VALUES (?,?,?,?,?,?,?,?,?)",
            (kwargs.get("ts", time.time()), kwargs.get("symbol"), kwargs.get("provider"),
             kwargs.get("model"), kwargs.get("items_in"), kwargs.get("items_out"),
             1 if kwargs.get("ok") else 0, kwargs.get("latency_ms"), kwargs.get("error")))
        self.conn.commit()

    def log_enrichment(self, **kwargs) -> None:
        self.conn.execute(
            "INSERT INTO enrichments (ts, symbol, verdict, block, budget_ms, elapsed_ms,"
            " sources_ok, sources_failed) VALUES (?,?,?,?,?,?,?,?)",
            (kwargs.get("ts", time.time()), kwargs.get("symbol"), kwargs.get("verdict"),
             kwargs.get("block"), kwargs.get("budget_ms"), kwargs.get("elapsed_ms"),
             ",".join(kwargs.get("sources_ok") or []), ",".join(kwargs.get("sources_failed") or [])))
        self.conn.commit()

    # -- сквозной дедуп отправок ---------------------------------------------- #

    def reserve_published(self, source: str, external_id: str, message_type: str = None,
                          ticker: str = None, now_ts: float = None) -> bool:
        """Занять ключ (source, external_id). False — по нему уже отправляли.

        Ключ бронируется ДО отправки: даже если два процесса дошли до отправки
        одновременно, второй получит False. Не отправилось — бронь снимается.
        """
        try:
            self.conn.execute(
                "INSERT INTO published (source, external_id, message_type, ticker, reserved_at)"
                " VALUES (?,?,?,?,?)",
                (source, external_id, message_type, ticker,
                 now_ts if now_ts is not None else time.time()))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def release_published(self, source: str, external_id: str) -> None:
        """Снять бронь: отправка не состоялась (тень, лимит, ошибка сети)."""
        self.conn.execute(
            "DELETE FROM published WHERE source = ? AND external_id = ? AND sent_at IS NULL",
            (source, external_id))
        self.conn.commit()

    def mark_published_sent(self, source: str, external_id: str, now_ts: float = None) -> None:
        self.conn.execute(
            "UPDATE published SET sent_at = ? WHERE source = ? AND external_id = ?",
            (now_ts if now_ts is not None else time.time(), source, external_id))
        self.conn.commit()

    def published_count(self, since_ts: float, message_type: str = None) -> int:
        sql = "SELECT COUNT(*) AS n FROM published WHERE sent_at >= ?"
        params = [since_ts]
        if message_type:
            sql += " AND message_type = ?"
            params.append(message_type)
        return int(self.conn.execute(sql, params).fetchone()["n"] or 0)

    def log_publish_decision(self, **kwargs) -> None:
        self.conn.execute(
            "INSERT INTO publish_log (ts, message_type, ticker, source, external_id, label, text)"
            " VALUES (?,?,?,?,?,?,?)",
            (kwargs.get("ts", time.time()), kwargs.get("message_type"), kwargs.get("ticker"),
             kwargs.get("source"), kwargs.get("external_id"), kwargs.get("label"),
             (kwargs.get("text") or "")[:1000]))
        self.conn.commit()

    def publish_log_tail(self, limit: int = 100) -> list:
        rows = self.conn.execute(
            "SELECT * FROM publish_log ORDER BY id DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]

    # -- журнал отправленных сообщений ---------------------------------------- #

    def remember_sent(self, text: str, symbol: str = None, sender: str = None,
                      now_ts: float = None) -> bool:
        """True — сообщение новое и его можно отправлять.

        Ключ — хэш самого текста. Это защита не от «своего» дедупа, а от любого
        второго отправителя: два пайплайна, два процесса, рестарт посреди отправки —
        всё равно одно сообщение уйдёт один раз.
        """
        digest = hashlib.sha1((text or "").strip().encode("utf-8")).hexdigest()
        try:
            self.conn.execute(
                "INSERT INTO sent_messages (hash, ts, symbol, sender) VALUES (?,?,?,?)",
                (digest, now_ts if now_ts is not None else time.time(), symbol, sender))
            self.conn.commit()
            return True
        except sqlite3.IntegrityError:
            return False

    def sent_count(self, since_ts: float) -> int:
        row = self.conn.execute("SELECT COUNT(*) AS n FROM sent_messages WHERE ts >= ?",
                                (since_ts,)).fetchone()
        return int(row["n"] or 0)

    # -- состояние монитора --------------------------------------------------- #

    def set_state(self, key: str, value) -> None:
        self.conn.execute(
            "INSERT INTO monitor_state (key, value, updated_ts) VALUES (?,?,?)"
            " ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_ts = excluded.updated_ts",
            (key, json.dumps(value, ensure_ascii=False), time.time()))
        self.conn.commit()

    def get_state(self, key: str, default=None):
        row = self.conn.execute("SELECT value FROM monitor_state WHERE key = ?", (key,)).fetchone()
        if not row:
            return default
        try:
            return json.loads(row["value"])
        except json.JSONDecodeError:
            return default

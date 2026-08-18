"""Heartbeat монитора и перехват зависшего инстанса.

Триггер планировщика поднимает процесс заново, только если предыдущий **умер**. Зависший
на сетевом запросе процесс жив с точки зрения ОС, но не работает — и такое состояние
может держаться часами. Поэтому живость определяется не наличием процесса, а прогрессом:

* каждый цикл контура A отмечается в кэше своим таймстемпом;
* новый инстанс при старте смотрит отметки. Все свежие — значит работающий владелец
  уже есть, и новый просто уходит;
* если хотя бы один цикл просрочен (или общая отметка старше `heartbeat_stale_sec`),
  владелец считается зависшим: его убивают и место занимает новый инстанс.

Просрочка считается по интервалу самого цикла: у опроса анонсов это минута, а у
справочника монет — сутки, и одна планка на всех давала бы ложные срабатывания.
"""

import logging
import os
import subprocess
import time
from typing import Callable, List, Optional, Tuple

log = logging.getLogger("context.heartbeat")

OWNER_KEY = "monitor_owner"
LOOP_PREFIX = "monitor_loop:"


def mark_loop(cache, name: str, interval_sec: float, now_ts: float = None) -> None:
    """Отметка прогресса конкретного цикла. Пишется ПОСЛЕ успешной итерации."""
    now_ts = now_ts if now_ts is not None else time.time()
    cache.set_state(f"{LOOP_PREFIX}{name}",
                    {"ts": now_ts, "interval": float(interval_sec), "pid": os.getpid()})


def mark_owner(cache, now_ts: float = None) -> None:
    now_ts = now_ts if now_ts is not None else time.time()
    cache.set_state(OWNER_KEY, {"pid": os.getpid(), "ts": now_ts})


def loop_marks(cache) -> dict:
    rows = cache.conn.execute(
        "SELECT key, value FROM monitor_state WHERE key LIKE ?", (LOOP_PREFIX + "%",)).fetchall()
    out = {}
    for row in rows:
        try:
            import json
            out[row["key"][len(LOOP_PREFIX):]] = json.loads(row["value"])
        except Exception:
            continue
    return out


def stale_loops(cache, cfg: dict, now_ts: float = None) -> List[Tuple[str, float, float]]:
    """Просроченные циклы: (имя, возраст отметки, допустимый предел)."""
    now_ts = now_ts if now_ts is not None else time.time()
    monitor = cfg["monitor"]
    factor = float(monitor.get("heartbeat_interval_factor", 3))
    floor = float(monitor.get("heartbeat_stale_sec", 900))
    overdue = []
    for name, mark in loop_marks(cache).items():
        try:
            age = now_ts - float(mark["ts"])
            limit = max(floor, float(mark.get("interval") or 60.0) * factor)
        except (KeyError, TypeError, ValueError):
            continue
        if age > limit:
            overdue.append((name, age, limit))
    return overdue


def process_alive(pid: int) -> bool:
    """Живёт ли процесс. Неизвестность трактуем как «жив» — убивать наугад нельзя."""
    if not pid:
        return False
    try:
        out = subprocess.run(["tasklist", "/FI", f"PID eq {int(pid)}", "/NH"],
                             capture_output=True, text=True, timeout=15)
        return str(pid) in (out.stdout or "")
    except Exception:
        return True


def kill_process(pid: int) -> bool:
    if not pid:
        return False
    try:
        subprocess.run(["taskkill", "/PID", str(int(pid)), "/F"], capture_output=True, timeout=20)
        return not process_alive(pid)
    except Exception as exc:
        log.warning("не удалось снять процесс %s: %s", pid, exc)
        return False


def claim_leadership(cache, cfg: dict, now_ts: float = None,
                     alive: Optional[Callable[[int], bool]] = None,
                     killer: Optional[Callable[[int], bool]] = None) -> dict:
    """Решение при старте: работать самому, уйти или перехватить у зависшего.

    Возвращает {'claim': bool, 'reason': str, 'killed_pid': int|None}.
    Зависимости внедряются, чтобы сценарии проверялись тестами без реальных процессов.
    """
    now_ts = now_ts if now_ts is not None else time.time()
    alive = alive or process_alive
    killer = killer or kill_process
    floor = float(cfg["monitor"].get("heartbeat_stale_sec", 900))

    owner = cache.get_state(OWNER_KEY) or {}
    owner_pid = int(owner.get("pid") or 0)
    owner_age = now_ts - float(owner.get("ts") or 0) if owner.get("ts") else None

    if not owner_pid or owner_pid == os.getpid():
        return {"claim": True, "reason": "владельца нет", "killed_pid": None}
    if not alive(owner_pid):
        return {"claim": True, "reason": f"прежний владелец {owner_pid} мёртв", "killed_pid": None}

    overdue = stale_loops(cache, cfg, now_ts)
    heartbeat_stale = owner_age is not None and owner_age > floor
    if not overdue and not heartbeat_stale:
        return {"claim": False,
                "reason": f"владелец {owner_pid} работает, отметки свежие", "killed_pid": None}

    # процесс жив, но прогресса нет — это зависание, а не работа
    detail = (", ".join(f"{name}: {age / 60:.0f} мин (предел {limit / 60:.0f})"
                        for name, age, limit in overdue)
              or f"общая отметка старше {floor / 60:.0f} мин")
    killed = killer(owner_pid)
    return {"claim": True,
            "reason": f"владелец {owner_pid} завис ({detail}); снят: {'да' if killed else 'нет'}",
            "killed_pid": owner_pid if killed else None}

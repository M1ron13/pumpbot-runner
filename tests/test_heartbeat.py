"""Heartbeat: перехват прав у зависшего инстанса.

Главный сценарий — «процесс жив, а прогресса нет». Триггер планировщика его не ловит,
потому что для ОС такой процесс исправен.
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from context import config as ctx_config     # noqa: E402
from context import heartbeat                # noqa: E402
from context.cache import Cache              # noqa: E402

NOW = 1_700_000_000.0
MINUTE = 60.0


class HeartbeatTestCase(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="hb-test-")
        self.cache = Cache(os.path.join(self.tmpdir, "cache.db"))
        self.cfg = ctx_config.load()
        self.killed = []

    def tearDown(self):
        self.cache.close()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def killer(self, pid):
        self.killed.append(pid)
        return True

    def claim(self, now_ts=NOW, alive=lambda pid: True):
        return heartbeat.claim_leadership(self.cache, self.cfg, now_ts=now_ts,
                                         alive=alive, killer=self.killer)

    def set_owner(self, pid, ts):
        self.cache.set_state(heartbeat.OWNER_KEY, {"pid": pid, "ts": ts})

    def test_no_owner_means_claim(self):
        result = self.claim()
        self.assertTrue(result["claim"])
        self.assertEqual(self.killed, [])

    def test_healthy_owner_makes_new_instance_step_aside(self):
        self.set_owner(4242, NOW - MINUTE)
        heartbeat.mark_loop(self.cache, "анонсы", 60, now_ts=NOW - MINUTE)
        result = self.claim()
        self.assertFalse(result["claim"], result["reason"])
        self.assertEqual(self.killed, [], "работающего владельца убивать нельзя")

    def test_dead_owner_is_replaced_without_kill(self):
        self.set_owner(4242, NOW - 10 * MINUTE)
        result = self.claim(alive=lambda pid: False)
        self.assertTrue(result["claim"])
        self.assertIn("мёртв", result["reason"])
        self.assertEqual(self.killed, [])

    def test_alive_owner_with_stale_heartbeat_is_killed(self):
        """Ключевой сценарий: процесс жив, отметка протухла — это зависание."""
        self.set_owner(4242, NOW - 20 * MINUTE)
        heartbeat.mark_loop(self.cache, "анонсы", 60, now_ts=NOW - 20 * MINUTE)
        result = self.claim()
        self.assertTrue(result["claim"], result["reason"])
        self.assertEqual(self.killed, [4242], "зависший владелец обязан быть снят")
        self.assertIn("завис", result["reason"])
        self.assertEqual(result["killed_pid"], 4242)

    def test_single_stuck_loop_is_enough(self):
        """Один цикл висит на сети, остальные работают — инстанс всё равно зависший."""
        self.set_owner(4242, NOW - MINUTE)
        heartbeat.mark_loop(self.cache, "анонсы", 60, now_ts=NOW - MINUTE)
        heartbeat.mark_loop(self.cache, "фандинг", 300, now_ts=NOW - 30 * MINUTE)
        result = self.claim()
        self.assertTrue(result["claim"])
        self.assertIn("фандинг", result["reason"])

    def test_slow_loops_are_not_false_positives(self):
        """У справочника монет интервал сутки — 20 минут молчания не зависание."""
        self.set_owner(4242, NOW - MINUTE)
        heartbeat.mark_loop(self.cache, "справочник монет", 86400, now_ts=NOW - 20 * MINUTE)
        heartbeat.mark_loop(self.cache, "анонсы", 60, now_ts=NOW - MINUTE)
        result = self.claim()
        self.assertFalse(result["claim"], result["reason"])
        self.assertEqual(self.killed, [])

    def test_stale_limit_respects_loop_interval(self):
        heartbeat.mark_loop(self.cache, "фандинг", 300, now_ts=NOW - 20 * MINUTE)
        overdue = heartbeat.stale_loops(self.cache, self.cfg, now_ts=NOW)
        self.assertEqual([name for name, _age, _limit in overdue], ["фандинг"])
        limit = overdue[0][2]
        self.assertGreaterEqual(limit, float(self.cfg["monitor"]["heartbeat_stale_sec"]))

    def test_own_pid_is_never_killed(self):
        self.set_owner(os.getpid(), NOW - 60 * MINUTE)
        result = self.claim()
        self.assertTrue(result["claim"])
        self.assertEqual(self.killed, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)

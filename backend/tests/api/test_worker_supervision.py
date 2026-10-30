"""Process-level coverage for the forked worker supervisor."""

import multiprocessing
import os
import signal
import time
import unittest
from unittest.mock import Mock, patch

from api.management.commands.rqworker import Command, _wait_for_child_exit


def _fork_descendant(connection):
    descendant = os.fork()
    if descendant == 0:
        time.sleep(60)
        os._exit(0)
    connection.send(descendant)
    time.sleep(60)


def _ignore_termination(connection):
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    connection.send(True)
    time.sleep(60)


@unittest.skipUnless("fork" in multiprocessing.get_all_start_methods(), "requires fork")
class WorkerSupervisionTests(unittest.TestCase):
    def test_a_poll_timeout_is_not_a_worker_failure(self):
        process = Mock()
        process.is_alive.return_value = True
        with patch(
            "api.management.commands.rqworker.wait",
            side_effect=[[], KeyboardInterrupt],
        ) as wait:
            with self.assertRaises(KeyboardInterrupt):
                _wait_for_child_exit([process])
        self.assertEqual(wait.call_count, 2)

    def test_detects_dead_pool_parent_while_descendant_holds_sentinel(self):
        ctx = multiprocessing.get_context("fork")
        reader, writer = ctx.Pipe(duplex=False)
        process = ctx.Process(target=_fork_descendant, args=(writer,))
        process.start()
        writer.close()
        self.assertTrue(reader.poll(5))
        descendant = reader.recv()
        try:
            os.kill(process.pid, signal.SIGKILL)
            started = time.monotonic()
            self.assertIs(_wait_for_child_exit([process]), process)
            self.assertLess(time.monotonic() - started, 2)
            self.assertEqual(process.exitcode, -signal.SIGKILL)
        finally:
            os.kill(descendant, signal.SIGKILL)
            if process.is_alive():
                process.kill()
            process.join(timeout=2)
            reader.close()

    def test_fault_cleanup_escalates_an_uncooperative_direct_child(self):
        ctx = multiprocessing.get_context("fork")
        reader, writer = ctx.Pipe(duplex=False)
        process = ctx.Process(target=_ignore_termination, args=(writer,))
        process.start()
        writer.close()
        self.assertTrue(reader.poll(5))
        reader.recv()
        try:
            started = time.monotonic()
            self.assertTrue(Command()._shutdown_after_failure([process], grace_seconds=0.1))
            self.assertLess(time.monotonic() - started, 3)
            self.assertEqual(process.exitcode, -signal.SIGKILL)
        finally:
            if process.is_alive():
                process.kill()
            process.join(timeout=2)
            reader.close()

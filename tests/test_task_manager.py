# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import threading
import time

import pytest

from blinkview.core.task_manager import TaskManager


@pytest.fixture
def task_manager():
    tm = TaskManager(max_workers=4)
    yield tm
    tm.shutdown()


def _wait_until(predicate, timeout=2.0, interval=0.01):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def test_run_task_executes_and_returns_result(task_manager):
    future = task_manager.run_task(lambda x, y: x + y, 2, 3)
    assert future.result(timeout=2.0) == 5


def test_run_periodic_calls_function_repeatedly(task_manager):
    calls = []
    lock = threading.Lock()

    def record():
        with lock:
            calls.append(time.time())

    task_manager.run_periodic(0.05, record)

    assert _wait_until(lambda: len(calls) >= 3, timeout=2.0)


def test_stop_periodic_halts_further_invocations(task_manager):
    count = [0]
    lock = threading.Lock()

    def record():
        with lock:
            count[0] += 1

    task_id = task_manager.run_periodic(0.05, record)

    assert _wait_until(lambda: count[0] >= 1, timeout=2.0)

    task_manager.stop_periodic(task_id)

    # let any in-flight dispatch settle, then snapshot the count
    time.sleep(0.1)
    with lock:
        stopped_at = count[0]

    time.sleep(0.3)
    with lock:
        assert count[0] == stopped_at


def test_stop_periodic_unknown_id_is_noop(task_manager):
    task_manager.stop_periodic("does-not-exist")


def test_periodic_task_is_not_dispatched_concurrently(task_manager):
    concurrent = [0]
    max_concurrent = [0]
    calls = [0]
    lock = threading.Lock()

    def slow_task():
        with lock:
            concurrent[0] += 1
            max_concurrent[0] = max(max_concurrent[0], concurrent[0])
        time.sleep(0.15)
        with lock:
            concurrent[0] -= 1
            calls[0] += 1

    task_manager.run_periodic(0.05, slow_task)

    assert _wait_until(lambda: calls[0] >= 2, timeout=3.0)
    with lock:
        assert max_concurrent[0] == 1


def test_shutdown_stops_scheduler_thread_cleanly(task_manager):
    task_manager.run_periodic(0.05, lambda: None)
    time.sleep(0.1)

    task_manager.shutdown()

    assert not task_manager._scheduler_thread.is_alive()


def test_shutdown_does_not_raise_submit_after_shutdown(task_manager):
    task_manager.run_periodic(0.01, lambda: None)
    # shutdown races the scheduler's dispatch pass; the join-before-executor-shutdown
    # ordering in TaskManager.shutdown must prevent "cannot schedule new futures" errors.
    task_manager.shutdown()


def test_multiple_periodic_tasks_run_independently(task_manager):
    counts = {"a": 0, "b": 0}
    lock = threading.Lock()

    def make_recorder(key):
        def record():
            with lock:
                counts[key] += 1

        return record

    task_manager.run_periodic(0.05, make_recorder("a"))
    task_manager.run_periodic(0.08, make_recorder("b"))

    assert _wait_until(lambda: counts["a"] >= 2 and counts["b"] >= 2, timeout=3.0)

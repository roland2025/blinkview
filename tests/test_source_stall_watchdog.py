# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from concurrent.futures import Future

import pytest

from blinkview.core.source_stall_watchdog import STALL_RECONNECT_MAX_S, SourceStallWatchdog
from blinkview.io.BaseReader import BaseReader

S = 1_000_000_000


class RecordingLogger:
    def __init__(self):
        self.lines = []

    def _add(self, level, msg, *args, **kwargs):
        self.lines.append((level, msg % args if args else msg))

    def info(self, msg, *args, **kwargs):
        self._add("info", msg, *args)

    def warn(self, msg, *args, **kwargs):
        self._add("warn", msg, *args)

    def error(self, msg, *args, **kwargs):
        self._add("error", msg, *args)

    def exception(self, msg, *args, **kwargs):
        self._add("exception", msg, *args)

    def texts(self, level=None):
        return [text for lvl, text in self.lines if level is None or lvl == level]


class FakeSource:
    """Duck-types what the watchdog reads off a BaseReader."""

    supports_reconnect = True
    stall_hint = "Check the USB hub."

    def __init__(self, warn_s=30, reconnect_s=60, started_ns=0):
        self.stall_warn_s = warn_s
        self.stall_reconnect_s = reconnect_s
        self.enabled = True
        self.is_running = True
        self._last_ingest_ns = 0
        self._started_ns = started_ns
        self._ready_ns = started_ns
        self.is_connected = True
        self.reconnect_clock = None  # set to a Clock: reconnect() comes back up at once, like the RTT reader
        self.logger_link = RecordingLogger()
        self.logger = self.logger_link
        self.reconnect_calls = 0
        self.reconnect_result = True
        self.diagnosis = None

    def reconnect(self):
        self.reconnect_calls += 1
        if self.reconnect_clock is not None:
            self._ready_ns = self.reconnect_clock.now
        return self.reconnect_result

    def describe_stall(self):
        return self.diagnosis


class FakeTasks:
    """run_task runs the call at once, unless hold is set: then it returns a Future that stays
    pending, like a reconnect stuck in a DLL call."""

    def __init__(self):
        self.hold = False
        self.held = []
        self.periodic = {}
        self.stopped = []

    def run_task(self, func, *args):
        future = Future()
        if self.hold:
            self.held.append((future, func, args))
        else:
            func(*args)
            future.set_result(None)
        return future

    def run_periodic(self, interval, func):
        self.periodic["task"] = (interval, func)
        return "task"

    def stop_periodic(self, task_id):
        self.stopped.append(task_id)


class Clock:
    def __init__(self):
        self.now = 0

    def __call__(self):
        return self.now

    def at(self, seconds):
        self.now = int(seconds * S)


@pytest.fixture
def env():
    clock, tasks, sources = Clock(), FakeTasks(), []
    watchdog = SourceStallWatchdog(lambda: sources, tasks, clock=clock, logger=RecordingLogger())
    return clock, tasks, sources, watchdog


def tick_at(env, seconds):
    clock, _, _, watchdog = env
    clock.at(seconds)
    watchdog.tick()


def ingest_at(env, source, seconds):
    clock = env[0]
    clock.at(seconds)
    source._last_ingest_ns = clock.now


class TestWarning:
    def test_warns_once_after_stall_warn_s_and_logs_resume(self, env):
        source = FakeSource(warn_s=30, reconnect_s=0)
        env[2].append(source)

        tick_at(env, 0)
        ingest_at(env, source, 5)
        tick_at(env, 6)
        tick_at(env, 34)
        assert source.logger_link.texts("warn") == []

        tick_at(env, 35)
        tick_at(env, 40)
        tick_at(env, 80)
        assert source.logger_link.texts("warn") == ["No data for 30 s."]

        ingest_at(env, source, 90)
        tick_at(env, 91)
        assert source.logger_link.texts("info") == ["Data resumed after 85 s without data."]

        # A new stall warns again.
        tick_at(env, 125)
        assert source.logger_link.texts("warn") == ["No data for 30 s.", "No data for 35 s."]

    def test_counts_from_start_when_nothing_has_arrived(self, env):
        source = FakeSource(warn_s=30, reconnect_s=0, started_ns=10 * S)
        env[2].append(source)

        tick_at(env, 39)
        assert source.logger_link.lines == []
        tick_at(env, 40)
        assert source.logger_link.texts("warn") == ["No data for 30 s."]

    def test_nothing_when_off_disabled_or_not_running(self, env):
        off = FakeSource(warn_s=0, reconnect_s=0)
        disabled = FakeSource()
        disabled.enabled = False
        stopped = FakeSource()
        stopped.is_running = False
        env[2].extend([off, disabled, stopped])

        for t in range(0, 2000, 10):
            tick_at(env, t)

        for source in (off, disabled, stopped):
            assert source.logger_link.lines == []
            assert source.reconnect_calls == 0

    def test_steady_data_never_warns(self, env):
        source = FakeSource(warn_s=30, reconnect_s=60)
        env[2].append(source)
        for t in range(0, 600, 10):
            ingest_at(env, source, t)
            tick_at(env, t + 5)
        assert source.logger_link.lines == []
        assert source.reconnect_calls == 0

    def test_restart_starts_the_clock_again(self, env):
        source = FakeSource(warn_s=30, reconnect_s=0)
        env[2].append(source)
        tick_at(env, 20)
        source._started_ns = 25 * S  # the source was disabled and enabled again
        tick_at(env, 40)
        assert source.logger_link.lines == []
        tick_at(env, 55)
        assert source.logger_link.texts("warn") == ["No data for 30 s."]

    def test_diagnosis_is_logged_after_the_warning(self, env):
        source = FakeSource(warn_s=30, reconnect_s=0)
        source.diagnosis = "RTT running=1"
        env[2].append(source)
        tick_at(env, 30)
        assert source.logger_link.texts("warn") == ["No data for 30 s.", "Stall diagnosis: RTT running=1"]

    def test_a_raising_diagnosis_is_reported_not_raised(self, env):
        source = FakeSource(warn_s=30, reconnect_s=0)

        def boom():
            raise RuntimeError("DLL gone")

        source.describe_stall = boom
        env[2].append(source)
        tick_at(env, 30)
        assert source.logger_link.texts("warn")[-1] == "Stall diagnosis: diagnosis failed: DLL gone"


class TestReconnect:
    def test_backs_off_by_doubling_and_hints_after_three(self, env):
        source = FakeSource(warn_s=30, reconnect_s=60)
        env[2].append(source)

        attempts = []
        for t in range(0, 2000):
            before = source.reconnect_calls
            tick_at(env, t)
            if source.reconnect_calls != before:
                attempts.append(t)

        # 60 s, then 120, 240, 480 s after each attempt, then capped.
        assert attempts[:5] == [60, 180, 420, 900, 900 + STALL_RECONNECT_MAX_S]
        warns = source.logger_link.texts("warn")
        assert warns[0] == "No data for 30 s."
        assert "Reconnecting (attempt 1) after 60 s without data." in warns
        hints = [w for w in warns if "have not brought the data back" in w]
        assert hints == ["3 reconnects have not brought the data back. Check the USB hub."]
        assert warns.index(hints[0]) == warns.index("Reconnecting (attempt 4) after 900 s without data.") - 1

    def test_data_resets_the_backoff(self, env):
        source = FakeSource(warn_s=30, reconnect_s=60)
        env[2].append(source)
        tick_at(env, 60)
        tick_at(env, 180)
        assert source.reconnect_calls == 2

        ingest_at(env, source, 200)
        tick_at(env, 201)
        assert source.logger_link.texts("info") == ["Data resumed after 200 s without data (2 reconnects)."]

        tick_at(env, 259)
        assert source.reconnect_calls == 2
        tick_at(env, 260)
        assert source.reconnect_calls == 3

    def test_warns_first_when_the_warning_is_off(self, env):
        source = FakeSource(warn_s=0, reconnect_s=60)
        env[2].append(source)
        tick_at(env, 60)
        assert source.logger_link.texts("warn")[:2] == [
            "No data for 60 s.",
            "Reconnecting (attempt 1) after 60 s without data.",
        ]

    def test_no_second_reconnect_while_one_is_pending(self, env):
        _, tasks, sources, _ = env
        stuck = FakeSource(warn_s=0, reconnect_s=60)
        other = FakeSource(warn_s=30, reconnect_s=0)
        sources.extend([stuck, other])

        tasks.hold = True
        for t in range(0, 1000, 10):
            tick_at(env, t)

        reconnects = [h for h in tasks.held if h[1].__name__ == "_reconnect"]
        assert len(reconnects) == 1
        assert other.logger_link.texts("warn") == ["No data for 30 s."]  # still checked on time

        # The stuck call returns: the next attempt follows the backoff from the last attempt.
        tasks.hold = False
        reconnects[0][0].set_result(None)
        tick_at(env, 1000)
        assert stuck.logger_link.texts("warn")[-1] == "Reconnecting (attempt 2) after 1000 s without data."

    def test_failed_reconnect_is_logged(self, env):
        source = FakeSource(warn_s=0, reconnect_s=60)
        source.reconnect_result = False
        env[2].append(source)
        tick_at(env, 60)
        assert source.logger_link.texts("warn")[-1] == "Reconnect did not run."

    def test_raising_reconnect_is_logged_not_raised(self, env):
        source = FakeSource(warn_s=0, reconnect_s=60)

        def boom():
            raise RuntimeError("DLL gone")

        source.reconnect = boom
        env[2].append(source)
        tick_at(env, 60)
        assert source.logger_link.texts("error") == ["Reconnect failed."]

    def test_reader_without_reconnect_only_warns(self, env):
        source = FakeSource(warn_s=30, reconnect_s=60)
        source.supports_reconnect = False
        env[2].append(source)
        for t in range(0, 1000, 10):
            tick_at(env, t)
        assert source.reconnect_calls == 0
        assert source.logger_link.texts("warn") == ["No data for 30 s."]


class TestConnection:
    def test_waits_while_not_connected_and_counts_from_the_connect(self, env):
        source = FakeSource(warn_s=30, reconnect_s=60)
        source.is_connected = False
        env[2].append(source)
        for t in range(0, 300, 10):
            tick_at(env, t)
        assert source.logger_link.lines == []

        source.is_connected = True
        source._ready_ns = 300 * S
        tick_at(env, 329)
        assert source.logger_link.lines == []
        tick_at(env, 330)
        assert source.logger_link.texts("warn") == ["No data for 30 s."]

    def test_a_reconnect_that_comes_up_keeps_the_backoff_and_the_total(self, env):
        clock = env[0]
        source = FakeSource(warn_s=30, reconnect_s=60)
        source.reconnect_clock = clock
        env[2].append(source)

        attempts = []
        for t in range(0, 1000):
            before = source.reconnect_calls
            tick_at(env, t)
            if source.reconnect_calls != before:
                attempts.append(t)

        assert attempts == [60, 180, 420, 900]
        assert source.logger_link.texts("warn").count("No data for 30 s.") == 1
        assert "Reconnecting (attempt 4) after 900 s without data." in source.logger_link.texts("warn")

        ingest_at(env, source, 950)
        tick_at(env, 951)
        assert source.logger_link.texts("info") == ["Data resumed after 950 s without data (4 reconnects)."]


class TestLifecycle:
    def test_start_registers_once_and_stop_unregisters(self, env):
        _, tasks, _, watchdog = env
        watchdog.start()
        watchdog.start()
        assert list(tasks.periodic) == ["task"]
        watchdog.stop()
        assert tasks.stopped == ["task"]

    def test_a_failing_tick_is_logged(self, env):
        _, tasks, _, _ = env
        logger = RecordingLogger()

        def broken():
            raise RuntimeError("sources changed")

        watchdog = SourceStallWatchdog(broken, tasks, logger=logger)
        watchdog._tick_safe()
        assert logger.texts("exception") == ["Source stall watchdog tick failed"]


class TestBaseReaderStamps:
    def test_distribute_and_start_stamp_the_clock(self):
        reader = BaseReader()
        reader.enabled = False  # start() returns before making a thread
        assert reader._last_ingest_ns == 0 and reader._started_ns == 0 and reader._ready_ns == 0

        reader.start()
        assert reader._started_ns > 0 and reader._ready_ns == reader._started_ns
        assert reader.is_connected is True

        received = []
        reader.subscribers = [type("Sub", (), {"put": lambda self, batch: received.append(batch)})()]
        reader.distribute("batch")
        assert reader._last_ingest_ns >= reader._started_ns
        assert received == ["batch"]

    def test_defaults_are_off_on_the_base_and_on_for_rtt(self):
        from blinkview.io.rtt import JLinkRTTReader
        from blinkview.io.uart import UARTReader

        def defaults(cls):
            props = cls.get_config_schema()["properties"]
            return props["stall_warn_s"]["default"], props["stall_reconnect_s"]["default"]

        assert defaults(UARTReader) == (0, 0)
        assert defaults(JLinkRTTReader) == (30, 60)
        assert JLinkRTTReader.supports_reconnect and not UARTReader.supports_reconnect

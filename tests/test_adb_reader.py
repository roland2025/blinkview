# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import queue
import subprocess
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_history import IdHistory
from blinkview.core.logger import PrintLogger
from blinkview.io.adb_reader import AdbReader


def make_reader(**config_overrides):
    reader = AdbReader()
    reader.logger = PrintLogger("test.adb_reader")
    reader.apply_config(config_overrides)
    return reader


class FakeStream:
    """Stand-in for a subprocess pipe's stdin/stdout half, backed by an in-memory byte queue
    rather than a real OS pipe - used for query()/send_data() tests that don't need a real
    read1()-driven event loop."""

    def __init__(self, chunks=()):
        self._chunks = list(chunks)
        self.written = bytearray()

    def write(self, data: bytes):
        self.written.extend(data)

    def flush(self):
        pass

    def read1(self, _size):
        if self._chunks:
            return self._chunks.pop(0)
        return b""


class FakeShell:
    def __init__(self, response_chunks=()):
        self.stdin = FakeStream()
        self.stdout = FakeStream(response_chunks)


class TestIsConnected:
    def test_false_when_not_enabled(self):
        reader = make_reader()
        reader.enabled = False
        reader._shell = object()
        reader._process = object()
        assert not reader.is_connected()

    def test_false_when_shell_or_process_missing(self):
        reader = make_reader()
        reader.enabled = True
        reader._shell = None
        reader._process = object()
        assert not reader.is_connected()

    def test_true_when_enabled_with_shell_and_process(self):
        # `enabled and self._shell and self._process` short-circuits to the last truthy operand
        # (self._process itself), not a real bool - assert truthiness, not identity with True.
        reader = make_reader()
        reader.enabled = True
        reader._shell = object()
        reader._process = object()
        assert reader.is_connected()


class TestQuery:
    def test_returns_empty_list_without_a_shell(self):
        reader = make_reader()
        reader._shell = None
        assert reader.query("echo hi") == []

    def test_writes_the_command_with_sentinel_and_returns_stripped_lines(self):
        reader = make_reader()
        reader._shell = FakeShell(response_chunks=[b"line one\nline two\n__BlinkSync_Done__\n"])

        result = reader.query("some command")

        assert result == ["line one", "line two"]
        assert reader._shell.stdin.written == b"some command; echo __BlinkSync_Done__\n"

    def test_stops_reading_once_the_sentinel_appears_but_keeps_trailing_text_on_its_line(self):
        # replace() only strips the sentinel substring itself - text sharing its chunk/line
        # (here "extra ignored") is not truncated, since the read loop stops pulling *more*
        # chunks once the sentinel shows up, but doesn't trim what already arrived alongside it.
        reader = make_reader()
        reader._shell = FakeShell(response_chunks=[b"a\n", b"b\n__BlinkSync_Done__extra ignored"])

        result = reader.query("cmd")

        assert result == ["a", "b", "extra ignored"]

    def test_empty_output_before_sentinel_returns_empty_list(self):
        reader = make_reader()
        reader._shell = FakeShell(response_chunks=[])  # read1 immediately returns b""

        assert reader.query("cmd") == []


class TestSendData:
    def test_delegates_to_query(self):
        reader = make_reader()
        calls = []
        reader.query = lambda cmd: calls.append(cmd) or []

        reader.send_data("some command")

        assert calls == ["some command"]


class TestPidHistoryKey:
    def test_packs_device_id_and_pid_into_a_single_uint64(self):
        assert AdbReader._pid_history_key(1, 2) == (1 << 32) | 2

    def test_different_device_ids_never_collide_for_the_same_pid(self):
        key_a = AdbReader._pid_history_key(1, 500)
        key_b = AdbReader._pid_history_key(2, 500)
        assert key_a != key_b


class FakePipeline:
    def __init__(self, sources, device_id):
        self.sources_ = sources
        self.local = SimpleNamespace(device_id=SimpleNamespace(id=device_id))


class TestOwningPipelineDeviceIds:
    def test_returns_empty_list_when_no_pipelines_attribute(self):
        reader = make_reader()
        reader.reference_id = "adb1"
        reader.shared = SimpleNamespace(registry=SimpleNamespace(pipelines=SimpleNamespace()))

        assert reader._owning_pipeline_device_ids() == []

    def test_matches_pipelines_whose_sources_include_this_readers_reference_id(self):
        reader = make_reader()
        reader.reference_id = "adb1"
        pipelines = {
            "p1": FakePipeline(sources=["adb1"], device_id=100),
            "p2": FakePipeline(sources=["other"], device_id=200),
            "p3": FakePipeline(sources="adb1", device_id=300),  # single string, not a list
        }
        reader.shared = SimpleNamespace(
            registry=SimpleNamespace(pipelines=SimpleNamespace(pipelines=pipelines))
        )

        assert sorted(reader._owning_pipeline_device_ids()) == [100, 300]


class TestResolveProcessName:
    def test_returns_first_resolved_name_across_owning_pipelines(self):
        reader = make_reader()
        reader._owning_pipeline_device_ids = lambda: [1, 2]
        resolved = {}

        class FakeHistory:
            def resolve(self, key, ts_ns):
                return resolved.get(key)

        reader.shared = SimpleNamespace(pid_history=FakeHistory())
        resolved[AdbReader._pid_history_key(2, 42)] = "com.example.app"

        assert reader.resolve_process_name(42, ts_ns=123) == "com.example.app"

    def test_returns_none_when_no_pipeline_resolves_it(self):
        reader = make_reader()
        reader._owning_pipeline_device_ids = lambda: [1]
        reader.shared = SimpleNamespace(pid_history=SimpleNamespace(resolve=lambda key, ts_ns: None))

        assert reader.resolve_process_name(42, ts_ns=123) is None


class TestRefreshProcessIds:
    def test_populates_process_ids_from_ps_output(self):
        reader = make_reader()
        reader.reference_id = "adb1"
        reader.query = lambda cmd: ["NAME PID", "com.example.app 1234", "system_server 55"]
        reader.shared = SimpleNamespace(
            time_ns=lambda: 1_000,
            pid_history=IdHistory(),
            registry=SimpleNamespace(pipelines=SimpleNamespace(pipelines={})),
        )

        reader._refresh_process_ids()

        assert reader._process_ids == {"com.example.app": 1234, "system_server": 55}

    def test_skips_malformed_lines(self):
        reader = make_reader()
        reader.query = lambda cmd: ["not-a-valid-line-at-all", "good_proc 99"]
        reader.shared = SimpleNamespace(
            time_ns=lambda: 1_000,
            pid_history=IdHistory(),
            registry=SimpleNamespace(pipelines=SimpleNamespace(pipelines={})),
        )

        reader._refresh_process_ids()

        assert reader._process_ids == {"good_proc": 99}

    def test_feeds_pid_history_for_each_owning_device(self):
        reader = make_reader()
        history = IdHistory()
        reader.query = lambda cmd: ["app_a 10"]
        reader._owning_pipeline_device_ids = lambda: [7]
        reader.shared = SimpleNamespace(time_ns=lambda: 5_000, pid_history=history)

        reader._refresh_process_ids()

        key = AdbReader._pid_history_key(7, 10)
        assert history.resolve(key, ts_ns=5_000) == "app_a"

    def test_closes_history_for_pids_that_disappeared_since_last_poll(self):
        reader = make_reader()
        history = IdHistory()
        reader._owning_pipeline_device_ids = lambda: [7]
        reader.shared = SimpleNamespace(time_ns=lambda: 1_000, pid_history=history)

        reader.query = lambda cmd: ["app_a 10"]
        reader._refresh_process_ids()

        reader.query = lambda cmd: []  # app_a's pid is gone this poll
        reader.shared = SimpleNamespace(time_ns=lambda: 2_000, pid_history=history)
        reader._refresh_process_ids()

        key = AdbReader._pid_history_key(7, 10)
        # Resolving at a timestamp after the close should no longer find the name.
        assert history.resolve(key, ts_ns=2_500) is None
        # But it did exist at the earlier timestamp, before the close.
        assert history.resolve(key, ts_ns=1_500) == "app_a"


class TestGetNameFromPid:
    def test_strips_trailing_nulls_and_updates_process_ids(self):
        reader = make_reader()
        reader.query = lambda cmd: ["com.example.app\x00\x00"]

        name = reader.get_name_from_pid(1234)

        assert name == "com.example.app"
        assert reader._process_ids["com.example.app"] == 1234

    def test_returns_none_when_query_yields_nothing(self):
        reader = make_reader()
        reader.query = lambda cmd: []
        assert reader.get_name_from_pid(1234) is None


class TestGetPidFromName:
    def test_finds_the_pid_of_an_exact_match(self):
        reader = make_reader()
        reader.query = lambda cmd: ["1234 com.example.app", "5678 com.example.app.other"]

        pid = reader.get_pid_from_name("com.example.app")

        assert pid == 1234
        assert reader._process_ids["com.example.app"] == 1234

    def test_returns_none_when_nothing_matches_exactly(self):
        reader = make_reader()
        reader.query = lambda cmd: ["5678 com.example.app.other"]

        assert reader.get_pid_from_name("com.example.app") is None


class TestGetBestCoarseAnchor:
    def test_picks_the_sample_with_the_lowest_rtt(self):
        reader = make_reader()
        # Two samples: first has a big RTT (10ms), second a small one (1ms) - the anchor should
        # come from the second, lower-RTT sample.
        clock = iter([0, 10_000_000, 20_000_000, 20_500_000])
        reader.shared = SimpleNamespace(time_ns=lambda: next(clock))
        reader.query = lambda cmd: ["1733138.750000000 0"]

        phone_ns, pc_ns, rtt = reader._get_best_coarse_anchor(num_tries=2)

        assert rtt == 500_000  # the second, tighter sample
        assert phone_ns == 1_733_138_750_000_000

    def test_falls_back_when_no_sample_parses(self):
        reader = make_reader()
        reader.shared = SimpleNamespace(time_ns=lambda: 42_000)
        reader.query = lambda cmd: []  # no output at all

        phone_ns, pc_ns, rtt = reader._get_best_coarse_anchor(num_tries=3)

        assert (phone_ns, pc_ns, rtt) == (42_000, 42_000, 0)


class TestCalculateSleepOffsetNs:
    def test_returns_the_median_of_valid_samples(self):
        reader = make_reader()
        # boot=10s, mono=9s -> offset = 1s in ns, consistent across samples
        reader.query = lambda cmd: ["10.0 9.0"]

        offset = reader._calculate_sleep_offset_ns(num_samples=3)

        assert offset == 1_000_000_000

    def test_returns_zero_when_no_valid_samples(self):
        reader = make_reader()
        reader.query = lambda cmd: ["garbage output"]

        assert reader._calculate_sleep_offset_ns(num_samples=2) == 0


class FakeProc:
    def __init__(self, pid=1234, terminate_raises=None, wait_raises=None):
        self.pid = pid
        self.terminated = False
        self.waited = False
        self.killed = False
        self._terminate_raises = terminate_raises
        self._wait_raises = wait_raises

    def terminate(self):
        self.terminated = True
        if self._terminate_raises:
            raise self._terminate_raises

    def wait(self, timeout=None):
        self.waited = True
        if self._wait_raises:
            raise self._wait_raises

    def kill(self):
        self.killed = True


class TestFinalizeSubprocess:
    def test_terminates_and_waits_on_the_happy_path(self):
        reader = make_reader()
        proc = FakeProc()

        reader._finalize_subprocess(proc)

        assert proc.terminated is True
        assert proc.waited is True
        assert proc.killed is False

    def test_kills_when_wait_times_out(self):
        reader = make_reader()
        proc = FakeProc(wait_raises=subprocess.TimeoutExpired(cmd="x", timeout=2.0))

        reader._finalize_subprocess(proc)

        assert proc.killed is True

    def test_swallows_exceptions_from_terminate(self):
        reader = make_reader()
        proc = FakeProc(terminate_raises=RuntimeError("boom"))

        reader._finalize_subprocess(proc)  # must not raise


class TestCleanupProcess:
    def test_terminates_process_and_clears_reference(self):
        reader = make_reader()
        proc = FakeProc()
        reader._process = proc

        reader._cleanup_process()

        assert proc.terminated is True
        assert reader._process is None

    def test_polite_exit_write_fails_silently_but_termination_still_happens(self):
        # Real _shell.stdin is a binary-mode pipe (Popen(..., text=False)) - writing the str
        # "exit\n" to it raises TypeError, swallowed by the surrounding except-Exception:pass, so
        # the "polite" exit never actually reaches the shell. Harmless because terminate()/wait()
        # unconditionally follow regardless - documenting the real (silently-broken) behavior
        # rather than asserting on a write that never lands.
        reader = make_reader()
        shell = FakeShell()
        shell.pid = 999
        shell.terminate = lambda: setattr(shell, "terminated", True)
        shell.wait = lambda timeout=None: None
        reader._shell = shell

        reader._cleanup_process()

        assert shell.stdin.written == bytearray()  # the str write never actually landed
        assert shell.terminated is True
        assert reader._shell is None

    def test_stops_the_pid_poll_task_if_one_is_registered(self):
        reader = make_reader()
        stopped = []
        reader.shared = SimpleNamespace(tasks=SimpleNamespace(stop_periodic=lambda tid: stopped.append(tid)))
        reader._pid_poll_task_id = "task-123"

        reader._cleanup_process()

        assert stopped == ["task-123"]
        assert reader._pid_poll_task_id is None


class QueueParser:
    def __init__(self):
        self.queue: "queue.Queue[bytes]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                self.queue.put(bytes(msg))


def _drain_until(q, expected, timeout):
    collected = bytearray()
    deadline = time.time() + timeout
    while bytes(collected) != expected:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            collected += q.get(timeout=remaining)
        except queue.Empty:
            break
    return bytes(collected)


class TestRunRealPipeIngestion:
    """Runs AdbReader.run() for real against a genuine OS pipe standing in for the ADB
    subprocess's stdout - open() (which needs a real adb binary/device) is monkeypatched to just
    set self._process to a fake object wrapping the pipe's read end, so the actual
    os.read()-driven batch/distribute loop gets exercised end-to-end without any device."""

    def test_ingests_bytes_from_a_real_os_pipe(self):
        reader = make_reader(delay=20)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        read_fd, write_fd = os.pipe()
        # Default buffering (not buffering=0) - the code path unconditionally binds
        # self._process.stdout.read1 (even though it's unused later in favor of os.read), and
        # unbuffered FileIO has no read1() method, only BufferedReader does.
        fake_stdout = os.fdopen(read_fd, "rb")

        def fake_open():
            reader._process = SimpleNamespace(
                stdout=fake_stdout, terminate=lambda: None, wait=lambda timeout=None: None, kill=lambda: None, pid=-1
            )

        reader.open = fake_open

        subscriber = QueueParser()
        reader.subscribe(subscriber)

        expected = b"hello from a fake adb logcat stream"

        reader.start()
        try:
            os.write(write_fd, expected)

            # The time-based flush check only runs inside the "a chunk just arrived" branch, so
            # a single burst with nothing after it never gets flushed on its own - wait past the
            # configured delay, then write a trailing nudge byte to trigger the next os.read()
            # cycle, whose flush check now sees enough elapsed time and distributes the batch
            # (as two separate rows; the first one alone already equals `expected`).
            time.sleep(0.05)
            os.write(write_fd, b"x")

            received = _drain_until(subscriber.queue, expected, timeout=5.0)
        finally:
            os.close(write_fd)  # EOF wakes the blocking os.read() so stop() doesn't hang
            reader.stop()

        assert received == expected

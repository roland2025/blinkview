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
        self.closed = False

    def write(self, data: bytes):
        self.written.extend(data)

    def flush(self):
        pass

    def close(self):
        self.closed = True

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


class LiveFakeShell(FakeShell):
    """A FakeShell that is still running (poll() is None) until killed; a killed one reads EOF."""

    def __init__(self, response_chunks=(), block_reads=False, write_raises=None):
        super().__init__(response_chunks)
        self.returncode = None
        self.killed = False
        self._block = block_reads
        self._write_raises = write_raises
        self._killed_event = __import__("threading").Event()
        if write_raises is not None:
            self.stdin.write = self._raise_on_write
        if block_reads:
            self.stdout.read1 = self._blocking_read1

    def _raise_on_write(self, data):
        raise self._write_raises

    def _blocking_read1(self, _size):
        self._killed_event.wait(5)
        return b""

    def poll(self):
        return self.returncode

    def kill(self):
        self.killed = True
        self.returncode = -9
        self._killed_event.set()


def connected_reader(shell):
    reader = make_reader()
    reader._shell = shell
    reader._process = object()  # logcat is up
    reader._base_cmd = ["adb"]
    return reader


class TestQuery:
    def test_returns_empty_list_when_not_connected(self):
        reader = make_reader()
        reader._shell = None
        assert reader.query("echo hi") == []

    def test_runs_the_command_in_its_own_sh_and_returns_stripped_lines(self):
        reader = connected_reader(LiveFakeShell([b"line one\nline two\n__BlinkSync_Done__ 0\n"]))

        result = reader.query("some command")

        assert result == ["line one", "line two"]
        assert reader._shell.stdin.written == b"sh -c 'some command' </dev/null 2>&1; echo __BlinkSync_Done__ $?\n"

    def test_sentinel_split_across_chunks(self):
        reader = connected_reader(LiveFakeShell([b"a\n__BlinkSync_Do", b"ne_", b"_ 0", b"\r\n"]))

        assert reader.query("cmd") == ["a"]

    def test_echoed_input_does_not_count_as_the_sentinel(self):
        # A shell with a pty echoes the line it was sent, which contains the sentinel text
        echoed = b"sh -c 'cmd' </dev/null 2>&1; echo __BlinkSync_Done__ $?\r\n"
        reader = connected_reader(LiveFakeShell([echoed, b"out\n__BlinkSync_Done__ 0\n"]))

        assert reader.query("cmd")[-1] == "out"

    def test_shell_ending_before_the_sentinel_returns_empty_list_and_drops_the_shell(self):
        shell = LiveFakeShell([b"partial\n"])
        reader = connected_reader(shell)

        assert reader.query("cmd") == []
        assert reader._shell is None
        assert shell.killed


class TestRunIsolatesTheCommand:
    def test_trailing_newline_is_quoted_away(self):
        # The bug: "cmd\n; echo SENTINEL" put "; echo" on its own line, a syntax error that made
        # the non-interactive shell exit after the first send.
        reader = connected_reader(LiveFakeShell([b"__BlinkSync_Done__ 0\n"]))

        reader.send_data("am broadcast -a X --es msg hi\n")

        assert reader._shell.stdin.written == (
            b"sh -c 'am broadcast -a X --es msg hi' </dev/null 2>&1; echo __BlinkSync_Done__ $?\n"
        )

    def test_quotes_and_shell_syntax_stay_inside_the_sh_c_argument(self):
        import shlex

        text = "echo 'it''s' \"x\"; exit; (unclosed"
        reader = connected_reader(LiveFakeShell([b"__BlinkSync_Done__ 0\n"]))

        reader.send_data(text)

        wire = bytes(reader._shell.stdin.written).decode()
        assert shlex.split(wire.split(" </dev/null")[0]) == ["sh", "-c", text]


class TestSendData:
    def test_returns_quietly_on_exit_code_zero(self):
        reader = connected_reader(LiveFakeShell([b"Broadcast completed\n__BlinkSync_Done__ 0\n"]))

        reader.send_data("am broadcast")  # must not raise

    def test_nonzero_exit_code_raises_with_the_last_output_line(self):
        import pytest

        from blinkview.io.adb_reader import AdbShellError

        reader = connected_reader(LiveFakeShell([b"sh: amm: inaccessible or not found\n__BlinkSync_Done__ 127\n"]))

        with pytest.raises(AdbShellError, match="127.*amm: inaccessible"):
            reader.send_data("amm broadcast")

    def test_raises_when_not_connected(self):
        import pytest

        from blinkview.io.adb_reader import AdbShellError

        reader = make_reader()
        with pytest.raises(AdbShellError, match="not connected"):
            reader.send_data("am broadcast")

    def test_closed_pipe_restarts_the_shell_and_delivers_the_command_once(self):
        # The symptom: OSError(22) writing to a shell that had exited
        dead = LiveFakeShell(write_raises=OSError(22, "Invalid argument"))
        fresh = LiveFakeShell([b"__BlinkSync_Done__ 0\n"])
        reader = connected_reader(dead)
        reader._start_shell = lambda base_cmd: fresh

        reader.send_data("am broadcast")

        assert dead.killed
        assert reader._shell is fresh
        assert fresh.stdin.written.count(b"am broadcast") == 1

    def test_exited_shell_is_restarted_before_writing(self):
        dead = LiveFakeShell()
        dead.returncode = 1
        fresh = LiveFakeShell([b"__BlinkSync_Done__ 0\n"])
        reader = connected_reader(dead)
        reader._start_shell = lambda base_cmd: fresh

        reader.send_data("am broadcast")

        assert dead.stdin.written == b""
        assert fresh.stdin.written.count(b"am broadcast") == 1

    def test_shell_ending_mid_command_raises_and_is_not_retried(self):
        import pytest

        from blinkview.io.adb_reader import AdbShellError

        shell = LiveFakeShell([b"Broadcasting: Intent\n"])
        reader = connected_reader(shell)
        starts = []
        reader._start_shell = lambda base_cmd: starts.append(1) or LiveFakeShell()

        with pytest.raises(AdbShellError, match="ended before"):
            reader.send_data("am broadcast")

        assert starts == []  # the command may have run: no second attempt
        assert reader._shell is None

    def test_a_stuck_command_is_killed_after_the_timeout(self):
        import pytest

        from blinkview.io.adb_reader import AdbShellError

        shell = LiveFakeShell(block_reads=True)
        reader = connected_reader(shell)

        with pytest.raises(AdbShellError, match="no reply"):
            reader._run("top", timeout_s=0.2)

        assert shell.killed
        assert reader._shell is None


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
        reader.shared = SimpleNamespace(registry=SimpleNamespace(pipelines=SimpleNamespace(pipelines=pipelines)))

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
        reader._run = lambda cmd: (0, ["NAME PID", "com.example.app 1234", "system_server 55"])
        reader.shared = SimpleNamespace(
            time_ns=lambda: 1_000,
            pid_history=IdHistory(),
            registry=SimpleNamespace(pipelines=SimpleNamespace(pipelines={})),
        )

        reader._refresh_process_ids()

        assert reader._process_ids == {"com.example.app": 1234, "system_server": 55}

    def test_skips_malformed_lines(self):
        reader = make_reader()
        reader._run = lambda cmd: (0, ["not-a-valid-line-at-all", "good_proc 99"])
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
        reader._run = lambda cmd: (0, ["app_a 10"])
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

        reader._run = lambda cmd: (0, ["app_a 10"])
        reader._refresh_process_ids()

        reader._run = lambda cmd: (0, [])  # app_a's pid is gone this poll
        reader.shared = SimpleNamespace(time_ns=lambda: 2_000, pid_history=history)
        reader._refresh_process_ids()

        key = AdbReader._pid_history_key(7, 10)
        # Resolving at a timestamp after the close should no longer find the name.
        assert history.resolve(key, ts_ns=2_500) is None
        # But it did exist at the earlier timestamp, before the close.
        assert history.resolve(key, ts_ns=1_500) == "app_a"


class TestRefreshProcessIdsOnShellFailure:
    def test_failed_poll_keeps_the_known_pids_open(self):
        from blinkview.io.adb_reader import AdbShellError

        reader = make_reader()
        history = IdHistory()
        reader._owning_pipeline_device_ids = lambda: [7]
        reader.shared = SimpleNamespace(time_ns=lambda: 1_000, pid_history=history)
        reader._run = lambda cmd: (0, ["app_a 10"])
        reader._refresh_process_ids()

        def fail(cmd):
            raise AdbShellError("ADB shell ended")

        reader._run = fail
        reader.shared = SimpleNamespace(time_ns=lambda: 2_000, pid_history=history)
        reader._refresh_process_ids()  # must not raise

        assert reader._process_ids == {"app_a": 10}
        assert history.resolve(AdbReader._pid_history_key(7, 10), ts_ns=2_500) == "app_a"


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

    def test_polite_exit_writes_bytes_and_closes_stdin_before_termination(self):
        # Real _shell.stdin is a binary-mode pipe (Popen(..., text=False)) - FakeStream's
        # bytearray.extend() rejects str the same way, so this fails if the write regresses to str.
        reader = make_reader()
        shell = FakeShell()
        shell.pid = 999
        shell.terminate = lambda: setattr(shell, "terminated", True)
        shell.wait = lambda timeout=None: None
        reader._shell = shell

        reader._cleanup_process()

        assert shell.stdin.written == bytearray(b"exit\n")
        assert shell.stdin.closed is True
        assert shell.terminated is True
        assert reader._shell is None

    def test_still_terminates_when_shell_ignores_polite_exit(self):
        reader = make_reader()
        shell = FakeShell()
        shell.pid = 999
        shell.terminate = lambda: setattr(shell, "terminated", True)
        waits = []

        def wait(timeout=None):
            waits.append(timeout)
            if len(waits) == 1:
                raise subprocess.TimeoutExpired("adb shell", timeout)

        shell.wait = wait
        reader._shell = shell

        reader._cleanup_process()

        assert waits[0] == 0.5
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


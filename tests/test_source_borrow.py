# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Lending a source's port: SourceBorrowWatcher (io/source_borrow.py) against a fake reader, the
BaseReader lifecycle that owns it, and one end-to-end run - a real UARTReader on a socket://
port, borrowed by a real `blink borrow` subprocess."""

import socket
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

import pytest

from blinkview import borrow
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.core.task_manager import TaskManager
from blinkview.io import source_borrow
from blinkview.io.source_borrow import SourceBorrowWatcher
from blinkview.io.uart import UARTReader


@pytest.fixture
def borrow_dir(tmp_path, monkeypatch):
    directory = tmp_path / "borrow"
    monkeypatch.setenv(borrow.BORROW_DIR_ENV, str(directory))
    return directory


class FakeTasks:
    def __init__(self):
        self.started = []
        self.stopped = []

    def run_periodic(self, interval, func, *args, **kwargs):
        self.started.append((interval, func))
        return f"task-{len(self.started)}"

    def stop_periodic(self, task_id):
        self.stopped.append(task_id)


class FakeReader:
    """The pause interface only. `pauses_after_checks` delays is_paused like a reader thread that
    is still inside a read when the pause is requested."""

    def __init__(self, pauses_after_checks=0):
        self.pause_requested = False
        self.reasons = []
        self.resumes = 0
        self._pauses_after_checks = pauses_after_checks
        self._checks = 0

    def request_pause(self, reason):
        self.pause_requested = True
        self.reasons.append(reason)
        self._checks = 0

    def request_resume(self):
        self.pause_requested = False
        self.resumes += 1

    @property
    def is_paused(self):
        if not self.pause_requested:
            return False
        self._checks += 1
        return self._checks > self._pauses_after_checks


class FakeClock:
    """Advances a little on every read, so the watcher's bounded waits end without real sleeps."""

    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        self.now += 0.01
        return self.now


def make_watcher(reader=None, timeout_s=120, alive=lambda pid: True, name="COM5"):
    paths = borrow.resolve(name)
    clock = FakeClock()
    watcher = SourceBorrowWatcher(
        reader or FakeReader(),
        paths,
        timeout_s=timeout_s,
        logger=PrintLogger("test.borrow"),
        tasks=FakeTasks(),
        clock=clock,
        pid_alive=alive,
    )
    return watcher, paths, clock


class TestStart:
    def test_removes_leftover_files_and_starts_polling(self, borrow_dir):
        watcher, paths, _ = make_watcher()
        borrow.write_borrow(paths, pid=1)
        borrow.write_lent(paths, {})

        watcher.start()

        assert not paths.borrow.exists()
        assert not paths.lent.exists()
        assert watcher.tasks.started == [(source_borrow.POLL_S, watcher.check)]

    def test_start_twice_polls_once(self, borrow_dir):
        watcher, _, _ = make_watcher()
        watcher.start()
        watcher.start()
        assert len(watcher.tasks.started) == 1


class TestLending:
    def test_a_borrow_pauses_the_reader_and_acks(self, borrow_dir):
        watcher, paths, _ = make_watcher()
        borrow.write_borrow(paths, pid=1234, by="pio")

        watcher.check()

        assert watcher.reader.pause_requested
        assert watcher.reader.reasons == ["lent to pio (pid 1234)"]
        assert watcher.is_lent
        assert borrow.is_lent(paths)

    def test_nothing_happens_without_a_borrow(self, borrow_dir):
        watcher, paths, _ = make_watcher()
        watcher.check()
        assert not watcher.reader.pause_requested
        assert not paths.lent.exists()

    def test_the_ack_waits_for_the_port_to_actually_close(self, borrow_dir):
        reader = FakeReader(pauses_after_checks=10_000)
        watcher, paths, _ = make_watcher(reader)
        borrow.write_borrow(paths, pid=1)

        watcher.check()  # gives up waiting after PAUSE_CONFIRM_S (fake time)

        assert reader.pause_requested
        assert not paths.lent.exists()

        reader._pauses_after_checks = 0
        watcher.check()

        assert paths.lent.exists()

    def test_return_resumes_and_removes_the_ack(self, borrow_dir):
        watcher, paths, _ = make_watcher()
        borrow.write_borrow(paths, pid=1)
        watcher.check()

        borrow.remove(paths.borrow)
        watcher.check()

        assert not watcher.reader.pause_requested
        assert watcher.reader.resumes == 1
        assert not paths.lent.exists()
        assert not watcher.is_lent

    def test_a_borrow_without_json_stays_lent_until_removed(self, borrow_dir):
        """A tool that only touches the file: no owner to check, so only its removal (or the
        timeout) ends it."""
        watcher, paths, _ = make_watcher(alive=borrow.pid_alive)
        borrow_dir.mkdir()
        paths.borrow.write_text("")

        watcher.check()
        watcher.check()
        assert watcher.is_lent

        borrow.remove(paths.borrow)
        watcher.check()
        assert not watcher.is_lent

    def test_lends_again_after_a_return(self, borrow_dir):
        watcher, paths, _ = make_watcher()
        for _ in range(2):
            borrow.write_borrow(paths, pid=1)
            watcher.check()
            assert borrow.is_lent(paths)
            borrow.remove(paths.borrow)
            watcher.check()
            assert not borrow.is_lent(paths)
        assert watcher.reader.resumes == 2


class TestSafetyExits:
    def test_a_dead_owner_ends_the_borrow(self, borrow_dir):
        """PlatformIO skips its post-upload action when the upload fails: the borrow file stays,
        but the pio process that owns it exits."""
        alive = {"value": True}
        watcher, paths, _ = make_watcher(alive=lambda pid: alive["value"])
        borrow.write_borrow(paths, pid=1234)
        watcher.check()

        alive["value"] = False
        watcher.check()

        assert not watcher.reader.pause_requested
        assert not paths.lent.exists()
        assert paths.borrow.exists()  # not blink's file to delete

    def test_the_left_behind_file_is_ignored_until_it_changes(self, borrow_dir):
        alive = {"value": True}
        watcher, paths, _ = make_watcher(alive=lambda pid: alive["value"])
        borrow.write_borrow(paths, pid=1234)
        watcher.check()
        alive["value"] = False
        watcher.check()

        watcher.check()
        assert not watcher.reader.pause_requested

        time.sleep(0.01)  # a new ts
        alive["value"] = True
        borrow.write_borrow(paths, pid=5678)
        watcher.check()
        assert watcher.reader.pause_requested

    def test_the_timeout_takes_the_port_back(self, borrow_dir):
        watcher, paths, clock = make_watcher(timeout_s=60)
        borrow.write_borrow(paths, pid=1)
        watcher.check()

        clock.now += 30
        watcher.check()
        assert watcher.reader.pause_requested

        clock.now += 31
        watcher.check()
        assert not watcher.reader.pause_requested
        assert not paths.lent.exists()

        watcher.check()
        assert not watcher.reader.pause_requested  # same file: ignored

    def test_timeout_zero_never_takes_it_back(self, borrow_dir):
        watcher, paths, clock = make_watcher(timeout_s=0)
        borrow.write_borrow(paths, pid=1)
        watcher.check()
        clock.now += 10_000
        watcher.check()
        assert watcher.reader.pause_requested

    def test_a_new_borrow_while_lent_restarts_the_clock(self, borrow_dir):
        watcher, paths, clock = make_watcher(timeout_s=60)
        borrow.write_borrow(paths, pid=1)
        watcher.check()
        clock.now += 50

        time.sleep(0.01)
        borrow.write_borrow(paths, pid=2)
        watcher.check()
        clock.now += 50
        watcher.check()

        assert watcher.reader.pause_requested
        assert watcher.reader.resumes == 0


class TestStop:
    def test_stop_while_lent_resumes_and_removes_the_ack(self, borrow_dir):
        watcher, paths, _ = make_watcher()
        watcher.start()
        borrow.write_borrow(paths, pid=1)
        watcher.check()

        watcher.stop()

        assert watcher.tasks.stopped == ["task-1"]
        assert not watcher.reader.pause_requested
        assert not paths.lent.exists()

    def test_errors_in_a_poll_are_logged_not_raised(self, borrow_dir, monkeypatch):
        watcher, _, _ = make_watcher()

        def boom(paths):
            raise RuntimeError("disk gone")

        monkeypatch.setattr(borrow, "read_borrow", boom)
        watcher.check()  # must not raise


def make_uart(**config):
    reader = UARTReader()
    reader.logger = PrintLogger("test.uart")
    reader.shared = SimpleNamespace(tasks=FakeTasks(), time_ns=time.time_ns, array_pool=NumpyArrayPool())
    reader.apply_config({"url": "socket://127.0.0.1:1", **config})
    return reader


class TestReaderLifecycle:
    def test_start_creates_the_watcher_with_the_ports_name(self, borrow_dir, monkeypatch):
        monkeypatch.setattr(UARTReader, "run", lambda self: self._stop_event.wait())
        reader = make_uart(enabled=True, allow_borrow=True, url="COM7")

        reader.start()
        try:
            assert reader._borrow_watcher is not None
            assert reader._borrow_watcher.paths == borrow.resolve("COM7")
            assert reader._borrow_watcher.timeout_s == 120
        finally:
            reader.stop()

        assert reader._borrow_watcher is None
        assert reader.shared.tasks.stopped == ["task-1"]

    def test_borrow_name_overrides_the_port_name(self, borrow_dir, monkeypatch):
        monkeypatch.setattr(UARTReader, "run", lambda self: self._stop_event.wait())
        reader = make_uart(enabled=True, allow_borrow=True, url="COM7", borrow_name="esp32-main")

        reader.start()
        try:
            assert reader._borrow_watcher.paths == borrow.resolve("esp32-main")
        finally:
            reader.stop()

    def test_no_watcher_without_allow_borrow(self, borrow_dir, monkeypatch):
        monkeypatch.setattr(UARTReader, "run", lambda self: self._stop_event.wait())
        reader = make_uart(enabled=True, url="COM7")

        reader.start()
        try:
            assert reader._borrow_watcher is None
            assert reader.shared.tasks.started == []
        finally:
            reader.stop()

    def test_no_watcher_for_a_disabled_source(self, borrow_dir):
        reader = make_uart(enabled=False, allow_borrow=True, url="COM7")
        reader.start()
        assert reader._borrow_watcher is None


class MultiAcceptServer:
    """A TCP peer that accepts any number of connections - the reader reconnects after a borrow."""

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(4)
        self.port = self._sock.getsockname()[1]
        self.connections = []
        self._cond = threading.Condition()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self):
        while True:
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            with self._cond:
                self.connections.append(conn)
                self._cond.notify_all()

    def wait_for_connection(self, count, timeout=10.0):
        with self._cond:
            assert self._cond.wait_for(lambda: len(self.connections) >= count, timeout), "no connection"
            return self.connections[count - 1]

    def close(self):
        for conn in self.connections:
            conn.close()
        self._sock.close()
        self._thread.join(timeout=2.0)


class Recorder:
    def __init__(self):
        self.data = b""
        self._lock = threading.Lock()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                with self._lock:
                    self.data += msg

    def wait_for(self, expected: bytes, timeout=10.0):
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._lock:
                if expected in self.data:
                    return
            time.sleep(0.01)
        raise AssertionError(f"never received {expected!r}, got {self.data!r}")


def wait_until(predicate, timeout=10.0, what="condition"):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return
        time.sleep(0.02)
    raise AssertionError(f"timed out waiting for {what}")


def connection_closed(conn) -> bool:
    conn.settimeout(0.05)
    try:
        return conn.recv(1) == b""
    except socket.timeout:
        return False
    except OSError:
        return True


@pytest.fixture
def running_reader(borrow_dir):
    server = MultiAcceptServer()
    tasks = TaskManager(max_workers=2)
    reader = UARTReader()
    reader.logger = PrintLogger("test.uart")
    reader.shared = SimpleNamespace(tasks=tasks, time_ns=time.time_ns, array_pool=NumpyArrayPool())
    reader.apply_config(
        {
            "url": f"socket://127.0.0.1:{server.port}",
            "delay": 20,
            "enabled": True,
            "allow_borrow": True,
            "borrow_name": "e2e-board",
        }
    )
    recorder = Recorder()
    reader.subscribe(recorder)
    reader.start()
    try:
        conn = server.wait_for_connection(1)
        wait_until(lambda: reader.is_connected, what="the reader to connect")
        conn.sendall(b"before;")
        recorder.wait_for(b"before;")
        yield SimpleNamespace(reader=reader, server=server, recorder=recorder, paths=borrow.resolve("e2e-board"))
    finally:
        reader.stop()
        tasks.shutdown(wait=False)
        server.close()


def wait_reconnected(r):
    """Waits for the reader's second connection to be fully open: pyserial's open() flushes the
    input buffer right after connecting, so bytes sent the moment the server accepts can be lost."""
    conn = r.server.wait_for_connection(2)
    wait_until(lambda: r.reader.is_connected and not r.reader.is_paused, what="the reader to reconnect")
    return conn


def blink(*args, **kwargs):
    return subprocess.Popen([sys.executable, "-m", "blinkview", *args], **kwargs)


class TestEndToEnd:
    """A real `blink borrow` in a subprocess against a running UARTReader: the port is closed
    for exactly the command's lifetime, and data flows again afterwards."""

    def test_borrow_with_a_command(self, running_reader, tmp_path):
        r = running_reader
        started, release = tmp_path / "started", tmp_path / "release"
        child_code = (
            "import pathlib, sys, time\n"
            f"pathlib.Path({str(started)!r}).touch()\n"
            f"while not pathlib.Path({str(release)!r}).exists(): time.sleep(0.02)\n"
            "sys.exit(3)\n"
        )
        tool = blink("borrow", "e2e-board", "--timeout", "10", "--", sys.executable, "-c", child_code)
        try:
            wait_until(started.exists, what="the borrowed command to start")

            # The command only runs once blink acked, and the port is closed by then.
            assert borrow.is_lent(r.paths)
            assert r.reader.is_paused
            assert not r.reader.is_connected
            assert connection_closed(r.server.connections[0])

            release.touch()
            assert tool.wait(timeout=10) == 3  # the command's exit code
        finally:
            tool.kill()

        assert not r.paths.borrow.exists()  # returned even though the command failed
        conn = wait_reconnected(r)
        assert not r.paths.lent.exists()
        conn.sendall(b"after;")
        r.recorder.wait_for(b"after;")

    def test_split_borrow_and_return(self, running_reader):
        r = running_reader
        assert blink("borrow", "e2e-board", "--timeout", "10").wait(timeout=15) == 0
        assert r.reader.is_paused

        assert blink("return", "e2e-board").wait(timeout=15) == 0
        conn = wait_reconnected(r)
        conn.sendall(b"after;")
        r.recorder.wait_for(b"after;")

    def test_reset_after_return_resets_once_the_port_is_back(self, running_reader, monkeypatch):
        r = running_reader
        resets = []
        monkeypatch.setattr(r.reader, "reset_device", lambda: resets.append(r.reader.is_connected))
        r.reader.reset_after_return = True

        assert blink("borrow", "e2e-board", "--timeout", "10").wait(timeout=15) == 0
        assert resets == []
        assert blink("return", "e2e-board").wait(timeout=15) == 0

        r.server.wait_for_connection(2)
        wait_until(lambda: resets, what="the reset")
        assert resets == [True]  # reset on the reopened port, so the boot log is read

    def test_a_split_borrow_ends_when_its_owner_dies(self, running_reader):
        """The PlatformIO case: the upload fails, `blink return` never runs, pio exits."""
        r = running_reader
        owner = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            tool = blink("borrow", "e2e-board", "--timeout", "10", "--pid", str(owner.pid))
            assert tool.wait(timeout=15) == 0
            assert r.reader.is_paused
        finally:
            owner.kill()
            owner.wait()

        conn = wait_reconnected(r)
        assert r.paths.borrow.exists()  # left behind - and ignored
        conn.sendall(b"after;")
        r.recorder.wait_for(b"after;")
        time.sleep(source_borrow.POLL_S * 1.5)
        assert not r.reader.is_paused

    def test_no_blink_lending_times_out_and_runs_the_command(self, borrow_dir):
        tool = blink(
            "borrow",
            "nobody-lends-this",
            "--timeout",
            "0.3",
            "--",
            sys.executable,
            "-c",
            "print('ran')",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out, err = tool.communicate(timeout=15)
        assert tool.returncode == 0
        assert out.strip() == "ran"
        assert "no blink is lending nobody-lends-this" in err
        assert not borrow.resolve("nobody-lends-this").borrow.exists()

    def test_strict_fails_without_running_the_command(self, borrow_dir):
        tool = blink(
            "borrow",
            "nobody-lends-this",
            "--timeout",
            "0.3",
            "--strict",
            "--",
            sys.executable,
            "-c",
            "print('ran')",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        out, err = tool.communicate(timeout=15)
        assert tool.returncode == 1
        assert out == ""
        assert not borrow.resolve("nobody-lends-this").borrow.exists()

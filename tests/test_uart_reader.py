# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import socket
import threading
import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.io.uart import UARTReader


class FakeTasks:
    """Duck-typed stand-in for TaskManager's run_periodic/stop_periodic, avoiding the real
    TaskManager's background scheduler thread."""

    def __init__(self):
        self.started = []
        self.stopped = []

    def run_periodic(self, interval, func, *args, **kwargs):
        self.started.append((interval, func))
        return "task-id"

    def stop_periodic(self, task_id):
        self.stopped.append(task_id)


class FakeSerial:
    def __init__(self, is_open=True):
        self.is_open = is_open
        self.dtr = None
        self.rts = None
        self.written = []

    def write(self, data):
        self.written.append(data)

    def close(self):
        self.is_open = False


class BrokenDtrSerial:
    """Raises when dtr is set - exercises reset_device()'s exception handling path."""

    def __init__(self):
        self.is_open = True
        self.rts = None

    @property
    def dtr(self):
        return getattr(self, "_dtr", None)

    @dtr.setter
    def dtr(self, value):
        raise OSError("hardware gone")


def make_reader(now_ns=1_000_000_000, **config_overrides):
    reader = UARTReader()
    reader.logger = PrintLogger("test.uart")
    reader.shared = SimpleNamespace(tasks=FakeTasks(), time_ns=lambda: now_ns)
    reader.apply_config(config_overrides)
    return reader


class TestDefaults:
    def test_default_config_values(self):
        reader = make_reader()
        assert reader.url == ""
        assert reader.baudrate == 115200
        assert reader.delay == 100
        assert reader.suppress_auto_reset is False
        assert reader.flash_handshake is False
        assert reader.flash_handshake_id == "uart"

    def test_apply_config_wires_up_logger_send(self):
        reader = make_reader()
        assert reader.logger_send is not None


class TestSendData:
    def test_writes_encoded_bytes_when_open(self):
        reader = make_reader()
        reader.serial = FakeSerial(is_open=True)
        reader.send_data("hello")
        assert reader.serial.written == [b"hello"]

    def test_noop_when_serial_is_none(self):
        reader = make_reader()
        reader.serial = None
        reader.send_data("hello")  # must not raise

    def test_noop_when_serial_is_closed(self):
        reader = make_reader()
        reader.serial = FakeSerial(is_open=False)
        reader.send_data("hello")
        assert reader.serial.written == []

    def test_write_exception_marks_serial_broken(self):
        reader = make_reader()
        serial = FakeSerial(is_open=True)

        def boom(data):
            raise OSError("gone")

        serial.write = boom
        reader.serial = serial

        reader.send_data("hello")

        assert reader.serial_broken is True


class TestResetDevice:
    def test_toggles_dtr_rts_and_ends_in_run_mode(self):
        reader = make_reader()
        serial = FakeSerial(is_open=True)
        reader.serial = serial

        reader.reset_device()

        # Final state per the ESP32 EN-high/GPIO0-high (normal run) sequence.
        assert serial.dtr is False
        assert serial.rts is False

    def test_warns_and_is_a_noop_when_not_open(self):
        reader = make_reader()
        reader.serial = None
        reader.reset_device()  # must not raise

    def test_exception_marks_serial_broken(self):
        reader = make_reader()
        reader.serial = BrokenDtrSerial()

        reader.reset_device()

        assert reader.serial_broken is True


class TestIsConnected:
    def test_true_when_serial_is_open(self):
        reader = make_reader()
        reader.serial = FakeSerial(is_open=True)
        assert reader.is_connected() is True

    def test_false_when_serial_is_none(self):
        reader = make_reader()
        reader.serial = None
        assert reader.is_connected() is False

    def test_false_when_serial_is_closed(self):
        reader = make_reader()
        reader.serial = FakeSerial(is_open=False)
        assert reader.is_connected() is False


class TestApplyConfigFlashHandshakeTeardown:
    def test_stops_periodic_task_when_flash_handshake_is_disabled(self):
        reader = make_reader(flash_handshake=True)
        reader._handshake_task_id = "abc123"

        reader.apply_config({"flash_handshake": False})

        assert reader.shared.tasks.stopped == ["abc123"]
        assert reader._handshake_task_id is None

    def test_leaves_task_alone_when_flash_handshake_stays_enabled(self):
        reader = make_reader(flash_handshake=True)
        reader._handshake_task_id = "abc123"

        reader.apply_config({"flash_handshake": True})

        assert reader.shared.tasks.stopped == []
        assert reader._handshake_task_id == "abc123"


class TestClearHandshakeFiles:
    def test_removes_lock_and_closed_files_when_enabled(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        reader = make_reader(flash_handshake=True, flash_handshake_id="esp32")
        (tmp_path / ".bv_source_esp32.lock").write_text("x")
        (tmp_path / ".bv_source_esp32.closed").write_text("x")

        reader._clear_handshake_files()

        assert not (tmp_path / ".bv_source_esp32.lock").exists()
        assert not (tmp_path / ".bv_source_esp32.closed").exists()
        assert reader._flash_active is False

    def test_noop_when_flash_handshake_disabled(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        reader = make_reader(flash_handshake=False)
        (tmp_path / ".bv_source_uart.lock").write_text("x")

        reader._clear_handshake_files()

        assert (tmp_path / ".bv_source_uart.lock").exists()  # left untouched


class TestCheckFlashHandshake:
    def test_lock_file_activates_handshake_and_writes_closed_marker(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        reader = make_reader(flash_handshake=True, flash_handshake_id="esp32")
        reader.serial = None  # already closed, so the wait-for-close loop exits immediately
        (tmp_path / ".bv_source_esp32.lock").write_text("x")

        reader._check_flash_handshake()

        assert reader._flash_active is True
        assert (tmp_path / ".bv_source_esp32.closed").exists()

    def test_lock_removed_clears_active_flag_and_closed_marker(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        reader = make_reader(flash_handshake=True, flash_handshake_id="esp32")
        reader._flash_active = True
        (tmp_path / ".bv_source_esp32.closed").write_text("x")

        reader._check_flash_handshake()

        assert reader._flash_active is False
        assert not (tmp_path / ".bv_source_esp32.closed").exists()

    def test_stale_lock_past_60s_forces_a_clear_and_returns_early(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        reader = make_reader(now_ns=100_000_000_000, flash_handshake=True, flash_handshake_id="esp32")
        reader._flash_active = True
        reader._flash_lock_ts = 0  # 100s ago per shared.time_ns(), past the 60s timeout
        (tmp_path / ".bv_source_esp32.lock").write_text("x")

        reader._check_flash_handshake()

        assert not (tmp_path / ".bv_source_esp32.lock").exists()
        assert reader._flash_active is False


class TestCommands:
    def test_get_commands_lists_reset(self):
        reader = make_reader()
        assert reader.get_commands() == [("reset", "Reset MCU")]

    def test_send_command_reset_calls_reset_device(self, monkeypatch):
        reader = make_reader()
        called = []
        monkeypatch.setattr(reader, "reset_device", lambda: called.append(True))

        reader.send_command("reset")

        assert called == [True]

    def test_send_command_unknown_falls_back_to_raw_send(self, monkeypatch):
        reader = make_reader()
        sent = []
        monkeypatch.setattr(reader, "send_data", lambda cmd: sent.append(cmd))

        reader.send_command("custom raw command")

        assert sent == ["custom raw command"]

    def test_send_command_strips_whitespace(self, monkeypatch):
        reader = make_reader()
        sent = []
        monkeypatch.setattr(reader, "send_data", lambda cmd: sent.append(cmd))

        reader.send_command("  hello  ")

        assert sent == ["hello"]


class TestGetConfigSchema:
    def test_injects_live_ports_and_the_socket_url_fallback(self, monkeypatch):
        class FakePort:
            def __init__(self, device, description):
                self.device = device
                self.description = description

        monkeypatch.setattr(
            "serial.tools.list_ports.comports",
            lambda: [FakePort("COM3", "Widget Board")],
        )

        schema = UARTReader.get_config_schema()

        url_prop = schema["properties"]["url"]
        assert "COM3" in url_prop["enum"]
        assert "socket://localhost:1234" in url_prop["enum"]
        assert "Widget Board" in url_prop["enum_tooltips"]
        assert url_prop["_allow_custom"] is True


class LoopbackTCPServer:
    """A tiny real TCP server used to drive UARTReader through PySerial's actual socket://
    transport ("serial in TCP client mode") - no hardware and no PySerial mocking required,
    unlike a real COM-port loopback which isn't available in CI."""

    def __init__(self):
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(1)
        self.port = self._sock.getsockname()[1]
        self.conn = None
        self._accepted = threading.Event()
        self._thread = threading.Thread(target=self._accept_loop, daemon=True)
        self._thread.start()

    def _accept_loop(self):
        try:
            self.conn, _ = self._sock.accept()
            self._accepted.set()
        except OSError:
            pass

    def wait_for_client(self, timeout=5.0):
        assert self._accepted.wait(timeout), "client never connected"
        return self.conn

    def close(self):
        if self.conn is not None:
            try:
                self.conn.close()
            except OSError:
                pass
        self._sock.close()
        self._thread.join(timeout=2.0)


class TestOpenRealSocketTransport:
    """Exercises UARTReader.open() against PySerial's real socket:// URL handler - the same
    "serial over TCP" mode used for network-attached UART bridges - rather than a fake serial
    object, so the actual serial_for_url()/set_buffer_size() integration gets covered."""

    def test_open_connects_and_round_trips_data(self):
        server = LoopbackTCPServer()
        try:
            reader = make_reader(url=f"socket://127.0.0.1:{server.port}")

            ser = reader.open()
            try:
                assert ser is not None
                assert ser.is_open
                assert reader.serial is ser

                conn = server.wait_for_client()
                conn.sendall(b"hello uart loopback")

                data = ser.read(len(b"hello uart loopback"))
                assert data == b"hello uart loopback"

                ser.write(b"reply")
                assert conn.recv(5) == b"reply"
            finally:
                ser.close()
        finally:
            server.close()

    def test_open_returns_none_when_nothing_is_listening(self):
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()  # freed immediately -> connection refused

        reader = make_reader(url=f"socket://127.0.0.1:{port}", delay=50)

        assert reader.open() is None
        assert reader.serial is None


class RecordingSubscriber:
    """Captures the raw message bytes of every row in every batch handed to put(), copying
    them out (via PooledLogBatch.__iter__'s .tobytes()) before the batch is released.

    A single burst of bytes can land as more than one row/batch - UARTReader.run() only
    extends the current row within one loop iteration's burst read; if the OS hasn't yet
    delivered the rest of the payload by the time in_waiting is checked, the remainder shows
    up as a new row on a later iteration (and possibly a separate distribute() call). joined()
    reassembles the full stream regardless of how it was chunked."""

    def __init__(self):
        self.messages = []
        self._lock = threading.Lock()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                with self._lock:
                    self.messages.append(msg)

    def joined(self) -> bytes:
        with self._lock:
            return b"".join(self.messages)


class TestRunRealSocketIngestion:
    """End-to-end: runs UARTReader.run() for real against a live socket:// connection and
    confirms bytes sent by a real TCP peer come out the other end as a distributed batch -
    the one code path none of the fake-serial unit tests above can reach."""

    def test_run_ingests_bytes_from_a_real_tcp_peer(self):
        server = LoopbackTCPServer()
        try:
            reader = make_reader(url=f"socket://127.0.0.1:{server.port}", delay=20)
            reader.enabled = True
            reader.shared = SimpleNamespace(
                array_pool=NumpyArrayPool(),
                time_ns=time.time_ns,
                tasks=FakeTasks(),
            )

            subscriber = RecordingSubscriber()
            reader.subscribe(subscriber)

            expected = b"hello from a real socket"

            reader.start()
            try:
                conn = server.wait_for_client()

                # Wait until UARTReader.open() has fully completed - PySerial's own open()
                # internally calls reset_input_buffer() right after connecting, which would
                # silently discard bytes sent too early. server.accept() unblocking only means
                # the TCP handshake finished, not that open() (running on the reader thread)
                # has gotten past that flush yet - a race that reliably loses under a slower
                # reader thread (e.g. under coverage.py's line-tracing overhead).
                deadline = time.time() + 5.0
                while time.time() < deadline and not reader.is_connected():
                    time.sleep(0.01)
                assert reader.is_connected(), "reader never finished opening the connection"

                conn.sendall(expected)

                deadline = time.time() + 5.0
                while time.time() < deadline and subscriber.joined() != expected:
                    time.sleep(0.01)
            finally:
                reader.stop()

            assert subscriber.joined() == expected
        finally:
            server.close()

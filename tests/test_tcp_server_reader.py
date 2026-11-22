# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import socket
import struct
import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.io.tcp_server import TCPReader


def make_reader(**config_overrides):
    reader = TCPReader()
    reader.logger = PrintLogger("test.tcp_server")
    reader.apply_config(config_overrides)
    return reader


class TestDefaults:
    def test_default_config_values(self):
        reader = make_reader()
        assert reader.host == "0.0.0.0"
        assert reader.port == 5000
        assert reader.buffer_size == 65535
        assert reader.delay == 100


class TestOpen:
    def test_binds_and_listens_on_the_configured_host(self):
        reader = make_reader(host="127.0.0.1", port=0)  # port=0 -> OS picks a free ephemeral port
        try:
            sock = reader.open()
            assert sock is not None
            assert reader.server_sock is sock
            assert sock.getsockname()[0] == "127.0.0.1"
        finally:
            reader._close_server()

    def test_accepts_a_real_client_connection(self):
        reader = make_reader(host="127.0.0.1", port=0)
        try:
            reader.open()
            bound_port = reader.server_sock.getsockname()[1]

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(("127.0.0.1", bound_port))
            try:
                conn, addr = reader.server_sock.accept()
                try:
                    assert addr[0] == "127.0.0.1"
                finally:
                    conn.close()
            finally:
                client.close()
        finally:
            reader._close_server()

    def test_returns_none_when_the_port_is_already_bound(self):
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        port = blocker.getsockname()[1]

        try:
            reader = make_reader(host="127.0.0.1", port=port)
            result = reader.open()

            assert result is None
            assert reader.server_sock is None
        finally:
            blocker.close()


class TestCloseClient:
    def test_closes_and_clears_the_active_client_socket(self):
        reader = make_reader()
        a, b = socket.socketpair()
        reader.client_sock = a

        reader._close_client()

        assert reader.client_sock is None
        with pytest.raises(OSError):
            a.send(b"x")
        b.close()

    def test_is_a_noop_when_already_none(self):
        reader = make_reader()
        reader.client_sock = None
        reader._close_client()  # must not raise


class TestCloseServer:
    def test_closes_and_clears_the_listening_socket(self):
        reader = make_reader(host="127.0.0.1", port=0)
        reader.open()

        reader._close_server()

        assert reader.server_sock is None

    def test_is_a_noop_when_already_none(self):
        reader = make_reader()
        reader.server_sock = None
        reader._close_server()  # must not raise


class TestSendData:
    def test_sends_encoded_bytes_over_the_client_socket(self):
        reader = make_reader()
        a, b = socket.socketpair()
        reader.client_sock = a
        try:
            reader.send_data("hello")
            assert b.recv(1024) == b"hello"
        finally:
            a.close()
            b.close()

    def test_noop_when_there_is_no_active_client(self):
        reader = make_reader()
        reader.client_sock = None
        reader.send_data("hello")  # must not raise

    def test_send_failure_closes_the_client_socket(self):
        reader = make_reader()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.close()  # already-closed socket: sendall raises immediately
        reader.client_sock = sock

        reader.send_data("hello")

        assert reader.client_sock is None


class QueueParser:
    """Minimal stand-in for a real downstream parser (e.g. BinaryParser) - the object
    BaseDaemon.distribute() calls .put(batch) on. Pushes each row's raw bytes onto a
    queue.Queue so the test can observe distribute() calls made by a live reader thread."""

    def __init__(self):
        self.queue: "queue.Queue[bytes]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                self.queue.put(msg)


def drain_until(q: "queue.Queue[bytes]", expected: bytes, timeout: float) -> bytes:
    """Pulls messages off the queue, reassembling them, until the accumulated bytes match
    `expected` or the timeout elapses. A single send can arrive split across more than one
    distribute() call, so this can't just do a single q.get()."""
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


class TestRunRealSocketIngestion:
    """End-to-end: runs TCPReader.run() for real, accepting a live client connection, and
    confirms bytes sent by that client are correctly batched and handed to distribute() - the
    one code path none of the synchronous tests above (open/_close_*/send_data) reach."""

    def test_run_ingests_bytes_from_a_real_tcp_client(self):
        reader = make_reader(host="127.0.0.1", port=0, delay=20)  # port=0 -> OS-assigned
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        parser = QueueParser()
        reader.subscribe(parser)

        expected = b"hello from a real tcp client"

        reader.start()
        try:
            # run()'s open() binds asynchronously on the reader thread; wait for it to publish
            # the bound ephemeral port before connecting.
            deadline = time.time() + 5.0
            while reader.server_sock is None and time.time() < deadline:
                time.sleep(0.01)
            assert reader.server_sock is not None, "reader never bound its listening socket"
            bound_port = reader.server_sock.getsockname()[1]

            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect(("127.0.0.1", bound_port))
            try:
                client.sendall(expected)
                assert drain_until(parser.queue, expected, timeout=5.0) == expected
            finally:
                client.close()
        finally:
            reader.stop()


def start_running_reader(parser: QueueParser) -> tuple[TCPReader, int]:
    """A live reader whose timed flush cannot fire during the test (delay 60 s), so anything
    the parser sees was handed on by a flush on the disconnect path. Returns it with its port."""
    reader = make_reader(host="127.0.0.1", port=0, delay=60_000)
    reader.enabled = True
    reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)
    reader.subscribe(parser)
    reader.start()
    deadline = time.time() + 5.0
    while reader.server_sock is None and time.time() < deadline:
        time.sleep(0.01)
    assert reader.server_sock is not None, "reader never bound its listening socket"
    return reader, reader.server_sock.getsockname()[1]


class TestRunStopsDuringRetryWait:
    def test_stop_interrupts_the_wait_after_a_failed_bind(self):
        reader = make_reader(host="127.0.0.1", port=0)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)
        reader.open = lambda: None  # the bind keeps failing: run() waits 1 s between tries
        reader.start()
        thread = reader._thread
        time.sleep(0.3)

        t0 = time.perf_counter()
        reader.stop()
        elapsed = time.perf_counter() - t0

        assert not thread.is_alive()
        assert elapsed < 0.5


class TestRunFlushesOnDisconnect:
    """The lines a client sent just before it went away are handed on while the reader is still
    running and no new client connects, not held until the next client or stop()."""

    def test_client_close_hands_on_the_last_lines(self):
        parser = QueueParser()
        expected = b"last line before the client closed\n"

        reader, port = start_running_reader(parser)
        try:
            client = socket.create_connection(("127.0.0.1", port))
            client.sendall(expected)
            client.close()

            assert drain_until(parser.queue, expected, timeout=3.0) == expected
            assert reader._thread.is_alive()
        finally:
            reader.stop()

    def test_receive_error_hands_on_the_last_lines(self):
        parser = QueueParser()
        expected = b"last line before the connection reset\n"

        reader, port = start_running_reader(parser)
        try:
            client = socket.create_connection(("127.0.0.1", port))
            client.sendall(expected)
            # Let the reader take the bytes off the socket before the reset discards them.
            time.sleep(0.3)
            # Linger 0: close() sends RST, so the reader's recv_into raises instead of returning 0.
            client.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            client.close()

            assert drain_until(parser.queue, expected, timeout=3.0) == expected
            assert reader._thread.is_alive()
        finally:
            reader.stop()

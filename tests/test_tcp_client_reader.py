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
from blinkview.io.tcp_client import TCPClientReader


def make_reader(**config_overrides):
    reader = TCPClientReader()
    reader.logger = PrintLogger("test.tcp_client")
    reader.apply_config(config_overrides)
    return reader


@pytest.fixture
def listening_server():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    yield srv
    srv.close()


class TestDefaults:
    def test_default_config_values(self):
        reader = make_reader()
        assert reader.host == "127.0.0.1"
        assert reader.port == 5000
        assert reader.buffer_size == 65535
        assert reader.delay == 100
        assert reader.reconnect_interval == 5


class TestOpen:
    def test_connects_successfully_to_a_listening_server(self, listening_server):
        port = listening_server.getsockname()[1]
        reader = make_reader(host="127.0.0.1", port=port)

        sock = reader.open()
        try:
            assert sock is not None
            assert reader.client_sock is sock

            # Confirm the server side actually observed the connection.
            conn, _ = listening_server.accept()
            conn.close()
        finally:
            sock.close()

    def test_returns_none_and_leaves_client_sock_unset_when_connection_is_refused(self):
        # Bind then immediately release: connecting to a freed loopback port reliably yields
        # ECONNREFUSED almost instantly, rather than an environment-dependent timeout.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        reader = make_reader(host="127.0.0.1", port=port, delay=50)

        result = reader.open()

        assert result is None
        assert reader.client_sock is None


class TestCloseClient:
    def test_closes_and_clears_the_active_socket(self):
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
        assert reader.client_sock is None


class TestSendData:
    def test_sends_encoded_bytes_over_the_socket(self):
        reader = make_reader()
        a, b = socket.socketpair()
        reader.client_sock = a
        try:
            reader.send_data("hello")
            assert b.recv(1024) == b"hello"
        finally:
            a.close()
            b.close()

    def test_noop_when_there_is_no_active_connection(self):
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
    """End-to-end: runs TCPClientReader.run() for real, connecting to a live TCP peer, and
    confirms bytes sent by that peer are correctly batched and handed to distribute() - the
    one code path none of the synchronous tests above (open/_close_client/send_data) reach."""

    def test_run_ingests_bytes_from_a_real_tcp_peer(self, listening_server):
        port = listening_server.getsockname()[1]
        reader = make_reader(host="127.0.0.1", port=port, delay=20)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        parser = QueueParser()
        reader.subscribe(parser)

        expected = b"hello from a real tcp peer"

        reader.start()
        try:
            conn, _ = listening_server.accept()
            try:
                conn.sendall(expected)
                assert drain_until(parser.queue, expected, timeout=5.0) == expected
            finally:
                conn.close()
        finally:
            reader.stop()


def start_running_reader(port: int, parser: QueueParser) -> TCPClientReader:
    """A live reader whose timed flush cannot fire during the test (delay 60 s), so anything
    the parser sees was handed on by a flush on the disconnect path. reconnect_interval=1 keeps
    stop() short while the peer stays down."""
    reader = make_reader(host="127.0.0.1", port=port, delay=60_000, reconnect_interval=1)
    reader.enabled = True
    reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)
    reader.subscribe(parser)
    reader.start()
    return reader


class TestRunStopsDuringReconnectWait:
    def test_stop_interrupts_the_reconnect_wait(self):
        # A freed loopback port: every connect is refused, so run() sits in the reconnect wait.
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
        probe.close()

        reader = make_reader(host="127.0.0.1", port=port, delay=50, reconnect_interval=30)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)
        reader.start()
        thread = reader._thread
        time.sleep(0.5)

        t0 = time.perf_counter()
        reader.stop(timeout=5.0)
        elapsed = time.perf_counter() - t0

        assert not thread.is_alive()
        assert elapsed < 1.5


class TestRunFlushesOnDisconnect:
    """The lines received just before the peer goes away are handed on while the reader is still
    running and the peer stays down, not held until a reconnect or stop()."""

    def test_peer_close_hands_on_the_last_lines(self, listening_server):
        port = listening_server.getsockname()[1]
        parser = QueueParser()
        expected = b"last line before the peer closed\n"

        reader = start_running_reader(port, parser)
        try:
            conn, _ = listening_server.accept()
            # The peer stays down: reconnects are refused from here on.
            listening_server.close()
            conn.sendall(expected)
            conn.close()

            assert drain_until(parser.queue, expected, timeout=3.0) == expected
            assert reader._thread.is_alive()
        finally:
            reader.stop()

    def test_receive_error_hands_on_the_last_lines(self, listening_server):
        port = listening_server.getsockname()[1]
        parser = QueueParser()
        expected = b"last line before the connection reset\n"

        reader = start_running_reader(port, parser)
        try:
            conn, _ = listening_server.accept()
            listening_server.close()
            conn.sendall(expected)
            # Let the reader take the bytes off the socket before the reset discards them.
            time.sleep(0.3)
            # Linger 0: close() sends RST, so the reader's recv_into raises instead of returning 0.
            conn.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            conn.close()

            assert drain_until(parser.queue, expected, timeout=3.0) == expected
            assert reader._thread.is_alive()
        finally:
            reader.stop()

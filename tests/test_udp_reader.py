# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import socket
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.io.udp_reader import UDPReader


def make_reader(**config_overrides):
    reader = UDPReader()
    reader.logger = PrintLogger("test.udp")
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
    def test_binds_on_the_configured_host_and_port(self):
        reader = make_reader(host="127.0.0.1", port=0)  # port=0 -> OS picks a free ephemeral port
        try:
            sock = reader.open()
            assert sock is not None
            assert reader.sock is sock
            assert sock.getsockname()[0] == "127.0.0.1"
        finally:
            if reader.sock:
                reader.sock.close()

    def test_returns_none_when_the_port_is_already_bound(self):
        blocker = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        blocker.bind(("127.0.0.1", 0))
        port = blocker.getsockname()[1]

        try:
            reader = make_reader(host="127.0.0.1", port=port)
            result = reader.open()

            assert result is None
            assert reader.sock is None
        finally:
            blocker.close()


class TestSendData:
    def test_warns_and_is_a_noop_when_no_client_has_sent_data_yet(self):
        reader = make_reader()
        reader.target_address = None
        reader.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            reader.send_data("hello")  # must not raise
        finally:
            reader.sock.close()

    def test_is_a_noop_when_socket_is_not_open(self):
        reader = make_reader()
        reader.sock = None
        reader.target_address = ("127.0.0.1", 12345)
        reader.send_data("hello")  # must not raise

    def test_sends_a_datagram_to_the_bound_target_address(self):
        reader = make_reader()
        reader.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        reader.sock.bind(("127.0.0.1", 0))

        listener = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        listener.bind(("127.0.0.1", 0))
        listener.settimeout(2.0)

        try:
            reader.target_address = listener.getsockname()
            reader.send_data("hello")

            data, addr = listener.recvfrom(1024)
            assert data == b"hello"
        finally:
            reader.sock.close()
            listener.close()

    def test_send_failure_is_caught_and_does_not_raise(self):
        reader = make_reader()
        reader.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        reader.sock.close()  # already-closed socket: sendto raises immediately
        reader.target_address = ("127.0.0.1", 12345)

        reader.send_data("hello")  # must not raise


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
    """End-to-end: runs UDPReader.run() for real, binding a real UDP socket, and confirms a
    datagram sent by a real peer is correctly batched and handed to distribute() - the one
    code path none of the synchronous tests above (open/send_data) reach."""

    def test_run_ingests_a_real_udp_datagram(self):
        reader = make_reader(host="127.0.0.1", port=0, delay=20)  # port=0 -> OS-assigned
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        parser = QueueParser()
        reader.subscribe(parser)

        expected = b"hello from a real udp peer"

        reader.start()
        try:
            # run()'s open() binds asynchronously on the reader thread; wait for it to publish
            # the bound ephemeral port before sending.
            deadline = time.time() + 5.0
            while reader.sock is None and time.time() < deadline:
                time.sleep(0.01)
            assert reader.sock is not None, "reader never bound its UDP socket"
            bound_port = reader.sock.getsockname()[1]

            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                client.sendto(expected, ("127.0.0.1", bound_port))
                assert drain_until(parser.queue, expected, timeout=5.0) == expected
            finally:
                client.close()
        finally:
            reader.stop()

    def test_run_records_target_address_from_the_sender(self):
        """UDPReader.send_data() replies to whichever peer most recently sent a datagram -
        confirms run() actually records that address."""
        reader = make_reader(host="127.0.0.1", port=0, delay=20)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        parser = QueueParser()
        reader.subscribe(parser)

        expected = b"ping"

        reader.start()
        try:
            deadline = time.time() + 5.0
            while reader.sock is None and time.time() < deadline:
                time.sleep(0.01)
            assert reader.sock is not None
            bound_port = reader.sock.getsockname()[1]

            client = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            client.bind(("127.0.0.1", 0))
            try:
                client.settimeout(5.0)
                client.sendto(expected, ("127.0.0.1", bound_port))
                assert drain_until(parser.queue, expected, timeout=5.0) == expected

                assert reader.target_address == client.getsockname()

                reader.send_data("pong")
                data, _ = client.recvfrom(1024)
                assert data == b"pong"
            finally:
                client.close()
        finally:
            reader.stop()

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.io.binary_file_reader import BinaryFileReader


def make_reader(**config_overrides):
    reader = BinaryFileReader()
    reader.logger = PrintLogger("test.binary_file_reader")
    reader.apply_config(config_overrides)
    return reader


class QueueParser:
    """Minimal stand-in for a real downstream parser - the object BaseDaemon.distribute()
    calls .put(batch) on. Reassembles each row's raw bytes onto a queue.Queue."""

    def __init__(self):
        self.queue: "queue.Queue[bytes]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                self.queue.put(bytes(msg))


def drain_until(q: "queue.Queue[bytes]", expected_len: int, timeout: float) -> bytes:
    collected = bytearray()
    deadline = time.time() + timeout
    while len(collected) < expected_len:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            collected += q.get(timeout=remaining)
        except queue.Empty:
            break
    return bytes(collected)


def run_and_collect(reader, timeout=5.0, expected_len=None):
    parser = QueueParser()
    reader.subscribe(parser)
    reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

    reader.start()
    try:
        if expected_len is not None:
            return drain_until(parser.queue, expected_len, timeout)
        time.sleep(timeout)
        return None
    finally:
        reader.stop()


class TestDefaults:
    def test_default_config_values(self):
        reader = make_reader(file_path="x.bin", read_mode="stream", loop=False)
        assert reader.chunk_size == 8
        assert reader.frequency == 100
        assert reader.delay == 30
        assert reader.loop is False


class TestStreamMode:
    def test_reads_file_content_and_stops_when_loop_is_false(self, tmp_path):
        content = b"hello binary world!"
        f = tmp_path / "data.bin"
        f.write_bytes(content)

        reader = make_reader(file_path=str(f), read_mode="stream", chunk_size=4, frequency=1000, delay=5, loop=False)
        reader.enabled = True

        received = run_and_collect(reader, timeout=5.0, expected_len=len(content))

        assert received == content

    def test_missing_file_logs_error_and_returns_without_hanging(self, tmp_path):
        missing = tmp_path / "does_not_exist.bin"
        reader = make_reader(file_path=str(missing), read_mode="stream", loop=False)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        reader.start()
        try:
            # run() returns almost immediately on a missing file - the thread shouldn't
            # still be alive shortly after start().
            deadline = time.time() + 2.0
            while reader.is_running and time.time() < deadline:
                time.sleep(0.02)
            assert not reader.is_running
        finally:
            reader.stop()

    def test_loop_true_restarts_from_the_beginning_at_eof(self, tmp_path):
        content = b"abcd"
        f = tmp_path / "loop.bin"
        f.write_bytes(content)

        reader = make_reader(file_path=str(f), read_mode="stream", chunk_size=4, frequency=1000, delay=5, loop=True)
        reader.enabled = True

        # With loop=True the reader never stops on its own - collect more bytes than the
        # file contains to prove it wrapped around and kept streaming.
        received = run_and_collect(reader, timeout=2.0, expected_len=len(content) * 3)

        assert received.startswith(content)
        assert len(received) >= len(content) * 3


class TestMemoryMode:
    def test_reads_file_content_and_stops_when_loop_is_false(self, tmp_path):
        content = b"in-memory replay data"
        f = tmp_path / "mem.bin"
        f.write_bytes(content)

        reader = make_reader(file_path=str(f), read_mode="memory", chunk_size=4, frequency=1000, delay=5, loop=False)
        reader.enabled = True

        received = run_and_collect(reader, timeout=5.0, expected_len=len(content))

        assert received == content

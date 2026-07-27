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
from blinkview.io.file_tail_reader import FileTailReader


def make_reader(**config_overrides):
    reader = FileTailReader()
    reader.logger = PrintLogger("test.file_tail_reader")
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


def run_reader(reader):
    parser = QueueParser()
    reader.subscribe(parser)
    reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)
    reader.enabled = True
    reader.start()
    return parser


class TestDefaults:
    def test_default_config_values(self):
        reader = make_reader(file_path="x.log")
        assert reader.from_start is False
        assert reader.poll_interval == 200
        assert reader.chunk_size == 65536
        assert reader.delay == 100


class TestFromStart:
    def test_reads_existing_content_when_from_start_true(self, tmp_path):
        f = tmp_path / "app.log"
        f.write_bytes(b"line one\nline two\n")

        reader = make_reader(file_path=str(f), from_start=True, poll_interval=20, delay=20)
        parser = run_reader(reader)
        try:
            received = drain_until(parser.queue, len(b"line one\nline two\n"), timeout=5.0)
            assert received == b"line one\nline two\n"
        finally:
            reader.stop()

    def test_ignores_existing_content_when_from_start_false(self, tmp_path):
        f = tmp_path / "app.log"
        f.write_bytes(b"stale line\n")

        reader = make_reader(file_path=str(f), from_start=False, poll_interval=20, delay=20)
        parser = run_reader(reader)
        try:
            # Give the reader a moment to open the file and seek to the end.
            time.sleep(0.3)

            with f.open("ab") as fh:
                fh.write(b"fresh line\n")

            received = drain_until(parser.queue, len(b"fresh line\n"), timeout=5.0)
            assert received == b"fresh line\n"
        finally:
            reader.stop()


class TestTailing:
    def test_streams_appended_data_as_it_arrives(self, tmp_path):
        f = tmp_path / "app.log"
        f.write_bytes(b"")

        reader = make_reader(file_path=str(f), from_start=True, poll_interval=20, delay=20)
        parser = run_reader(reader)
        try:
            with f.open("ab") as fh:
                fh.write(b"first\n")

            received = drain_until(parser.queue, len(b"first\n"), timeout=5.0)
            assert received == b"first\n"

            with f.open("ab") as fh:
                fh.write(b"second\n")

            received = drain_until(parser.queue, len(b"second\n"), timeout=5.0)
            assert received == b"second\n"
        finally:
            reader.stop()

    def test_waits_for_file_to_appear(self, tmp_path):
        f = tmp_path / "not_yet.log"

        reader = make_reader(file_path=str(f), from_start=True, poll_interval=20, delay=20)
        parser = run_reader(reader)
        try:
            time.sleep(0.2)
            f.write_bytes(b"hello\n")

            received = drain_until(parser.queue, len(b"hello\n"), timeout=5.0)
            assert received == b"hello\n"
        finally:
            reader.stop()


class TestRotation:
    def test_reopens_from_start_when_file_is_truncated_and_rewritten(self, tmp_path):
        f = tmp_path / "app.log"
        f.write_bytes(b"before rotation\n")

        reader = make_reader(file_path=str(f), from_start=True, poll_interval=20, delay=20)
        parser = run_reader(reader)
        try:
            received = drain_until(parser.queue, len(b"before rotation\n"), timeout=5.0)
            assert received == b"before rotation\n"

            # Simulate log rotation: truncate and rewrite with new (shorter) content.
            with f.open("wb") as fh:
                fh.write(b"after rotation\n")

            received = drain_until(parser.queue, len(b"after rotation\n"), timeout=5.0)
            assert received == b"after rotation\n"
        finally:
            reader.stop()

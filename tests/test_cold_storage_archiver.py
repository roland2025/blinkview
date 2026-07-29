# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import threading
import time

import pytest

from blinkview.core import cold_storage_archiver as archiver_module
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_storage_archiver import ColdStorageArchiver
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.log_level import LogLevel


def make_segment(pool, msg=b"hello"):
    batch = pool.create(
        PooledLogBatch,
        req_capacity=4,
        buffer_bytes=64,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=True,
        has_tids=True,
    )
    assert batch.insert(100, 100, msg, LogLevel.INFO.value, 0, 0, 1)
    return batch


def drain(collected, expected_count, timeout=5.0):
    deadline = time.time() + timeout
    while len(collected) < expected_count and time.time() < deadline:
        time.sleep(0.01)
    return collected


@pytest.fixture
def global_pool():
    return NumpyArrayPool()


class TestArchiveWritesToDisk:
    def test_archived_segment_is_written_and_callback_fires(self, global_pool, tmp_path):
        archived = []
        archiver = ColdStorageArchiver(tmp_path, on_archived=archived.append)
        try:
            segment = make_segment(global_pool, b"payload")
            assert archiver.archive(segment) is True

            drain(archived, 1)
            assert len(archived) == 1

            cold_segment = archived[0]
            assert cold_segment.size == 1
            assert bytes(cold_segment.bundle.buffer[:7]) == b"payload"
            assert list(tmp_path.glob("*.blkseg")) != []

            cold_segment.release()
        finally:
            archiver.stop()
            archiver.cleanup()

    def test_multiple_segments_get_distinct_files(self, global_pool, tmp_path):
        archived = []
        archiver = ColdStorageArchiver(tmp_path, on_archived=archived.append)
        try:
            for i in range(3):
                archiver.archive(make_segment(global_pool, f"msg{i}".encode()))

            drain(archived, 3)
            assert len(archived) == 3
            assert len(set(seg.metadata.path for seg in archived)) == 3

            for seg in archived:
                seg.release()
        finally:
            archiver.stop()
            archiver.cleanup()


class TestBackpressure:
    def test_full_queue_drops_segment_instead_of_blocking(self, global_pool, tmp_path, monkeypatch):
        """Deterministically forces the queue-full path: the archiver thread is blocked mid-write
        on the first segment (holding the worker), a second segment fills the depth-1 queue, and
        a third must be dropped (archive() returns False, segment released immediately) rather
        than blocking the caller - ingestion must never wait on disk."""
        release_writer = threading.Event()
        real_write = archiver_module.write_cold_segment_file

        def blocking_write(path, bundle):
            release_writer.wait(timeout=5.0)
            return real_write(path, bundle)

        monkeypatch.setattr(archiver_module, "write_cold_segment_file", blocking_write)

        archived = []
        archiver = ColdStorageArchiver(tmp_path, on_archived=archived.append, queue_depth=1)
        try:
            seg1 = make_segment(global_pool, b"one")
            seg2 = make_segment(global_pool, b"two")
            seg3 = make_segment(global_pool, b"three")

            assert archiver.archive(seg1) is True  # picked up immediately, blocks in write
            time.sleep(0.1)  # let the thread actually dequeue seg1 before we fill the queue
            assert archiver.archive(seg2) is True  # fits in the depth-1 queue

            assert archiver.archive(seg3) is False  # queue full -> dropped, not blocked
            assert seg3.bundle is None  # released synchronously by archive()

            release_writer.set()
            drain(archived, 2)
            assert len(archived) == 2

            for seg in archived:
                seg.release()
        finally:
            release_writer.set()
            archiver.stop()
            archiver.cleanup()


class TestPersistAndResume:
    def test_persist_true_skips_cleanup_rmtree(self, global_pool, tmp_path):
        archiver = ColdStorageArchiver(tmp_path, on_archived=lambda seg: seg.release(), persist=True)
        segment = make_segment(global_pool, b"payload")
        archiver.archive(segment)
        drain([], 0, timeout=0.3)  # let the background thread finish writing
        archiver.stop()

        archiver.cleanup()

        assert list(tmp_path.glob("*.blkseg")) != [], "persist=True must not delete the directory's files"

    def test_persist_false_still_wipes_directory_as_before(self, global_pool, tmp_path):
        archiver = ColdStorageArchiver(tmp_path, on_archived=lambda seg: seg.release())
        archiver.archive(make_segment(global_pool, b"payload"))
        time.sleep(0.3)
        archiver.stop()

        archiver.cleanup()

        assert not tmp_path.exists() or list(tmp_path.glob("*.blkseg")) == []

    def test_counter_continues_past_existing_segment_files(self, tmp_path):
        (tmp_path / "segment_0000000000.blkseg").write_bytes(b"")
        (tmp_path / "segment_0000000005.blkseg").write_bytes(b"")
        (tmp_path / "segment_0000000002.blkseg").write_bytes(b"")

        archiver = ColdStorageArchiver(tmp_path, on_archived=lambda seg: seg.release())
        try:
            assert archiver._counter == 6
            assert archiver._next_path().name == "segment_0000000006.blkseg"
        finally:
            archiver.stop()
            archiver.cleanup()

    def test_counter_starts_at_zero_for_an_empty_directory(self, tmp_path):
        archiver = ColdStorageArchiver(tmp_path, on_archived=lambda seg: seg.release())
        try:
            assert archiver._counter == 0
        finally:
            archiver.stop()
            archiver.cleanup()

    def test_resuming_archiver_does_not_overwrite_existing_files(self, global_pool, tmp_path):
        """End-to-end: archive one segment, stop (persist=True so the file survives), then start
        a *new* archiver pointed at the same directory and archive another - the original file's
        content must be untouched."""
        first = ColdStorageArchiver(tmp_path, on_archived=lambda seg: seg.release(), persist=True)
        first.archive(make_segment(global_pool, b"first-payload"))
        time.sleep(0.3)
        first.stop()
        first.cleanup()  # no-op, persist=True

        existing = list(tmp_path.glob("*.blkseg"))
        assert len(existing) == 1
        original_bytes = existing[0].read_bytes()

        second_archived = []
        second = ColdStorageArchiver(tmp_path, on_archived=second_archived.append, persist=True)
        try:
            second.archive(make_segment(global_pool, b"second-payload"))
            drain(second_archived, 1)

            assert len(list(tmp_path.glob("*.blkseg"))) == 2
            assert existing[0].read_bytes() == original_bytes, "resumed archiver overwrote the original file"

            second_archived[0].release()
        finally:
            second.stop()
            second.cleanup()

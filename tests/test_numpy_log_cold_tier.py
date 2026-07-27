# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.log_fetch import LogSegmentScanner
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool
from blinkview.utils.log_level import LogLevel
from tests.fakes.devices import esp32_wifi


def make_source_batch(pool, msg, ts, seq_hint=0, module=0, device=0):
    """One-row transient batch, mirroring how a real source hands data to batch_append."""
    batch = pool.create(
        PooledLogBatch,
        req_capacity=1,
        buffer_bytes=max(len(msg), 1),
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
    )
    assert batch.insert(ts, ts, msg, LogLevel.INFO.value, module, device, seq_hint)
    return batch


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def snapshot_seqs(pool):
    with pool.get_snapshot() as segments:
        return [int(s) for seg in segments for s in seg.bundle.sequences[: seg.size]]


def push_rows(pool, global_pool, count, ts_start, module=0, device=0):
    """Pushes `count` one-row batches, pacing them slightly so the archiver's single background
    writer thread can keep up. Without pacing, this loop (pure in-memory batch_append calls)
    produces evictions far faster than real disk I/O (open/write/close/reopen+mmap per segment)
    can absorb through the archiver's bounded queue - which would trigger the *documented*
    queue-full drop path (already covered deterministically in test_cold_storage_archiver.py)
    instead of exercising the capping/eviction logic these tests are actually about."""
    for i in range(count):
        batch = make_source_batch(global_pool, f"row{i}".encode(), ts=ts_start + i, module=module, device=device)
        pool.batch_append(batch)
        batch.release()
        time.sleep(0.01)


def _permissive_sidebar():
    return False, np.zeros(0, dtype=np.uint8)


@pytest.fixture
def global_pool():
    # min_bytes=8 gives an exact 1-row-capacity segment for segment_capacity=1 (int64 timestamps
    # -> 8 bytes/row) - see TestToxicMessageHandling in test_numpy_log.py for the same trick.
    # The default NumpyArrayPool(min_bytes=1024) would round every column up to a 1024-byte slab
    # (128 int64 rows), silently defeating segment_capacity=1 and never rotating at all.
    return NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024)


@pytest.fixture
def cold_log_pool(global_pool, tmp_path):
    """max_pieces=2, cold_max_pieces=2, segment_capacity=1: every batch_append rotates a new
    one-row segment, so pushing 5+ single-row batches forces real hot-tier eviction into the
    archiver and real cold-tier eviction+file-deletion once the cold tier itself fills."""
    pool = CircularLogPool(
        global_pool, max_pieces=2, cold_max_pieces=2, cold_storage_dir=str(tmp_path), final_buffer_bytes=1024
    )
    pool.segment_capacity = 1
    pool._optimized = True  # freeze capacity=1; skip the auto-tune heuristic (see test_numpy_log.py)
    pool.clear()  # rebuild the active segment using the new capacity
    yield pool
    pool.release_all()


class TestHotToColdEviction:
    def test_evicted_hot_segments_land_in_cold_tier(self, cold_log_pool, global_pool):
        push_rows(cold_log_pool, global_pool, 4, ts_start=100)

        # 4 rows in, 1-row segments, max_pieces=2 hot + cold_max_pieces=2 cold => nothing lost
        # yet (4 total slots across both tiers) - waits for the archiver to settle rather than
        # asserting on a transient mid-archiving count, since production (this loop) runs far
        # faster than the background disk writer.
        assert wait_for(lambda: snapshot_seqs(cold_log_pool) == [1, 2, 3, 4])
        assert len(cold_log_pool.cold_segments) >= 1

    def test_cold_tier_caps_at_cold_max_pieces_and_deletes_evicted_files(self, cold_log_pool, global_pool):
        push_rows(cold_log_pool, global_pool, 8, ts_start=100)

        # 8 rows in, only max_pieces(2) + cold_max_pieces(2) = 4 one-row segments retained -> the
        # oldest 4 rows (seq 1-4) are genuinely gone, exactly like today's RAM-only drop-on-evict.
        assert wait_for(lambda: snapshot_seqs(cold_log_pool) == [5, 6, 7, 8])
        assert len(cold_log_pool.cold_segments) == cold_log_pool.cold_max_pieces

        cold_dir_files = list(cold_log_pool._archiver._dir.glob("*.blkseg"))
        # Exactly the still-referenced cold segments' files should remain - evicted ones deleted.
        assert len(cold_dir_files) == len(cold_log_pool.cold_segments)

    def test_get_time_bounds_spans_cold_and_hot(self, cold_log_pool, global_pool):
        push_rows(cold_log_pool, global_pool, 6, ts_start=1000)

        # 6 rows in, 4 total slots -> seq 1,2 dropped entirely; seq 3,4 cold; seq 5,6 hot.
        assert wait_for(lambda: snapshot_seqs(cold_log_pool) == [3, 4, 5, 6])

        earliest, latest = cold_log_pool.get_time_bounds()
        # Hot tier alone (max_pieces=2, 1 row/segment) only spans the newest 2 rows (seq 5,6 ->
        # ts 1004,1005) - a correct bound needs the cold tier's oldest retained row (seq 3 -> ts
        # 1002), not just the hot tier's.
        assert (earliest, latest) == (1002, 1005)


class TestRealKernelFetchThroughColdTier:
    def test_scan_history_window_retrieves_rows_that_only_exist_in_cold_storage(
        self, cold_log_pool, global_pool, id_registry, log_filter
    ):
        device, module = esp32_wifi(id_registry)

        push_rows(cold_log_pool, global_pool, 6, ts_start=2000, module=module.id, device=device.id)

        # 6 rows in, 4 total slots (max_pieces=2 hot + cold_max_pieces=2 cold) -> seq 1,2 dropped
        # entirely; seq 3,4 only exist in cold storage now; seq 5,6 still hot.
        assert wait_for(lambda: snapshot_seqs(cold_log_pool) == [3, 4, 5, 6])

        scanner = LogSegmentScanner(
            id_registry,
            lambda: cold_log_pool,
            log_filter,
            get_sidebar_filter=_permissive_sidebar,
            get_show_hidden=lambda: True,
        )

        collected = []

        def consume(segment, indices_array, match_count):
            collected.extend(int(s) for s in segment.bundle.sequences[indices_array[:match_count]])

        # scan_tail(start_seq=SEQ_NONE, ...) is documented as a "from scratch" full rescan,
        # bounded only by max_rows - scans newest-to-oldest across both tiers via the real
        # segment_filter_reversed kernel running against a live np.memmap-backed LogBundle for
        # the cold rows, not a mock.
        result = scanner.scan_tail(start_seq=0, max_rows=100, consume=consume)

        # seq 3,4 come back through the real segment_filter_reversed kernel running against a
        # live np.memmap-backed LogBundle (they exist nowhere else); seq 5,6 come from the
        # ordinary RAM path - proving both tiers are transparently scannable through one API.
        assert sorted(collected) == [3, 4, 5, 6]
        assert result.reached_live_edge is True
        assert result.total_new_rows == 4

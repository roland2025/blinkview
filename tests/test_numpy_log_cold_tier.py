# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time
from pathlib import Path

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


def _force_one_row_segment_capacity(pool):
    """Like `pool.segment_capacity = 1; pool._optimized = True; pool.clear()`, but without
    clear()'s side effect of also wiping cold_segments/sequence back to empty/zero - for use on
    a pool that already has real remounted state (from _mount_existing_cold_segments) worth
    keeping. Swaps out just the initial, default-capacity active segment for a fresh one built
    with the new capacity."""
    pool.segment_capacity = 1
    pool._optimized = True
    with pool._lock:
        old_active = pool.segments.popleft() if pool.segments else None
        pool.active_segment = None
    if old_active is not None:
        old_active.release()
    pool._rotate_segment()


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

    def test_locked_cold_segment_file_is_retried_and_eventually_deleted(self, cold_log_pool, global_pool):
        """Regression test: a concurrent reader (e.g. a GUI fetch's SegmentSnapshot) holding its
        own retain() on a cold segment at the exact moment it's evicted must not permanently
        leak that segment's file - PooledLogBatch.release() only actually closes the underlying
        mmap once the ref-count hits zero, and on Windows an open mapping blocks deletion."""
        push_rows(cold_log_pool, global_pool, 4, ts_start=100)
        assert wait_for(lambda: len(cold_log_pool.cold_segments) >= 1)

        # Simulate a concurrent GUI fetch holding a retain() on the oldest cold segment right as
        # it's about to fall out of the cold tier.
        held = cold_log_pool.cold_segments[0].retain()
        held_path = held.metadata.path

        # Push enough more rows to evict this exact segment out of the cold tier entirely.
        push_rows(cold_log_pool, global_pool, 6, ts_start=200)
        assert wait_for(lambda: held_path not in [s.metadata.path for s in cold_log_pool.cold_segments])

        # The file must still exist - eviction's own release() didn't drop the ref-count to zero
        # (held's retain() is still outstanding), so the unlink attempt should have failed and
        # been deferred rather than silently leaked.
        assert Path(held_path).exists()
        assert held_path in cold_log_pool._pending_cold_deletions

        # Once the concurrent "reader" finishes and releases its own reference, the file becomes
        # deletable - the *next* eviction (for a different segment) should retry and succeed.
        held.release()
        push_rows(cold_log_pool, global_pool, 2, ts_start=300)
        assert wait_for(lambda: not Path(held_path).exists())
        assert held_path not in cold_log_pool._pending_cold_deletions

    def test_get_time_bounds_spans_cold_and_hot(self, cold_log_pool, global_pool):
        push_rows(cold_log_pool, global_pool, 6, ts_start=1000)

        # 6 rows in, 4 total slots -> seq 1,2 dropped entirely; seq 3,4 cold; seq 5,6 hot.
        assert wait_for(lambda: snapshot_seqs(cold_log_pool) == [3, 4, 5, 6])

        earliest, latest = cold_log_pool.get_time_bounds()
        # Hot tier alone (max_pieces=2, 1 row/segment) only spans the newest 2 rows (seq 5,6 ->
        # ts 1004,1005) - a correct bound needs the cold tier's oldest retained row (seq 3 -> ts
        # 1002), not just the hot tier's.
        assert (earliest, latest) == (1002, 1005)

    def test_get_counts_max_total_accounts_for_the_cold_tier(self, cold_log_pool, global_pool):
        """Regression test: get_counts()'s current_total already summed rows across both hot and
        cold segments, but max_total used to only multiply the hot tier's max_pieces by a
        segment's capacity, silently ignoring cold_max_pieces entirely - so once cold storage
        held real data, the toolbar's reported usage (current_total / max_total) would be
        inflated, potentially past 100%, since the numerator grew with cold rows the denominator
        never counted."""
        push_rows(cold_log_pool, global_pool, 8, ts_start=100)

        # 8 rows in, only max_pieces(2) + cold_max_pieces(2) = 4 one-row segments retained.
        assert wait_for(lambda: snapshot_seqs(cold_log_pool) == [5, 6, 7, 8])

        current_total, max_total, _ = cold_log_pool.get_counts()

        assert current_total == 4  # 4 retained rows (seq 5-8), 1 row/segment
        # max_total must account for BOTH tiers' piece ceilings, not just the hot tier's -
        # otherwise this pool (genuinely full at 4/4 slots) would misreport as 4/2 = 200% used.
        assert max_total == (cold_log_pool.max_pieces + cold_log_pool.cold_max_pieces) * 1
        assert current_total <= max_total


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


class TestPersistColdStorageOnClose:
    """cold_storage_persist_on_close: keep cold segment files on disk instead of wiping them at
    release_all(), and flush the hot tier to disk too so the whole session is archived - not
    just what had already been evicted. A later CircularLogPool pointed at the same directory
    should remount everything directly (CircularLogPool._mount_existing_cold_segments) instead
    of starting empty."""

    def test_release_all_flushes_hot_tier_and_keeps_files_on_disk(self, global_pool, tmp_path):
        pool = CircularLogPool(
            global_pool,
            max_pieces=2,
            cold_max_pieces=4,
            cold_storage_dir=str(tmp_path),
            final_buffer_bytes=1024,
            persist_cold_storage=True,
        )
        pool.segment_capacity = 1
        pool._optimized = True
        pool.clear()

        # 4 rows, 2 hot slots + 2 cold slots -> seq 1,2 archived to cold already, seq 3,4 still
        # in the hot tier (never evicted, so never archived by the ordinary eviction path).
        push_rows(pool, global_pool, 4, ts_start=100)
        assert wait_for(lambda: len(pool.cold_segments) >= 2)

        pool.release_all()

        # Nothing deleted - persist=True.
        blkseg_files = sorted(tmp_path.glob("segment_*.blkseg"))
        assert len(blkseg_files) == 4  # all 4 rows, one per one-row segment, now all on disk

        # Confirm the hot rows (seq 3, 4) actually made it to disk, not just the already-cold ones.
        from blinkview.core.cold_segment import read_cold_segment_header

        last_seqs = sorted(read_cold_segment_header(p).last_seq for p in blkseg_files)
        assert last_seqs == [1, 2, 3, 4]

    def test_reopening_the_same_directory_remounts_and_continues_sequence(self, global_pool, tmp_path):
        pool = CircularLogPool(
            global_pool,
            max_pieces=2,
            cold_max_pieces=4,
            cold_storage_dir=str(tmp_path),
            final_buffer_bytes=1024,
            persist_cold_storage=True,
        )
        pool.segment_capacity = 1
        pool._optimized = True
        pool.clear()
        push_rows(pool, global_pool, 4, ts_start=100)
        assert wait_for(lambda: len(pool.cold_segments) >= 2)
        pool.release_all()

        reopened = CircularLogPool(
            NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024),
            max_pieces=2,
            cold_max_pieces=4,
            cold_storage_dir=str(tmp_path),
            final_buffer_bytes=1024,
            persist_cold_storage=True,
        )
        try:
            assert reopened.resumed_from_existing_cold_storage is True
            assert len(reopened.cold_segments) == 4
            assert sorted(int(s.last_sequence_id) for s in reopened.cold_segments) == [1, 2, 3, 4]
            # New inserts must continue the sequence, not restart at 1 and collide with what
            # was just remounted.
            assert int(reopened.sequence) == 4

            _force_one_row_segment_capacity(reopened)
            push_rows(reopened, NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024), 1, ts_start=500)
            assert wait_for(lambda: int(reopened.sequence) == 5)
        finally:
            reopened.release_all()

    def test_archiving_after_reopen_does_not_overwrite_remounted_files(self, global_pool, tmp_path):
        """Regression test for ColdStorageArchiver's counter - without continuing numbering past
        what's already on disk, a freshly-reopened archiver would start back at
        segment_0000000000.blkseg and silently clobber the very file it just remounted from."""
        pool = CircularLogPool(
            global_pool,
            max_pieces=1,
            cold_max_pieces=4,  # generous cap so nothing gets evicted/deleted mid-test
            cold_storage_dir=str(tmp_path),
            final_buffer_bytes=1024,
            persist_cold_storage=True,
        )
        pool.segment_capacity = 1
        pool._optimized = True
        pool.clear()
        push_rows(pool, global_pool, 2, ts_start=100)
        assert wait_for(lambda: len(pool.cold_segments) >= 1)
        pool.release_all()

        first_run_files = {p.name for p in tmp_path.glob("segment_*.blkseg")}
        assert len(first_run_files) >= 1

        reopened_pool = NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024)
        reopened = CircularLogPool(
            reopened_pool,
            max_pieces=1,
            cold_max_pieces=4,  # generous cap so the remounted segment isn't immediately evicted
            cold_storage_dir=str(tmp_path),
            final_buffer_bytes=1024,
            persist_cold_storage=True,
        )
        _force_one_row_segment_capacity(reopened)
        push_rows(reopened, reopened_pool, 2, ts_start=200)
        assert wait_for(lambda: len(reopened.cold_segments) >= len(first_run_files) + 1)
        # release_all() (persist=True) flushes the still-hot 2nd row (seq 4) too - only the
        # first of the two new rows gets rotated/archived by push_rows' own pacing alone.
        reopened.release_all()

        # The original file(s) must still exist, byte-identical in content (same header), not
        # silently overwritten by the resumed archiver's first new write.
        from blinkview.core.cold_segment import read_cold_segment_header

        for name in first_run_files:
            assert (tmp_path / name).exists()
        all_last_seqs = sorted(read_cold_segment_header(p).last_seq for p in tmp_path.glob("segment_*.blkseg"))
        # Original seq 1,2 preserved untouched, plus new seq 3,4 from the resumed session.
        assert all_last_seqs == [1, 2, 3, 4]

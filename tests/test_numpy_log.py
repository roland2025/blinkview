# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool
from blinkview.utils.log_level import LogLevel


def make_source_batch(pool, messages, ts_start=1, module=0, device=0, level=None):
    """Builds a small transient PooledLogBatch (not a segment) with one row per message,
    mirroring how a real source pipeline hands batches to CircularLogPool.batch_append."""
    level = LogLevel.INFO.value if level is None else level
    batch = pool.create(
        PooledLogBatch,
        req_capacity=max(len(messages), 1),
        buffer_bytes=max(sum(len(m) for m in messages), 1),
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
    )
    for i, msg in enumerate(messages):
        assert batch.insert(ts_start + i, ts_start + i, msg, level, module, device, 0)
    return batch


@pytest.fixture
def global_pool():
    return NumpyArrayPool()


@pytest.fixture
def log_pool(global_pool):
    pool = CircularLogPool(global_pool, max_pieces=16)
    yield pool
    pool.release_all()


class TestInitialState:
    def test_starts_with_one_active_segment_and_no_sequence(self, log_pool):
        assert log_pool.active_segment is not None
        assert len(log_pool.segments) == 1
        assert log_pool.latest_sequence() == SEQ_NONE

    def test_get_counts_starts_empty(self, log_pool):
        current_total, max_total, seq = log_pool.get_counts()
        assert current_total == 0
        assert seq == int(SEQ_NONE)
        assert max_total > 0

    def test_get_time_bounds_empty_pool_is_zero(self, log_pool):
        assert log_pool.get_time_bounds() == (0, 0)


class TestBatchAppend:
    def test_empty_batch_is_noop(self, log_pool, global_pool):
        batch = make_source_batch(global_pool, [])
        log_pool.batch_append(batch)
        assert log_pool.latest_sequence() == SEQ_NONE
        current_total, _, _ = log_pool.get_counts()
        assert current_total == 0
        batch.release()

    def test_appends_rows_and_assigns_sequential_seq_ids(self, log_pool, global_pool):
        batch = make_source_batch(global_pool, [b"hello", b"world", b"third"])
        log_pool.batch_append(batch)

        assert log_pool.latest_sequence() == 3
        current_total, _, seq = log_pool.get_counts()
        assert current_total == 3
        assert seq == 3

        with log_pool.get_snapshot() as segments:
            seg = segments[0]
            assert list(seg.bundle.sequences[:3]) == [1, 2, 3]
        batch.release()

    def test_time_bounds_reflect_appended_rows(self, log_pool, global_pool):
        batch = make_source_batch(global_pool, [b"a", b"b", b"c"], ts_start=100)
        log_pool.batch_append(batch)

        earliest, latest = log_pool.get_time_bounds()
        assert earliest == 100
        assert latest == 102
        batch.release()

    def test_successive_appends_continue_the_sequence(self, log_pool, global_pool):
        batch1 = make_source_batch(global_pool, [b"a", b"b"])
        log_pool.batch_append(batch1)
        batch1.release()

        batch2 = make_source_batch(global_pool, [b"c"])
        log_pool.batch_append(batch2)
        batch2.release()

        assert log_pool.latest_sequence() == 3
        current_total, _, _ = log_pool.get_counts()
        assert current_total == 3


class TestSegmentRotationAndEviction:
    def test_rotates_and_evicts_oldest_segment_beyond_max_pieces(self, global_pool):
        # A tiny min_bytes gives exact, predictable segment row capacity (4 rows/segment).
        tiny_pool = NumpyArrayPool(min_bytes=32, max_bytes=1024 * 1024)
        log_pool = CircularLogPool(tiny_pool, max_pieces=2)
        log_pool.segment_capacity = 4
        log_pool._optimized = True  # keep our explicit capacity; skip the auto-tune heuristic
        log_pool.clear()  # rebuild the active segment using the new capacity

        try:
            messages = [f"m{i}".encode() for i in range(10)]
            batch = make_source_batch(tiny_pool, messages)
            log_pool.batch_append(batch)
            batch.release()

            # 10 rows at capacity 4/segment => segments of [4, 4, 2] rows were created in
            # order; only the newest max_pieces=2 (4 + 2 = 6 rows) should remain retained.
            assert len(log_pool.segments) == 2
            current_total, _, seq = log_pool.get_counts()
            assert current_total == 6
            assert seq == 10
        finally:
            log_pool.release_all()

    def test_update_max_pieces_trims_immediately(self, global_pool):
        tiny_pool = NumpyArrayPool(min_bytes=32, max_bytes=1024 * 1024)
        log_pool = CircularLogPool(tiny_pool, max_pieces=5)
        log_pool.segment_capacity = 4
        log_pool._optimized = True  # keep our explicit capacity; skip the auto-tune heuristic
        log_pool.clear()

        try:
            messages = [f"m{i}".encode() for i in range(20)]
            batch = make_source_batch(tiny_pool, messages)
            log_pool.batch_append(batch)
            batch.release()

            assert len(log_pool.segments) == 5

            log_pool.update_max_pieces(2)
            assert len(log_pool.segments) == 2
            assert log_pool.max_pieces == 2
        finally:
            log_pool.release_all()

    def test_update_max_pieces_rejects_non_positive(self, log_pool):
        with pytest.raises(ValueError):
            log_pool.update_max_pieces(0)
        with pytest.raises(ValueError):
            log_pool.update_max_pieces(-1)

    def test_update_max_pieces_noop_when_unchanged(self, log_pool):
        # Should not rebuild anything (and in particular must not raise/deadlock) when the
        # new value equals the current one.
        log_pool.update_max_pieces(log_pool.max_pieces)
        assert len(log_pool.segments) == 1


class TestToxicMessageHandling:
    def test_oversized_message_forces_rotation_and_truncated_error_insert(self, global_pool):
        # min_bytes=8 + segment_capacity=1 gives an exact 1-row-capacity segment, guaranteeing
        # the second row in a 2-row batch can never fit the active segment and must go through
        # the toxic-message path (rotate + insert_truncated_error).
        tiny_pool = NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024)
        log_pool = CircularLogPool(tiny_pool, max_pieces=16)
        log_pool.segment_capacity = 1
        log_pool.current_buffer_bytes = 10  # toxic_threshold = min(10, 1MB) = 10 bytes
        log_pool.clear()

        try:
            small_msg = b"hi"
            big_msg = b"x" * 15  # > toxic_threshold(10) => treated as toxic
            batch = make_source_batch(tiny_pool, [small_msg, big_msg])
            log_pool.batch_append(batch)
            batch.release()

            assert log_pool.latest_sequence() == 2
            current_total, _, _ = log_pool.get_counts()
            assert current_total == 2

            with log_pool.get_snapshot() as segments:
                assert len(segments) == 2
                first_seg, second_seg = segments
                assert first_seg.size == 1
                assert second_seg.size == 1

                b = second_seg.bundle
                assert int(b.levels[0]) == LogLevel.ERROR.value
                off, length = int(b.offsets[0]), int(b.lengths[0])
                # Well under the 512-byte truncation limit, so the message is preserved as-is.
                assert bytes(b.buffer[off : off + length]) == big_msg
        finally:
            log_pool.release_all()


class TestSnapshots:
    def test_get_snapshot_is_chronological_and_reversed_is_newest_first(self, global_pool):
        tiny_pool = NumpyArrayPool(min_bytes=32, max_bytes=1024 * 1024)
        log_pool = CircularLogPool(tiny_pool, max_pieces=16)
        log_pool.segment_capacity = 4
        log_pool.clear()

        try:
            messages = [f"m{i}".encode() for i in range(9)]
            batch = make_source_batch(tiny_pool, messages)
            log_pool.batch_append(batch)
            batch.release()

            with log_pool.get_snapshot() as segments:
                ids_forward = [seg.metadata for seg in segments]
            with log_pool.get_reversed_snapshot() as segments:
                ids_reversed = [seg.metadata for seg in segments]

            assert ids_reversed == list(reversed(ids_forward))
        finally:
            log_pool.release_all()

    def test_snapshot_retains_segments_and_releases_on_exit(self, log_pool, global_pool):
        batch = make_source_batch(global_pool, [b"a"])
        log_pool.batch_append(batch)
        batch.release()

        seg = log_pool.active_segment
        with log_pool.get_snapshot() as segments:
            held = segments[0]
            assert held is seg
            assert held._ref_count == 2  # pool's own reference + the snapshot's retain()

        # After the snapshot's __exit__, the extra retain() should have been released.
        assert seg._ref_count == 1


class TestClearAndReleaseAll:
    def test_clear_resets_sequence_and_rebuilds_single_segment(self, log_pool, global_pool):
        batch = make_source_batch(global_pool, [b"a", b"b"])
        log_pool.batch_append(batch)
        batch.release()
        assert log_pool.latest_sequence() == 2

        log_pool.clear()

        assert log_pool.latest_sequence() == SEQ_NONE
        assert len(log_pool.segments) == 1
        assert log_pool.active_segment is not None
        assert log_pool.active_segment.metadata == 0
        current_total, _, _ = log_pool.get_counts()
        assert current_total == 0

    def test_release_all_empties_segments_without_recreating(self, log_pool, global_pool):
        batch = make_source_batch(global_pool, [b"a"])
        log_pool.batch_append(batch)
        batch.release()

        log_pool.release_all()

        assert len(log_pool.segments) == 0
        assert log_pool.active_segment is None


class TestConfigUpdates:
    def test_update_final_buffer_bytes_rejects_non_positive(self, log_pool):
        with pytest.raises(ValueError):
            log_pool.update_final_buffer_bytes(0)
        with pytest.raises(ValueError):
            log_pool.update_final_buffer_bytes(-5)

    def test_update_final_buffer_bytes_resets_optimized_flag(self, log_pool):
        log_pool._optimized = True
        log_pool.update_final_buffer_bytes(log_pool.final_buffer_bytes + 1)
        assert log_pool._optimized is False

    def test_update_final_buffer_bytes_noop_when_unchanged(self, log_pool):
        log_pool._optimized = True
        log_pool.update_final_buffer_bytes(log_pool.final_buffer_bytes)
        assert log_pool._optimized is True  # untouched: no-op path taken


class TestAcquireIndicesBuffer:
    def test_default_capacity_matches_segment_capacity(self, log_pool):
        handle = log_pool.acquire_indices_buffer()
        try:
            assert len(handle.array) >= log_pool.segment_capacity
        finally:
            handle.release()

    def test_explicit_capacity_is_honored(self, log_pool):
        handle = log_pool.acquire_indices_buffer(capacity=123)
        try:
            assert len(handle.array) >= 123
        finally:
            handle.release()


class TestFindTsNRowsAway:
    def _pool_with_rows(self, global_pool, count, ts_start=100, max_pieces=16, segment_capacity=4):
        """10 rows across multiple 4-row segments (3 segments: [4,4,2]) so stepping exercises
        real segment-boundary crossing, not just within-segment indexing."""
        tiny_pool = NumpyArrayPool(min_bytes=32, max_bytes=1024 * 1024)
        log_pool = CircularLogPool(tiny_pool, max_pieces=max_pieces)
        log_pool.segment_capacity = segment_capacity
        log_pool._optimized = True
        log_pool.clear()

        messages = [f"m{i}".encode() for i in range(count)]
        batch = make_source_batch(tiny_pool, messages, ts_start=ts_start)
        log_pool.batch_append(batch)
        batch.release()
        return log_pool

    def test_steps_forward_across_segment_boundaries(self, global_pool):
        log_pool = self._pool_with_rows(global_pool, 10)
        try:
            # rows at ts 100..109 (seq 1..10). From ts=100 (row 1), 3 rows forward -> ts=103.
            assert log_pool.find_ts_n_rows_away(100, 3) == 103
            # From ts=100, 9 rows forward -> the last row (ts=109).
            assert log_pool.find_ts_n_rows_away(100, 9) == 109
        finally:
            log_pool.release_all()

    def test_steps_backward_across_segment_boundaries(self, global_pool):
        log_pool = self._pool_with_rows(global_pool, 10)
        try:
            assert log_pool.find_ts_n_rows_away(109, -3) == 106
            assert log_pool.find_ts_n_rows_away(109, -9) == 100
        finally:
            log_pool.release_all()

    def test_clamps_when_overshooting_forward(self, global_pool):
        log_pool = self._pool_with_rows(global_pool, 10)
        try:
            assert log_pool.find_ts_n_rows_away(105, 1000) == 109
        finally:
            log_pool.release_all()

    def test_clamps_when_overshooting_backward(self, global_pool):
        log_pool = self._pool_with_rows(global_pool, 10)
        try:
            assert log_pool.find_ts_n_rows_away(105, -1000) == 100
        finally:
            log_pool.release_all()

    def test_zero_delta_is_a_noop(self, global_pool):
        log_pool = self._pool_with_rows(global_pool, 10)
        try:
            assert log_pool.find_ts_n_rows_away(104, 0) == 104
        finally:
            log_pool.release_all()

    def test_ties_at_current_ts_are_not_recounted(self, global_pool):
        """Two rows can legitimately share a timestamp (same-ns arrivals). Stepping from that
        exact timestamp must land on a genuinely different row on either side, not re-select one
        of the tied rows."""
        tiny_pool = NumpyArrayPool(min_bytes=32, max_bytes=1024 * 1024)
        log_pool = CircularLogPool(tiny_pool, max_pieces=16)
        log_pool.segment_capacity = 4
        log_pool._optimized = True
        log_pool.clear()
        try:
            batch = tiny_pool.create(
                PooledLogBatch,
                req_capacity=5,
                buffer_bytes=32,
                has_levels=True,
                has_modules=True,
                has_devices=True,
                has_sequences=True,
            )
            # ts: 100, 100, 100, 105, 110 - three-way tie at 100.
            for ts, msg in [(100, b"a"), (100, b"b"), (100, b"c"), (105, b"d"), (110, b"e")]:
                assert batch.insert(ts, ts, msg, LogLevel.INFO.value, 0, 0, 0)
            log_pool.batch_append(batch)
            batch.release()

            assert log_pool.find_ts_n_rows_away(100, 1) == 105
            assert log_pool.find_ts_n_rows_away(105, -1) == 100
        finally:
            log_pool.release_all()

    def test_empty_pool_returns_current_ts_unchanged(self, log_pool):
        assert log_pool.find_ts_n_rows_away(12345, 5) == 12345
        assert log_pool.find_ts_n_rows_away(12345, -5) == 12345

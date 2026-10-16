# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_segment import (
    COLUMN_SPECS,
    ColdSegmentMeta,
    open_cold_segment_arrays,
    read_cold_segment_header,
    write_cold_segment_file,
)
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS
from blinkview.ops.segments import segment_filter
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH
from blinkview.utils.log_level import LogLevel


def make_real_segment(pool, rows):
    """Builds a real PooledLogBatch with CircularLogPool's exact segment schema
    (has_levels/modules/devices/sequences/pids/tids=True), one row per (ts, msg, level, module,
    device, seq, pid, tid) tuple in `rows`."""
    batch = pool.create(
        PooledLogBatch,
        req_capacity=max(len(rows), 1),
        buffer_bytes=max(sum(len(r[1]) for r in rows), 1),
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=True,
        has_tids=True,
    )
    for ts, msg, level, module, device, seq, pid, tid in rows:
        assert batch.insert(ts, ts, msg, level, module, device, seq, pid=pid, tid=tid)
    return batch


@pytest.fixture
def global_pool():
    return NumpyArrayPool()


class TestWriteReadRoundTrip:
    def test_header_fields_match_source_bundle(self, global_pool, tmp_path):
        rows = [
            (100, b"hello", LogLevel.INFO.value, 1, 2, 1, 10, 20),
            (150, b"world", LogLevel.WARN.value, 1, 2, 2, 10, 21),
            (200, b"third", LogLevel.ERROR.value, 3, 2, 3, 11, 22),
        ]
        segment = make_real_segment(global_pool, rows)
        path = tmp_path / "segment_0000000000.blkseg"

        header = write_cold_segment_file(path, segment.bundle)

        assert header.row_count == 3
        assert header.first_seq == 1
        assert header.last_seq == 3
        assert header.earliest_ts == 100
        assert header.latest_ts == 200
        assert header.buffer_len == sum(len(r[1]) for r in rows)

        reread = read_cold_segment_header(path)
        assert reread.row_count == header.row_count
        assert reread.table == header.table

        segment.release()

    def test_columns_match_byte_for_byte(self, global_pool, tmp_path):
        rows = [
            (100, b"hello", LogLevel.INFO.value, 1, 2, 1, 10, 20),
            (150, b"world", LogLevel.WARN.value, 1, 2, 2, 10, 21),
            (200, b"third", LogLevel.ERROR.value, 3, 2, 3, 11, 22),
        ]
        segment = make_real_segment(global_pool, rows)
        path = tmp_path / "segment.blkseg"
        write_cold_segment_file(path, segment.bundle)

        header, handles = open_cold_segment_arrays(path)
        try:
            n = header.row_count
            src = segment.bundle
            assert list(handles["timestamps"].array) == list(src.timestamps[:n])
            assert list(handles["rx_timestamps"].array) == list(src.rx_timestamps[:n])
            assert list(handles["offsets"].array) == list(src.offsets[:n])
            assert list(handles["lengths"].array) == list(src.lengths[:n])
            assert list(handles["levels"].array) == list(src.levels[:n])
            assert list(handles["modules"].array) == list(src.modules[:n])
            assert list(handles["devices"].array) == list(src.devices[:n])
            assert list(handles["sequences"].array) == list(src.sequences[:n])
            assert list(handles["pids"].array) == list(src.pids[:n])
            assert list(handles["tids"].array) == list(src.tids[:n])
            assert bytes(handles["buffer"].array) == bytes(src.buffer[: segment.msg_cursor])

            # Writable (copy-on-write): matches the live-pool array type so segment kernels don't
            # recompile a second specialization for cold data (see cold_segment._MmapFileRef), but
            # a write never reaches the on-disk file - the frozen segment file itself is untouched.
            assert handles["timestamps"].array.flags.writeable is True
            assert handles["buffer"].array.flags.writeable is True
            handles["buffer"].array[0] = handles["buffer"].array[0] ^ 0xFF
            with open(path, "rb") as f:
                f.seek(header.table[-1][0])
                on_disk_first_byte = f.read(1)[0]
            assert on_disk_first_byte == bytes(src.buffer[: segment.msg_cursor])[0]
        finally:
            for h in handles.values():
                h.release()
            segment.release()

    def test_zero_row_segment_round_trips(self, global_pool, tmp_path):
        segment = make_real_segment(global_pool, [])
        path = tmp_path / "empty.blkseg"

        header = write_cold_segment_file(path, segment.bundle)
        assert header.row_count == 0
        assert header.buffer_len == 0

        _header, handles = open_cold_segment_arrays(path)
        try:
            for name, _dt in COLUMN_SPECS:
                assert len(handles[name].array) == 0
        finally:
            for h in handles.values():
                h.release()
            segment.release()

    def test_bad_magic_raises(self, tmp_path):
        path = tmp_path / "not_a_segment.blkseg"
        path.write_bytes(b"not a real cold segment file" + b"\x00" * 4096)

        with pytest.raises(ValueError):
            read_cold_segment_header(path)


class TestColdSegmentCheapPropertiesReadFromCacheNotArrays:
    """PooledLogBatch.start_ts/end_ts/first_sequence_id/last_sequence_id are polled frequently by
    segment-scan early-break checks (scan_tail, scan_history_window, build_snapshot_as_of, ...) -
    see plans/fetch-telemetry-window-cold-segment-perf.md. For a cold (memmap-backed) segment
    from_memmap() populates them once, up front, straight from the on-disk header - not on every
    read - to avoid a page-fault on every single poll. Proven here by corrupting the underlying
    mmap'd arrays *after* construction: if the properties were still doing a live array read,
    they'd pick up the corrupted values; instead they must keep returning what was cached at
    construction time."""

    def test_properties_survive_the_underlying_arrays_being_corrupted_afterward(self, global_pool, tmp_path):
        rows = [
            (100, b"hello", LogLevel.INFO.value, 1, 2, 5, 0, 0),
            (200, b"world", LogLevel.WARN.value, 1, 2, 6, 0, 0),
        ]
        segment = make_real_segment(global_pool, rows)
        path = tmp_path / "segment.blkseg"
        write_cold_segment_file(path, segment.bundle)
        segment.release()

        cold_segment = PooledLogBatch.from_memmap(path, metadata=ColdSegmentMeta(str(path), 100, 200, 5, 6))

        assert cold_segment.start_ts == 100
        assert cold_segment.end_ts == 200
        assert cold_segment.first_sequence_id == 5
        assert cold_segment.last_sequence_id == 6

        # Corrupt the real mmap'd arrays after construction - a genuine array read would now
        # return these values instead of what was cached at from_memmap() time.
        cold_segment.bundle.timestamps[0] = 999
        cold_segment.bundle.timestamps[-1] = 888
        cold_segment.bundle.sequences[0] = 77
        cold_segment.bundle.sequences[-1] = 66

        assert cold_segment.start_ts == 100
        assert cold_segment.end_ts == 200
        assert cold_segment.first_sequence_id == 5
        assert cold_segment.last_sequence_id == 6

        cold_segment.release()


class TestFromMemmapMatchesLiveKernels:
    def test_segment_filter_produces_identical_matches_via_mmap(self, global_pool, tmp_path):
        rows = [
            (100, b"hello", LogLevel.INFO.value, 1, 2, 1, 0, 0),
            (150, b"world", LogLevel.WARN.value, 1, 2, 2, 0, 0),
            (200, b"third", LogLevel.ERROR.value, 3, 2, 3, 0, 0),
        ]
        segment = make_real_segment(global_pool, rows)
        path = tmp_path / "segment.blkseg"
        write_cold_segment_file(path, segment.bundle)

        cold_segment = PooledLogBatch.from_memmap(path, metadata=ColdSegmentMeta(str(path), 100, 200, 1, 3))

        mask = np.full(4, LogLevel.ALL.value, dtype=np.uint8)

        live_indices = np.zeros(10, dtype=np.int64)
        live_count = segment_filter(
            segment.bundle,
            effective_mask=mask,
            out_indices=live_indices,
            max_matches=10,
            start_seq=0,
            kv=EMPTY_KV_CONDITIONS,
            text=EMPTY_TEXT_SEARCH,
        )

        cold_indices = np.zeros(10, dtype=np.int64)
        cold_count = segment_filter(
            cold_segment.bundle,
            effective_mask=mask,
            out_indices=cold_indices,
            max_matches=10,
            start_seq=0,
            kv=EMPTY_KV_CONDITIONS,
            text=EMPTY_TEXT_SEARCH,
        )

        assert cold_count == live_count == 3
        assert list(cold_indices[:cold_count]) == list(live_indices[:live_count])
        assert list(cold_segment.bundle.sequences[cold_indices[:cold_count]]) == list(
            segment.bundle.sequences[live_indices[:live_count]]
        )

        cold_segment.release()
        segment.release()


class TestMmapClosesCleanlyAfterRelease:
    """plans/bundle-push-fast-path.md stage 2 gave PooledLogBatch memoryviews onto some of its
    columns. On a cold segment the columns are mmap-backed, and a live memoryview makes
    mmap.close() raise BufferError - which _MmapFileRef.release() catches and swallows, leaving
    the mapping (and the file behind it) alive until GC. Only the always-owned size/msg_cursor
    counters get views eagerly; sequences/timestamps are built lazily by note_appended_rows(),
    which a frozen cold segment never calls."""

    ROWS = [
        (100, b"hello", LogLevel.INFO.value, 1, 2, 1, 10, 20),
        (150, b"world", LogLevel.WARN.value, 1, 2, 2, 10, 21),
        (200, b"third", LogLevel.ERROR.value, 3, 2, 3, 11, 22),
    ]

    def test_release_closes_the_mapping_with_no_swallowed_buffer_error(self, global_pool, tmp_path):
        segment = make_real_segment(global_pool, self.ROWS)
        path = tmp_path / "segment_0000000001.blkseg"
        write_cold_segment_file(path, segment.bundle)
        segment.release()

        cold = PooledLogBatch.from_memmap(path)

        # Touch exactly the properties that go through the counter memoryviews.
        assert cold.size == 3
        assert len(cold) == 3
        assert cold.msg_cursor == sum(len(r[1]) for r in self.ROWS)
        assert cold.last_sequence_id == 3
        assert cold.first_sequence_id == 1
        assert cold.start_ts == 100
        assert cold.end_ts == 200

        # The lazy views stay unbuilt: these two columns are the mmap-backed ones.
        assert cold._seq_mv is None
        assert cold._ts_mv is None

        mapping = cold._ts_h._file_ref._mmap
        assert mapping is not None

        cold.release()

        # A mapping that really closed raises ValueError on any access; one whose close() hit
        # BufferError and got swallowed would still be perfectly readable here.
        with pytest.raises(ValueError):
            mapping.read(1)

        # And with nothing holding the mapping open, the file is deletable right away.
        path.unlink()
        assert not path.exists()

    def test_counter_views_are_released_even_though_the_columns_are_mapped(self, global_pool, tmp_path):
        segment = make_real_segment(global_pool, self.ROWS)
        path = tmp_path / "segment_0000000002.blkseg"
        write_cold_segment_file(path, segment.bundle)
        segment.release()

        cold = PooledLogBatch.from_memmap(path)
        assert cold._size_mv is not None
        assert cold._cursor_mv is not None

        cold.release()

        assert cold._size_mv is None
        assert cold._cursor_mv is None
        assert cold.size == 0
        assert cold.msg_cursor == 0

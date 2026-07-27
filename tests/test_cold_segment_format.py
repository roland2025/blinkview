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

            # Read-only: a cold segment is frozen and must never be silently mutable.
            assert handles["timestamps"].array.flags.writeable is False
            assert handles["buffer"].array.flags.writeable is False
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

        cold_segment = PooledLogBatch.from_memmap(path, metadata=ColdSegmentMeta(str(path), 100, 200))

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

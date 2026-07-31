# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.constants import CHAR_EQUALS, CHAR_SPACE
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS, build_kv_condition_arrays
from blinkview.ops.segments import (
    nb_bundle_extend,
    nb_bundle_push,
    nb_bundle_push_len,
    nb_can_push,
    nb_copy_batch_to_segment,
    nb_filter_segment,
    nb_find_next_module_index,
    nb_find_next_module_match,
    nb_segment_extract_fields,
    nb_segment_filter_reversed,
    segment_filter,
    segment_filter_reversed,
)
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH, build_text_search_arrays
from tests.fakes.log_bundle import make_log_bundle


def make_bundle(timestamps, rx_timestamps, devices, levels, modules, sequences, messages, pids=None, tids=None):
    """Builds a minimal LogBundle backing a fixed set of rows, for kernel-level testing.

    has_pids/has_tids reflect whether pids/tids were actually passed - several kernel tests in
    this file (e.g. test_nb_copy_batch_to_segment_zeros_pids_when_batch_lacks_them) rely on a
    bundle built without pids/tids behaving as if it has no such column at all."""
    return make_log_bundle(
        timestamps,
        devices,
        levels,
        modules,
        sequences,
        messages,
        rx_timestamps=rx_timestamps,
        pids=pids,
        tids=tids,
        has_pids=pids is not None,
        has_tids=tids is not None,
    )


def make_out_bundle(capacity, max_msg_bytes, has_pids=False, has_tids=False):
    """Builds a zeroed LogBundle sized to receive nb_segment_extract_fields output."""
    return LogBundle(
        timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        offsets=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        buffer=np.zeros(capacity * max_msg_bytes, dtype=dtypes.BYTE),
        levels=np.zeros(capacity, dtype=dtypes.LEVEL_TYPE),
        modules=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        devices=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        sequences=np.zeros(capacity, dtype=dtypes.SEQ_TYPE),
        pids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        tids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        ext_u32_1=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u32_2=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u64_1=np.zeros(capacity, dtype=dtypes.UINT64),
        size=np.array([0], dtype=np.int64),
        msg_cursor=np.array([0], dtype=np.int64),
        capacity=capacity,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=has_pids,
        has_tids=has_tids,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def test_nb_segment_extract_fields_basic_roundtrip():
    """All rows pass the mask; fields and messages should come back untouched, in order."""
    bundle = make_bundle(
        timestamps=[100, 200, 300],
        rx_timestamps=[101, 201, 301],
        devices=[0, 0, 1],
        levels=[2, 3, 4],
        modules=[0, 1, 2],
        sequences=[1, 2, 3],
        messages=["hello", "world", "third message"],
    )

    effective_mask = np.zeros(3, dtype=dtypes.LEVEL_TYPE)  # allow everything (>= 0)
    indices = np.zeros(3, dtype=np.int64)

    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=3,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )
    assert match_count == 3

    max_msg_bytes = 32
    out_bundle = make_out_bundle(3, max_msg_bytes)

    written = nb_segment_extract_fields(
        bundle,
        indices,
        match_count,
        out_bundle,
        0,
        max_msg_bytes,
    )

    assert written == 3
    assert list(out_bundle.timestamps) == [100, 200, 300]
    assert list(out_bundle.rx_timestamps) == [101, 201, 301]
    assert list(out_bundle.devices) == [0, 0, 1]
    assert list(out_bundle.levels) == [2, 3, 4]
    assert list(out_bundle.modules) == [0, 1, 2]
    assert list(out_bundle.sequences) == [1, 2, 3]

    decoded = []
    for i in range(3):
        msg_len = int(out_bundle.lengths[i])
        off = int(out_bundle.offsets[i])
        decoded.append(out_bundle.buffer[off : off + msg_len].tobytes().decode("utf-8"))

    assert decoded == ["hello", "world", "third message"]


def test_nb_segment_extract_fields_respects_mask_filtering():
    """Rows whose level is below the per-module effective mask threshold are excluded."""
    bundle = make_bundle(
        timestamps=[1, 2, 3, 4],
        rx_timestamps=[1, 2, 3, 4],
        devices=[0, 0, 0, 0],
        levels=[1, 5, 1, 5],  # module 0 gated at 1, module 1 gated at 5+
        modules=[0, 0, 1, 1],
        sequences=[1, 2, 3, 4],
        messages=["a", "b", "c", "d"],
    )

    # module 0 -> only level >= 5 passes ; module 1 -> only level >= 5 passes as well
    effective_mask = np.array([5, 5], dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(4, dtype=np.int64)

    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=4,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    # Only rows with level==5 (indices 1 and 3) should match.
    assert match_count == 2
    assert sorted(indices[:match_count].tolist()) == [1, 3]


def test_nb_segment_extract_fields_truncates_long_messages():
    long_msg = "x" * 20
    bundle = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[1],
        messages=[long_msg],
    )

    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(1, dtype=np.int64)
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=1,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )
    assert match_count == 1

    max_msg_bytes = 8
    out_bundle = make_out_bundle(1, max_msg_bytes)

    nb_segment_extract_fields(
        bundle,
        indices,
        match_count,
        out_bundle,
        0,
        max_msg_bytes,
    )

    assert int(out_bundle.lengths[0]) == max_msg_bytes
    assert out_bundle.buffer.tobytes().decode("utf-8") == long_msg[:max_msg_bytes]


def test_nb_segment_extract_fields_writes_at_row_offset():
    """out_row_offset lets callers append into a shared, larger output buffer."""
    bundle = make_bundle(
        timestamps=[42],
        rx_timestamps=[42],
        devices=[3],
        levels=[7],
        modules=[9],
        sequences=[5],
        messages=["hi"],
    )

    effective_mask = np.zeros(10, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(1, dtype=np.int64)
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=1,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )
    assert match_count == 1

    capacity = 5
    max_msg_bytes = 16
    out_bundle = make_out_bundle(capacity, max_msg_bytes)

    row_offset = 3
    nb_segment_extract_fields(
        bundle,
        indices,
        match_count,
        out_bundle,
        row_offset,
        max_msg_bytes,
    )

    assert out_bundle.timestamps[row_offset] == 42
    assert out_bundle.devices[row_offset] == 3
    assert out_bundle.levels[row_offset] == 7
    assert out_bundle.modules[row_offset] == 9
    assert out_bundle.sequences[row_offset] == 5

    off = int(out_bundle.offsets[row_offset])
    msg_len = int(out_bundle.lengths[row_offset])
    assert out_bundle.buffer[off : off + msg_len].tobytes().decode("utf-8") == "hi"

    # Untouched rows remain zeroed.
    assert out_bundle.timestamps[0] == 0
    assert out_bundle.timestamps[row_offset + 1] == 0


def test_nb_segment_filter_reversed_end_seq_bounds_history_before_fetch():
    """end_seq caps matches to seq <= end_seq - used for 'history before an anchor' scans."""
    bundle = make_bundle(
        timestamps=[1, 2, 3, 4, 5],
        rx_timestamps=[1, 2, 3, 4, 5],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )

    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # anchor_seq = 4 -> "before" fetch should only see seq <= 3 (rows a, b, c).
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=dtypes.SEQ_NONE,
        end_seq=3,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert list(indices[:match_count]) == [0, 1, 2]  # ascending (chronological) order, seq 1..3


def test_nb_segment_filter_reversed_end_seq_default_is_unbounded():
    bundle = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[0] * 3,
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )

    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(3, dtype=np.int64)

    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=3,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3


def test_filter_segment_forward_ascending_matches_from_start_seq():
    """nb_filter_segment is the forward (ascending) counterpart used for 'history after an anchor'."""
    bundle = make_bundle(
        timestamps=[1, 2, 3, 4, 5],
        rx_timestamps=[1, 2, 3, 4, 5],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )

    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # start_seq=2 -> matches seq > 2, i.e. rows c, d, e (indices 2, 3, 4), ascending.
    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=2,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert list(indices[:match_count]) == [2, 3, 4]


def test_nb_segment_filter_reversed_end_ts_is_inclusive_upper_bound():
    """end_ts caps matches to ts <= end_ts, mirroring end_seq - the ts-anchored counterpart of
    a 'history before an anchor' scan (LogViewerWidget._fetch_history_window's anchor_ts path,
    which passes end_ts=anchor_ts - 1 to exclude the anchor row itself)."""
    bundle = make_bundle(
        timestamps=[10, 20, 30, 40, 50],
        rx_timestamps=[10, 20, 30, 40, 50],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )

    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # end_ts=30 -> rows a, b, c (ts 10/20/30) all included - the boundary row itself matches.
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=30,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert list(indices[:match_count]) == [0, 1, 2]


def test_filter_segment_forward_start_ts_is_inclusive_lower_bound():
    """start_ts matches ts >= start_ts - unlike start_seq (exclusive, matches seq > start_seq).
    LogViewerWidget._fetch_history_window relies on this asymmetry: a ts-anchored 'history
    after an anchor' scan passes start_ts=anchor_ts directly (no -1), while the seq-anchored
    path passes start_seq=anchor_seq - 1 to include the anchor row."""
    bundle = make_bundle(
        timestamps=[10, 20, 30, 40, 50],
        rx_timestamps=[10, 20, 30, 40, 50],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )

    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # start_ts=30 -> rows c, d, e (ts 30/40/50) all included - the boundary row itself matches.
    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=dtypes.SEQ_NONE,
        start_ts=30,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert list(indices[:match_count]) == [2, 3, 4]


def test_nb_copy_batch_to_segment_copies_pids_and_tids():
    batch = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0, 0, 0],
        levels=[0, 0, 0],
        modules=[0, 0, 0],
        sequences=[0, 0, 0],  # source batches don't carry real sequence ids yet
        messages=["a", "b", "c"],
        pids=[111, 222, 333],
        tids=[11, 22, 33],
    )

    segment = make_out_bundle(capacity=5, max_msg_bytes=8, has_pids=True, has_tids=True)

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 3
    assert list(segment.pids[:3]) == [111, 222, 333]
    assert list(segment.tids[:3]) == [11, 22, 33]


def test_nb_copy_batch_to_segment_does_not_copy_ext_columns():
    """ext_u32_1/ext_u32_2/ext_u64_1 are source-to-pipeline-local data (e.g. CAN arbitration id)
    and are intentionally never copied into the central segment - this is not a bug."""
    batch = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[0],
        messages=["a"],
    )
    batch.ext_u32_1[:] = 999
    batch.ext_u32_2[:] = 888
    batch.ext_u64_1[:] = 777

    segment = make_out_bundle(capacity=5, max_msg_bytes=8)

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 1
    assert list(segment.ext_u32_1[:1]) == [0]
    assert list(segment.ext_u32_2[:1]) == [0]
    assert list(segment.ext_u64_1[:1]) == [0]


def test_nb_copy_batch_to_segment_skips_pids_when_segment_lacks_the_column():
    """A batch that has pids but a target segment that doesn't (has_pids=False) must not write
    into (or crash against) the segment's empty pids array."""
    batch = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[0],
        messages=["a"],
        pids=[42],
    )

    segment = make_out_bundle(capacity=5, max_msg_bytes=8, has_pids=False, has_tids=False)

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 1


def test_nb_copy_batch_to_segment_zeros_pids_when_batch_lacks_them():
    """A segment with has_pids=True receiving rows from a batch without pids (e.g. system logs,
    CAN) must write 0, not leak whatever stale value the array-pool-recycled segment slot
    happened to hold from a previous segment rotation."""
    batch = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[0],
        messages=["a"],
        # pids/tids not passed -> has_pids=False on this batch
    )

    segment = make_out_bundle(capacity=5, max_msg_bytes=8, has_pids=True, has_tids=True)
    segment.pids[:] = 999  # simulate stale leftover data from a prior segment rotation
    segment.tids[:] = 888

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 1
    assert int(segment.pids[0]) == 0
    assert int(segment.tids[0]) == 0


def test_nb_copy_batch_to_segment_stops_at_segment_capacity():
    batch = make_bundle(
        timestamps=[1, 2, 3, 4],
        rx_timestamps=[1, 2, 3, 4],
        devices=[0] * 4,
        levels=[0] * 4,
        modules=[0] * 4,
        sequences=[0] * 4,
        messages=["a", "b", "c", "d"],
    )

    segment = make_out_bundle(capacity=2, max_msg_bytes=8)  # only room for 2 rows

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 2
    assert segment.size[0] == 2


def test_nb_copy_batch_to_segment_stops_when_buffer_would_overflow():
    batch = make_bundle(
        timestamps=[1, 2],
        rx_timestamps=[1, 2],
        devices=[0, 0],
        levels=[0, 0],
        modules=[0, 0],
        sequences=[0, 0],
        messages=["aaaaa", "bbbbb"],  # 5 bytes each
    )

    # Segment buffer only has room for one 5-byte message.
    segment = make_out_bundle(capacity=5, max_msg_bytes=8)
    segment = segment._replace(buffer=np.zeros(5, dtype=dtypes.BYTE))

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 1


def test_nb_copy_batch_to_segment_returns_zero_when_nothing_fits():
    batch = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[0],
        messages=["a"],
    )

    segment = make_out_bundle(capacity=0, max_msg_bytes=8)  # zero capacity

    copied = nb_copy_batch_to_segment(segment, batch, 0, 0)

    assert copied == 0


def test_nb_copy_batch_to_segment_assigns_sequential_ids_after_start_seq():
    batch = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[0] * 3,
        sequences=[0, 0, 0],
        messages=["a", "b", "c"],
    )

    segment = make_out_bundle(capacity=5, max_msg_bytes=8)

    nb_copy_batch_to_segment(segment, batch, 0, start_seq_id=100)

    assert list(segment.sequences[:3]) == [101, 102, 103]


# ---------------------------------------------------------------------------
# nb_segment_filter_reversed / nb_filter_segment: start_seq / start_ts / empty-range
# ---------------------------------------------------------------------------


def test_segment_filter_reversed_start_seq_excludes_older_rows():
    bundle = make_bundle(
        timestamps=[1, 2, 3, 4, 5],
        rx_timestamps=[1, 2, 3, 4, 5],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # start_seq is exclusive: seq > 2 -> rows with seq 3,4,5 (indices 2,3,4).
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=2,
        end_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert sorted(indices[:match_count].tolist()) == [2, 3, 4]


def test_segment_filter_reversed_start_ts_excludes_older_rows():
    bundle = make_bundle(
        timestamps=[10, 20, 30, 40, 50],
        rx_timestamps=[10, 20, 30, 40, 50],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # start_ts is inclusive: ts >= 30 -> rows c, d, e.
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=dtypes.SEQ_NONE,
        end_seq=dtypes.SEQ_NONE,
        start_ts=30,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert sorted(indices[:match_count].tolist()) == [2, 3, 4]


def test_segment_filter_reversed_empty_range_returns_zero():
    bundle = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[0] * 3,
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(3, dtype=np.int64)

    # start_seq excludes everything above end_seq -> empty range.
    match_count = nb_segment_filter_reversed(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=3,
        start_seq=3,
        end_seq=1,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 0


def test_filter_segment_forward_end_ts_bounds_upper_range():
    bundle = make_bundle(
        timestamps=[10, 20, 30, 40, 50],
        rx_timestamps=[10, 20, 30, 40, 50],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    # end_ts is inclusive: ts <= 30 -> rows a, b, c.
    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=5,
        start_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=30,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 3
    assert list(indices[:match_count]) == [0, 1, 2]


def test_filter_segment_forward_empty_range_returns_zero():
    bundle = make_bundle(
        timestamps=[10, 20, 30],
        rx_timestamps=[10, 20, 30],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[0] * 3,
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(3, dtype=np.int64)

    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=3,
        start_seq=dtypes.SEQ_NONE,
        start_ts=100,  # excludes every row
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 0


def test_filter_segment_forward_respects_max_matches_cutoff():
    bundle = make_bundle(
        timestamps=[1, 2, 3, 4, 5],
        rx_timestamps=[1, 2, 3, 4, 5],
        devices=[0] * 5,
        levels=[0] * 5,
        modules=[0] * 5,
        sequences=[1, 2, 3, 4, 5],
        messages=["a", "b", "c", "d", "e"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(5, dtype=np.int64)

    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=2,
        start_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 2
    assert list(indices[:match_count]) == [0, 1]


# ---------------------------------------------------------------------------
# Python wrapper entry points (segment_filter_reversed / segment_filter)
# ---------------------------------------------------------------------------


def test_segment_filter_reversed_wrapper_delegates_to_kernel():
    bundle = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[0] * 3,
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(3, dtype=np.int64)

    match_count = segment_filter_reversed(bundle, effective_mask, indices, max_matches=3)

    assert match_count == 3


def test_segment_filter_wrapper_delegates_to_kernel():
    bundle = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[0] * 3,
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(3, dtype=np.int64)

    match_count = segment_filter(bundle, effective_mask, indices, max_matches=3)

    assert match_count == 3


# ---------------------------------------------------------------------------
# kv / text filtering integration
# ---------------------------------------------------------------------------


def test_filter_segment_respects_kv_conditions():
    bundle = make_bundle(
        timestamps=[1, 2],
        rx_timestamps=[1, 2],
        devices=[0, 0],
        levels=[0, 0],
        modules=[0, 0],
        sequences=[1, 2],
        messages=["status=ok user=alice", "status=fail user=bob"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(2, dtype=np.int64)
    kv = build_kv_condition_arrays([(b"status", b"ok")])

    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=2,
        start_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=kv,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=EMPTY_TEXT_SEARCH,
    )

    assert match_count == 1
    assert indices[0] == 0


def test_filter_segment_respects_text_needle_in_message_body():
    bundle = make_bundle(
        timestamps=[1, 2],
        rx_timestamps=[1, 2],
        devices=[0, 0],
        levels=[0, 0],
        modules=[0, 0],
        sequences=[1, 2],
        messages=["hello world", "goodbye"],
    )
    effective_mask = np.zeros(1, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(2, dtype=np.int64)
    text = build_text_search_arrays("world", devices={}, modules={})

    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=2,
        start_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=text,
    )

    assert match_count == 1
    assert indices[0] == 0


def test_filter_segment_text_matches_via_module_name_mask_even_without_body_hit():
    """A text query matching the *module's own name* should surface every row on that module,
    even when the message body itself doesn't contain the needle - mirrors 'filter
    device/module/message' semantics from build_text_search_arrays' docstring."""
    bundle = make_bundle(
        timestamps=[1, 2],
        rx_timestamps=[1, 2],
        devices=[0, 0],
        levels=[0, 0],
        modules=[7, 8],
        sequences=[1, 2],
        messages=["nothing interesting here", "also nothing"],
    )
    effective_mask = np.zeros(9, dtype=dtypes.LEVEL_TYPE)
    indices = np.zeros(2, dtype=np.int64)

    mod_mask = np.zeros(9, dtype=np.bool_)
    mod_mask[7] = True  # module 7's name happens to match the query
    from blinkview.ops.text_filter import TextSearchArrays

    text = TextSearchArrays(
        needle_buf=np.frombuffer(b"wifi", dtype=dtypes.BYTE),
        needle_len=4,
        dev_mask=np.zeros(1, dtype=np.bool_),
        mod_mask=mod_mask,
    )

    match_count = nb_filter_segment(
        bundle,
        effective_mask=effective_mask,
        out_indices=indices,
        max_matches=2,
        start_seq=dtypes.SEQ_NONE,
        start_ts=dtypes.TS_UNSPECIFIED,
        end_ts=dtypes.TS_UNSPECIFIED,
        kv=EMPTY_KV_CONDITIONS,
        kv_field_delim=CHAR_SPACE,
        kv_kv_delim=CHAR_EQUALS,
        text=text,
    )

    assert match_count == 1
    assert indices[0] == 0  # only the row on module 7 matches, via mod_mask not body text


# ---------------------------------------------------------------------------
# nb_segment_extract_fields: pids/tids copy branch
# ---------------------------------------------------------------------------


def test_segment_extract_fields_copies_pids_and_tids_when_both_sides_have_them():
    bundle = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[1],
        messages=["a"],
        pids=[55],
        tids=[66],
    )
    indices = np.array([0], dtype=np.int64)
    out_bundle = make_out_bundle(1, 8, has_pids=True, has_tids=True)

    nb_segment_extract_fields(bundle, indices, 1, out_bundle, 0, 8)

    assert int(out_bundle.pids[0]) == 55
    assert int(out_bundle.tids[0]) == 66


def test_segment_extract_fields_skips_pids_when_output_lacks_the_column():
    bundle = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[0],
        sequences=[1],
        messages=["a"],
        pids=[55],
        tids=[66],
    )
    indices = np.array([0], dtype=np.int64)
    out_bundle = make_out_bundle(1, 8, has_pids=False, has_tids=False)

    # Must not crash writing into the (empty-semantics) output pids/tids arrays.
    written = nb_segment_extract_fields(bundle, indices, 1, out_bundle, 0, 8)

    assert written == 1


# ---------------------------------------------------------------------------
# nb_find_next_module_match / nb_find_next_module_index
# ---------------------------------------------------------------------------


def test_find_next_module_match_finds_first_occurrence_after_start_seq():
    bundle = make_bundle(
        timestamps=[1, 2, 3, 4],
        rx_timestamps=[1, 2, 3, 4],
        devices=[0] * 4,
        levels=[0] * 4,
        modules=[0, 1, 1, 1],
        sequences=[1, 2, 3, 4],
        messages=["a", "b", "c", "d"],
    )

    seq, idx = nb_find_next_module_match(bundle, target_module=1, start_seq=2)

    assert seq == 3  # row at index 2 (seq=3) is the first module-1 row with seq > 2
    assert idx == 2


def test_find_next_module_match_start_seq_none_allows_first_row():
    bundle = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[5],
        sequences=[1],
        messages=["a"],
    )

    seq, idx = nb_find_next_module_match(bundle, target_module=5, start_seq=dtypes.SEQ_NONE)

    assert seq == 1
    assert idx == 0


def test_find_next_module_match_not_found_returns_zero_tuple():
    bundle = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[5],
        sequences=[1],
        messages=["a"],
    )

    seq, idx = nb_find_next_module_match(bundle, target_module=99, start_seq=dtypes.SEQ_NONE)

    assert seq == 0
    assert idx == 0


def test_find_next_module_index_finds_first_occurrence_from_start_idx():
    bundle = make_bundle(
        timestamps=[1, 2, 3],
        rx_timestamps=[1, 2, 3],
        devices=[0] * 3,
        levels=[0] * 3,
        modules=[2, 2, 3],
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )

    found, idx = nb_find_next_module_index(bundle, target_module=2, start_idx=1)

    assert found
    assert idx == 1


def test_find_next_module_index_not_found_returns_false():
    bundle = make_bundle(
        timestamps=[1],
        rx_timestamps=[1],
        devices=[0],
        levels=[0],
        modules=[2],
        sequences=[1],
        messages=["a"],
    )

    found, idx = nb_find_next_module_index(bundle, target_module=99, start_idx=0)

    assert not found
    assert idx == 0


# ---------------------------------------------------------------------------
# nb_bundle_push / nb_bundle_push_len / nb_can_push / nb_bundle_extend
# ---------------------------------------------------------------------------


def _pool_batch(capacity=4, buffer_bytes=64, **flags):
    pool = NumpyArrayPool()
    return pool.create(PooledLogBatch, capacity, buffer_bytes, **flags)


def _raw_push_bundle(capacity, buffer_bytes):
    """A LogBundle with an exact (non-pool-rounded) capacity/buffer size, for boundary tests -
    NumpyArrayPool always rounds up to at least 1024 bytes per column, which would swallow the
    small capacity/buffer-overflow scenarios these tests are trying to trigger."""
    return LogBundle(
        timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        offsets=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        buffer=np.zeros(buffer_bytes, dtype=dtypes.BYTE),
        levels=np.zeros(capacity, dtype=dtypes.LEVEL_TYPE),
        modules=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        devices=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        sequences=np.zeros(capacity, dtype=dtypes.SEQ_TYPE),
        pids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        tids=np.zeros(capacity, dtype=dtypes.ID_TYPE),
        ext_u32_1=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u32_2=np.zeros(capacity, dtype=dtypes.UINT32),
        ext_u64_1=np.zeros(capacity, dtype=dtypes.UINT64),
        size=np.array([0], dtype=np.int64),
        msg_cursor=np.array([0], dtype=np.int64),
        capacity=capacity,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=False,
        has_tids=False,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def test_bundle_push_writes_row_and_advances_cursors():
    batch = _pool_batch(has_levels=True, has_modules=True, has_devices=True, has_sequences=True)
    msg = np.frombuffer(b"hello", dtype=dtypes.BYTE)

    ok = nb_bundle_push(
        batch.bundle, 100, 100, msg, level=2, module=3, device=1, seq=7, ext_u32_1=0, ext_u32_2=0, ext_u64_1=0
    )

    assert ok
    assert batch.bundle.size[0] == 1
    assert batch.bundle.timestamps[0] == 100
    assert batch.bundle.levels[0] == 2
    assert batch.bundle.modules[0] == 3
    assert batch.bundle.devices[0] == 1
    assert batch.bundle.sequences[0] == 7
    off = int(batch.bundle.offsets[0])
    length = int(batch.bundle.lengths[0])
    assert batch.bundle.buffer[off : off + length].tobytes() == b"hello"

    batch.release()


def test_bundle_push_fails_when_capacity_exhausted():
    bundle = _raw_push_bundle(capacity=1, buffer_bytes=64)
    msg = np.frombuffer(b"a", dtype=dtypes.BYTE)

    first = nb_bundle_push(bundle, 1, 1, msg, 0, 0, 0, 0, 0, 0, 0)
    second = nb_bundle_push(bundle, 2, 2, msg, 0, 0, 0, 0, 0, 0, 0)

    assert first
    assert not second  # capacity is 1, second row must be rejected


def test_bundle_push_fails_when_buffer_would_overflow():
    bundle = _raw_push_bundle(capacity=4, buffer_bytes=3)
    msg = np.frombuffer(b"toolong", dtype=dtypes.BYTE)

    ok = nb_bundle_push(bundle, 1, 1, msg, 0, 0, 0, 0, 0, 0, 0)

    assert not ok
    assert bundle.size[0] == 0  # rejected before any metadata was written


def test_bundle_push_writes_pids_and_tids_when_present():
    batch = _pool_batch(has_pids=True, has_tids=True)
    msg = np.frombuffer(b"x", dtype=dtypes.BYTE)

    nb_bundle_push(batch.bundle, 1, 1, msg, 0, 0, 0, 0, 0, 0, 0, pid=42, tid=7)

    assert int(batch.bundle.pids[0]) == 42
    assert int(batch.bundle.tids[0]) == 7

    batch.release()


def test_can_push_projects_timestamp_and_packs_flags():
    batch = _pool_batch(has_ext_u32_1=True, has_ext_u32_2=True)
    data = np.frombuffer(b"\x01\x02\x03", dtype=dtypes.BYTE)

    ok = nb_can_push(
        batch.bundle,
        raw_timestamp=0.5,
        offset_ns=1_000_000_000,
        arb_id=0x123,
        data=data,
        is_ext=True,
        is_rem=False,
        is_err=False,
        is_fd=True,
        is_rx=True,
        brs=False,
        esi=False,
    )

    assert ok
    # ts_ns = offset_ns + int(0.5 * 1e9) = 1_500_000_000
    assert batch.bundle.timestamps[0] == 1_500_000_000
    assert int(batch.bundle.ext_u32_1[0]) == 0x123
    # is_ext (bit0) | is_fd (bit3) | is_rx (bit4) = 0x01 | 0x08 | 0x10 = 0x19
    assert int(batch.bundle.ext_u32_2[0]) == 0x19

    batch.release()


def test_bundle_extend_appends_to_last_message():
    batch = _pool_batch()
    first = np.frombuffer(b"hello", dtype=dtypes.BYTE)
    nb_bundle_push(batch.bundle, 1, 1, first, 0, 0, 0, 0, 0, 0, 0)

    more = np.frombuffer(b" world", dtype=dtypes.BYTE)
    ok = nb_bundle_extend(batch.bundle, more)

    assert ok
    assert batch.bundle.size[0] == 1  # still one logical row
    off = int(batch.bundle.offsets[0])
    length = int(batch.bundle.lengths[0])
    assert batch.bundle.buffer[off : off + length].tobytes() == b"hello world"

    batch.release()


def test_bundle_extend_on_empty_bundle_returns_false():
    batch = _pool_batch()
    more = np.frombuffer(b"x", dtype=dtypes.BYTE)

    ok = nb_bundle_extend(batch.bundle, more)

    assert not ok

    batch.release()


def test_bundle_extend_fails_when_buffer_would_overflow():
    bundle = _raw_push_bundle(capacity=4, buffer_bytes=6)
    first = np.frombuffer(b"abc", dtype=dtypes.BYTE)
    nb_bundle_push(bundle, 1, 1, first, 0, 0, 0, 0, 0, 0, 0)

    too_much = np.frombuffer(b"toolong", dtype=dtypes.BYTE)
    ok = nb_bundle_extend(bundle, too_much)

    assert not ok

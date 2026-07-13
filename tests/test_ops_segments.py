import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.segments import nb_filter_segment, nb_segment_extract_fields, nb_segment_filter_reversed


def make_bundle(timestamps, rx_timestamps, devices, levels, modules, sequences, messages):
    """Builds a minimal LogBundle backing a fixed set of rows, for kernel-level testing."""
    lengths = np.array([len(m) for m in messages], dtype=dtypes.LEN_TYPE)
    offsets = np.zeros(len(messages), dtype=dtypes.OFFSET_TYPE)

    cursor = 0
    for i, m in enumerate(messages):
        offsets[i] = cursor
        cursor += len(m.encode("utf-8"))

    buffer = np.zeros(max(cursor, 1), dtype=dtypes.BYTE)
    cursor = 0
    for m in messages:
        b = m.encode("utf-8")
        if b:
            buffer[cursor : cursor + len(b)] = np.frombuffer(b, dtype=dtypes.BYTE)
        cursor += len(b)

    size = len(messages)
    return LogBundle(
        timestamps=np.array(timestamps, dtype=dtypes.TS_TYPE),
        rx_timestamps=np.array(rx_timestamps, dtype=dtypes.TS_TYPE),
        offsets=offsets,
        lengths=lengths,
        buffer=buffer,
        levels=np.array(levels, dtype=dtypes.LEVEL_TYPE),
        modules=np.array(modules, dtype=dtypes.ID_TYPE),
        devices=np.array(devices, dtype=dtypes.ID_TYPE),
        sequences=np.array(sequences, dtype=dtypes.SEQ_TYPE),
        ext_u32_1=np.zeros(size, dtype=dtypes.UINT32),
        ext_u32_2=np.zeros(size, dtype=dtypes.UINT32),
        ext_u64_1=np.zeros(size, dtype=dtypes.UINT64),
        size=np.array([size], dtype=np.int64),
        msg_cursor=np.array([cursor], dtype=np.int64),
        capacity=size,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_ext_u32_1=False,
        has_ext_u32_2=False,
        has_ext_u64_1=False,
    )


def make_out_bundle(capacity, max_msg_bytes):
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
    match_count = nb_segment_filter_reversed(bundle, effective_mask=effective_mask, out_indices=indices, max_matches=1)
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
    match_count = nb_segment_filter_reversed(bundle, effective_mask=effective_mask, out_indices=indices, max_matches=1)
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
        bundle, effective_mask=effective_mask, out_indices=indices, max_matches=5, end_seq=3
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

    match_count = nb_segment_filter_reversed(bundle, effective_mask=effective_mask, out_indices=indices, max_matches=3)

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
        bundle, effective_mask=effective_mask, out_indices=indices, max_matches=5, start_seq=2
    )

    assert match_count == 3
    assert list(indices[:match_count]) == [2, 3, 4]

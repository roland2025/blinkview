# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numba_config import NUMBA_DISABLE
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ops.segments import nb_bundle_push_len


@pytest.fixture
def pool():
    return NumpyArrayPool()


def make_batch(pool, capacity=8, buffer_bytes=256, **flags):
    return pool.create(PooledLogBatch, capacity, buffer_bytes, **flags)


class TestBasicProperties:
    def test_starts_empty(self, pool):
        batch = make_batch(pool)
        assert batch.size == 0
        assert len(batch) == 0
        assert batch.msg_cursor == 0
        batch.release()

    def test_capacity_reflects_requested_size_rounded_up(self, pool):
        batch = make_batch(pool, capacity=8)
        assert batch.capacity >= 8
        batch.release()

    def test_insert_increments_size_and_msg_cursor(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"hello")
        assert batch.size == 1
        assert len(batch) == 1
        assert batch.msg_cursor == 5
        batch.release()

    def test_buffer_capacity_reflects_requested_bytes(self, pool):
        batch = make_batch(pool, buffer_bytes=256)
        assert batch.buffer_capacity() >= 256
        batch.release()

    def test_buffer_capacity_is_zero_after_release(self, pool):
        batch = make_batch(pool)
        batch.release()
        assert batch.buffer_capacity() == 0


class TestClear:
    def test_clear_resets_size_and_cursor(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"hello")

        batch.clear()

        assert batch.size == 0
        assert batch.msg_cursor == 0
        batch.release()

    def test_clear_can_update_metadata(self, pool):
        batch = pool.create(PooledLogBatch, 8, 256, metadata="old")
        batch.clear(new_metadata="new")
        assert batch.metadata == "new"
        batch.release()


class TestRetainRelease:
    def test_release_frees_the_bundle(self, pool):
        batch = make_batch(pool)
        batch.release()
        assert batch.bundle is None
        assert batch.in_use is False

    def test_retain_requires_an_extra_release_before_freeing(self, pool):
        batch = make_batch(pool)
        batch.retain()

        batch.release()
        assert batch.bundle is not None  # still alive - one retain outstanding

        batch.release()
        assert batch.bundle is None

    def test_retain_after_full_release_raises(self, pool):
        batch = make_batch(pool)
        batch.release()

        with pytest.raises(RuntimeError):
            batch.retain()

    def test_context_manager_releases_on_exit(self, pool):
        batch = make_batch(pool)
        with batch:
            batch.insert(100, 100, b"x")
        assert batch.bundle is None


class TestIteration:
    def test_iterates_inserted_rows_in_order(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"first")
        batch.insert(200, 200, b"second")

        rows = list(batch)

        assert len(rows) == 2
        assert rows[0][0] == 100
        assert rows[0][1] == b"first"
        assert rows[1][0] == 200
        assert rows[1][1] == b"second"
        batch.release()

    def test_iterating_an_empty_batch_yields_nothing(self, pool):
        batch = make_batch(pool)
        assert list(batch) == []
        batch.release()

    def test_iterating_a_released_batch_yields_nothing(self, pool):
        batch = make_batch(pool)
        batch.release()
        assert list(batch) == []

    def test_optional_columns_are_none_when_not_enabled(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"x", level=9, module=9, device=9)

        row = next(iter(batch))
        _ts, _msg, _rx_ts, level, module, device, seq, *_rest = row
        assert level is None
        assert module is None
        assert device is None
        assert seq is None
        batch.release()

    def test_optional_columns_are_populated_when_enabled(self, pool):
        batch = make_batch(pool, has_levels=True, has_modules=True, has_devices=True, has_sequences=True)
        batch.insert(100, 100, b"x", level=3, module=7, device=2, seq=42)

        row = next(iter(batch))
        _ts, _msg, _rx_ts, level, module, device, seq, *_rest = row
        assert level == 3
        assert module == 7
        assert device == 2
        assert seq == 42
        batch.release()


class TestGetItem:
    def test_positive_index_returns_the_row(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"first")
        batch.insert(200, 200, b"second")

        row = batch[1]
        assert row[0] == 200
        assert row[1] == b"second"
        batch.release()

    def test_negative_index_counts_from_the_end(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"first")
        batch.insert(200, 200, b"second")

        assert batch[-1][1] == b"second"
        batch.release()

    def test_out_of_range_index_raises(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"only")

        with pytest.raises(IndexError):
            batch[5]
        batch.release()

    def test_non_integer_index_raises_type_error(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"only")

        with pytest.raises(TypeError):
            batch["not-an-index"]
        batch.release()

    def test_slice_returns_a_list_of_rows(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"a")
        batch.insert(200, 200, b"b")
        batch.insert(300, 300, b"c")

        rows = batch[0:2]
        assert [r[1] for r in rows] == [b"a", b"b"]
        batch.release()

    def test_getitem_on_released_batch_raises(self, pool):
        batch = make_batch(pool)
        batch.release()
        with pytest.raises(RuntimeError):
            batch[0]


class TestStartTs:
    def test_empty_batch_returns_max_int64_sentinel(self, pool):
        batch = make_batch(pool)
        assert batch.start_ts == 9223372036854775807
        batch.release()

    def test_returns_the_first_messages_timestamp(self, pool):
        batch = make_batch(pool)
        batch.insert(555, 555, b"x")
        assert batch.start_ts == 555
        batch.release()


class TestGetDevice:
    def test_returns_zero_when_device_tracking_disabled(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"x", device=7)
        assert batch.get_device() == 0
        batch.release()

    def test_returns_zero_when_empty(self, pool):
        batch = make_batch(pool, has_devices=True)
        assert batch.get_device() == 0
        batch.release()

    def test_returns_the_first_rows_device_id(self, pool):
        batch = make_batch(pool, has_devices=True)
        batch.insert(100, 100, b"x", device=7)
        batch.insert(100, 100, b"y", device=9)
        assert batch.get_device() == 7
        batch.release()


class TestSequenceIds:
    def test_first_and_last_sequence_id_default_to_seq_none_when_empty(self, pool):
        batch = make_batch(pool, has_sequences=True)
        assert batch.first_sequence_id == SEQ_NONE
        assert batch.last_sequence_id == SEQ_NONE
        batch.release()

    def test_first_and_last_sequence_id_track_the_boundary_rows(self, pool):
        batch = make_batch(pool, has_sequences=True)
        batch.insert(100, 100, b"a", seq=10)
        batch.insert(100, 100, b"b", seq=20)
        batch.insert(100, 100, b"c", seq=30)

        assert batch.first_sequence_id == 10
        assert batch.last_sequence_id == 30
        batch.release()


class TestRepr:
    def test_repr_reports_size_and_capacity_when_alive(self, pool):
        batch = make_batch(pool)
        batch.insert(100, 100, b"x")

        text = repr(batch)

        assert "size=1" in text
        assert str(batch) == text
        batch.release()

    def test_repr_reports_released_state_after_release(self, pool):
        batch = make_batch(pool)
        batch.release()
        assert "released" in repr(batch)


class TestCounterViewConsistency:
    """plans/bundle-push-fast-path.md stage 2. note_appended_rows() used to cache
    b.sequences[i]/b.timestamps[i] - numpy scalars - into the same "plain-int cache" that
    _note_inserted_row fills with real Python ints, so last_sequence_id's *type* depended on
    which path had filled the batch. Reading through a memoryview returns a plain int and makes
    the two paths agree."""

    SCHEMA = dict(
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=True,
        has_tids=True,
    )

    def test_insert_and_note_appended_rows_agree_on_type_and_value(self, pool):
        from blinkview.ops.segments import nb_copy_batch_to_segment

        rows = [(100, b"aaa"), (150, b"bbb"), (200, b"ccc")]

        inserted = make_batch(pool, capacity=8, buffer_bytes=256, **self.SCHEMA)
        for i, (ts, msg) in enumerate(rows):
            assert inserted.insert(ts, ts, msg, seq=28 + i)

        appended = make_batch(pool, capacity=8, buffer_bytes=256, **self.SCHEMA)
        copied = nb_copy_batch_to_segment(appended.bundle, inserted.bundle, 0, 27)
        assert copied == len(rows)
        appended.note_appended_rows(copied)

        assert appended.last_sequence_id == inserted.last_sequence_id == 30
        assert appended.first_sequence_id == inserted.first_sequence_id == 28
        assert appended.end_ts == inserted.end_ts == 200
        assert appended.start_ts == inserted.start_ts == 100

        for name in ("last_sequence_id", "first_sequence_id", "start_ts", "end_ts"):
            assert type(getattr(appended, name)) is int, name
            assert type(getattr(inserted, name)) is type(getattr(appended, name)), name

        inserted.release()
        appended.release()

    def test_note_appended_rows_tracks_later_appends(self, pool):
        from blinkview.ops.segments import nb_copy_batch_to_segment

        src = make_batch(pool, capacity=8, buffer_bytes=256, **self.SCHEMA)
        assert src.insert(500, 500, b"x")

        segment = make_batch(pool, capacity=8, buffer_bytes=256, **self.SCHEMA)
        for expected_seq in (1, 2, 3):
            copied = nb_copy_batch_to_segment(segment.bundle, src.bundle, 0, expected_seq - 1)
            assert copied == 1
            segment.note_appended_rows(copied)
            assert segment.last_sequence_id == expected_seq
            assert type(segment.last_sequence_id) is int

        assert segment.first_sequence_id == 1
        assert segment.size == 3
        src.release()
        segment.release()

    def test_size_and_msg_cursor_survive_clear_and_reinsert(self, pool):
        batch = make_batch(pool, capacity=8, buffer_bytes=256, **self.SCHEMA)
        assert batch.insert(100, 100, b"hello", seq=1)
        assert batch.size == 1
        assert batch.msg_cursor == 5
        assert type(batch.size) is int
        assert type(batch.msg_cursor) is int

        batch.clear()
        assert batch.size == 0
        assert len(batch) == 0
        assert batch.msg_cursor == 0
        assert batch.last_sequence_id == SEQ_NONE

        assert batch.insert(200, 200, b"yo", seq=2)
        assert batch.size == 1
        assert batch.msg_cursor == 2
        assert batch.bundle.size[0] == 1
        batch.release()

    def test_counters_read_zero_after_release(self, pool):
        batch = make_batch(pool, capacity=8, buffer_bytes=256, **self.SCHEMA)
        assert batch.insert(100, 100, b"hello")
        batch.release()

        assert batch.size == 0
        assert len(batch) == 0
        assert batch.msg_cursor == 0


ALL_COLUMNS = (
    "has_levels",
    "has_modules",
    "has_devices",
    "has_sequences",
    "has_pids",
    "has_tids",
    "has_ext_u32_1",
    "has_ext_u32_2",
    "has_ext_u64_1",
)

ROW_COLUMNS = (
    "timestamps",
    "rx_timestamps",
    "offsets",
    "lengths",
    "levels",
    "modules",
    "devices",
    "sequences",
    "pids",
    "tids",
    "ext_u32_1",
    "ext_u32_2",
    "ext_u64_1",
)

# The column sets real call sites create batches with, plus none and all.
SHAPES = {
    "none": (),
    "lvl_mod_dev": ("has_levels", "has_modules", "has_devices"),
    "plus_pid_tid": ("has_levels", "has_modules", "has_devices", "has_pids", "has_tids"),
    "plus_seq": ("has_levels", "has_modules", "has_devices", "has_sequences"),
    "can_bus": ("has_ext_u32_1", "has_ext_u32_2"),
    "all": ALL_COLUMNS,
}


def kernel_push(batch, ts, rx, src, n, level=0, module=0, device=0, seq=0, e1=0, e2=0, e3=0, pid=0, tid=0):
    """What insert()/insert_view() did before stage 3, kept as the reference."""
    return nb_bundle_push_len(batch.bundle, ts, rx, src, n, level, module, device, seq, e1, e2, e3, pid, tid)


def assert_same_rows(actual, expected):
    a, e = actual.bundle, expected.bundle
    assert actual.size == expected.size
    assert actual.msg_cursor == expected.msg_cursor
    n = expected.size
    for name in ROW_COLUMNS:
        a_col, e_col = getattr(a, name), getattr(e, name)
        if len(e_col) == 0:
            assert len(a_col) == 0, name
            continue
        np.testing.assert_array_equal(a_col[:n], e_col[:n], err_msg=name)
    assert bytes(a.buffer[: expected.msg_cursor]) == bytes(e.buffer[: expected.msg_cursor])


class TestPythonInsertMatchesKernel:
    """plans/bundle-push-fast-path.md stage 3: insert()/insert_any()/insert_view() write rows in
    plain Python over memoryviews instead of crossing into nb_bundle_push_len. A hand-written
    copy of a kernel silently drifts from it, so each column shape real callers use is pinned
    here against the kernel itself, row by row and byte by byte."""

    ROWS = [
        # ts, rx, payload, level, module, device, seq, e1, e2, e3, pid, tid
        (100, 101, b"hello", 3, 7, 2, 1, 0x123, 0x5, 9, 4242, 17),
        (200, 205, b"", 0, 0, 0, 2, 0, 0, 0, 0, 0),
        (300, 300, bytearray(b"ba"), 255, 2**32 - 1, 1, 2**64 - 1, 2**32 - 1, 1, 2**64 - 1, 1, 2),
        (np.int64(400), np.int64(401), memoryview(b"mv"), np.uint8(4), np.uint32(5), 6, np.uint64(7), 8, 9, 10, 11, 12),
        (-5, 0, np.frombuffer(b"arr", dtype=np.uint8), 1, 1, 1, 0, 0, 0, 0, 0, 0),
    ]

    @pytest.mark.parametrize("shape", SHAPES)
    def test_insert_writes_what_the_kernel_writes(self, pool, shape):
        flags = {name: True for name in SHAPES[shape]}
        python = make_batch(pool, capacity=16, buffer_bytes=256, **flags)
        kernel = make_batch(pool, capacity=16, buffer_bytes=256, **flags)

        for ts, rx, payload, level, module, device, seq, e1, e2, e3, pid, tid in self.ROWS:
            assert python.insert(ts, rx, payload, level, module, device, seq, e1, e2, e3, pid, tid)
            assert kernel_push(kernel, ts, rx, payload, len(payload), level, module, device, seq, e1, e2, e3, pid, tid)

        assert_same_rows(python, kernel)
        python.release()
        kernel.release()

    @pytest.mark.parametrize("shape", SHAPES)
    def test_insert_view_copies_only_msg_length_bytes(self, pool, shape):
        flags = {name: True for name in SHAPES[shape]}
        python = make_batch(pool, capacity=16, buffer_bytes=256, **flags)
        kernel = make_batch(pool, capacity=16, buffer_bytes=256, **flags)
        read_buf = np.frombuffer(b"0123456789abcdef", dtype=np.uint8)

        for i, n in enumerate((16, 5, 0, 1)):
            assert python.insert_view(1000 + i, 1000 + i, read_buf, n, level=1, module=i, seq=i)
            assert kernel_push(kernel, 1000 + i, 1000 + i, read_buf, n, level=1, module=i, seq=i)

        assert_same_rows(python, kernel)
        python.release()
        kernel.release()

    def test_insert_any_takes_the_raw_bytes_of_any_buffer(self, pool):
        batch = make_batch(pool, capacity=8, buffer_bytes=256)
        wide = np.array([1, 2, 0x0304], dtype=np.int16)

        assert batch.insert_any(1, 1, wide)
        assert batch.insert_any(2, 2, memoryview(b"xyz").cast("c"))
        assert batch.insert_any(3, 3, bytearray(b"q"))

        assert batch[0][1] == wide.tobytes()
        assert batch[1][1] == b"xyz"
        assert batch[2][1] == b"q"
        batch.release()

    @pytest.mark.skipif(
        NUMBA_DISABLE,
        reason="The wrap/convert behavior being preserved is the JIT's; the plain-Python kernel "
        "raises on the same inputs (numpy refuses an out-of-range store), so there is no "
        "reference to compare against.",
    )
    def test_values_a_memoryview_rejects_fall_back_to_the_kernel(self, pool):
        """Out-of-range ints, floats, np.bool_ and a non-"B" payload all raise on a memoryview
        store where the kernel wraps or converts them. _push redoes those rows through the
        kernel, so they keep the results they always had."""
        flags = {name: True for name in ALL_COLUMNS}
        python = make_batch(pool, capacity=8, buffer_bytes=256, **flags)
        kernel = make_batch(pool, capacity=8, buffer_bytes=256, **flags)
        rows = [
            dict(ts=1, payload=b"wrap", level=300, module=-1),
            dict(ts=2.75, payload=b"float"),
            dict(ts=3, payload=b"bool", level=np.bool_(True)),
            dict(ts=4, payload=np.array([-1, 65], dtype=np.int8)),
        ]
        for row in rows:
            ts, payload = row["ts"], row["payload"]
            level, module = row.get("level", 0), row.get("module", 0)
            assert python.insert(ts, ts, payload, level=level, module=module)
            assert kernel_push(kernel, ts, ts, payload, len(payload), level, module)

        assert_same_rows(python, kernel)
        assert python.bundle.levels[0] == 300 % 256
        assert python.bundle.modules[0] == 2**32 - 1
        assert bytes(python.bundle.buffer[python.bundle.offsets[3] :][:2]) == b"\xffA"
        python.release()
        kernel.release()


class TestPythonInsertEdges:
    def test_full_rows_refuse_and_leave_the_batch_unchanged(self, pool):
        batch = make_batch(pool, capacity=2, buffer_bytes=256, has_levels=True)
        capacity = batch.capacity
        for i in range(capacity):
            assert batch.insert(i, i, b"x", level=1)

        assert batch.insert(99, 99, b"y", level=2) is False
        assert batch.size == capacity
        assert batch.msg_cursor == capacity
        assert batch.end_ts == capacity - 1
        batch.release()

    def test_full_buffer_refuses_and_leaves_the_batch_unchanged(self, pool):
        batch = make_batch(pool, capacity=8, buffer_bytes=8)
        room = len(batch.bundle.buffer)
        assert batch.insert(1, 1, b"a" * (room - 1))

        assert batch.insert(2, 2, b"bb") is False
        assert batch.size == 1
        assert batch.msg_cursor == room - 1
        # Exactly filling the buffer is still allowed.
        assert batch.insert(3, 3, b"c")
        assert batch.msg_cursor == room
        batch.release()

    def test_views_are_built_on_first_insert_and_released_with_the_batch(self, pool):
        batch = make_batch(pool, capacity=8, buffer_bytes=256, has_levels=True)
        assert batch._insert_views is None

        assert batch.insert(1, 1, b"a", level=1)
        views = batch._insert_views
        assert views is not None
        # Disabled columns get no view: their EMPTY_* placeholders are shared and must never
        # be written.
        assert views[7:] == (None,) * 8

        batch.release()
        assert batch._insert_views is None
        with pytest.raises(ValueError):
            views[1][0]  # a released memoryview refuses all access

    def test_insert_after_release_refuses_instead_of_writing_recycled_memory(self, pool):
        batch = make_batch(pool, capacity=8, buffer_bytes=256, has_levels=True)
        assert batch.insert(1, 1, b"a", level=1)
        batch.release()

        reused = make_batch(pool, capacity=8, buffer_bytes=256, has_levels=True)
        assert batch.insert(2, 2, b"b", level=2) is False
        assert batch.insert_any(2, 2, b"b") is False
        assert batch.insert_view(2, 2, np.frombuffer(b"b", dtype=np.uint8), 1) is False
        assert reused.size == 0
        reused.release()

    def test_a_recycled_slab_never_leaks_an_old_value_into_a_new_row(self, pool):
        """Pool slabs aren't zeroed on acquire. Every enabled column has to be written on every
        insert - including with the default 0 - or the new row shows the old batch's value."""
        flags = {name: True for name in ALL_COLUMNS}
        batch = make_batch(pool, capacity=8, buffer_bytes=256, **flags)
        # What an earlier batch leaves in a slab - set by hand, because which column gets which
        # same-dtype slab back depends on the pool's LIFO order.
        for name in ROW_COLUMNS:
            getattr(batch.bundle, name)[:] = 77

        assert batch.insert(1, 2, b"new")

        b = batch.bundle
        assert (b.timestamps[0], b.rx_timestamps[0], b.offsets[0], b.lengths[0]) == (1, 2, 0, 3)
        for name in ROW_COLUMNS[4:]:
            assert getattr(b, name)[0] == 0, name
        batch.release()

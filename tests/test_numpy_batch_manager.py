# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_batch_manager import PooledLogBatch


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

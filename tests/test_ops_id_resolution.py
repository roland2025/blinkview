# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.modules import MODULE_ID_FULL, MODULE_ID_UNKNOWN, MODULE_TEMP_ID_BASE, ModuleTrackerState
from blinkview.ops.id_resolution import nb_resolve_names_batch, nb_resolve_scoped_id, nb_resolve_scoped_names_batch


def make_buf(text: str) -> np.ndarray:
    return np.frombuffer(text.encode("utf-8"), dtype=np.uint8)


def make_table(use_values=False, initial_capacity=8, buffer_size_bytes=256):
    return IndexedStringTable(
        initial_capacity=initial_capacity,
        buffer_size_bytes=buffer_size_bytes,
        use_hashes=True,
        values_dtype=dtypes.ID_TYPE if use_values else None,
    )


def make_tracker(name_bytes, capacity=16):
    return ModuleTrackerState(
        count=np.zeros(1, dtype=dtypes.ID_TYPE),
        bytes_cursor=np.zeros(1, dtype=dtypes.OFFSET_TYPE),
        starts=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        hashes=np.zeros(capacity, dtype=dtypes.HASH_TYPE),
        name_bytes=name_bytes,
    )


class TestResolveNamesBatch:
    """Device-style (unscoped, one flat global namespace) resolution via nb_resolve_names_batch,
    which drives the existing ops/discovery.py nb_resolve_module_id unmodified."""

    def test_new_names_get_distinct_temp_ids(self):
        buf = make_buf("client server client")
        off = np.array([0, 7, 14], dtype=np.int64)
        ln = np.array([6, 6, 6], dtype=np.int64)
        table = make_table()
        tracker = make_tracker(buf)
        out = np.zeros(3, dtype=np.int64)

        nb_resolve_names_batch(buf, off, ln, 3, table.bundle(), tracker, out)

        assert out[0] == out[2]  # same name within the batch -> same temp id
        assert out[0] != out[1]
        assert out[0] >= MODULE_TEMP_ID_BASE
        assert tracker.count[0] == 2

    def test_known_names_resolve_directly_without_a_temp_id(self):
        buf = make_buf("client server")
        off = np.array([0, 7], dtype=np.int64)
        ln = np.array([6, 6], dtype=np.int64)
        table = make_table()
        table.register_name(0, "client")
        table.register_name(1, "server")
        tracker = make_tracker(buf)
        out = np.zeros(2, dtype=np.int64)

        nb_resolve_names_batch(buf, off, ln, 2, table.bundle(), tracker, out)

        assert list(out) == [0, 1]
        assert tracker.count[0] == 0

    def test_empty_name_returns_unknown_sentinel(self):
        buf = make_buf("x")
        off = np.array([0], dtype=np.int64)
        ln = np.array([0], dtype=np.int64)
        table = make_table()
        tracker = make_tracker(buf)
        out = np.zeros(1, dtype=np.int64)

        nb_resolve_names_batch(buf, off, ln, 1, table.bundle(), tracker, out)

        assert out[0] == MODULE_ID_UNKNOWN

    def test_tracker_full_returns_full_sentinel(self):
        buf = make_buf("aaa bbb ccc")
        off = np.array([0, 4, 8], dtype=np.int64)
        ln = np.array([3, 3, 3], dtype=np.int64)
        table = make_table()
        tracker = make_tracker(buf, capacity=1)  # room for exactly one distinct unresolved name
        out = np.zeros(3, dtype=np.int64)

        nb_resolve_names_batch(buf, off, ln, 3, table.bundle(), tracker, out)

        assert out[0] >= MODULE_TEMP_ID_BASE
        assert out[0] < MODULE_ID_FULL
        assert out[1] == MODULE_ID_FULL
        assert out[2] == MODULE_ID_FULL


class TestResolveScopedId:
    """Module-style (per-device-scoped) resolution - the same name string under two different
    device ids must never collapse into the same id."""

    def test_same_name_different_devices_get_distinct_temp_ids(self):
        buf = make_buf("log log")
        table = make_table(use_values=True)
        tracker = make_tracker(buf)

        id_a = nb_resolve_scoped_id(buf, 0, 3, 0, table.bundle(), tracker)
        id_b = nb_resolve_scoped_id(buf, 4, 3, 1, table.bundle(), tracker)

        assert id_a != id_b
        assert id_a >= MODULE_TEMP_ID_BASE and id_b >= MODULE_TEMP_ID_BASE

    def test_same_name_same_device_dedupes_to_one_temp_id(self):
        buf = make_buf("log log")
        table = make_table(use_values=True)
        tracker = make_tracker(buf)

        id_a = nb_resolve_scoped_id(buf, 0, 3, 0, table.bundle(), tracker)
        id_b = nb_resolve_scoped_id(buf, 4, 3, 0, table.bundle(), tracker)

        assert id_a == id_b
        assert tracker.count[0] == 1

    def test_permanent_table_scoped_lookup_skips_wrong_device_entry(self):
        buf = make_buf("log")
        table = make_table(use_values=True)
        # Two devices' "log" module land at different ids but (absent scoping) could hash to
        # the same bucket/collide on name+length - registered here to exercise the values[]
        # disambiguation check, not just the tracker's mixed-hash path.
        table.register_name(10, "log", value=0)
        table.register_name(11, "log", value=1)
        tracker = make_tracker(buf)

        resolved_for_device_0 = nb_resolve_scoped_id(buf, 0, 3, 0, table.bundle(), tracker)
        resolved_for_device_1 = nb_resolve_scoped_id(buf, 0, 3, 1, table.bundle(), tracker)

        assert resolved_for_device_0 == 10
        assert resolved_for_device_1 == 11

    def test_empty_name_returns_unknown_sentinel(self):
        buf = make_buf("x")
        table = make_table(use_values=True)
        tracker = make_tracker(buf)

        assert nb_resolve_scoped_id(buf, 0, 0, 0, table.bundle(), tracker) == MODULE_ID_UNKNOWN


class TestResolveScopedNamesBatch:
    def test_batch_scoping_matches_row_by_row(self):
        buf = make_buf("log log")
        off = np.array([0, 4], dtype=np.int64)
        ln = np.array([3, 3], dtype=np.int64)
        device_ids = np.array([0, 1], dtype=np.int64)
        table = make_table(use_values=True)
        table.register_name(10, "log", value=0)
        table.register_name(11, "log", value=1)
        tracker = make_tracker(buf)
        out = np.zeros(2, dtype=np.int64)

        nb_resolve_scoped_names_batch(buf, off, ln, device_ids, 2, table.bundle(), tracker, out)

        assert list(out) == [10, 11]

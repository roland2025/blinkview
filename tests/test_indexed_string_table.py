# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.id_registry.tables import IndexedStringTable


class TestRegisterAndGetString:
    @pytest.mark.parametrize("use_hashes", [True, False])
    def test_roundtrip_single_entry(self, use_hashes):
        table = IndexedStringTable(initial_capacity=4, use_hashes=use_hashes)
        table.register_name(0, "hello")
        assert table.get_string(0) == "hello"
        assert table[0] == "hello"

    @pytest.mark.parametrize("use_hashes", [True, False])
    def test_multiple_entries_are_byte_accurate(self, use_hashes):
        table = IndexedStringTable(initial_capacity=4, use_hashes=use_hashes)
        names = ["wifi", "wifi.tx", "wifi.rx", "ble"]
        for i, name in enumerate(names):
            table.register_name(i, name)

        for i, name in enumerate(names):
            assert table.get_string(i) == name

    def test_get_string_out_of_range_returns_empty(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "a")
        assert table.get_string(-1) == ""
        assert table.get_string(5) == ""

    def test_empty_name_roundtrips(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "")
        assert table.get_string(0) == ""

    def test_unicode_name_roundtrips(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "tempé°C")
        assert table.get_string(0) == "tempé°C"


class TestCapacityGrowth:
    def test_registering_beyond_initial_capacity_grows_and_preserves_data(self):
        table = IndexedStringTable(initial_capacity=1, use_hashes=True)
        table.register_name(0, "first")
        table.register_name(1, "second")  # id >= initial_capacity(1) forces a resize
        table.register_name(5, "sixth")  # jumps ahead, forces another resize

        assert table.get_string(0) == "first"
        assert table.get_string(1) == "second"
        assert table.get_string(5) == "sixth"

    def test_registering_long_name_grows_the_byte_buffer(self):
        table = IndexedStringTable(initial_capacity=4, buffer_size_bytes=4)
        long_name = "x" * 100
        table.register_name(0, long_name)
        assert table.get_string(0) == long_name

    def test_growth_rebuilds_hash_index_lookup_still_works(self):
        table = IndexedStringTable(initial_capacity=1, use_hashes=True)
        for i in range(10):
            table.register_name(i, f"mod_{i}")

        for i in range(10):
            assert table.get_string(i) == f"mod_{i}"


class TestActiveIds:
    def test_get_active_ids_reflects_registered_entries(self):
        table = IndexedStringTable(initial_capacity=8)
        table.register_name(1, "a")
        table.register_name(3, "b")

        active = table.get_active_ids()
        assert sorted(active.tolist()) == [1, 3]

    def test_get_active_ids_empty_table(self):
        table = IndexedStringTable(initial_capacity=8)
        assert table.get_active_ids().tolist() == []


class TestValues:
    def test_values_dtype_stores_and_bundles_values(self):
        table = IndexedStringTable(initial_capacity=4, values_dtype=dtypes.VALUES_TYPE)
        table.register_name(0, "warn", value=30)
        table.register_name(1, "error", value=40)

        bundle = table.bundle()
        assert int(bundle.values[0]) == 30
        assert int(bundle.values[1]) == 40

    def test_no_values_dtype_yields_empty_values_array(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "a")
        bundle = table.bundle()
        assert len(bundle.values) == 0


class TestBundle:
    def test_bundle_is_cached_until_mutation(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "a")

        b1 = table.bundle()
        b2 = table.bundle()
        assert b1 is b2

        table.register_name(1, "b")
        b3 = table.bundle()
        assert b3 is not b1

    def test_bundle_reflects_count_and_buffer_contents(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "a")
        table.register_name(1, "bb")

        bundle = table.bundle()
        assert bundle.count == 2
        off0, len0 = int(bundle.offsets[0]), int(bundle.lens[0])
        off1, len1 = int(bundle.offsets[1]), int(bundle.lens[1])
        assert bundle.buffer[off0 : off0 + len0].tobytes() == b"a"
        assert bundle.buffer[off1 : off1 + len1].tobytes() == b"bb"

    def test_use_hashes_false_yields_empty_hash_index(self):
        table = IndexedStringTable(initial_capacity=4, use_hashes=False)
        table.register_name(0, "a")
        bundle = table.bundle()
        assert len(bundle.hash_index) == 0


class TestRelease:
    def test_release_clears_internal_arrays(self):
        table = IndexedStringTable(initial_capacity=4)
        table.register_name(0, "a")
        table.release()

        assert table._buffer is None
        assert table._offsets is None
        assert table._bundle is None


def test_warmup_smoke():
    # IndexedStringTable.warmup builds its own tables internally rather than reading anything
    # off the passed helper, so it can be called directly (mirrors LogSegmentScanner.warmup
    # smoke-testing in tests/test_log_fetch.py).
    IndexedStringTable.warmup(None)

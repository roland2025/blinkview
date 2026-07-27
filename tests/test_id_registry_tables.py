# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.id_registry.tables import IndexedStringTable


class TestRegisterAndGetString:
    def test_round_trips_a_single_name(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "hello")
        assert table.get_string(0) == "hello"

    def test_multiple_names_stay_independently_addressable(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "alpha")
        table.register_name(1, "beta")
        table.register_name(2, "gamma")

        assert table.get_string(0) == "alpha"
        assert table.get_string(1) == "beta"
        assert table.get_string(2) == "gamma"

    def test_getitem_is_sugar_for_get_string(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "hello")
        assert table[0] == "hello"

    def test_out_of_range_id_returns_empty_string(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "hello")
        assert table.get_string(5) == ""
        assert table.get_string(-1) == ""


class TestGrowth:
    def test_registering_beyond_initial_capacity_grows_metadata_arrays(self):
        table = IndexedStringTable(initial_capacity=1, buffer_size_bytes=256, use_hashes=True)
        table.register_name(0, "a")
        table.register_name(5, "far-beyond-capacity")

        assert table.get_string(0) == "a"
        assert table.get_string(5) == "far-beyond-capacity"

    def test_registering_beyond_initial_buffer_grows_the_byte_buffer(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=4)
        long_name = "x" * 100
        table.register_name(0, long_name)
        assert table.get_string(0) == long_name

    def test_growth_works_without_hashes_too(self):
        table = IndexedStringTable(initial_capacity=1, buffer_size_bytes=4, use_hashes=False)
        table.register_name(0, "a")
        table.register_name(3, "grown-without-index")

        assert table.get_string(0) == "a"
        assert table.get_string(3) == "grown-without-index"


class TestValues:
    def test_stores_and_exposes_associated_values(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256, values_dtype=np.int32)
        table.register_name(0, "a", value=42)
        table.register_name(1, "b", value=99)

        assert table._values[0] == 42
        assert table._values[1] == 99

    def test_no_values_dtype_means_no_values_array(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "a", value=42)
        assert table._values is None


class TestActiveIds:
    def test_get_active_ids_returns_registered_ids(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "a")
        table.register_name(2, "c")

        active = table.get_active_ids()
        assert set(active.tolist()) == {0, 2}

    def test_get_active_ids_empty_when_nothing_registered(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        assert table.get_active_ids().size == 0


class TestBundle:
    def test_bundle_is_cached_until_mutated(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "a")

        first = table.bundle()
        second = table.bundle()
        assert first is second

    def test_bundle_is_invalidated_after_a_new_registration(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "a")
        first = table.bundle()

        table.register_name(1, "b")
        second = table.bundle()

        assert first is not second
        assert second.count == 2

    def test_bundle_reflects_the_current_count(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "a")
        table.register_name(1, "b")

        assert table.bundle().count == 2


class TestRelease:
    def test_release_clears_internal_references(self):
        table = IndexedStringTable(initial_capacity=8, buffer_size_bytes=256)
        table.register_name(0, "a")

        table.release()

        assert table._buffer is None
        assert table._offsets is None
        assert table._lens is None
        assert table._hashes is None
        assert table._values is None
        assert table._bundle is None

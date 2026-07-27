# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.utils.dict_utils import get_by_path


class TestPathTraversal:
    def test_empty_path_returns_the_whole_document(self):
        data = {"a": 1}
        assert get_by_path(data, "") is data

    def test_root_slash_returns_the_whole_document(self):
        data = {"a": 1}
        assert get_by_path(data, "/") is data

    def test_traverses_nested_dict_keys(self):
        data = {"devices": {"ABC": {"device": "value"}}}
        assert get_by_path(data, "/devices/ABC/device") == "value"

    def test_traverses_list_indices(self):
        data = {"items": ["a", "b", "c"]}
        assert get_by_path(data, "/items/1") == "b"

    def test_missing_dict_key_returns_default(self):
        data = {"a": 1}
        assert get_by_path(data, "/missing", default="fallback") == "fallback"

    def test_out_of_range_list_index_returns_default(self):
        data = {"items": ["a"]}
        assert get_by_path(data, "/items/5", default="fallback") == "fallback"

    def test_non_numeric_list_index_returns_default(self):
        data = {"items": ["a"]}
        assert get_by_path(data, "/items/notanumber", default="fallback") == "fallback"

    def test_traversing_into_a_primitive_returns_default(self):
        data = {"a": 1}
        assert get_by_path(data, "/a/b", default="fallback") == "fallback"

    def test_default_defaults_to_none(self):
        data = {}
        assert get_by_path(data, "/missing") is None


class TestDropKeys:
    def test_drops_specified_top_level_keys(self):
        data = {"keep": 1, "drop": 2}
        result = get_by_path(data, "", drop_keys=["drop"])
        assert result == {"keep": 1}

    def test_returns_shallow_copy_not_the_original(self):
        data = {"keep": 1, "drop": 2}
        result = get_by_path(data, "", drop_keys=["drop"])
        assert result is not data

    def test_drop_keys_on_a_list_result_is_a_noop(self):
        data = {"items": [1, 2, 3]}
        result = get_by_path(data, "/items", drop_keys=["drop"])
        assert result == [1, 2, 3]


class TestDepthLimiting:
    def test_depth_zero_hollows_out_the_top_level_container_itself(self):
        data = {"a": {"b": 1}, "c": [1, 2]}
        result = get_by_path(data, "", depth=0)
        assert result == {}

    def test_depth_limits_nested_dicts_but_keeps_primitives(self):
        data = {"a": {"b": {"c": 1}}}
        result = get_by_path(data, "", depth=1)
        assert result == {"a": {}}

    def test_depth_deep_enough_keeps_everything(self):
        data = {"a": {"b": {"c": 1}}}
        result = get_by_path(data, "", depth=10)
        assert result == {"a": {"b": {"c": 1}}}

    def test_depth_limiting_returns_a_new_structure_not_the_original(self):
        data = {"a": {"b": 1}}
        result = get_by_path(data, "", depth=5)
        assert result is not data
        assert result["a"] is not data["a"]


class TestDeepCopy:
    def test_make_deep_copy_returns_an_independent_clone(self):
        data = {"a": {"b": [1, 2, 3]}}
        result = get_by_path(data, "/a", make_deep_copy=True)

        assert result == {"b": [1, 2, 3]}
        result["b"].append(4)
        assert data["a"]["b"] == [1, 2, 3]

    def test_without_make_deep_copy_mutations_propagate(self):
        data = {"a": {"b": [1, 2, 3]}}
        result = get_by_path(data, "/a")

        result["b"].append(4)
        assert data["a"]["b"] == [1, 2, 3, 4]

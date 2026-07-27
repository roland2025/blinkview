# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.utils.config_node import ConfigNode


class FakeManager:
    def __init__(self):
        self.created = []  # (path, name, drop_keys, editable, depth)
        self.shown = []
        self.factory_types_map = {}
        self.factory_schema_map = {}
        self.broadcast_deletions = []

    def create_node(self, path, name=None, drop_keys=None, editable=True, depth=None):
        self.created.append((path, name, drop_keys, editable, depth))
        return ConfigNode(self, path, name, drop_keys=drop_keys, depth=depth)

    def show(self, path, child_name=None):
        self.shown.append((path, child_name))

    def get_factory_types(self, category):
        return self.factory_types_map.get(category, [])

    def get_factory_schema(self, category, type_name):
        return self.factory_schema_map.get((category, type_name), {})

    def broadcast_deletion(self, path):
        self.broadcast_deletions.append(path)


def make_node(qapp, path="/devices/abc", **kwargs):
    manager = FakeManager()
    node = ConfigNode(manager, path, **kwargs)
    return node, manager


class TestCreateChild:
    def test_joins_paths_without_double_slashes(self, qapp):
        node, manager = make_node(qapp, path="/devices")
        node.create_child("ABC")
        assert manager.created[0][0] == "/devices/ABC"

    def test_strips_leading_slash_on_relative_path(self, qapp):
        node, manager = make_node(qapp, path="/devices")
        node.create_child("/ABC")
        assert manager.created[0][0] == "/devices/ABC"

    def test_from_root_path(self, qapp):
        node, manager = make_node(qapp, path="")
        node.create_child("ABC")
        assert manager.created[0][0] == "/ABC"


class TestCreateAbsolute:
    def test_delegates_directly_to_manager(self, qapp):
        node, manager = make_node(qapp)
        node.create_absolute("/other/path", name="Other")
        assert manager.created[0] == ("/other/path", "Other", None, True, None)


class TestRecvConfigSchema:
    def test_exact_match_updates_config_and_schema_and_emits(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        received = []
        node.signal_received.connect(lambda cfg, schema: received.append((cfg, schema)))

        node.recv_config_schema("/devices/abc", {"a": 1}, {"properties": {"a": {"type": "integer"}}})

        assert node.config == {"a": 1}
        assert node.schema == {"properties": {"a": {"type": "integer"}}}
        assert received == [({"a": 1}, {"properties": {"a": {"type": "integer"}}})]

    def test_drop_keys_prune_both_config_and_schema(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc", drop_keys=["secret"])

        node.recv_config_schema(
            "/devices/abc",
            {"a": 1, "secret": "hidden"},
            {"properties": {"a": {"type": "integer"}, "secret": {"type": "string"}}, "required": ["secret"]},
        )

        assert "secret" not in node.config
        assert "secret" not in node.schema["properties"]
        assert "secret" not in node.schema.get("required", [])

    def test_depth_prunes_nested_schema_properties(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc", depth=1)

        deep_schema = {
            "properties": {
                "a": {"type": "object", "properties": {"b": {"type": "object", "properties": {"c": {"type": "string"}}}}}
            }
        }
        node.recv_config_schema("/devices/abc", {}, deep_schema)

        # depth=1: top level (0) keeps properties, level-1 node "a" is at current_depth=1 >=
        # max_depth=1, so ITS "properties" get stripped.
        assert "properties" not in node.schema["properties"]["a"]

    def test_none_config_and_schema_default_to_empty(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")

        node.recv_config_schema("/devices/abc", None, None)

        assert node.config == {}
        assert node.schema == {}

    def test_child_path_change_triggers_refetch(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        fetch_calls = []
        node.get_fn = lambda path: fetch_calls.append(path)

        node.recv_config_schema("/devices/abc/port", {}, {})

        assert fetch_calls == ["/devices/abc"]

    def test_parent_path_change_triggers_refetch(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        fetch_calls = []
        node.get_fn = lambda path: fetch_calls.append(path)

        node.recv_config_schema("/devices", {}, {})

        assert fetch_calls == ["/devices/abc"]

    def test_root_update_always_triggers_refetch(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        fetch_calls = []
        node.get_fn = lambda path: fetch_calls.append(path)

        node.recv_config_schema("/", {}, {})

        assert fetch_calls == ["/devices/abc"]

    def test_unrelated_sibling_path_does_not_trigger_refetch(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        fetch_calls = []
        node.get_fn = lambda path: fetch_calls.append(path)

        node.recv_config_schema("/devices/xyz", {}, {})

        assert fetch_calls == []


class TestSendAndFetch:
    def test_send_calls_send_fn_with_path_and_patch(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        calls = []
        node.send_fn = lambda path, patch: calls.append((path, patch))

        node.send([{"op": "replace", "path": "/x", "value": 1}])

        assert calls == [("/devices/abc", [{"op": "replace", "path": "/x", "value": 1}])]

    def test_send_is_a_noop_without_send_fn(self, qapp):
        node, _manager = make_node(qapp)
        node.send()  # must not raise

    def test_fetch_calls_get_fn_with_active_path(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        calls = []
        node.get_fn = lambda path: calls.append(path)

        node.fetch()

        assert calls == ["/devices/abc"]

    def test_update_path_mutates_active_path(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        node.update_path("/devices/xyz")
        assert node.active_path == "/devices/xyz"


class TestDeregisterAndDeletion:
    def test_deregister_emits_signal_unregister(self, qapp):
        node, _manager = make_node(qapp)
        received = []
        node.signal_unregister.connect(lambda n: received.append(n))

        node.deregister()

        assert received == [node]

    def test_handle_deletion_emits_signal_deleted_and_deregisters(self, qapp):
        node, _manager = make_node(qapp)
        deleted_received = []
        unregister_received = []
        node.signal_deleted.connect(lambda: deleted_received.append(True))
        node.signal_unregister.connect(lambda n: unregister_received.append(n))

        node.handle_deletion()

        assert deleted_received == [True]
        assert unregister_received == [node]

    def test_delete_sends_remove_patch_and_broadcasts(self, qapp):
        node, manager = make_node(qapp, path="/devices/abc")
        calls = []
        node.send_fn = lambda path, patch: calls.append((path, patch))

        node.delete()

        assert calls == [("/devices/abc", [{"op": "remove", "path": ""}])]
        assert manager.broadcast_deletions == ["/devices/abc"]

    def test_delete_refuses_to_delete_the_root(self, qapp):
        node, manager = make_node(qapp, path="/")
        calls = []
        node.send_fn = lambda path, patch: calls.append((path, patch))

        node.delete()

        assert calls == []
        assert manager.broadcast_deletions == []


class TestGetters:
    def test_get_returns_whole_config_without_key(self, qapp):
        node, _manager = make_node(qapp)
        node.config = {"a": 1}
        assert node.get() == {"a": 1}

    def test_get_returns_default_for_missing_key(self, qapp):
        node, _manager = make_node(qapp)
        node.config = {"a": 1}
        assert node.get("missing", "fallback") == "fallback"

    def test_get_copy_returns_a_deep_copy(self, qapp):
        node, _manager = make_node(qapp)
        node.config = {"a": {"nested": 1}}

        copy = node.get_copy()
        copy["a"]["nested"] = 2

        assert node.config["a"]["nested"] == 1


class TestShowAndFactories:
    def test_show_delegates_to_manager_with_child_path(self, qapp):
        node, manager = make_node(qapp, path="/devices/abc", name="ABC")
        node.show("port")
        assert manager.shown == [("/devices/abc/port", "ABC")]

    def test_show_without_child_path_uses_own_path(self, qapp):
        node, manager = make_node(qapp, path="/devices/abc", name="ABC")
        node.show()
        assert manager.shown == [("/devices/abc", "ABC")]

    def test_factory_types_delegates_to_manager(self, qapp):
        node, manager = make_node(qapp)
        manager.factory_types_map["can"] = [("cantools", "desc")]
        assert node.factory_types("can") == [("cantools", "desc")]

    def test_factory_schema_delegates_to_manager(self, qapp):
        node, manager = make_node(qapp)
        manager.factory_schema_map[("can", "cantools")] = {"type": "object"}
        assert node.factory_schema("can", "cantools") == {"type": "object"}


class TestSendConfig:
    def test_computes_a_json_patch_diff_and_sends_it(self, qapp):
        node, _manager = make_node(qapp, path="/devices/abc")
        node.config = {"a": 1}
        calls = []
        node.send_fn = lambda path, patch: calls.append((path, patch))

        node.send_config({"a": 2})

        assert calls == [("/devices/abc", [{"op": "replace", "path": "/a", "value": 2}])]
        assert node.config == {"a": 2}

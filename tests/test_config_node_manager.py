# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import pytest

from blinkview.ui.utils.config_node import ConfigNode
from blinkview.ui.utils.config_node_manager import ConfigNodeManager


class FakeBackendConfigManager:
    """Stands in for core.config_manager.ConfigManager - only the surface
    ConfigNodeManager actually touches."""

    def __init__(self):
        self.config_changed_cb = None
        self.get_config_schema_calls = []
        self.get_config_schema_result = ({}, {})
        self.apply_patch_calls = []

    def get_config_schema(self, path, drop_keys=None, editable=True):
        self.get_config_schema_calls.append((path, drop_keys, editable))
        return self.get_config_schema_result

    def apply_patch(self, path, patch):
        self.apply_patch_calls.append((path, patch))


class FakeGuiContext:
    def __init__(self):
        self.run_task_calls = []
        self.create_widget_calls = []
        self.reference_values_map = {}
        self.registry = SimpleNamespace(
            system_ctx=SimpleNamespace(
                tasks=SimpleNamespace(run_task=self._run_task),
                factories=SimpleNamespace(
                    get_category_types=lambda category: [("t", "desc")],
                    get_factory=lambda category: SimpleNamespace(get_schema=lambda type_name: {"schema": type_name}),
                ),
            ),
            get_reference_values=lambda name: self.reference_values_map.get(name, []),
        )

    def _run_task(self, fn, *args):
        # Run synchronously (real TaskManager runs it on a worker thread; tests don't need that).
        self.run_task_calls.append((fn, args))
        return fn(*args)

    def create_widget(self, cls_name, title, as_window=False, params=None, **kwargs):
        self.create_widget_calls.append((cls_name, title, as_window, params))


@pytest.fixture
def gui_context():
    return FakeGuiContext()


@pytest.fixture
def backend():
    return FakeBackendConfigManager()


@pytest.fixture
def manager(qapp, gui_context, backend):
    return ConfigNodeManager(gui_context, config_manager=backend)


class TestInit:
    def test_wires_backend_config_changed_callback_to_the_signal(self, manager, backend):
        assert backend.config_changed_cb == manager.signal_received_config_schema.emit

    def test_starts_with_no_nodes(self, manager):
        assert manager.nodes == []


class TestCreateNode:
    def test_returns_a_config_node_for_the_given_path(self, manager):
        node = manager.create_node("/devices/abc")
        assert isinstance(node, ConfigNode)
        assert node.active_path == "/devices/abc"

    def test_appends_the_node_to_the_tracked_list(self, manager):
        node = manager.create_node("/devices/abc")
        assert node in manager.nodes

    def test_wires_get_fn_to_fetch_via_the_backend(self, manager, backend, qtbot):
        backend.get_config_schema_result = ({"a": 1}, {"properties": {}})
        node = manager.create_node("/devices/abc")

        node.fetch()

        assert backend.get_config_schema_calls == [("/devices/abc", None, True)]
        assert node.config == {"a": 1}

    def test_wires_send_fn_to_apply_patch_via_the_backend(self, manager, backend):
        node = manager.create_node("/devices/abc")

        node.send([{"op": "replace", "path": "/x", "value": 1}])

        assert backend.apply_patch_calls == [("/devices/abc", [{"op": "replace", "path": "/x", "value": 1}])]

    def test_schedules_an_initial_fetch_on_the_event_loop(self, manager, backend, qtbot):
        manager.create_node("/devices/abc")

        qtbot.waitUntil(lambda: len(backend.get_config_schema_calls) == 1, timeout=1000)

        assert backend.get_config_schema_calls == [("/devices/abc", None, True)]

    def test_signal_unregister_is_wired_to_deregister_node(self, manager):
        node = manager.create_node("/devices/abc")

        node.deregister()

        assert node not in manager.nodes


class TestDeregisterNode:
    def test_removes_a_tracked_node(self, manager):
        node = manager.create_node("/devices/abc")
        manager.deregister_node(node)
        assert node not in manager.nodes

    def test_is_a_noop_for_an_unknown_node(self, manager):
        node = manager.create_node("/devices/abc")
        manager.deregister_node(node)  # already removed by nothing yet - just ensure no raise
        manager.deregister_node(node)  # second call: definitely not tracked anymore


class TestBroadcast:
    def test_forwards_updates_to_every_tracked_node(self, manager):
        node_a = manager.create_node("/devices/a")
        node_b = manager.create_node("/devices/b")

        manager._broadcast("/devices/a", {"x": 1}, {"properties": {}})

        assert node_a.config == {"x": 1}
        assert node_b.config == {}  # unrelated path, untouched

    def test_exception_in_one_node_does_not_block_others(self, manager):
        node_a = manager.create_node("/devices/a")
        node_b = manager.create_node("/devices/b")

        def boom(path, config, schema):
            raise RuntimeError("kaboom")

        node_a.recv_config_schema = boom

        manager._broadcast("/devices/b", {"y": 2}, {"properties": {}})

        assert node_b.config == {"y": 2}


class TestShow:
    def test_delegates_to_gui_context_create_widget(self, manager, gui_context):
        manager.show("/devices/abc", child_name="ABC", drop_keys=["secret"], editable=False)

        assert gui_context.create_widget_calls == [
            (
                "DynamicConfigWidget",
                "Settings: ABC",
                False,
                {"drop_keys": ["secret"], "editable": False, "path": "/devices/abc"},
            )
        ]

    def test_falls_back_to_path_in_title_without_child_name(self, manager, gui_context):
        manager.show("/devices/abc")
        assert gui_context.create_widget_calls[0][1] == "Settings: /devices/abc"


class TestFactoryAndReferenceDelegation:
    def test_get_factory_types_delegates_to_registry_factories(self, manager):
        assert manager.get_factory_types("can") == [("t", "desc")]

    def test_get_factory_schema_delegates_to_registry_factories(self, manager):
        assert manager.get_factory_schema("can", "cantools") == {"schema": "cantools"}

    def test_get_reference_values_delegates_to_registry(self, manager, gui_context):
        gui_context.reference_values_map["/sources"] = ["a", "b"]
        assert manager.get_reference_values("/sources") == ["a", "b"]


class TestBroadcastDeletion:
    def test_calls_handle_deletion_on_exact_match(self, manager):
        node = manager.create_node("/devices/abc")

        manager.broadcast_deletion("/devices/abc")

        assert node not in manager.nodes  # handle_deletion -> deregister()

    def test_calls_handle_deletion_on_child_nodes(self, manager):
        node = manager.create_node("/devices/abc/port")

        manager.broadcast_deletion("/devices/abc")

        assert node not in manager.nodes

    def test_does_not_touch_unrelated_nodes(self, manager):
        node = manager.create_node("/devices/xyz")

        manager.broadcast_deletion("/devices/abc")

        assert node in manager.nodes

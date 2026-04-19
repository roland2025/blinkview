# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any

from qtpy.QtCore import QObject, QTimer, Signal, Slot

from blinkview.core.config_manager import ConfigManager
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.config_node import ConfigNode


class ConfigNodeManager(QObject):
    """
    The Global Configuration Hub.
    Bridges the Backend Registry with the UI ConfigNodes.
    """

    # Global bus for registry updates
    signal_received_config_schema = Signal(str, object, object)

    def __init__(self, context: GUIContext, config_manager=None, parent=None):
        super().__init__(parent)
        self.gui_context = context
        self.manager: ConfigManager = config_manager or self.gui_context.registry.config

        self.signal_received_config_schema.connect(self._broadcast)

        self.manager.config_changed_cb = (
            self.signal_received_config_schema.emit
        )  # Redirect backend updates to the signal

        self.nodes = []

    def create_node(
        self,
        path: str,
        name: str = None,
        drop_keys: list = None,
        editable: bool = True,
        depth: int = None,
        on_update=None,
    ) -> ConfigNode:
        """Creates a ConfigNode and wires it securely to the backend registry."""
        print(f"[ConfigManager] Creating node for {path}")
        node = ConfigNode(self, path, name, drop_keys=drop_keys, depth=depth, on_update=on_update)

        # Extract backend functions
        run_task = self.gui_context.registry.system_ctx.tasks.run_task

        get_config_schema = self.manager.get_config_schema
        set_config = self.manager.apply_patch

        # Wire the GET function
        def fetch(path_):
            try:
                print(f"[ConfigManager] Fetching '{path_}'")
                config, schema = get_config_schema(path_, drop_keys=drop_keys, editable=editable)
                # Feed the result directly back into the node
                node.recv_config_schema(path_, config, schema)
            except Exception as e:
                print(f"[ConfigManager] Fetching '{path_}' failed: {e}")

        node.get_fn = lambda path_: run_task(fetch, path_)

        # Wire the SEND function
        node.send_fn = lambda path_, patch_: run_task(set_config, path_, patch_)

        # Wire the cleanup lifecycle
        node.signal_unregister.connect(self.deregister_node)

        self.nodes.append(node)

        QTimer.singleShot(0, node.fetch)

        return node

    @Slot(object)
    def deregister_node(self, node: ConfigNode):
        """Removes a node when its UI component closes."""
        print(f"[ConfigManager] Deregistering node for {node.active_path}")
        if node in self.nodes:
            self.nodes.remove(node)

    @Slot(str, Any, dict)
    def _broadcast(self, path: str, config: dict, schema: dict):
        """
        Takes global updates from the backend and pushes them to all active nodes.
        Nodes will automatically ignore updates that don't match their active_path.
        """
        for node in self.nodes:
            try:
                node.recv_config_schema(path, config, schema)
            except Exception as e:
                print(f"[ConfigManager] Broadcasting '{path}' failed: {e}")

    def show(self, path: str, child_name=None, drop_keys: list = None, editable: bool = True):
        print(
            f"[ConfigManager] Request to show config for '{path}' with name='{child_name}', drop_keys={drop_keys}, editable={editable}"
        )
        print(f"context: {self.gui_context}, create_widget: {self.gui_context.create_widget}")
        self.gui_context.create_widget(
            "DynamicConfigWidget",
            f"Settings: {child_name or path}",
            False,
            params={"drop_keys": drop_keys, "editable": editable, "path": path},
        )

    def get_factory_types(self, category: str) -> list[tuple[str, str]]:
        return self.gui_context.registry.system_ctx.factories.get_category_types(category)

    def get_factory_schema(self, category: str, type_name: str) -> dict:
        return self.gui_context.registry.system_ctx.factories.get_factory(category).get_schema(type_name)

    def get_reference_values(self, ref_name: str) -> list:
        return self.gui_context.registry.get_reference_values(ref_name)

    def broadcast_deletion(self, deleted_path: str):
        """Notifies all nodes on this path (or its children) that the config was deleted."""
        print(f"[ConfigManager] Broadcasting deletion for '{deleted_path}'")

        # Ensure trailing slash to prevent substring false-positives (e.g., "/dev/A" vs "/dev/ABC")
        deleted_slashed = deleted_path if deleted_path.endswith("/") else deleted_path + "/"

        # IMPORTANT: Iterate over a copy of the list (list(self.nodes))
        # because nodes will call deregister() and remove themselves during the loop!
        for node in list(self.nodes):
            is_exact_match = node.active_path == deleted_path
            is_child = node.active_path.startswith(deleted_slashed)

            if is_exact_match or is_child:
                print(f"[ConfigManager] Triggering deletion lifecycle for node '{node.active_path}'")
                node.handle_deletion()

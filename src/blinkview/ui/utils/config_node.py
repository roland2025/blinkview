# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import weakref
from copy import deepcopy
from typing import Any

from qtpy.QtCore import QObject, Signal


class ConfigNode(QObject):
    signal_received = Signal(object, object)
    signal_unregister = Signal(object)
    signal_deleted = Signal()  # Widgets will connect to this to close themselves

    def __init__(
        self,
        manager,
        active_path: str,
        name: str = None,
        drop_keys: list = None,
        depth: int = None,
        on_update=None,
        parent=None,
    ):
        super().__init__(parent)

        # Safely hold a reference back to the ConfigManager
        self.manager = weakref.proxy(manager)

        self.name = name or active_path
        self.active_path = active_path

        self.drop_keys = drop_keys or []
        self.depth = depth

        self._last_config = None
        self._last_schema = None

        self.send_fn = None
        self.get_fn = None

        self.config = {}
        self.schema = {}

        if on_update is not None:
            self.on_update(on_update)

    def on_update(self, callback):
        """Registers a callback to be called whenever this node receives new config/schema data."""
        self.signal_received.connect(callback)

    def create_child(
        self,
        relative_path: str,
        name: str = None,
        drop_keys: list = None,
        editable: bool = True,
        depth: int = None,
    ) -> "ConfigNode":
        """
        Creates a new ConfigNode branching off this node's path.
        Example: If this node is "/devices", create_child("ABC") creates "/devices/ABC"
        """
        # Safely join the paths (prevents double slashes like //devices//ABC)
        clean_base = self.active_path.rstrip("/")
        clean_rel = relative_path.lstrip("/")
        full_path = f"{clean_base}/{clean_rel}" if clean_base else f"/{clean_rel}"

        # Ask the manager to construct and wire up the new node
        return self.manager.create_node(full_path, name, drop_keys, editable, depth)

    def create_absolute(
        self,
        absolute_path: str,
        name: str = None,
        drop_keys: list = None,
        editable: bool = True,
        depth: int = None,
    ):
        """Creates a new ConfigNode anywhere in the global tree."""
        return self.manager.create_node(absolute_path, name, drop_keys, editable, depth)

    def recv_config_schema(self, path: str, config: Any, schema: dict):
        """
        Receives broadcasted config/schema data.
        If exact match: updates immediately.
        If parent or child match: triggers a fresh fetch to stay in sync.
        """
        if path == self.active_path:
            from blinkview.utils.dict_utils import (
                get_by_path,  # Adjust your import path!
            )

            # Prune the DATA using your awesome utility function!
            # We pass path="/" because we are already at the exact node data.
            pruned_config = get_by_path(
                data=config if config else {},
                path="/",
                drop_keys=self.drop_keys,
                depth=self.depth,
                make_deep_copy=True,
            )

            # Prune the SCHEMA using custom JSON-Schema-aware logic
            pruned_schema = deepcopy(schema) if schema else {}

            if self.drop_keys and isinstance(pruned_schema, dict) and "properties" in pruned_schema:
                for k in self.drop_keys:
                    pruned_schema["properties"].pop(k, None)
                    if "required" in pruned_schema and k in pruned_schema["required"]:
                        pruned_schema["required"].remove(k)

            if self.depth is not None:
                pruned_schema = self._prune_schema_depth(pruned_schema, 0, self.depth)

            self.config = pruned_config
            self.schema = pruned_schema
            # print(f"[ConfigNode] Received update for '{path}' Config: {json.dumps(pruned_config)} Schema: {json.dumps(pruned_schema)}")

            self.signal_received.emit(self.config, self.schema)
            return

        # Check for Parent or Child overlaps
        # Ensure trailing slashes to prevent substring false-positives
        # (e.g., "/devices/A" vs "/devices/ABC")
        my_path_slashed = self.active_path if self.active_path.endswith("/") else self.active_path + "/"
        incoming_slashed = path if path.endswith("/") else path + "/"

        is_root = self.active_path == "/"
        incoming_is_root = path == "/"

        # Did a child of this node change? (e.g., I am /devices/ABC, update is /devices/ABC/port)
        is_child_updated = path.startswith(my_path_slashed) or is_root

        # Did a parent of this node change? (e.g., I am /devices/ABC, update is /devices)
        is_parent_updated = self.active_path.startswith(incoming_slashed) or incoming_is_root

        # If overlapping, our local state is stale. Re-fetch!
        if is_child_updated or is_parent_updated:
            print(f"[ConfigNode] Overlapping change detected at '{path}'. Re-fetching '{self.active_path}'...")
            self.fetch()

    def _prune_schema_depth(self, schema_node: dict, current_depth: int, max_depth: int) -> dict:
        """Recursively removes 'properties' from schema nodes beyond the max_depth."""
        if not isinstance(schema_node, dict):
            return schema_node

        if current_depth >= max_depth:
            schema_node.pop("properties", None)
            schema_node.pop("additionalProperties", None)
            return schema_node

        if "properties" in schema_node:
            for key, val in schema_node["properties"].items():
                schema_node["properties"][key] = self._prune_schema_depth(val, current_depth + 1, max_depth)

        if "additionalProperties" in schema_node and isinstance(schema_node["additionalProperties"], dict):
            schema_node["additionalProperties"] = self._prune_schema_depth(
                schema_node["additionalProperties"], current_depth + 1, max_depth
            )

        return schema_node

    def send(self, patch: list = None):
        """
        Takes the updated config dictionary and the RFC 6902 JSON Patch
        from the UI and broadcasts it back to the main application.
        """
        if self.send_fn is not None:
            # We only send the path, the new state (for immediate overwrites if needed),
            # and the exact patch instructions.
            self.send_fn(self.active_path, patch)

    def update_path(self, new_path: str):
        self.active_path = new_path

    def fetch(self):
        """
        Triggers the main application to fetch the latest config/schema for this node's active path.
        The main application should then call recv_config_schema with the updated data.
        """
        if self.get_fn is not None:
            self.get_fn(self.active_path)

    def deregister(self):
        """Cleanly disconnects this node from the backend data supplier."""

        # Tell the manager to drop this node from its list
        self.signal_unregister.emit(self)

    def get(self, key=None, default=None):
        if key is None:
            return self.config
        return self.config.get(key, default)

    def show(self, child_path: str = None, name=None):
        path = f"{self.active_path}/{child_path}" if child_path else self.active_path
        if name is None:
            name = self.name
        self.manager.show(path, child_name=name)

    def factory_types(self, category: str) -> list[tuple[str, str]]:
        """Returns a list of (type_name, description) tuples for the given factory category."""
        return self.manager.get_factory_types(category)

    def factory_schema(self, category: str, type_name: str) -> dict:
        """Returns the schema blueprint for a specific factory class."""
        # Assuming you add a matching get_factory_schema to your manager/registry
        # print(f"[ConfigNode] factory schema for category '{category}', type '{type_name}' FETCHING...")
        schema = self.manager.get_factory_schema(category, type_name)
        # print(f"[ConfigNode] factory schema for category '{category}', type '{type_name}' schema: '{json.dumps(schema)}'")
        return schema

    def send_config(self, config: dict):
        """Sends the entire config dictionary back to the main application."""

        import jsonpatch

        self.send(jsonpatch.make_patch(self.config, config).patch)
        self.config = config

    def get_copy(self):
        """Returns a deep copy of the current config."""
        return deepcopy(self.config)

    def delete(self):
        """
        Triggers the removal of this node from the backend JSON configuration.
        """
        if not self.active_path or self.active_path == "/":
            print("[ConfigNode] Warning: Attempted to delete the root configuration node. Operation aborted.")
            return

        # Construct the RFC 6902 JSON patch to remove this specific path
        patch = [{"op": "remove", "path": self.active_path}]

        # Send the patch to the root ("/") so the active_path resolves correctly
        if self.send_fn is not None:
            self.send_fn("/", patch)

        # Clean up the node locally since it will no longer exist in the backend
        self.manager.broadcast_deletion(self.active_path)

    def handle_deletion(self):
        """
        Called by the manager when this node's path (or a parent path) is deleted.
        """
        self.signal_deleted.emit()
        self.deregister()

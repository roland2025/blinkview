# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from threading import Lock
from typing import TYPE_CHECKING

from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.ops.id_registry import NO_PARENT

if TYPE_CHECKING:
    from blinkview.core.id_registry import IDRegistry


class ModuleIdentity:
    __slots__ = (
        "id",
        "name",
        "short_name",
        "depth",
        "device",
        "submodules",
        "submodule_list",
    )

    def __init__(self, module_id: int, name: str, full_path: str, depth: int, device_identity: "DeviceIdentity"):
        self.id = module_id
        self.name = full_path
        self.short_name = name
        self.depth = depth
        self.device = device_identity

        self.submodules: dict[str, "ModuleIdentity"] = {}
        self.submodule_list: list["ModuleIdentity"] = []

    @property
    def parent(self) -> "ModuleIdentity | None":
        """Dynamic lookup: Breaks circular references."""
        return self.device.id_registry.get_parent(self.id)

    def get_all_descendants(self) -> list["ModuleIdentity"]:
        """Delegates to the registry for the heavy lifting."""
        return self.device.id_registry.get_descendant_modules(self.id)

    def name_with_device(self) -> str:
        return f"{self.device.name}.{self.name}"

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"ModuleIdentity({self.id}: '{self.device.name}.{self.name}')"


def print_tree_recursive(node: ModuleIdentity, indent=0):
    print("  " * indent + f"{node.name} (ID: {node.id}) full_path: {node.name}")
    for child in node.submodule_list:
        print_tree_recursive(child, indent + 1)


class DeviceIdentity:
    __slots__ = (
        "id",
        "name",
        "root",
        "modules",
        "path_lookup",
        "id_registry",
        "module_list",
        "device_ref",
        "_lock",
        "modules_table",
    )

    _VALID_NAME_REGEX = re.compile(r"^[a-z0-9_.]+$")

    def __init__(self, device_id: int, name: str, id_registry: "IDRegistry", device_ref=None):
        self.id = device_id
        self.name = name
        self.device_ref = device_ref
        self.id_registry = id_registry

        self._lock = Lock()

        self.modules_table = IndexedStringTable(initial_capacity=1024, use_hashes=True)

        # Create the Super Root
        # This module represents the device itself in the tree.
        root_id = self.id_registry.generate_module_id()
        self.root = ModuleIdentity(
            module_id=root_id,
            name=self.name.lower(),
            full_path="",  # Root has no path prefix
            depth=0,
            device_identity=self,
        )

        # Registries
        self.path_lookup: dict[str, ModuleIdentity] = {"": self.root}

        self.modules_table.register_name(root_id, "")

        self.id_registry.register_new_modules([(self.root, NO_PARENT)])

    def get_module(self, path: str) -> ModuleIdentity:
        path = path.lower()

        if (m := self.path_lookup.get(path)) is not None:
            return m

        if not self._VALID_NAME_REGEX.match(path):
            raise ValueError(f"Invalid path: {path.encode()}")

        # print(f"[DeviceIdentity] '{self.name}' => '{path}'")

        parts = path.split(".")

        # Start the traversal from the Super Root
        parent_node = self.root
        traversed_parts = []

        new_registrations = []  # Collect (Module, ParentID)

        with self._lock:
            if (m := self.path_lookup.get(path)) is not None:
                return m

            for part in parts:
                traversed_parts.append(part)
                current_full_path = ".".join(traversed_parts)

                if (target_node := parent_node.submodules.get(part)) is None:
                    global_id = self.id_registry.generate_module_id()

                    target_node = ModuleIdentity(
                        module_id=global_id,
                        name=part,
                        full_path=current_full_path,
                        depth=parent_node.depth + 1,
                        device_identity=self,
                    )

                    self.path_lookup[current_full_path] = target_node
                    parent_node.submodules[part] = target_node
                    parent_node.submodule_list.append(target_node)

                    # 3. Register in the local INGEST table for Numba
                    self.modules_table.register_name(global_id, current_full_path)

                    new_registrations.append((target_node, parent_node.id))

                parent_node = target_node

            # Hand off the batch with parent context
            if new_registrations:
                self.id_registry.register_new_modules(new_registrations)

        return target_node

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"DeviceIdentity({self.id}: '{self.name}')"

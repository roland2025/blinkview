# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from threading import Lock
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from blinkview.core.id_registry import IDRegistry


class ModuleIdentity:
    __slots__ = (
        "id",
        "name",
        "short_name",
        "depth",
        "device",
        "parent",
        "submodules",
        "submodule_list",
        "latest_row",
        "_descendant_cache",
        "meta",
    )

    def __init__(
        self, module_id: int, name: str, full_path: str, depth: int, device_identity: "DeviceIdentity", parent=None
    ):
        self.id = module_id
        self.name = full_path
        self.short_name = name
        self.depth = depth
        self.device = device_identity
        self.parent: "ModuleIdentity" = parent

        self.submodules: dict[str, "ModuleIdentity"] = {}
        self.submodule_list: list["ModuleIdentity"] = []

        self.latest_row = None
        self._descendant_cache: list["ModuleIdentity"] = []

        self.meta = None

    def _bubble_up_new_child(self, new_module: "ModuleIdentity"):
        """Appends the new module to this node's cache and continues up."""
        self._descendant_cache = self._descendant_cache + [new_module]  # noqa

        if self.parent:
            self.parent._bubble_up_new_child(new_module)
        # If no parent, we are the Device Root; bubbling stops here.

    def get_all_descendants(self) -> list["ModuleIdentity"]:
        """Returns all descendants in the subtree rooted at this module, excluding itself."""
        return self._descendant_cache

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
    __slots__ = ("id", "name", "root", "modules", "path_lookup", "_id_registry", "module_list", "device_ref", "_lock")

    _VALID_NAME_REGEX = re.compile(r"^[a-z0-9_.]+$")

    def __init__(self, device_id: int, name: str, id_registry: "IDRegistry", device_ref=None):
        self.id = device_id
        self.name = name
        self.device_ref = device_ref
        self._id_registry = id_registry

        self._lock = Lock()

        # Create the Super Root
        # This module represents the device itself in the tree.
        root_id = self._id_registry._generate_module_id()
        self.root = ModuleIdentity(
            module_id=root_id,
            name=self.name.lower(),
            full_path="",  # Root has no path prefix
            depth=0,
            device_identity=self,
        )

        # Registries
        self.path_lookup: dict[str, ModuleIdentity] = {}
        self.modules: dict[int, ModuleIdentity] = {root_id: self.root}
        self.module_list: list[ModuleIdentity] = [self.root]

        self.path_lookup[""] = self.root  # Register the root path

        self._id_registry._register_new_modules([self.root])

    def get_module(self, path: str) -> ModuleIdentity:
        path = path.lower()

        # print(f"Device '{self.name}': Requesting module for path '{path}'")

        # --- HOT PATH ---
        try:
            return self.path_lookup[path]
        except KeyError:
            pass

        if not self._VALID_NAME_REGEX.match(path):
            raise ValueError(f"Invalid path: '{path}'")

        parts = path.split(".")

        # Start the traversal from the Super Root
        parent_node = self.root
        traversed_parts = []

        new_nodes_created = []  # Collect new modules to append to module_list at the end for better cache locality

        with self._lock:
            # Re-check inside the lock! Another thread might have
            # created it while we were waiting for the lock.
            try:
                return self.path_lookup[path]
            except KeyError:
                pass

            # --- DISCOVERY PATH ---
            for part in parts:
                traversed_parts.append(part)
                current_full_path = ".".join(traversed_parts)

                try:
                    target_node = parent_node.submodules[part]
                except KeyError:
                    # Create the branch
                    global_id = self._id_registry._generate_module_id()  # noqa
                    target_node = ModuleIdentity(
                        module_id=global_id,
                        name=part,
                        full_path=current_full_path,
                        depth=parent_node.depth + 1,
                        device_identity=self,
                        parent=parent_node,
                    )

                    # Global Registrations
                    self.modules[global_id] = target_node
                    new_nodes_created.append(target_node)
                    self.path_lookup[current_full_path] = target_node

                    # Local Tree Registrations
                    parent_node.submodules[part] = target_node
                    parent_node.submodule_list = parent_node.submodule_list + [target_node]  # noqa

                    # Bubble up the new module to the root and intermediate ancestors
                    parent_node._bubble_up_new_child(target_node)  # noqa

                    # print_tree_recursive(self.root)

                parent_node = target_node

            self.module_list = self.module_list + new_nodes_created  # noqa
            self._id_registry._register_new_modules(new_nodes_created)  # noqa

        return target_node

    def get_all_modules(self) -> list[ModuleIdentity]:
        """
        Returns modules in chronological order.
        Thread-safe: returning a list copy is safe even if append() occurs.
        """
        return self.module_list

    def __str__(self):
        return self.name

    def __repr__(self):
        return f"ModuleIdentity({self.id}: '{self.name}')"

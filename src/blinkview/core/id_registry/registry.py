# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import RLock
from typing import Dict, List

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.id_registry.types import RegistryParams
from blinkview.core.logger import PrintLogger
from blinkview.ops.id_registry import NO_PARENT, nb_get_descendants
from blinkview.utils.level_map import LevelMap
from blinkview.utils.log_level import LogLevel


class IDRegistry:
    __slots__ = (
        "_lock",
        "_device_id_counter",
        "_module_id_counter",
        "devices",
        "device_list",
        "device_lookup",
        "level_map",
        "logger",
        "module_list",
        "modules",
        "modules_table",
        "levels_table",
        "devices_table",
        "_parent_capacity",
        "_parent_array",
    )

    def __init__(self, array_pool):
        self._lock = RLock()
        self.logger = PrintLogger("id_registry")

        self._device_id_counter = 0
        self._module_id_counter = 0

        # Pre-allocate the parent array
        self._parent_capacity = 1024
        self._parent_array = np.full(self._parent_capacity, NO_PARENT, dtype=dtypes.ID_TYPE)

        # Fast Lookups
        self.devices: Dict[int, DeviceIdentity] = {}
        self.device_lookup: Dict[str, DeviceIdentity] = {}

        self.modules: Dict[int, ModuleIdentity] = {}

        # Thread-safe Snapshot List
        self.device_list: List[DeviceIdentity] = []
        self.module_list: List[ModuleIdentity] = []

        self.level_map = LevelMap()

        self.modules_table = IndexedStringTable(initial_capacity=1024, use_hashes=False)
        self.devices_table = IndexedStringTable(initial_capacity=10, use_hashes=False)
        self.levels_table = IndexedStringTable(
            initial_capacity=10, buffer_size_bytes=128, values_dtype=dtypes.VALUES_TYPE, use_hashes=False
        )

        self._init_level_maps()

    def module_count(self):
        return self._module_id_counter

    def get_all_modules(self):
        return self.module_list

    def _init_level_maps(self):
        for i, lvl in enumerate(LogLevel.LIST):
            # We use i as the sequential index, and lvl.value as the 'searchable' ID
            self.levels_table.register_name(i, lvl.name, value=lvl.value)

        self.levels_table.debug_print("LEVELS")

    def generate_module_id(self) -> int:
        """Internal callback passed to DeviceIdentity."""
        with self._lock:
            current = self._module_id_counter
            self._module_id_counter += 1
            return current

    def register_new_modules(self, registration_data: List[tuple[ModuleIdentity, int]]):
        """
        Internal callback called by DeviceIdentity when a new module is created.
        Uses Atomic Swap for the global list.
        """
        # This is called from within the DeviceIdentity's lock,
        # but we use our own lock to protect the global counters and list.
        with self._lock:
            required_capacity = self._module_id_counter
            if required_capacity > self._parent_capacity:
                # Double the capacity until it fits
                new_cap = max(required_capacity, self._parent_capacity * 2)

                # Create new array filled with NO_PARENT
                new_array = np.full(new_cap, NO_PARENT, dtype=dtypes.ID_TYPE)

                # Copy old data over
                new_array[: self._parent_capacity] = self._parent_array

                # Swap the reference (atomic in Python)
                self._parent_array = new_array
                self._parent_capacity = new_cap

            for module, parent_id in registration_data:
                # 1. Update Global List/Map
                self.modules[module.id] = module
                self.module_list.append(module)

                # 2. Update Table (using the name string already on the module)
                self.modules_table.register_name(module.id, module.name)

                # 3. Update Topology Array (The untangled link)
                if parent_id != NO_PARENT:
                    self._parent_array[module.id] = parent_id

    def get_device(self, name: str) -> DeviceIdentity:
        """Retrieve or create a DeviceIdentity by name."""
        name = name.lower()

        # HOT PATH: Simple lookup (Dicts are thread-safe for reading in CPython)
        if name in self.device_lookup:
            return self.device_lookup[name]

        # DISCOVERY PATH: Locked
        with self._lock:
            # Double-check inside lock
            if name in self.device_lookup:
                return self.device_lookup[name]

            new_id = self._device_id_counter
            self._device_id_counter += 1

            new_device = DeviceIdentity(new_id, name, self)

            # Update Registries
            self.devices[new_id] = new_device
            self.device_lookup[name] = new_device

            # Atomic Swap for the list
            self.device_list = self.device_list + [new_device]  # noqa

            self.logger.info(f"[Device] {new_id} -> {name}")

            self.devices_table.register_name(new_id, name)

            return new_device

    def get_all_devices(self) -> List[DeviceIdentity]:
        """Lock-free access to all registered hardware devices."""
        return self.device_list

    def resolve_module(self, mod_identifier):
        if mod_identifier is None:
            return None

        # if already a ModuleIdentifier object, return itself
        if isinstance(mod_identifier, ModuleIdentity):
            return mod_identifier

        if not mod_identifier or not isinstance(mod_identifier, str):
            return None

        try:
            dev_name, mod_name = mod_identifier.split(".", 1)

            return self.get_device(dev_name).get_module(mod_name)
        except Exception:
            return None

    def resolve_modules(self, identifiers: list) -> list[ModuleIdentity]:
        """
        Resolves a list of strings or identities into a list of valid ModuleIdentity objects.
        Automatically filters out any that could not be resolved.
        """
        if not identifiers:
            return []

        return [m for ident in identifiers if (m := self.resolve_module(ident)) is not None]

    def resolve_device(self, dev_identifier):
        if dev_identifier is None:
            return None

        if isinstance(dev_identifier, DeviceIdentity):
            return dev_identifier

        if not dev_identifier or not isinstance(dev_identifier, str):
            return None

        return self.get_device(dev_identifier)

    def module_from_int(self, mod: int):
        return self.modules[mod]

    def bundle(self) -> RegistryParams:
        """Returns a combined snapshot of all identity tables."""
        return RegistryParams(
            levels=self.levels_table.bundle(),
            modules=self.modules_table.bundle(),
            devices=self.devices_table.bundle(),
            parents=self._parent_array,
        )

    def get_descendant_ids(self, target_id: int) -> np.ndarray:
        """
        Fast-path lookup for descendant IDs.
        Delegates directly to the Numba topology kernel.
        """
        # Pass our cached array and current counter straight to Numba
        return nb_get_descendants(target_id, self._parent_array, self._module_id_counter)

    def get_descendant_modules(self, target_id: int) -> list[ModuleIdentity]:
        """Python path: Returns actual objects for UI or high-level logic."""
        # 1. Get the IDs from the Numba-optimized path
        ids = nb_get_descendants(target_id, self._parent_array, self._module_id_counter)

        modules = self.modules

        # 2. Map to objects (CPython dict lookups are very fast)
        # We use 'self.modules' directly here
        return [modules[mid] for mid in ids]

    def get_parent(self, mod_id: int) -> ModuleIdentity | None:
        parent_id = self._parent_array[mod_id]
        if parent_id == NO_PARENT:
            return None
        return self.modules.get(parent_id)


def create_mock_modules(iterations=1_000):
    mock_pool = NumpyArrayPool()
    registry = IDRegistry(mock_pool)

    # 3. Allocation Loop
    print(f"Registering {iterations} modules...")
    device = registry.get_device("stress_test_device")

    for i in range(iterations):
        # We wrap in a list because your _register_new_modules expects one
        new_mod = device.get_module(f"module_{i}")


def memory_test(iterations=1_000):

    from blinkview.utils.profile_memory import profile_memory

    profile_memory(create_mock_modules)


if __name__ == "__main__":
    memory_test()

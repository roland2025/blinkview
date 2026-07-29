# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from threading import RLock
from time import perf_counter_ns
from typing import TYPE_CHECKING, Dict, List

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.id_registry.types import RegistryParams
from blinkview.core.logger import PrintLogger
from blinkview.ops.id_registry import NO_PARENT, nb_get_descendants
from blinkview.utils.level_map import LevelMap
from blinkview.utils.log_level import LogLevel

if TYPE_CHECKING:
    from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity


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
        "logger_device",
        "module_list",
        "modules",
        "modules_table",
        "levels_table",
        "devices_table",
        "_parent_capacity",
        "_parent_array",
        "_essential_array",
        "discovery_log",
    )

    def __init__(self, array_pool, replay_source_dir=None):
        self._lock = RLock()
        self.logger = PrintLogger("id_registry")

        self.logger_device = self.logger.child("new_device")

        self._device_id_counter = 0
        self._module_id_counter = 0

        # Pre-allocate the parent array
        self._parent_capacity = 1024
        self._parent_array = np.full(self._parent_capacity, NO_PARENT, dtype=dtypes.ID_TYPE)

        self._essential_array = np.zeros(self._parent_capacity, dtype=np.bool_)

        # Fast Lookups
        self.devices: Dict[int, "DeviceIdentity"] = {}
        self.device_lookup: Dict[str, "DeviceIdentity"] = {}

        self.modules: Dict[int, "ModuleIdentity"] = {}

        # Thread-safe Snapshot List
        self.device_list: List["DeviceIdentity"] = []

        # non-sequential list of modules
        self.module_list: List["ModuleIdentity"] = []

        self.level_map = LevelMap()

        # Chronological, globally-interleaved record of every device/module *first creation*
        # event (device_id/module_id are plain monotonic counters - see get_device/
        # register_new_modules - so replaying these same creation calls, in this same order,
        # against a fresh IDRegistry reproduces bit-identical ids deterministically). Persisted
        # alongside cold storage (see Registry.stop()/configure_system()) so mounted cold-segment
        # files - which only carry numeric device_id/module_id columns - can have their names
        # restored on a later run without re-parsing/re-ingesting the original source. See
        # dump_discovery_log/replay_discovery_log below.
        self.discovery_log: List[tuple] = []

        self.modules_table = IndexedStringTable(initial_capacity=1024, use_hashes=False)
        self.devices_table = IndexedStringTable(initial_capacity=10, use_hashes=False)
        self.levels_table = IndexedStringTable(
            initial_capacity=10, buffer_size_bytes=128, values_dtype=dtypes.VALUES_TYPE, use_hashes=False
        )

        self._init_level_maps()

        if replay_source_dir is not None:
            self._rehydrate_from_persisted_cold_storage(replay_source_dir)

    def _rehydrate_from_persisted_cold_storage(self, replay_source_dir) -> None:
        """Self-contained rehydration at construction time - if `replay_source_dir` (the
        original session being replayed) has a persisted id_registry.json next to its cold
        storage (see Registry._dump_id_registry / CircularLogPool's
        cold_storage_persist_on_close), replay it immediately so this registry's device/module
        ids match whatever a remounted cold segment file's numeric columns expect, before
        anything else ever calls get_device/get_module.

        Assumes the fixed default cold-storage layout (`<replay_source_dir>/cold/`) -
        CentralStorage._resolve_cold_storage_dir()'s user-overridden `cold_storage_dir` case
        always gets a fresh, uniquely-named temp subdirectory per run, so there's nothing stable
        at a predictable path to look up there; persist-and-resume only meaningfully applies to
        the default location. Best-effort: a missing or unreadable dump is not an error, just
        nothing to rehydrate from - a fresh session obviously has no prior discovery log yet."""
        import json

        dump_path = Path(replay_source_dir) / "cold" / "id_registry.json"
        if not dump_path.exists():
            return

        try:
            log = json.loads(dump_path.read_text())
            self.replay_discovery_log(log)
        except (OSError, ValueError) as e:
            self.logger.warning(f"Failed to rehydrate id_registry from {dump_path}: {e}")

    def module_count(self):
        return self._module_id_counter

    def get_all_modules(self):
        return self.module_list

    def _init_level_maps(self):
        for i, lvl in enumerate(LogLevel.LIST):
            # We use i as the sequential index, and lvl.value as the 'searchable' ID
            self.levels_table.register_name(i, lvl.name, value=lvl.value)

        # self.levels_table.debug_print("LEVELS")

    def generate_module_id(self) -> int:
        """Internal callback passed to DeviceIdentity."""
        with self._lock:
            current = self._module_id_counter
            self._module_id_counter += 1
            return current

    def register_new_modules(self, registration_data: "List[tuple[ModuleIdentity, int]]"):
        """
        Internal callback called by DeviceIdentity when a new module is created.
        Uses Atomic Swap for the global list.
        """
        # This is called from within the DeviceIdentity's lock,
        # but we use our own lock to protect the global counters and list.
        with self._lock:
            required_capacity = self._module_id_counter
            if required_capacity > self._parent_capacity:
                new_cap = max(required_capacity, self._parent_capacity * 2)

                new_array = np.full(new_cap, NO_PARENT, dtype=dtypes.ID_TYPE)
                new_array[: self._parent_capacity] = self._parent_array

                new_essential = np.zeros(new_cap, dtype=np.bool_)
                new_essential[: self._parent_capacity] = self._essential_array

                self._parent_array = new_array
                self._essential_array = new_essential
                self._parent_capacity = new_cap

            for module, parent_id in registration_data:
                # 1. Update Global List/Map
                self.modules[module.id] = module
                self.module_list.append(module)
                self.discovery_log.append(("module", module.device.name, module.name))

                # 2. Update Table (using the name string already on the module)
                self.modules_table.register_name(module.id, module.name)

                # 3. Update Topology Array (The untangled link)
                if parent_id != NO_PARENT:
                    self._parent_array[module.id] = parent_id

                # 4. Capture the essential flag set at construction time
                self._essential_array[module.id] = module.is_essential

    def get_device(self, name: str, essential: bool = True) -> "DeviceIdentity":
        """Retrieve or create a DeviceIdentity by name."""
        from blinkview.core.device_identity import DeviceIdentity

        name = name.lower()

        # HOT PATH: Simple lookup (Dicts are thread-safe for reading in CPython)
        if name in self.device_lookup:
            return self.device_lookup[name]

        start_time = perf_counter_ns()

        # DISCOVERY PATH: Locked
        with self._lock:
            # Double-check inside lock
            if name in self.device_lookup:
                return self.device_lookup[name]

            new_id = self._device_id_counter
            self._device_id_counter += 1

            new_device = DeviceIdentity(new_id, name, self, default_essential=essential)
            self.discovery_log.append(("device", name))

            # Update Registries
            self.devices[new_id] = new_device
            self.device_lookup[name] = new_device

            # Atomic Swap for the list
            self.device_list = self.device_list + [new_device]  # noqa

            self.devices_table.register_name(new_id, name)

            end_time = perf_counter_ns()

            self.logger_device.info(f"id={new_id} tm_ms={(end_time - start_time) / 1_000_000:.4f} name={name}")

            return new_device

    def get_all_devices(self) -> List["DeviceIdentity"]:
        """Lock-free access to all registered hardware devices."""
        return self.device_list

    def resolve_module(self, mod_identifier):
        from blinkview.core.device_identity import ModuleIdentity

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

    def resolve_modules(self, identifiers: list) -> "list[ModuleIdentity]":
        """
        Resolves a list of strings or identities into a list of valid ModuleIdentity objects.
        Automatically filters out any that could not be resolved.
        """
        if not identifiers:
            return []

        return [m for ident in identifiers if (m := self.resolve_module(ident)) is not None]

    def resolve_device(self, dev_identifier):
        from blinkview.core.device_identity import DeviceIdentity

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

    def get_descendant_modules(self, target_id: int) -> "list[ModuleIdentity]":
        """Python path: Returns actual objects for UI or high-level logic."""
        # 1. Get the IDs from the Numba-optimized path
        ids = nb_get_descendants(target_id, self._parent_array, self._module_id_counter)

        modules = self.modules

        # 2. Map to objects (CPython dict lookups are very fast)
        # We use 'self.modules' directly here
        return [modules[mid] for mid in ids]

    def get_parent(self, mod_id: int) -> "ModuleIdentity | None":
        parent_id = self._parent_array[mod_id]
        if parent_id == NO_PARENT:
            return None
        return self.modules.get(parent_id)

    def is_module_essential(self, mod_id: int) -> bool:
        return bool(self._essential_array[mod_id])

    def init_logger(self, logger_creator):
        """
        Re-initializes loggers using the provided factory.
        Safely extracts the context path and essential flag from the existing loggers.
        """

        def recreate(existing_logger):
            if not existing_logger:
                return None

            # Safely grab the context string depending on the logger type
            ctx = getattr(existing_logger, "ctx", getattr(existing_logger, "module_path", ""))

            # Preserve the essential flag
            is_essential = getattr(existing_logger, "is_essential", False)

            # The creator returns a lambda, so we call it immediately with ()
            return logger_creator(category=ctx, essential=is_essential)()

        self.logger = recreate(self.logger)

        if hasattr(self, "logger_device"):
            self.logger_device = recreate(self.logger_device)

        for dev in self.device_list:
            if hasattr(dev, "logger"):
                dev.logger = recreate(dev.logger)

    def set_module_essential(self, mod_id: int, is_essential: bool):
        self._essential_array[mod_id] = is_essential

    def dump_discovery_log(self) -> List[list]:
        """JSON-serializable form of discovery_log - see its own docstring in __init__. Each
        entry is `["device", name]` or `["module", device_name, full_path]`."""
        return [list(event) for event in self.discovery_log]

    def replay_discovery_log(self, log: List[list]) -> None:
        """Reconstructs device/module ids on a fresh IDRegistry by replaying a previously
        dump_discovery_log()'d event list through the same public get_device/get_module API,
        in the same order - see discovery_log's docstring for why this reproduces identical ids.
        Root-module events (`path == ""`) are skipped: DeviceIdentity's own constructor always
        creates its root module as a side effect of get_device(), so replaying it explicitly
        would be redundant (get_module("") isn't even a valid call - "" doesn't match
        DeviceIdentity._VALID_NAME_REGEX)."""
        for event in log:
            kind = event[0]
            if kind == "device":
                self.get_device(event[1])
            elif kind == "module":
                _kind, device_name, path = event
                if path:
                    self.get_device(device_name).get_module(path)


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

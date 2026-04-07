# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock, RLock
from typing import Dict, List, NamedTuple, Set, Tuple

import numpy as np

from ..utils.level_map import LevelMap
from ..utils.log_level import LogLevel
from .device_identity import DeviceIdentity, ModuleIdentity
from .logger import PrintLogger


class ByteMapParams(NamedTuple):
    buffer: np.ndarray  # uint8
    offsets: np.ndarray  # uint32
    lens: np.ndarray  # uint32


class IdentityByteMap:
    __slots__ = ("buffer", "offsets", "lens", "cursor")

    def __init__(self, initial_capacity: int = 1024, buffer_size_kb: int = 128):
        # Metadata arrays indexed by ID
        self.offsets = np.zeros(initial_capacity, dtype=np.uint32)
        self.lens = np.zeros(initial_capacity, dtype=np.uint32)

        # The raw string data
        self.buffer = np.zeros(buffer_size_kb * 1024, dtype=np.uint8)
        self.cursor = 0

    def register_name(self, identity_id: int, name: str):
        """Encodes a string into the flat buffer and stores its offset/length."""
        name_bytes = name.encode("utf-8")
        n_len = len(name_bytes)

        # 1. Resize metadata arrays if ID exceeds current capacity
        if identity_id >= len(self.offsets):
            new_cap = max(identity_id + 1, len(self.offsets) * 2)
            self.offsets = np.resize(self.offsets, new_cap)
            self.lens = np.resize(self.lens, new_cap)

        # 2. Resize byte buffer if we run out of room
        if self.cursor + n_len > len(self.buffer):
            new_buf_size = max(len(self.buffer) * 2, self.cursor + n_len)
            new_buf = np.zeros(new_buf_size, dtype=np.uint8)
            new_buf[: len(self.buffer)] = self.buffer
            self.buffer = new_buf

        # 3. Write data
        start = self.cursor
        self.buffer[start : start + n_len] = np.frombuffer(name_bytes, dtype=np.uint8)
        self.offsets[identity_id] = start
        self.lens[identity_id] = n_len
        self.cursor += n_len

    def get_numba_params(self) -> ByteMapParams:
        return ByteMapParams(self.buffer, self.offsets, self.lens)


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
        "module_map",
        "level_map_bytes",
        "device_map",
    )

    def __init__(self):
        self._lock = RLock()
        self.logger = PrintLogger("id_registry")

        self._device_id_counter = 0
        self._module_id_counter = 0

        # Fast Lookups
        self.devices: Dict[int, DeviceIdentity] = {}
        self.device_lookup: Dict[str, DeviceIdentity] = {}

        self.modules: Dict[int, ModuleIdentity] = {}

        # Thread-safe Snapshot List
        self.device_list: List[DeviceIdentity] = []
        self.module_list: List[ModuleIdentity] = []

        self.level_map = LevelMap()

        self.module_map = IdentityByteMap(initial_capacity=1024)
        self.device_map = IdentityByteMap(initial_capacity=10)
        self.level_map_bytes = IdentityByteMap(initial_capacity=10)

        self._init_level_maps()

    def _init_level_maps(self):
        for lvl in LogLevel.LIST:
            self.level_map_bytes.register_name(lvl.value, lvl.name)

    def _generate_module_id(self) -> int:
        """Internal callback passed to DeviceIdentity."""
        with self._lock:
            current = self._module_id_counter
            self._module_id_counter += 1
            return current

    def _register_new_modules(self, modules: List[ModuleIdentity]):
        """
        Internal callback called by DeviceIdentity when a new module is created.
        Uses Atomic Swap for the global list.
        """
        # This is called from within the DeviceIdentity's lock,
        # but we use our own lock to protect the global counters and list.
        with self._lock:
            self.module_list = self.module_list + modules  # noqa

            for module in modules:
                self.modules[module.id] = module
                self.module_map.register_name(module.id, module.name)

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

            self.device_map.register_name(new_id, name)

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

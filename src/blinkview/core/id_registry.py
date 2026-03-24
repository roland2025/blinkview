# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock, RLock

from .device_identity import DeviceIdentity, ModuleIdentity
from typing import Dict, Set, List

from .logger import PrintLogger
from ..utils.level_map import LevelMap


class IDRegistry:
    __slots__ = (
        '_lock', '_device_id_counter', '_module_id_counter',
        'devices', 'device_list', 'device_lookup', 'level_map', 'logger', 'module_list'
    )

    def __init__(self):
        self._lock = RLock()
        self.logger = PrintLogger('id_registry')

        self._device_id_counter = 0
        self._module_id_counter = 0

        # Fast Lookups
        self.devices: Dict[int, DeviceIdentity] = {}
        self.device_lookup: Dict[str, DeviceIdentity] = {}

        # Thread-safe Snapshot List
        self.device_list: List[DeviceIdentity] = []
        self.module_list: List[ModuleIdentity] = []

        self.level_map = LevelMap()

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
            dev_name, mod_name = mod_identifier.split('.', 1)

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

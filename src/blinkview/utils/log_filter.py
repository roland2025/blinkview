# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Optional

from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.utils.log_level import LevelIdentity, LogLevel


class LogFilter:
    def __init__(
        self, id_registry, allowed_device=None, filtered_module=None, log_level=None, filtered_module_children=False
    ):
        self.registry = id_registry

        self.filter_index = None

        self.allowed_device: Optional[DeviceIdentity] = id_registry.resolve_device(allowed_device)
        self.filtered_module: Optional[ModuleIdentity] = id_registry.resolve_module(filtered_module)
        self.filtered_module_children = filtered_module_children
        self.log_level: Optional[LevelIdentity] = LogLevel.from_string(log_level, LogLevel.ALL)

        self._bake()

    def _bake(self):
        """
        Creates optimized closures. Branches based on constraint presence
        to yield the absolute fastest possible loops.
        """
        allowed_dev = self.allowed_device
        target_mod = self.filtered_module
        idx = self.filter_index
        base_level = self.log_level
        include_children = self.filtered_module_children

        print(
            f"[LogFilter] allowed_dev={allowed_dev} idx={idx}, base_level={base_level} target_mod={target_mod.name if target_mod else None}"
        )

        if base_level == LogLevel.ALL:
            base_level = None  # Treat ALL as no level constraint

        # --- ULTRA-FAST PATH: No constraints set ---
        if allowed_dev is None and target_mod is None and idx is None and base_level is None:

            def fast_matches(_) -> bool:
                return True

            def fast_filter_batch(batch: list, after_seq: int = -1) -> list:
                # Skip the list comprehension entirely if we aren't filtering anything
                if after_seq == -1:
                    return batch.copy()  # Or just return batch, depending on mutation rules
                return [msg for msg in batch if msg.seq > after_seq]

        elif base_level is None and idx is None and target_mod is None and allowed_dev is not None:

            def fast_matches(msg) -> bool:
                return msg.module.device is allowed_dev

            def fast_filter_batch(batch: list, after_seq: int = -1) -> list:
                if after_seq == -1:
                    return [msg for msg in batch if msg.module.device is allowed_dev]
                else:
                    return [msg for msg in batch if msg.seq > after_seq and msg.module.device is allowed_dev]

        # --- STANDARD PATH: Constraints exist ---
        else:

            def fast_matches(msg) -> bool:
                # --- LEVEL CHECK FIRST ---
                # Check the global log level constraint
                if base_level is not None and msg.level < base_level:
                    return False

                # Check granular registry-based filter (from metadata)
                try:
                    if idx is not None:
                        if msg.level < msg.module.meta.filter_conf[idx]:
                            return False
                except (AttributeError, TypeError, IndexError):
                    pass

                # --- HIERARCHY CHECK (Parent Traversal) ---
                if target_mod is not None:
                    if include_children:
                        # Traverse parents manually until we hit target_mod or the root (None)
                        curr = msg.module
                        found = False
                        while curr is not None:
                            if curr is target_mod:
                                found = True
                                break
                            curr = curr.parent

                        if not found:
                            return False
                    else:
                        # Strict identity match only
                        if msg.module is not target_mod:
                            return False

                # --- DEVICE CHECK ---
                if allowed_dev is not None and msg.module.device is not allowed_dev:
                    return False

                return True

            def fast_filter_batch(batch: list, after_seq: int = -1) -> list:
                if after_seq == -1:
                    return [msg for msg in batch if fast_matches(msg)]
                return [msg for msg in batch if msg.seq > after_seq and fast_matches(msg)]

        # Bind the baked functions
        self.matches = fast_matches
        self.filter_batch = fast_filter_batch

    def set_filter_index(self, index: int = None):
        self.filter_index = index
        self._bake()

    def set_level(self, log_level):
        self.log_level = LogLevel.from_string(log_level)
        self._bake()

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.device_identity import ModuleIdentity
from blinkview.utils.log_level import LogLevel


class LogFilter:
    def __init__(self, id_registry, allowed_device=None, filtered_module=None, log_level=None, filtered_module_children=False):
        self.registry = id_registry

        self.filter_index = None

        self.allowed_device = self._resolve_device(allowed_device)
        self.filtered_module: ModuleIdentity = self._resolve_module(filtered_module)
        self.filtered_module_children = filtered_module_children
        self.log_level = LogLevel.from_string(log_level, LogLevel.ALL)

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

        print(f"[LogFilter] allowed_dev={allowed_dev} idx={idx}, base_level={base_level} target_mod={target_mod.name if target_mod else None}")

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
                            if curr == target_mod:
                                found = True
                                break
                            curr = curr.parent

                        if not found:
                            return False
                    else:
                        # Strict identity match only
                        if msg.module != target_mod:
                            return False

                # --- DEVICE CHECK ---
                if allowed_dev is not None and msg.module.device != allowed_dev:
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

    def _resolve_module(self, mod_identifier):
        if not mod_identifier: return None
        if not isinstance(mod_identifier, str): return mod_identifier
        try:
            dev_name, mod_name = mod_identifier.split('.', 1)
            return self.registry.get_device(dev_name).get_module(mod_name)
        except Exception:
            return None

    def _resolve_device(self, dev_identifier):
        if not dev_identifier: return None
        return self.registry.get_device(dev_identifier)

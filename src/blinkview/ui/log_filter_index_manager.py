# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import QObject

from blinkview.core.device_identity import ModuleIdentity
from blinkview.ui.module_gui_meta import ModuleGUIMeta
from blinkview.utils.log_level import LogLevel


class LogFilterIndexManager(QObject):
    """
    High-performance index manager for surgical log filtering.
    Assumes thread-safe module_list access and persistent registry existence.
    """

    def __init__(self, gui_context, min_capacity: int = 1, parent=None):
        super().__init__(parent)
        self.ctx = gui_context
        self._min_capacity = min_capacity
        self._current_capacity = 0
        self._next_index = 0
        self._recycled_indices = []

    def checkout(self) -> int:
        """Assigns an index. Grows global metadata ONLY when capacity is exceeded."""
        if self._recycled_indices:
            popped_idx = self._recycled_indices.pop(0)
            self.log_state()
            return popped_idx

        if self._next_index >= self._current_capacity:
            self._grow_global_metadata()
            self._current_capacity += 1

        assigned_idx = self._next_index
        self._next_index += 1

        self.log_state()

        return assigned_idx

    def log_state(self):
        print(
            f"[IndexManager] capacity={self._current_capacity} next={self._next_index} recycled={self._recycled_indices}"
        )

    def log_module(self, module: ModuleIdentity):
        print(f"[IndexManager] module {module.name} filter_conf {module.meta.filter_conf}")

    def release(self, index: int):
        """Returns index and truncates the tail while keeping one slot free."""
        if index is None:
            return

        self._clear_lane(index)

        if index not in self._recycled_indices:
            self._recycled_indices.append(index)
            self._recycled_indices.sort()

        # Find the actual highest index still in active use
        highest_active = -1
        if self._next_index > 0:
            check_idx = self._next_index - 1
            while check_idx >= 0 and check_idx in self._recycled_indices:
                check_idx -= 1
            highest_active = check_idx

        # Capacity = highest_active + 1 (active) + 1 (buffer)
        target_capacity = max(self._min_capacity, highest_active + 2)

        if target_capacity < self._current_capacity:
            self._shrink_global_metadata(target_capacity)

        self.log_state()

    def _grow_global_metadata(self):
        """Appends one slot to all module filter arrays."""
        for module in self.ctx.id_registry.module_list:
            if module.meta is None:
                # Late-init meta if hardware thread hasn't yet
                module.meta = ModuleGUIMeta(self._current_capacity)

            module.meta.filter_conf.append(LogLevel.ALL)

            self.log_module(module)

        for device in self.ctx.id_registry.device_list:
            if device.root.meta is None:
                device.root.meta = ModuleGUIMeta(self._current_capacity)

            device.root.meta.filter_conf.append(LogLevel.ALL)
            self.log_module(device.root)

    def _shrink_global_metadata(self, new_cap: int):
        """Trims the filter_conf lists to the target capacity."""
        for module in self.ctx.id_registry.module_list:
            if module.meta is not None:
                module.meta.filter_conf = module.meta.filter_conf[:new_cap]
                self.log_module(module)

        for device in self.ctx.id_registry.device_list:
            if device.root.meta is not None:
                device.root.meta.filter_conf = device.root.meta.filter_conf[:new_cap]
                self.log_module(device.root)

        self._current_capacity = new_cap

        self._next_index = new_cap - 1

        self._recycled_indices = [i for i in self._recycled_indices if i < self._next_index]

    def _clear_lane(self, index: int):
        """Resets the filter value for this index to 0 (TRACE)."""
        for module in self.ctx.id_registry.module_list:
            if module.meta is not None:
                if index < len(module.meta.filter_conf):
                    module.meta.filter_conf[index] = LogLevel.ALL

                self.log_module(module)

        for device in self.ctx.id_registry.device_list:
            if device.root.meta is not None:
                if index < len(device.root.meta.filter_conf):
                    device.root.meta.filter_conf[index] = LogLevel.ALL
                self.log_module(device.root)

    def set_log_level(self, module: ModuleIdentity, index: int, level: LogLevel):
        """Sets the filter level for a specific module and index."""
        if index is None:
            return
        if module.meta is None:
            module.meta = ModuleGUIMeta(self._current_capacity)
        module.meta.filter_conf[index] = level
        self.log_module(module)

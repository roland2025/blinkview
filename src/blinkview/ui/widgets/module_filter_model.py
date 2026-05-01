# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import weakref
from dataclasses import dataclass
from typing import List, Set

import numpy as np
from qtpy.QtCore import QAbstractTableModel, QModelIndex, Qt, QTimer, Signal
from qtpy.QtGui import QColor

from blinkview.core import dtypes
from blinkview.ui.gui_context import GUIContext
from blinkview.utils.log_level import LogLevel


class FastModuleFilterModel(QAbstractTableModel):
    def __init__(self, registry, temp_log_filter, parent=None):
        super().__init__(parent)
        self.registry = registry
        self.filter = temp_log_filter

        # The Magic List: Maps UI Row -> Module ID
        self.row_to_id = np.empty(0, dtype=dtypes.ID_TYPE)
        # Track the global count we synced against
        self.known_module_count = 0

    def sync_registry(self, allowed_device=None):
        self.beginResetModel()

        # 1. Update our tracker to the current global count
        self.known_module_count = self.registry.module_count()

        # 2. Tell the filter to sync its array sizes with the registry
        self.filter.ensure_capacity(self.known_module_count)

        # 3. Build the UI map
        modules = self.registry.get_all_modules()
        if allowed_device:
            modules = [m for m in modules if m.device == allowed_device]

        modules.sort(key=lambda m: (m.device.name, m.name))
        self.row_to_id = np.array([m.id for m in modules], dtype=np.uint32)

        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self.row_to_id)

    def columnCount(self, parent=QModelIndex()):
        return 2

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self.row_to_id):
            return None

        mod_id = self.row_to_id[index.row()]
        col = index.column()

        # Let the view know the raw ID if it needs it
        if role == Qt.UserRole:
            return mod_id

        # --- Column 0: Checkbox & Name ---
        if col == 0:
            if role == Qt.CheckStateRole:
                return Qt.Checked if self.filter.enabled_mask[mod_id] else Qt.Unchecked

            if role == Qt.DisplayRole:
                mod = self.registry.module_from_int(mod_id)
                if mod.depth == 0:
                    return f"{mod.device.name}"
                indent = "    " * (mod.depth - 1)
                return f"{indent}├── {mod.short_name}"

        # --- Column 1: Log Level ---
        if col == 1:
            lvl_val = self.filter.level_mask[mod_id]
            level_obj = LogLevel.from_value(lvl_val, LogLevel.INFO)

            if role == Qt.DisplayRole:
                return level_obj.name_conf
            if role == Qt.ForegroundRole:
                from qtpy.QtGui import QColor

                return QColor(level_obj.color)
            if role == Qt.TextAlignmentRole:
                return Qt.AlignCenter

        return None

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid():
            return False

        mod_id = self.row_to_id[index.row()]
        col = index.column()

        if role == Qt.CheckStateRole and col == 0:
            is_checked = value == Qt.Checked or value == 2
            self.filter.set_module_enabled(mod_id, is_checked)
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        if role == Qt.EditRole and col == 1:
            # The delegate passes the LogLevel object back
            self.filter.set_module_level(mod_id, value)
            self.dataChanged.emit(index, index, [Qt.DisplayRole, Qt.ForegroundRole])
            return True

        return False

    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags

        f = Qt.ItemIsEnabled | Qt.ItemIsSelectable
        if index.column() == 0:
            f |= Qt.ItemIsUserCheckable
        elif index.column() == 1:
            f |= Qt.ItemIsEditable
        return f


#
# class ModuleFilterModel(QAbstractTableModel):
#     """
#     SHARED Source Model: Provides the hardware module list across all devices.
#     This model NO LONGER stores checkbox or level state.
#     """
#
#     sync_paused_changed = Signal(bool)
#     registry_synced = Signal(list)  # Emits the full list of ModuleIdentity
#
#     def __init__(self, gui_context, parent=None):
#         super().__init__(parent)
#         self.gui_context = gui_context
#         self._row_states: List["ModuleIdentity"] = []  # Just the identity objects
#         self._known_ids: Set[int] = set()
#         self._usage_count = 0
#
#         self._pause_count = 0
#
#     def pause_sync(self, pause: bool):
#         prev_pause = self._pause_count > 0
#         if pause:
#             self._pause_count += 1
#         else:
#             self._pause_count = max(0, self._pause_count - 1)
#
#         current_pause = self._pause_count > 0
#         if prev_pause != current_pause:
#             self.sync_paused_changed.emit(current_pause)
#
#         if not current_pause:
#             self.sync_registry()
#
#         print(
#             f"[ModuleFilterModel] pause_sync: pause={pause}, _pause_count={self._pause_count}, active_views={self._usage_count}"
#         )
#
#     def rowCount(self, parent=QModelIndex()):
#         return 0 if parent.isValid() else len(self._row_states)
#
#     def columnCount(self, parent=QModelIndex()):
#         return 2
#
#     def sync_registry(self):
#         if self._usage_count == 0 or self._pause_count > 0:
#             return
#
#         id_registry = self.gui_context.id_registry
#
#         if id_registry.module_count() == len(self._row_states):
#             return
#
#         self.beginResetModel()
#         self._row_states = list(id_registry.module_list)
#         # Sort by Device Name then Module path
#         self._row_states.sort(key=lambda m: (m.device.name, m.name))
#         self.endResetModel()
#
#         self.registry_synced.emit(self._row_states)
#
#     def data(self, index, role=Qt.DisplayRole):
#         if not index.isValid() or index.row() >= len(self._row_states):
#             return None
#
#         module = self._row_states[index.row()]
#         col = index.column()
#
#         # --- CRITICAL: Let the Proxy know WHICH module it is looking at ---
#         if role == Qt.UserRole:
#             return module
#
#         if role == Qt.DisplayRole and col == 0:
#             if module.depth == 0:
#                 return f"{module.device.name}"
#
#                 # Use '├─' for mid-nodes and '└─' for end nodes if you have that info
#                 # For a simple version, a consistent '└─' or '──' works well:
#             indent = "    " * (module.depth - 1)
#             prefix = "├── "
#             return f"{indent}{prefix}{module.short_name}"
#             # if module.depth == 0:
#             #     return f"● [{module.device.name}] {module.short_name}"
#             #
#             #     # Using the 'Light Vertical Bar' (U+23AF) or 'Middle Dot' (U+00B7)
#             # indent = "    "+("·   " * (module.depth-1))
#             # return f"{indent}{module.short_name}"
#
#         if role == Qt.ToolTipRole:
#             return f"ID: {module.id}\nPath: {module.name}"
#
#         return None
#
#     def flags(self, index):
#         # Base flags: only names are selectable/enabled here
#         return Qt.ItemIsEnabled | Qt.ItemIsSelectable
#
#     # --- Registration Logic ---
#     def register_consumer(self):
#         self._usage_count += 1
#         print(f"[ModuleFilterModel] register_consumer: _usage_count={self._usage_count}")
#         self.sync_registry()
#
#     def unregister_consumer(self):
#         self._usage_count = max(0, self._usage_count - 1)
#         print(f"[ModuleFilterModel] unregister_consumer: _usage_count={self._usage_count}")
#
#     # --- Sync Logic ---
#     def setData(self, index, value, role=Qt.EditRole):
#         if not index.isValid() or index.row() >= len(self._row_states):
#             return False
#
#         state = self._row_states[index.row()]
#
#         if index.column() == 0 and role == Qt.CheckStateRole:
#             state.is_checked = value == Qt.Checked
#         elif index.column() == 1 and role == Qt.EditRole:
#             state.min_level = value
#         else:
#             return False
#
#         self.dataChanged.emit(index, index, [role])
#         self.filter_changed.emit()
#         return True

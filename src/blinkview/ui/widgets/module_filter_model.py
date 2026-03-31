# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import weakref
from dataclasses import dataclass
from typing import List, Set

from qtpy.QtCore import QAbstractTableModel, QModelIndex, Qt, QTimer, Signal
from qtpy.QtGui import QColor

from blinkview.ui.gui_context import GUIContext
from blinkview.utils.log_level import LogLevel


class ModuleFilterModel(QAbstractTableModel):
    """
    SHARED Source Model: Provides the hardware module list across all devices.
    This model NO LONGER stores checkbox or level state.
    """

    sync_paused_changed = Signal(bool)
    registry_synced = Signal(list)  # Emits the full list of ModuleIdentity

    def __init__(self, gui_context, parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self._row_states: List["ModuleIdentity"] = []  # Just the identity objects
        self._known_ids: Set[int] = set()
        self._usage_count = 0

        self._pause_count = 0

    def pause_sync(self, pause: bool):
        prev_pause = self._pause_count > 0
        if pause:
            self._pause_count += 1
        else:
            self._pause_count = max(0, self._pause_count - 1)

        current_pause = self._pause_count > 0
        if prev_pause != current_pause:
            self.sync_paused_changed.emit(current_pause)

        if not current_pause:
            self.sync_registry()

        print(
            f"[ModuleFilterModel] pause_sync: pause={pause}, _pause_count={self._pause_count}, active_views={self._usage_count}"
        )

    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._row_states)

    def columnCount(self, parent=QModelIndex()):
        return 2

    def sync_registry(self):
        if self._usage_count == 0 or self._pause_count > 0:
            return

        all_current = []
        for device in self.gui_context.id_registry.device_list:
            all_current.append(device.root)
            all_current.extend(device.root.get_all_descendants())

        if len(all_current) == len(self._row_states):
            return

        new_found = [m for m in all_current if m.id not in self._known_ids]
        if new_found:
            self.beginResetModel()
            self._row_states = all_current
            self._known_ids.update(m.id for m in new_found)
            # Sort by Device Name then Module path
            self._row_states.sort(key=lambda m: (m.device.name, m.name))
            self.endResetModel()

            self.registry_synced.emit(self._row_states)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid() or index.row() >= len(self._row_states):
            return None

        module = self._row_states[index.row()]
        col = index.column()

        # --- CRITICAL: Let the Proxy know WHICH module it is looking at ---
        if role == Qt.UserRole:
            return module

        if role == Qt.DisplayRole and col == 0:
            if module.depth == 0:
                return f"{module.device.name}"

                # Use '├─' for mid-nodes and '└─' for end nodes if you have that info
                # For a simple version, a consistent '└─' or '──' works well:
            indent = "    " * (module.depth - 1)
            prefix = "├── "
            return f"{indent}{prefix}{module.short_name}"
            # if module.depth == 0:
            #     return f"● [{module.device.name}] {module.short_name}"
            #
            #     # Using the 'Light Vertical Bar' (U+23AF) or 'Middle Dot' (U+00B7)
            # indent = "    "+("·   " * (module.depth-1))
            # return f"{indent}{module.short_name}"

        if role == Qt.ToolTipRole:
            return f"ID: {module.id}\nPath: {module.name}"

        return None

    def flags(self, index):
        # Base flags: only names are selectable/enabled here
        return Qt.ItemIsEnabled | Qt.ItemIsSelectable

    # --- Registration Logic ---
    def register_consumer(self):
        self._usage_count += 1
        print(f"[ModuleFilterModel] register_consumer: _usage_count={self._usage_count}")
        self.sync_registry()

    def unregister_consumer(self):
        self._usage_count = max(0, self._usage_count - 1)
        print(f"[ModuleFilterModel] unregister_consumer: _usage_count={self._usage_count}")

    # --- Sync Logic ---
    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid() or index.row() >= len(self._row_states):
            return False

        state = self._row_states[index.row()]

        if index.column() == 0 and role == Qt.CheckStateRole:
            state.is_checked = value == Qt.Checked
        elif index.column() == 1 and role == Qt.EditRole:
            state.min_level = value
        else:
            return False

        self.dataChanged.emit(index, index, [role])
        self.filter_changed.emit()
        return True

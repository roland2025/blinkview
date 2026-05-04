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

        self.root_module_depth = 0

    def sync_registry(self, allowed_device=None, root_module=None, include_children=False):
        """
        Rebuilds the row map based on tab constraints.
        Filters out modules that are not part of the allowed scope.
        """
        self.beginResetModel()

        self.known_module_count = self.registry.module_count()
        self.filter.ensure_capacity(self.known_module_count)

        # 1. Determine which modules belong in this table
        if root_module:
            self.root_module_depth = root_module.depth
            if include_children:
                # Include the root and all its descendants
                modules = [root_module] + root_module.get_all_descendants()
            else:
                # Only show the single module
                modules = [root_module]
        else:
            # Global view: show all modules (optionally filtered by device)
            self.root_module_depth = 0
            modules = self.registry.get_all_modules()
            if allowed_device:
                modules = [m for m in modules if m.device == allowed_device]

        # 2. Sort and map to IDs
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

                # Calculate relative depth to keep indentation clean in sub-tabs
                # Depth 0 (Device) or the Root of a subtree has no indentation
                rel_depth = max(0, mod.depth - self.root_module_depth)

                if rel_depth == 0:
                    # If global view, show Device Name; if subtree, show Module Name
                    return f"{mod.device.name}" if mod.depth == 0 else f"{mod.short_name}"

                indent = "    " * (rel_depth - 1)
                return f"{indent}└── {mod.short_name}"

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

            if self.filter.enabled_mask[mod_id] == is_checked:
                return False

            self.filter.set_module_enabled(mod_id, is_checked)
            self.dataChanged.emit(index, index, [Qt.CheckStateRole])
            return True

        if role == Qt.EditRole and col == 1:
            # The delegate passes the LogLevel object back

            if self.filter.level_mask[mod_id] == value.value:
                return False

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

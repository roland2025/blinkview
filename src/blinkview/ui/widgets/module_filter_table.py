# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import weakref
from dataclasses import dataclass, field
from typing import List, Optional, Set

from PySide6.QtCore import QAbstractTableModel, QModelIndex, QObject, QSortFilterProxyModel, Qt, QTimer, Signal
from PySide6.QtGui import QColor, QFont
from PySide6.QtWidgets import QComboBox, QHeaderView, QStyledItemDelegate, QTableView

from blinkview.core.device_identity import ModuleIdentity
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.module_filter_model import ModuleFilterModel
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel


@dataclass(slots=True)
class ModuleFilterState:
    module: ModuleIdentity
    enabled: bool
    level: "LevelIdentity"
    override: bool = False


class TempLogFilter(QObject):
    filter_changed = Signal()

    def __init__(self, gui_context: GUIContext, log_filter: LogFilter):
        super().__init__()
        # State: {module_id: (is_enabled, min_level)}
        self.enabled = False
        self._states = {}
        self.gui_context = gui_context

        self.log_filter = log_filter

        self.gui_context.module_filter_model.register_consumer()
        self.gui_context.module_filter_model.registry_synced.connect(self._on_registry_synced)

        # initialize modules that are already in the registry (in case this filter is created after startup)
        self._on_registry_synced(self.gui_context.id_registry.module_list)
        print(f"[TempLogFilter] Initialized with {len(self._states)} modules from registry.")

    def __del__(self):
        # This ensures that if the log tab is closed, we don't
        # keep the background sync running forever.
        if hasattr(self, "gui_context"):
            print("[TempLogFilter] __del__ called, unregistering from module filter model.")
            self.gui_context.module_filter_model.unregister_consumer()

    def _on_registry_synced(self, modules):
        """Pre-initialize every module found in the registry."""
        for module in modules:
            self.get_module(module)

    def get_module(self, module: "ModuleIdentity") -> ModuleFilterState:
        mod = self._states.get(module)
        if mod is not None:
            return mod
        parent_mod = module.parent
        parent_log_level = self.log_filter.log_level or LogLevel.ALL
        parent_enabled = True
        parent_state = self._states.get(parent_mod)
        if parent_state is not None:
            parent_log_level = parent_state.level
            parent_enabled = parent_state.enabled
        if module.device.name == "abc_key":
            print(
                f"[TempLogFilter] get_module={module.name} parent={parent_mod is not None} log_level={parent_log_level} enabled={parent_enabled}"
            )
        mod = ModuleFilterState(module, parent_enabled, parent_log_level)
        self._states[module] = mod
        self.gui_context.index_manager.set_log_level(
            module, self.log_filter.filter_index, mod.level if mod.enabled else LogLevel.OFF
        )
        return mod

    def set_module_enabled(self, module: "ModuleIdentity", enabled):
        mod = self.get_module(module)
        mod.enabled = enabled
        mod.override = True
        self.gui_context.index_manager.set_log_level(
            module, self.log_filter.filter_index, mod.level if mod.enabled else LogLevel.OFF
        )

        self.filter_changed.emit()

    def set_module_level(self, module: "ModuleIdentity", level):
        mod = self.get_module(module)
        mod.level = level
        mod.override = True
        print(
            f"[TempLogFilter] set_module_level {module.name} to {level.name_conf}, enabled={mod.enabled} index={self.log_filter.filter_index}"
        )
        self.gui_context.index_manager.set_log_level(
            module, self.log_filter.filter_index, mod.level if mod.enabled else LogLevel.OFF
        )

        self.filter_changed.emit()

    def set_enabled(self, enabled: bool):
        self.enabled = enabled
        if enabled:
            self.log_filter.set_filter_index(self.gui_context.index_manager.checkout())
            # restore any existing state for this filter index
            for mod in self._states.values():
                self.gui_context.index_manager.set_log_level(
                    mod.module, self.log_filter.filter_index, mod.level if mod.enabled else LogLevel.OFF
                )
        else:
            filter_index = self.log_filter.filter_index
            self.log_filter.set_filter_index(None)
            self.gui_context.index_manager.release(filter_index)

        print(f"[TempLogFilter] set_enabled {enabled}, filter_index={self.log_filter.filter_index}")

        # self._bake()
        self.filter_changed.emit()

    def set_level(self, level: "LevelIdentity"):
        for module in self._states.keys():
            mod = self.get_module(module)
            mod.level = level

            self.gui_context.index_manager.set_log_level(
                module, self.log_filter.filter_index, mod.level if mod.enabled else LogLevel.OFF
            )

        self.filter_changed.emit()

    def get_state(self):
        return {
            f"{mod.module.device.name}.{mod.module.name}": {"enabled": mod.enabled, "level": mod.level.name_conf}
            for mod in self._states.values()
            if mod.override
        }

    def restore_state(self, state):
        if state is None:
            return

        for mod_id, mod_state in state.items():
            module = self.gui_context.id_registry.resolve_module(mod_id)
            if not module:
                print(f"[TempLogFilter] Warning: Module '{mod_id}' not found during state restore.")
                continue

            mod = self.get_module(module)
            mod.enabled = mod_state.get("enabled", True)
            mod.level = LogLevel.from_string(mod_state.get("level"), default=LogLevel.ALL)
            mod.override = True

            self.gui_context.index_manager.set_log_level(
                module, self.log_filter.filter_index, mod.level if mod.enabled else LogLevel.OFF
            )


class ModuleFilterProxyModel(QSortFilterProxyModel):
    """
    The 'State Mapper' Proxy.
    Redirects CheckState and LogLevel data to a tab-specific LogFilter instance.
    The Source Model remains the shared global list of hardware modules.
    """

    def __init__(self, log_filter: TempLogFilter, parent=None):
        super().__init__(parent)
        self.log_filter = log_filter

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        source_index = self.mapToSource(index)
        # We assume column 0 UserRole returns the ModuleIdentity object
        module = self.sourceModel().data(source_index, Qt.UserRole)
        if not module:
            return super().data(index, role)

        col = index.column()

        # --- Intercept Checkbox (Column 0) ---
        if role == Qt.CheckStateRole and col == 0:
            is_enabled = self.log_filter.get_module(module).enabled
            return Qt.Checked if is_enabled else Qt.Unchecked

        # --- Intercept Log Level (Column 1) ---
        if col == 1:
            level = self.log_filter.get_module(module).level
            if role == Qt.DisplayRole:
                return level.name_conf
            if role == Qt.ForegroundRole:
                return QColor(level.color)
            if role == Qt.TextAlignmentRole:
                return Qt.AlignCenter

        return super().data(index, role)

    def setData(self, index, value, role=Qt.EditRole):
        if not index.isValid():
            return False

        source_index = self.mapToSource(index)
        module = self.sourceModel().data(source_index, Qt.UserRole)
        if not module:
            return False

        col = index.column()
        changed = False

        if role == Qt.CheckStateRole and col == 0:
            # value is an integer (Qt.Checked/Unchecked), convert to bool
            is_checked = value == Qt.Checked or value == 2
            self.log_filter.set_module_enabled(module, is_checked)
            changed = True

        elif role == Qt.EditRole and col == 1:
            # value is the LevelIdentity object from the Delegate
            self.log_filter.set_module_level(module, value)
            changed = True

        if changed:
            # Emit dataChanged so the View repaints this specific cell
            self.dataChanged.emit(index, index, [role, Qt.DisplayRole])
            # Explicitly tell the filter to bake and notify the Log Viewer
            self.log_filter.filter_changed.emit()
            return True

        return False

    def filterAcceptsRow(self, source_row, source_parent):
        # Get the source index for the row (assuming module data is in column 0)
        source_index = self.sourceModel().index(source_row, 0, source_parent)

        # Extract the module object using your UserRole
        module = self.sourceModel().data(source_index, Qt.UserRole)

        if not module:
            # Fallback to default behavior if data is missing
            return super().filterAcceptsRow(source_row, source_parent)

        # Filter by device (using the allowed_device from your LogFilter)
        allowed_dev = self.log_filter.log_filter.allowed_device
        if allowed_dev is not None and module.device != allowed_dev:
            return False

        filtered_module = self.log_filter.log_filter.filtered_module
        if filtered_module is not None and module != filtered_module:
            return False

        # Optional: If you also use Qt's built-in text filtering (setFilterRegExp),
        # keep this call at the end so text searches still work on the remaining rows.
        return super().filterAcceptsRow(source_row, source_parent)

    def flags(self, index):
        if not index.isValid():
            return Qt.NoItemFlags

        source_index = self.mapToSource(index)
        # Get base flags from the actual hardware list model
        f = self.sourceModel().flags(source_index)

        if index.column() == 0:
            # Ensure it's checkable even if the source model isn't
            f |= Qt.ItemIsUserCheckable
        elif index.column() == 1:
            f |= Qt.ItemIsEditable

        return f


class LevelDelegate(QStyledItemDelegate):
    """Dropdown editor for the surgical Log Level column."""

    def createEditor(self, parent, option, index):
        editor = QComboBox(parent)
        for lvl in LogLevel.LIST:
            editor.addItem(lvl.name_conf, lvl)  # Store LevelIdentity in userData
        return editor

    def setEditorData(self, editor, index):
        current_text = index.data(Qt.DisplayRole)
        editor.setCurrentText(current_text)

    def setModelData(self, editor, model, index):
        level_obj = editor.currentData()
        model.setData(index, level_obj, Qt.EditRole)


class ModuleFilterTable(QTableView):
    """
    A standalone surgical filter sidebar.
    Automatically wraps the global ModuleFilterModel with a local Proxy
    linked to the provided LogFilter.
    """

    def __init__(self, gui_context, log_filter: TempLogFilter, parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self.log_filter = log_filter

        self._registered = False

        font = QFont("Consolas, monospace")
        # self.value_font.setPointSizeF(10.5)
        font.setBold(True)
        self.setFont(font)

        # Setup the Proxy specific to this Table instance
        self.proxy = ModuleFilterProxyModel(self.log_filter, self)
        # Point to the global shared model in the context
        self.proxy.setSourceModel(self.gui_context.module_filter_model)
        self.setModel(self.proxy)

        # View Styling
        self.setShowGrid(False)
        self.setFrameShape(QTableView.NoFrame)
        self.setSelectionMode(QTableView.NoSelection)
        self.setEditTriggers(QTableView.AllEditTriggers)  # Change level in one click

        # Header setup
        h_header = self.horizontalHeader()
        h_header.setSectionResizeMode(0, QHeaderView.Stretch)
        h_header.setSectionResizeMode(1, QHeaderView.Fixed)
        h_header.hide()

        self._is_hovered = False

        self.setColumnWidth(1, 90)  # Wide enough for "CRITICAL"

        v_header = self.verticalHeader()
        v_header.hide()
        v_header.setDefaultSectionSize(22)

        # Setup Delegate for the Level Column
        self.setItemDelegateForColumn(1, LevelDelegate(self))

    def showEvent(self, event):
        super().showEvent(event)
        # Register this specific instance with the global model to trigger 1Hz sync
        if not self._registered:
            self.gui_context.module_filter_model.register_consumer()
            self._registered = True

    def hideEvent(self, event):
        super().hideEvent(event)
        # Unregister to throttle the background sync if no sidebars are open
        if self._registered:
            self.gui_context.module_filter_model.unregister_consumer()
            self._registered = False
        if self._is_hovered:
            self._is_hovered = False
            self.gui_context.module_filter_model.pause_sync(False)

    def enterEvent(self, event):
        super().enterEvent(event)
        self._is_hovered = True
        self.gui_context.module_filter_model.pause_sync(True)

    def leaveEvent(self, event):
        super().leaveEvent(event)
        self._is_hovered = False
        self.gui_context.module_filter_model.pause_sync(False)

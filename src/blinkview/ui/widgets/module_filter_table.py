# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import weakref
from dataclasses import dataclass, field
from typing import List, Optional, Set

import numpy as np
from qtpy.QtCore import QAbstractTableModel, QModelIndex, QObject, QSortFilterProxyModel, Qt, QTimer, Signal
from qtpy.QtGui import QAction, QColor, QFont
from qtpy.QtWidgets import QComboBox, QHeaderView, QMenu, QStyledItemDelegate, QTableView

from blinkview.core import dtypes
from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.numba_config import app_njit
from blinkview.ops.id_registry import NO_PARENT
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.module_filter_model import FastModuleFilterModel
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LevelIdentity, LogLevel


@app_njit()
def nb_inherit_states(
    new_enabled: np.ndarray,
    new_level: np.ndarray,
    new_filter: np.ndarray,
    parent_array: np.ndarray,
    start_idx: int,
    end_idx: int,
):
    """
    Fast-path inheritance for newly allocated UI masks.
    Mutates new_enabled and new_level arrays in place.
    """
    for i in range(start_idx, end_idx):
        parent_id = parent_array[i]

        # Chronological guarantee: parent_id must be < i
        if parent_id != NO_PARENT and parent_id < i:
            new_enabled[i] = new_enabled[parent_id]
            new_level[i] = new_level[parent_id]
            new_filter[i] = new_filter[parent_id]


@app_njit()
def nb_update_subtree(
    enabled_mask: np.ndarray,
    level_mask: np.ndarray,
    filter_mask: np.ndarray,
    parent_array: np.ndarray,
    root_id: int,
    count: int,
    update_enabled: bool,
    new_enabled: bool,
    update_level: bool,
    new_level: int,
    off_value: int,
):
    """
    Updates the enabled state and/or log level for a root module and all its descendants.
    Exploits the chronological property (parent_id < child_id) for a single-pass update.
    """
    # Track which modules are part of the subtree
    is_in_subtree = np.zeros(count, dtype=np.bool_)
    is_in_subtree[root_id] = True

    for i in range(root_id, count):
        p_id = parent_array[i]

        # If this node is the root OR its parent is in our subtree mask
        if i == root_id or (p_id != -1 and is_in_subtree[p_id]):
            is_in_subtree[i] = True

            if update_enabled:
                enabled_mask[i] = new_enabled
            if update_level:
                level_mask[i] = new_level

            # Update the optimized baked mask
            if enabled_mask[i]:
                filter_mask[i] = level_mask[i]
            else:
                filter_mask[i] = off_value


class TempLogFilter(QObject):
    filter_changed = Signal()

    def __init__(self, gui_context, log_filter):
        super().__init__()
        self.gui_context = gui_context
        self.log_filter = log_filter
        self.registry = gui_context.id_registry

        # Start with current capacity
        cap = self.registry._parent_capacity
        self.enabled_mask = np.ones(cap, dtype=np.bool_)
        self.level_mask = np.full(cap, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)

        # The optimized combined mask
        self.filter_mask = np.full(cap, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)

        self.enabled = False

        # Track how many modules we've run inheritance on
        self._initialized_count = 0

    def ensure_capacity(self, target_count: int):
        current_cap = len(self.enabled_mask)

        if target_count > current_cap:
            new_cap = max(target_count, current_cap * 2)

            new_enabled = np.ones(new_cap, dtype=np.bool_)
            new_enabled[:current_cap] = self.enabled_mask

            new_level = np.full(new_cap, LogLevel.ALL.value, dtype=np.uint8)
            new_level[:current_cap] = self.level_mask

            # NEW: Resize filter mask
            new_filter = np.full(new_cap, LogLevel.ALL.value, dtype=np.uint8)
            new_filter[:current_cap] = self.filter_mask

            self.enabled_mask = new_enabled
            self.level_mask = new_level
            self.filter_mask = new_filter

        if target_count > self._initialized_count:
            nb_inherit_states(
                self.enabled_mask,
                self.level_mask,
                self.filter_mask,  # <-- NEW
                self.registry._parent_array,
                self._initialized_count,
                target_count,
            )
            self._initialized_count = target_count

    def set_module_enabled(self, module_id: int, enabled: bool):
        self.ensure_capacity(module_id + 1)
        self.enabled_mask[module_id] = enabled
        self.filter_mask[module_id] = self.level_mask[module_id] if enabled else LogLevel.OFF.value
        self.filter_changed.emit()

    def set_module_level(self, module_id: int, level: LevelIdentity):
        self.ensure_capacity(module_id + 1)
        self.level_mask[module_id] = level.value
        self.filter_mask[module_id] = level.value if self.enabled_mask[module_id] else LogLevel.OFF.value
        self.filter_changed.emit()

    def set_enabled(self, enabled: bool):
        """Toggles the global state of this specific filter tab."""
        self.enabled = enabled
        print(f"[TempLogFilter] set_enabled: {self.enabled}")

        # This signal will trigger your Numba backend to re-evaluate the active masks
        self.filter_changed.emit()

    def set_level(self, level_obj):
        """Mass-updates the log level for ALL known modules."""
        # Vectorized assignment sets every element in the array instantly
        self.level_mask[:] = level_obj.value

        self.filter_mask[:] = np.where(self.enabled_mask, self.level_mask, LogLevel.OFF.value)

        # Emit the update for the backend to catch
        self.filter_changed.emit()

    def get_state(self):
        """
        Serializes the current filter state.
        Only saves modules that deviate from the default state (acting as our 'override' check).
        """
        state = {}
        enabled_mask = self.enabled_mask
        level_mask = self.level_mask
        module_from_int = self.registry.module_from_int
        level_from_value = LogLevel.from_value

        loglevel_all_value = LogLevel.ALL.value

        for mod_id in range(self._initialized_count):
            is_enabled = bool(enabled_mask[mod_id])
            level_val = level_mask[mod_id]

            # If it deviates from the default state, we save it
            if not is_enabled or level_val != loglevel_all_value:
                module = module_from_int(mod_id)
                m_d_id = module.name_with_device()
                level = level_from_value(level_val)
                # print(
                #     f"TempLogFilter: mod_id={mod_id} name='{m_d_id}' is_enabled={is_enabled} level_val={level_val} level={level}"
                # )

                state[m_d_id] = {"enabled": is_enabled, "level": level.name_conf}

        # print(f"TempLogFilter: state={json.dumps(state, indent=4)}")
        return state

    def restore_state(self, state):
        """Restores state from a serialized dictionary directly into NumPy masks."""
        if not state:
            return

        for path_str, mod_state in state.items():
            module = self.registry.resolve_module(path_str)
            if not module:
                print(f"[TempLogFilter] Warning: Module '{path_str}' not found during state restore.")
                continue

            # Ensure our arrays are big enough to hold this module ID
            mod_id = module.id
            self.ensure_capacity(mod_id + 1)

            # Extract the saved state
            is_enabled = mod_state.get("enabled", True)
            level_obj = LogLevel.from_string(mod_state.get("level"), default=LogLevel.ALL)
            level_val = level_obj.value

            print(f"TempLogFilter.restore: mod_id={mod_id} is_enabled={is_enabled} level_val={level_val}")

            # Update the NumPy masks directly
            self.enabled_mask[mod_id] = is_enabled
            self.level_mask[mod_id] = level_val

            self.filter_mask[mod_id] = level_val if is_enabled else LogLevel.OFF.value

        print(
            f"TempLogFilter.restored: enabled_mask={self.enabled_mask[: self._initialized_count]} level_mask={self.level_mask[: self._initialized_count]}"
        )

        # Emit once after all restorations are complete
        self.filter_changed.emit()

    def sync_modules(self):
        """Instantly aligns the internal NumPy arrays with the global registry count."""
        self.ensure_capacity(self.registry.module_count())

    def get_filter(self):
        return self.enabled, self.filter_mask

    def set_subtree_enabled(self, root_module_id: int, enabled: bool):
        """Recursively enables/disables a module and all its children."""
        self.ensure_capacity(root_module_id + 1)
        nb_update_subtree(
            self.enabled_mask,
            self.level_mask,
            self.filter_mask,
            self.registry._parent_array,
            root_module_id,
            self._initialized_count,
            update_enabled=True,
            new_enabled=enabled,
            update_level=False,
            new_level=0,
            off_value=LogLevel.OFF.value,
        )
        self.filter_changed.emit()

    def set_subtree_level(self, root_module_id: int, level: LevelIdentity):
        """Recursively sets the log level for a module and all its children."""
        self.ensure_capacity(root_module_id + 1)
        nb_update_subtree(
            self.enabled_mask,
            self.level_mask,
            self.filter_mask,
            self.registry._parent_array,
            root_module_id,
            self._initialized_count,
            update_enabled=False,
            new_enabled=False,
            update_level=True,
            new_level=level.value,
            off_value=LogLevel.OFF.value,
        )
        self.filter_changed.emit()


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
    Directly uses the FastModuleFilterModel backed by NumPy arrays.
    """

    sync_paused = Signal(bool)

    def __init__(self, gui_context, log_filter: "TempLogFilter", parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self.log_filter = log_filter

        # Font setup
        font = QFont("Consolas, monospace")
        font.setBold(True)
        self.setFont(font)

        # 1. Bypass Proxy, create and set Fast Model directly
        self.fast_model = FastModuleFilterModel(gui_context.id_registry, log_filter, self)
        self.setModel(self.fast_model)

        # 2. Trigger the initial sync to build the rows
        self.fast_model.sync_registry(allowed_device=self.log_filter.log_filter.allowed_device)

        # View Styling
        self.setShowGrid(False)
        self.setFrameShape(QTableView.NoFrame)
        self.setSelectionMode(QTableView.NoSelection)
        self.setEditTriggers(QTableView.AllEditTriggers)

        # Headers
        h_header = self.horizontalHeader()
        h_header.setSectionResizeMode(0, QHeaderView.Stretch)
        h_header.setSectionResizeMode(1, QHeaderView.Fixed)
        h_header.hide()

        self.setColumnWidth(1, 90)

        v_header = self.verticalHeader()
        v_header.hide()
        v_header.setDefaultSectionSize(22)

        # Setup Delegate
        self.setItemDelegateForColumn(1, LevelDelegate(self))

        # --- Localized Sync Logic ---
        self._is_hovered = False

        # We use a local timer instead of a global consumer list.
        # It only runs when the widget is visible.
        self._sync_timer = QTimer(self)
        self._sync_timer.setInterval(1000)  # 1Hz
        self._sync_timer.timeout.connect(self.check_for_new_modules)

        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_context_menu)

    def check_for_new_modules(self):
        """Lightweight check to see if we need to rebuild the UI list."""
        is_editing = self.state() == QTableView.EditingState

        if self._is_hovered or is_editing:
            # Qt quirk: If the dropdown is open, the table receives a leaveEvent.
            # We re-emit True here to guarantee the "Paused" label stays visible
            # while the user is interacting with the menu.
            if is_editing:
                self.sync_paused.emit(True)
            return

        current_registry_count = self.gui_context.id_registry.module_count()

        # Compare against the model's known count, NOT the array capacity!
        if current_registry_count > self.fast_model.known_module_count:
            allowed_dev = self.log_filter.log_filter.allowed_device
            self.fast_model.sync_registry(allowed_device=allowed_dev)

    # --- Visibility Events: Start/Stop the Timer ---
    def showEvent(self, event):
        super().showEvent(event)
        # Only poll when the sidebar is actually open
        self._sync_timer.start()

    def hideEvent(self, event):
        super().hideEvent(event)
        self._sync_timer.stop()
        if self._is_hovered:
            self._is_hovered = False
            # Ensure it clears if the widget hides while hovered
            self.sync_paused.emit(False)

    # --- 2. Emit the signal on hover ---
    def enterEvent(self, event):
        super().enterEvent(event)
        self._is_hovered = True
        self.sync_paused.emit(True)

    def leaveEvent(self, event):
        super().leaveEvent(event)
        self._is_hovered = False
        self.sync_paused.emit(False)

    def _show_context_menu(self, pos):
        index = self.indexAt(pos)
        if not index.isValid():
            return

        # IMPORTANT: Extract the module_id for the clicked row.
        # If FastModuleFilterModel exposes the ID via UserRole:
        module_id = self.fast_model.data(self.fast_model.index(index.row(), 0), Qt.UserRole)

        # If your model doesn't use UserRole, you might need a direct method like:
        # module_id = self.fast_model.get_module_id_at_row(index.row())

        if module_id is None:
            return

        menu = QMenu(self)
        menu.addSection("Module + Children")
        # Action: Enable Subtree
        action_enable = QAction("Enable", self)
        action_enable.triggered.connect(lambda: self.log_filter.set_subtree_enabled(module_id, True))
        menu.addAction(action_enable)

        # Action: Disable Subtree
        action_disable = QAction("Disable", self)
        action_disable.triggered.connect(lambda: self.log_filter.set_subtree_enabled(module_id, False))
        menu.addAction(action_disable)

        menu.addSeparator()

        # Submenu: Set Level for Subtree
        level_menu = menu.addMenu("Set Level")
        for lvl in LogLevel.LIST:
            action_lvl = QAction(lvl.name_conf, self)

            # The lambda parameter capture (l=lvl) is crucial here so python doesn't bind
            # to the last item in the loop for every action.
            action_lvl.triggered.connect(lambda checked=False, l=lvl: self.log_filter.set_subtree_level(module_id, l))
            level_menu.addAction(action_lvl)

        # Show the menu at the cursor position
        menu.exec_(self.viewport().mapToGlobal(pos))

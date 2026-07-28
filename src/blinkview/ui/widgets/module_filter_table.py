# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


import numpy as np
from qtpy.QtCore import QObject, Qt, QTimer, Signal
from qtpy.QtGui import QAction, QFont
from qtpy.QtWidgets import QComboBox, QHeaderView, QMenu, QStyledItemDelegate, QTableView

from blinkview.core import dtypes
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.id_registry import NO_PARENT
from blinkview.ops.module_filter import nb_inherit_states, nb_rebuild_from_explicit, nb_update_subtree
from blinkview.ui.widgets.module_filter_model import FastModuleFilterModel
from blinkview.utils.log_level import LevelIdentity, LogLevel


class TempLogFilter(QObject):
    filter_changed = Signal()

    def __init__(self, gui_context, log_filter):
        super().__init__()
        self.gui_context = gui_context
        self.log_filter = log_filter
        self.registry = gui_context.id_registry

        # Start with current capacity
        cap = self.registry._parent_capacity

        is_constrained = self.log_filter.filtered_module is not None
        start_enabled = not is_constrained

        self.show_hidden = False

        self.enabled_mask = np.full(cap, start_enabled, dtype=np.bool_)
        self.level_mask = np.full(cap, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)

        # 1. Grab the real essential mask IMMEDIATELY
        self.essential_mask = np.zeros(cap, dtype=np.bool_)
        reg_cap = len(self.registry._essential_array)
        copy_len = min(cap, reg_cap)
        self.essential_mask[:copy_len] = self.registry._essential_array[:copy_len]

        # 2. Bake the initial filter_mask correctly using the essential mask
        if is_constrained:
            self.filter_mask = np.full(cap, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
        else:
            self.filter_mask = np.where(self.essential_mask, LogLevel.ALL.value, LogLevel.OFF.value).astype(
                dtypes.LEVEL_TYPE
            )

        self.enabled = False
        self._initialized_count = 0

        # If constrained, immediately enable only the allowed subtree/module
        if is_constrained:
            self._apply_tab_constraints()

    def set_show_hidden(self, show: bool):
        if self.show_hidden == show:
            return

        self.show_hidden = show
        old_mask = self.filter_mask.copy()

        # Instantly re-bake the entire filter mask in C using NumPy
        active_mask = self.enabled_mask & (self.show_hidden | self.essential_mask)
        self.filter_mask[:] = np.where(active_mask, self.level_mask, LogLevel.OFF.value)

        # If the mask changed, emit to redraw the log viewer immediately
        if not np.array_equal(old_mask, self.filter_mask):
            self.filter_changed.emit()

    def _apply_tab_constraints(self):
        """Force the masks to respect the tab's module/subtree limits."""
        if not (m := self.log_filter.filtered_module):
            return

        self.ensure_capacity(m.id + 1)

        if self.log_filter.filtered_module_children:
            # Tab allows the whole subtree; enable it in the sidebar mask
            self.set_subtree_enabled(m.id, True)
        else:
            # Tab allows only this module; enable only this ID
            self.set_module_enabled(m.id, True)

    def ensure_capacity(self, target_count: int):
        current_cap = len(self.enabled_mask)

        # Only run this block if we actually need to allocate larger arrays
        if target_count > current_cap:
            new_cap = max(target_count, current_cap * 2)

            new_enabled = np.ones(new_cap, dtype=np.bool_)
            new_enabled[:current_cap] = self.enabled_mask

            new_level = np.full(new_cap, LogLevel.ALL.value, dtype=np.uint8)
            new_level[:current_cap] = self.level_mask

            new_filter = np.full(new_cap, LogLevel.ALL.value, dtype=np.uint8)
            new_filter[:current_cap] = self.filter_mask

            # Default to false for safety when resizing
            new_essential = np.zeros(new_cap, dtype=np.bool_)
            new_essential[:current_cap] = self.essential_mask

            self.enabled_mask = new_enabled
            self.level_mask = new_level
            self.filter_mask = new_filter
            self.essential_mask = new_essential

        # THIS block runs anytime there are new modules to process,
        # even if we didn't need to resize the arrays above.
        if target_count > self._initialized_count:
            # Sync essential flags from registry for the newly initialized chunk
            self.essential_mask[self._initialized_count : target_count] = self.registry._essential_array[
                self._initialized_count : target_count
            ]

            nb_inherit_states(
                self.enabled_mask,
                self.level_mask,
                self.filter_mask,
                self.registry._parent_array,
                self.essential_mask,
                self.show_hidden,
                self._initialized_count,
                target_count,
                LogLevel.OFF.value,
            )

            self._initialized_count = target_count

    def set_module_enabled(self, module_id: int, enabled: bool):
        self.ensure_capacity(module_id + 1)
        self.enabled_mask[module_id] = enabled

        # Calculate new filter val respecting essential/hidden logic
        is_active = enabled and (self.show_hidden or self.essential_mask[module_id])
        new_filter_val = self.level_mask[module_id] if is_active else LogLevel.OFF.value

        if self.filter_mask[module_id] != new_filter_val:
            self.filter_mask[module_id] = new_filter_val
            self.filter_changed.emit()

    def set_module_level(self, module_id: int, level: LevelIdentity):
        self.ensure_capacity(module_id + 1)
        if self.level_mask[module_id] == level.value:
            return
        self.level_mask[module_id] = level.value

        is_active = self.enabled_mask[module_id] and (self.show_hidden or self.essential_mask[module_id])
        if is_active:
            self.filter_mask[module_id] = level.value
            self.filter_changed.emit()

    def set_enabled(self, enabled: bool):
        """Toggles the global state of this specific filter tab."""
        if self.enabled == enabled:
            return

        self.enabled = enabled
        print(f"[TempLogFilter] set_enabled: {self.enabled}")

        # This signal will trigger your Numba backend to re-evaluate the active masks
        self.filter_changed.emit()

    def get_state(self):
        """
        Serializes the current filter state efficiently.
        Only saves modules whose state DIFFERS from their inherited parent state.
        """
        state = {}
        enabled_mask = self.enabled_mask
        level_mask = self.level_mask
        parent_array = self.registry._parent_array
        module_from_int = self.registry.module_from_int

        # Determine global defaults for root modules
        is_constrained = self.log_filter.filtered_module is not None
        default_enabled = not is_constrained
        default_level = LogLevel.ALL.value

        for i in range(self._initialized_count):
            p_id = parent_array[i]

            # What would this module naturally inherit?
            if p_id != NO_PARENT and p_id < i:
                expected_enabled = enabled_mask[p_id]
                expected_level = level_mask[p_id]
            else:
                expected_enabled = default_enabled
                expected_level = default_level

            actual_enabled = enabled_mask[i]
            actual_level = level_mask[i]

            # If it deviates from its inherited expectation, it's an explicit override. Save it.
            if actual_enabled != expected_enabled or actual_level != expected_level:
                module = module_from_int(i)
                if module:
                    m_d_id = module.name_with_device()
                    level = LogLevel.from_value(actual_level)
                    state[m_d_id] = {"enabled": bool(actual_enabled), "level": level.name_conf}

        return state

    def restore_state(self, state):
        """Restores state efficiently by applying explicit overrides and inheriting the rest."""
        if not state:
            return

        is_constrained = self.log_filter.filtered_module is not None
        default_enabled = not is_constrained
        default_level = LogLevel.ALL.value

        # 1. Identify which modules have explicit overrides mapped to their integer ID
        explicit_states = {}
        max_id = self._initialized_count - 1  # Track max ID to expand arrays if needed

        for path_str, mod_state in state.items():
            module = self.registry.resolve_module(path_str)
            if not module:
                print(f"[TempLogFilter] Warning: Module '{path_str}' not found during state restore.")
                continue

            explicit_states[module.id] = mod_state
            if module.id > max_id:
                max_id = module.id

        # Ensure our arrays are big enough to hold the highest restored module ID
        self.ensure_capacity(max_id + 1)

        # 2. Create an explicit mask to feed to Numba
        explicit_mask = np.zeros(self._initialized_count, dtype=np.bool_)

        for mod_id, mod_state in explicit_states.items():
            explicit_mask[mod_id] = True
            self.enabled_mask[mod_id] = mod_state.get("enabled", True)

            level_obj = LogLevel.from_string(mod_state.get("level"), default=LogLevel.ALL)
            self.level_mask[mod_id] = level_obj.value

        # 3. Use a fast Numba pass to rebuild the arrays based on inheritance
        nb_rebuild_from_explicit(
            self.enabled_mask,
            self.level_mask,
            self.filter_mask,
            self.registry._parent_array,
            explicit_mask,
            self.registry._essential_array,
            self.show_hidden,
            self._initialized_count,
            default_enabled,
            default_level,
            LogLevel.OFF.value,
        )

        # Emit once after all restorations and inheritances are complete
        self.filter_changed.emit()

    def sync_modules(self):
        """Instantly aligns the internal NumPy arrays with the global registry count."""
        self.ensure_capacity(self.registry.module_count())

    def get_filter(self):
        return self.enabled, self.filter_mask

    def set_subtree_enabled(self, root_module_id: int, enabled: bool):
        self.ensure_capacity(root_module_id + 1)
        old_mask = self.filter_mask.copy()

        nb_update_subtree(
            self.enabled_mask,
            self.level_mask,
            self.filter_mask,
            self.registry._parent_array,
            self.registry._essential_array,
            self.show_hidden,
            root_module_id,
            self._initialized_count,
            update_enabled=True,
            new_enabled=enabled,
            update_level=False,
            new_level=0,
            off_value=LogLevel.OFF.value,
        )

        if not np.array_equal(old_mask, self.filter_mask):
            self.filter_changed.emit()

    def set_subtree_level(self, root_module_id: int, level: LevelIdentity):
        self.ensure_capacity(root_module_id + 1)
        old_mask = self.filter_mask.copy()

        nb_update_subtree(
            self.enabled_mask,
            self.level_mask,
            self.filter_mask,
            self.registry._parent_array,
            self.registry._essential_array,
            self.show_hidden,
            root_module_id,
            self._initialized_count,
            update_enabled=False,
            new_enabled=False,
            update_level=True,
            new_level=level.value,
            off_value=LogLevel.OFF.value,
        )

        if not np.array_equal(old_mask, self.filter_mask):
            self.filter_changed.emit()

    def reset_all(self):
        old_mask = self.filter_mask.copy()
        is_constrained = self.log_filter.filtered_module is not None
        self.enabled_mask[:] = not is_constrained
        self.level_mask[:] = LogLevel.ALL.value

        if is_constrained:
            self._apply_tab_constraints()
        else:
            # Respect the show_hidden toggle during reset
            active = self.show_hidden | self.essential_mask
            self.filter_mask[:] = np.where(active, LogLevel.ALL.value, LogLevel.OFF.value)

        if not np.array_equal(old_mask, self.filter_mask):
            self.filter_changed.emit()

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for nb_inherit_states, nb_update_subtree, and
        nb_rebuild_from_explicit against a small dummy mask set, mirroring how a real
        TempLogFilter builds/updates its enabled/level/filter arrays."""
        print("[Warmup] TempLogFilter ...")

        registry = helper.registry
        count = max(registry._parent_capacity, registry.module_count())

        enabled_mask = np.ones(count, dtype=np.bool_)
        level_mask = np.full(count, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)
        filter_mask = np.full(count, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)
        essential_mask = np.zeros(count, dtype=np.bool_)
        essential_mask[: len(registry._essential_array)] = registry._essential_array

        nb_inherit_states(
            enabled_mask,
            level_mask,
            filter_mask,
            registry._parent_array,
            essential_mask,
            False,
            0,
            count,
            LogLevel.OFF.value,
        )

        nb_update_subtree(
            enabled_mask,
            level_mask,
            filter_mask,
            registry._parent_array,
            essential_mask,
            False,
            0,
            count,
            update_enabled=True,
            new_enabled=True,
            update_level=True,
            new_level=LogLevel.ALL.value,
            off_value=LogLevel.OFF.value,
        )

        explicit_mask = np.zeros(count, dtype=np.bool_)
        nb_rebuild_from_explicit(
            enabled_mask,
            level_mask,
            filter_mask,
            registry._parent_array,
            explicit_mask,
            essential_mask,
            False,
            count,
            True,
            LogLevel.ALL.value,
            LogLevel.OFF.value,
        )

        print("[Warmup] TempLogFilter ... done")


class LevelDelegate(QStyledItemDelegate):
    """Dropdown editor for the surgical Log Level column."""

    def createEditor(self, parent, option, index):
        editor = QComboBox(parent)
        for lvl in LogLevel.LIST_UI:
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

    def __init__(self, gui_context, log_filter: "TempLogFilter", show_hidden=False, parent=None):
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

        self.show_non_essential = show_hidden

        # 2. Trigger the initial sync to build the rows
        f = self.log_filter.log_filter
        self.fast_model.sync_registry(
            allowed_device=f.allowed_device,
            root_module=f.filtered_module,
            include_children=f.filtered_module_children,
            show_non_essential=self.show_non_essential,
        )

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

        reg = self.gui_context.id_registry
        if reg.module_count() > self.fast_model.known_module_count:
            f = self.log_filter.log_filter  # Access the underlying LogFilter

            # Pass all constraints to the model so it can filter the row list
            self.fast_model.sync_registry(
                allowed_device=f.allowed_device,
                root_module=f.filtered_module,
                include_children=f.filtered_module_children,
                show_non_essential=self.show_non_essential,
            )

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

        # 1. Extract the module_id from the model
        module_id = self.fast_model.data(self.fast_model.index(index.row(), 0), Qt.UserRole)
        if module_id is None:
            return

        # 2. Resolve the module name from the registry
        module_obj = self.gui_context.id_registry.module_from_int(module_id)
        module_name = module_obj.name_with_device() if module_obj else f"ID: {module_id}"

        menu = QMenu(self)

        header_action = QAction(module_name, self)
        header_action.setEnabled(False)  # Makes it non-clickable and styled as a label
        menu.addAction(header_action)

        menu.addSeparator()

        subtree_header = QAction("Apply to subtree", self)
        subtree_header.setEnabled(False)
        # Optional: You can even set a specific font to make it look "header-y"
        # f = subtree_header.font(); f.setPointWeight(QFont.Bold); subtree_header.setFont(f)
        menu.addAction(subtree_header)

        # Action: Enable Subtree
        action_enable = QAction("Enable All", self)
        action_enable.triggered.connect(lambda: self.log_filter.set_subtree_enabled(module_id, True))
        menu.addAction(action_enable)

        # Action: Disable Subtree
        action_disable = QAction("Disable All", self)
        action_disable.triggered.connect(lambda: self.log_filter.set_subtree_enabled(module_id, False))
        menu.addAction(action_disable)

        menu.addSeparator()

        # Submenu: Set Level for Subtree
        level_menu = menu.addMenu("Set Level")
        for lvl in LogLevel.LIST_UI:
            action_lvl = QAction(lvl.name_conf, self)
            action_lvl.triggered.connect(lambda checked=False, l=lvl: self.log_filter.set_subtree_level(module_id, l))
            level_menu.addAction(action_lvl)

        menu.addSeparator()

        # Create a submenu for Reset
        reset_menu = menu.addMenu("Global Reset...")

        # Add a confirmation action inside the submenu
        action_confirm_reset = QAction("Confirm: Reset All Modules", self)
        action_confirm_reset.setToolTip("Enable all modules and set levels to ALL")
        action_confirm_reset.triggered.connect(self.log_filter.reset_all)
        reset_menu.addAction(action_confirm_reset)

        # Show the menu at the cursor position
        menu.exec_(self.viewport().mapToGlobal(pos))

    def set_show_non_essential(self, show: bool):
        self.show_non_essential = show

        # Force a rebuild of the list immediately
        f = self.log_filter.log_filter
        self.fast_model.sync_registry(
            allowed_device=f.allowed_device,
            root_module=f.filtered_module,
            include_children=f.filtered_module_children,
            show_non_essential=self.show_non_essential,
        )

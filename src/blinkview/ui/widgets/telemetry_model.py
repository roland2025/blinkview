# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter

from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt, Signal
from PySide6.QtGui import QColor

from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from dataclasses import dataclass

from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.action_button_delegate import TelemetryCol


@dataclass(slots=True)
class TelemetryRowState:
    module: 'ModuleIdentity'
    last_painted_row: 'LogRow | None' = None
    last_change_time: float = 0.0
    last_arrival_time: float = 0.0


class TelemetryModel(QAbstractTableModel):
    layout_changed = Signal()

    def __init__(self, gui_context, parent=None):
        super().__init__(parent)
        self.context: GUIContext = gui_context

        # A single, unified list of states
        self._row_states: list[TelemetryRowState] = []
        self._registered_views = set()
        self._active_cache = []  # Fast-access [(idx, state), ...]

        self._known_module_ids: set[int] = set()

        self.COLOR_STALE = QColor(120, 120, 120, 200)
        self.COLOR_NAME = QColor(200, 200, 200)
        self.COLOR_DEFAULT_VAL = QColor(255, 255, 255)

        self.sync_registry()

    def register_view(self, view):
        self._registered_views.add(view)
        self.sync_registry()

    def unregister_view(self, view):
        self._registered_views.discard(view)
        if not self._registered_views:
            self._active_cache = []
        else:
            self.refresh_active_cache()

    def refresh_active_cache(self):
        """
        The 'Pull' triggered by the View.
        Rebuilds the deduplicated cache of objects to process.
        """
        all_indices = set()
        for view in self._registered_views:
            # View provides its current filtered/visible source indices
            all_indices.update(view.get_active_indices())
        VAL_COL = TelemetryCol.VALUE
        # Pre-map the integers to the actual RowState objects for the 30fps loop
        self._active_cache = [
            (self._row_states[i], self.index(i, VAL_COL))
            for i in sorted(all_indices)
            if i < len(self._row_states)
        ]

    def sync_registry(self):
        current_modules = self.context.id_registry.module_list

        # 1. Fast-path exit if nothing changed
        if len(current_modules) == len(self._row_states):
            return

        # 2. Identify newly discovered modules
        new_states = []
        for m in current_modules:
            if m.id not in self._known_module_ids:
                new_states.append(TelemetryRowState(module=m))
                self._known_module_ids.add(m.id)

        # 3. Surgically insert them into the Qt View
        if new_states:
            first_new_idx = len(self._row_states)
            last_new_idx = first_new_idx + len(new_states) - 1

            # This tells Qt to allocate space at the bottom of the table
            self.beginInsertRows(QModelIndex(), first_new_idx, last_new_idx)
            self._row_states.extend(new_states)
            self.endInsertRows()

            self.layout_changed.emit()

        self.refresh_active_cache()

    def apply_updates(self):
        """
        The Ultra-Fast Loop: Zero lookups, zero index creation.
        """
        if not self._registered_views or not self._active_cache:
            return

        now = perf_counter()
        theme = self.context.theme
        fade_dur = theme.fade_duration
        stale_limit = theme.stale_threshold
        data_changed_emit = self.dataChanged.emit
        buffer = 0.1

        for state, val_idx in self._active_cache:
            current_row = state.module.latest_row
            if not current_row:
                continue

            # --- ARRIVAL CHECK ---
            # Did a new object arrive, regardless of content?
            is_new_arrival = (state.last_painted_row is not current_row)

            if is_new_arrival:
                # 1. Update arrival time to prevent 'Stale' color
                state.last_arrival_time = now

                # 2. Check for CONTENT change to trigger 'Flash'
                content_changed = (state.last_painted_row is None or
                                   current_row.message != state.last_painted_row.message)

                if content_changed:
                    state.last_change_time = now  # This triggers the Delegate flash

                state.last_painted_row = current_row
                data_changed_emit(val_idx, val_idx)
                continue

            # --- ANIMATION / STALE REFRESH ---
            # We need to keep emitting while the flash is active OR
            # when it's about to transition into the stale state.
            elapsed_flash = now - state.last_change_time
            elapsed_stale = now - state.last_arrival_time

            if elapsed_flash <= (fade_dur + buffer) or (stale_limit <= elapsed_stale <= stale_limit + buffer):
                data_changed_emit(val_idx, val_idx)

    def rowCount(self, parent=QModelIndex()):
        # Qt needs to know this to draw the scrollbars and manage memory
        return len(self._row_states)

    def columnCount(self, parent=QModelIndex()):
        # 0: Name, 1: Value, 2: Actions (Buttons)
        return 4

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        state = self._row_states[index.row()]

        if role == Qt.DisplayRole:
            if index.column() == TelemetryCol.DEVICE:
                return state.module.device.name
            if index.column() == TelemetryCol.NAME:
                return str(state.module.name)
            if index.column() == TelemetryCol.VALUE:
                return state.last_painted_row.message if state.last_painted_row else "---"

        # All other roles (Foreground, Background, Alignment)
        # are now handled by the Delegate's paint() method.
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if section == TelemetryCol.NAME:
                return "Module"
            elif section == TelemetryCol.VALUE:
                return "Value"
            elif section == TelemetryCol.DEVICE:
                return "Device"
            # elif section == TelemetryCol.TIMESTAMP:
            #     return "Last Update"
            elif section == TelemetryCol.ACTIONS:
                return "Actions"
        return super().headerData(section, orientation, role)

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from time import perf_counter

from qtpy.QtCore import QAbstractTableModel, QModelIndex, Qt, Signal
from qtpy.QtGui import QColor

from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from blinkview.core.module_snapshot import ModuleSnapshot
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.action_button_delegate import TelemetryCol


@dataclass(slots=True)
class TelemetryRowState:
    module: "ModuleIdentity"

    last_painted_seq: int = 0
    last_painted_msg: str = ""
    last_painted_level: int = 0
    last_change_time: float = 0.0
    last_arrival_time: float = 0.0

    val_index: QModelIndex = None


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

        self.tracker = None

        self.prev_apply = perf_counter()

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
        self._active_cache = [(self._row_states[i], i) for i in sorted(all_indices) if i < len(self._row_states)]

    def sync_registry(self):
        current_modules = self.context.id_registry.module_list

        # Fast-path exit if nothing changed
        if len(current_modules) == len(self._row_states):
            return

        # Identify newly discovered modules
        new_states = []
        for m in current_modules:
            if m.id not in self._known_module_ids:
                new_states.append(TelemetryRowState(module=m))
                self._known_module_ids.add(m.id)

        # Surgically insert them into the Qt View
        if new_states:
            first_new_idx = len(self._row_states)
            last_new_idx = first_new_idx + len(new_states) - 1

            # This tells Qt to allocate space at the bottom of the table
            self.beginInsertRows(QModelIndex(), first_new_idx, last_new_idx)
            self._row_states.extend(new_states)

            for i in range(first_new_idx, last_new_idx + 1):
                state = self._row_states[i]
                state.val_index = self.createIndex(i, TelemetryCol.VALUE)

            self.endInsertRows()

            self.layout_changed.emit()

        self.refresh_active_cache()

    def apply_updates(self):
        """
        The Ultra-Fast Loop with diagnostic prints to track update flow.
        """

        now = perf_counter()
        if now - self.prev_apply < 0.1:  # Target ~30Hz (1/30 = 0.033s)
            return
        self.prev_apply = now

        # self.sync_registry()

        # 2. Local snapshot of the cache to prevent iteration crashes
        cache_snapshot = self._active_cache
        if not cache_snapshot:
            return

        tracker = self.tracker
        if tracker is None:
            tracker = self.tracker = self.context.registry.module_value_tracker

        now = perf_counter()
        theme = self.context.theme
        fade_dur = theme.fade_duration
        stale_limit = theme.stale_threshold
        data_changed_emit = self.dataChanged.emit
        buffer = 0.02

        with tracker.get_snapshot() as snap:
            for state, row_idx in cache_snapshot:
                mod_id = state.module.id
                current_seq = snap.get_sequence(mod_id)

                if current_seq == 0:
                    continue

                # --- ARRIVAL CHECK ---
                if current_seq > state.last_painted_seq:
                    state.last_arrival_time = now
                    msg = snap.get_message(mod_id)
                    level = snap.get_level(mod_id)

                    if msg != state.last_painted_msg:
                        state.last_change_time = now
                        state.last_painted_msg = msg

                    state.last_painted_seq = current_seq
                    state.last_painted_level = level

                    # Emit for this specific row
                    idx = state.val_index
                    data_changed_emit(idx, idx)
                    continue

                # --- ANIMATION CHECK ---
                elapsed_flash = now - state.last_change_time
                elapsed_stale = now - state.last_arrival_time

                if elapsed_flash <= (fade_dur + buffer) or (stale_limit <= elapsed_stale <= stale_limit + buffer):
                    idx = state.val_index
                    data_changed_emit(idx, idx)

    def rowCount(self, parent=QModelIndex()):
        # Qt needs to know this to draw the scrollbars and manage memory
        return len(self._row_states)

    def columnCount(self, parent=QModelIndex()):
        # 0: Name, 1: Value, 2: Actions (Buttons)
        return 3

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        state = self._row_states[index.row()]

        if role == Qt.DisplayRole:
            col = index.column()
            if col == TelemetryCol.DEVICE:
                return state.module.device.name
            elif col == TelemetryCol.NAME:
                return str(state.module.name)
            elif col == TelemetryCol.VALUE:
                # Return the cached string
                return state.last_painted_msg if state.last_painted_seq > 0 else "---"

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if orientation == Qt.Horizontal and role == Qt.DisplayRole:
            if section == TelemetryCol.NAME:
                return "Module"
            elif section == TelemetryCol.VALUE:
                return "Value"
            elif section == TelemetryCol.DEVICE:
                return "Device"
            elif section == TelemetryCol.ACTIONS:
                return "Actions"
        return super().headerData(section, orientation, role)

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from time import perf_counter, perf_counter_ns
from typing import TYPE_CHECKING, Optional

import numpy as np
from PySide6.QtWidgets import QToolButton
from qtpy.QtCore import (
    QAbstractTableModel,
    QEvent,
    QMimeData,
    QModelIndex,
    QSize,
    Qt,
    QTimer,
    Signal,
)
from qtpy.QtGui import QAction, QColor, QDrag, QFont, QPainter, QPixmap
from qtpy.QtWidgets import (
    QApplication,
    QHeaderView,
    QLineEdit,
    QMenu,
    QTableView,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from blinkview.core import dtypes
from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.core.module_snapshot import MAX_MSG_BYTES, LatestModuleValueTracker
from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.playback_follow import ClockSnapshot, FollowActionKind, FollowEvent, PlaybackFollowMachine
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.telemetry_table import nb_initialize_new_modules, nb_update_visible_state
from blinkview.ui.constants import WidgetName
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.in_development import set_as_in_development
from blinkview.ui.widget_registry import register_widget_factory
from blinkview.ui.widgets.action_button_delegate import TelemetryCol, TelemetryDelegate

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper


class TelemetryTableModel(QAbstractTableModel):
    layout_changed = Signal()

    def __init__(self, gui_context, parent=None):
        super().__init__(parent)
        self.context: GUIContext = gui_context

        self.system_device = self.context.id_registry.get_device("system")

        self.logger = self.context.logger.child("telem", enabled=False)

        self.logger_count = self.logger.child("count")
        self.logger_total = self.logger.child("total_ms")

        self.logger_filter = self.logger.child("filter")

        self.modules: list[ModuleIdentity] = []
        # Pre-allocate parallel columnar arrays
        self.capacity = 1024

        self._visible_sort_keys: list = []
        self.visible_mod_ids = np.empty(0, dtype=np.int32)
        self.is_visible = np.zeros(self.capacity, dtype=np.bool_)

        self.painted_seqs = np.zeros(self.capacity, dtype=dtypes.SEQ_TYPE)
        self.painted_levels = np.zeros(self.capacity, dtype=dtypes.LEVEL_TYPE)
        self.arrival_times = np.zeros(self.capacity, dtype=np.float64)
        self.change_times = np.zeros(self.capacity, dtype=np.float64)

        # Flat 1D buffer mapping directly to the backend architecture
        self.painted_buffers = np.zeros(self.capacity * MAX_MSG_BYTES, dtype=dtypes.BYTE)
        self.painted_lengths = np.zeros(self.capacity, dtype=np.uint16)

        self.cache_strings = [None] * self.capacity
        self.cache_seqs = np.zeros(self.capacity, dtype=dtypes.SEQ_TYPE)

        # Initialize fast-access memoryviews
        self._init_memoryviews()

        # Filter settings
        self._positive_groups: list[list[str]] = []
        self._global_negatives: list[str] = []
        self.allowed_device: Optional[DeviceIdentity] = None
        self.allowed_module: Optional[ModuleIdentity] = None
        self.allowed_module_children = False

        self.hide_empty = False
        self.show_non_essential = False
        # No scroll/pause concept here (unlike LogViewerWidget/LogTableViewerWidget) - this table
        # always follows, so it only ever occupies LIVE or FOLLOWING (see
        # plans/playback-follow-state-machine.md).
        self._playback = PlaybackFollowMachine(supports_freeze=False)

        # Sort settings
        self.sort_column = TelemetryCol.DEVICE
        self.sort_order = Qt.AscendingOrder

        self.prev_apply = perf_counter()

    def _clock(self):
        registry = self.context.registry
        return registry.playback_clock if registry is not None else None

    @property
    def force_live(self) -> bool:
        return self._playback.force_live

    def _clock_snapshot(self) -> ClockSnapshot:
        clock = self._clock()
        if clock is None:
            return ClockSnapshot(mode=PlaybackMode.LIVE, current_ts_ns=0)
        return ClockSnapshot(mode=clock.mode, current_ts_ns=clock.current_ts_ns, is_playing=clock.is_playing)

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Exercises nb_initialize_new_modules/nb_update_visible_state - the two Numba kernels
        driving apply_updates() - against a private LatestModuleValueTracker built from the
        helper's dummy pool/registry, using scratch arrays sized to it instead of a real
        TelemetryTableModel. Kept private rather than reusing helper.tracker so this warmup
        doesn't depend on callback registration order."""
        print("[Warmup] TelemetryTableModel ...")

        tracker = LatestModuleValueTracker(
            helper.log_pool, helper.registry.modules_table, helper.array_pool, helper.time_ns
        )
        tracker.update()
        now = perf_counter()

        with tracker.get_snapshot() as snap:
            b = snap.bundle()
            sequences = b.sequence_ids
            levels = b.levels
            b_lengths = b.lengths
            b_buffer = b.buffer

            n_mods = len(sequences)
            if n_mods == 0:
                print("[Warmup] TelemetryTableModel ... done")
                return

            painted_seqs = np.zeros(n_mods, dtype=dtypes.SEQ_TYPE)
            painted_levels = np.zeros(n_mods, dtype=dtypes.LEVEL_TYPE)
            arrival_times = np.zeros(n_mods, dtype=np.float64)
            change_times = np.zeros(n_mods, dtype=np.float64)
            painted_buffers = np.zeros(n_mods * MAX_MSG_BYTES, dtype=dtypes.BYTE)
            painted_lengths = np.zeros(n_mods, dtype=np.uint16)
            newly_active_ids = np.zeros(n_mods, dtype=np.int32)
            is_visible = np.zeros(n_mods, dtype=np.bool_)

            new_count = nb_initialize_new_modules(
                n_mods,
                sequences,
                painted_seqs,
                arrival_times,
                change_times,
                levels,
                painted_levels,
                b_lengths,
                painted_lengths,
                b_buffer,
                painted_buffers,
                now,
                MAX_MSG_BYTES,
                newly_active_ids,
                is_visible,
            )

            visible_mod_ids = newly_active_ids[:new_count] if new_count > 0 else np.arange(n_mods, dtype=np.int32)

            nb_update_visible_state(
                visible_mod_ids,
                sequences,
                painted_seqs,
                levels,
                painted_levels,
                arrival_times,
                change_times,
                now,
                0.5,  # fade_dur
                5.0,  # stale_limit
                0.02,  # buffer_time
                b_buffer,
                b_lengths,
                painted_buffers,
                painted_lengths,
                MAX_MSG_BYTES,
            )

        print("[Warmup] TelemetryTableModel ... done")

    def _init_memoryviews(self):
        """Wraps numpy arrays to avoid scalar boxing overhead during pure-Python UI lookups."""
        self.seqs_mv = memoryview(self.painted_seqs)
        self.levels_mv = memoryview(self.painted_levels)
        self.arr_mv = memoryview(self.arrival_times)
        self.chg_mv = memoryview(self.change_times)
        self.buf_mv = memoryview(self.painted_buffers)
        self.len_mv = memoryview(self.painted_lengths)

        self.cache_seqs_mv = memoryview(self.cache_seqs)

    # --- Filter Setters Remain Identical ---
    def set_hide_empty(self, hide: bool):
        self.hide_empty = hide
        self.refresh_layout()

    def set_show_non_essential(self, show: bool):
        self.show_non_essential = show
        self.refresh_layout()

    def set_force_live(self, force: bool):
        self._playback.handle(FollowEvent.ToggleForceLive(force), self._clock_snapshot())
        # Filters like hide_empty read seqs_mv, which just changed data source -
        # re-derive visible rows before the next apply_updates paints values. Mirrors
        # set_hide_empty/set_show_non_essential: doesn't force an immediate apply_updates()
        # itself (the tracker may not even exist yet during construction/restore), the next
        # regular tick picks up the new data source.
        self.refresh_layout()

    def _passes_filters(self, mod_id: int) -> bool:
        """Evaluates if a single module passes the current UI filters."""
        module = self.modules[mod_id]

        # Essential Filter
        if module.device is self.system_device:
            if not self.show_non_essential and not module.is_essential:
                return False

        # Device Filter
        if self.allowed_device is not None and module.device != self.allowed_device:
            return False

        # Module/Hierarchy Filter
        if self.allowed_module is not None:
            if module.device != self.allowed_module.device:
                return False

            if self.allowed_module_children:
                curr = module
                found = False
                while curr is not None:
                    if curr == self.allowed_module:
                        found = True
                        break
                    curr = curr.parent
                if not found:
                    return False
            else:
                parent = self.allowed_module.parent
                if parent is not None:
                    curr = module
                    found = False
                    while curr is not None:
                        if curr == parent:
                            found = True
                            break
                        curr = curr.parent
                    if not found:
                        return False

        # Text Search Filters
        if self._positive_groups or self._global_negatives:
            row_content = f"{module.name} {module.device.name}".lower()

            if self._global_negatives and any(neg in row_content for neg in self._global_negatives):
                return False

            if self._positive_groups:
                passed_pos = False
                for group in self._positive_groups:
                    if all(term in row_content for term in group if term):
                        passed_pos = True
                        break
                if not passed_pos:
                    return False

        # Hide Empty Filter
        if self.hide_empty and self.seqs_mv[mod_id] == 0:
            return False

        return True

    def _sort_key(self, mod_id: int):
        module = self.modules[mod_id]
        if self.sort_column == TelemetryCol.DEVICE:
            return (module.device.name, module.name)
        return module.name

    def _bisect_insert_index(self, key) -> int:
        """O(log n) replacement for the old linear scan. visible_mod_ids/_visible_sort_keys
        are always kept sorted, so we can binary-search for the insertion point instead
        of comparing against every existing row."""
        keys = self._visible_sort_keys
        lo, hi = 0, len(keys)
        descending = self.sort_order == Qt.DescendingOrder
        while lo < hi:
            mid = (lo + hi) // 2
            mid_key = keys[mid]
            go_right = (mid_key >= key) if descending else (mid_key <= key)
            if go_right:
                lo = mid + 1
            else:
                hi = mid
        return lo

    def _insert_visible_module(self, mod_id: int):
        """Safely inserts a new module into the model while maintaining sort order."""
        if self.is_visible[mod_id]:
            return  # Already has a row — prevents duplicates when both
            # _sync_registry (on registration) and
            # nb_initialize_new_modules (on first data) add the same mod_id.

        filter_passed = self._passes_filters(mod_id)
        if not filter_passed:
            return

        start_time = perf_counter_ns()

        key = self._sort_key(mod_id)
        insert_idx = self._bisect_insert_index(key)

        self.beginInsertRows(QModelIndex(), insert_idx, insert_idx)
        self.visible_mod_ids = np.insert(self.visible_mod_ids, insert_idx, mod_id)
        self._visible_sort_keys.insert(insert_idx, key)
        self.endInsertRows()
        self.is_visible[mod_id] = True

        end_time = perf_counter_ns()

        self.logger_filter.debug(f"{(end_time - start_time) / 1_000_000:.6f}")

    def set_filter_text(self, text: str):
        clean_text = text.lower().strip()
        if not clean_text:
            self._positive_groups = []
            self._global_negatives = []
        else:
            chunks = clean_text.split()
            self._global_negatives = [c[1:] for c in chunks if c.startswith("-") and len(c) > 1]
            pos_chunks = [c for c in chunks if not c.startswith("-")]
            self._positive_groups = [c.split("+") for c in pos_chunks if c]
        self.refresh_layout()

    def set_allowed_device(self, device: Optional[DeviceIdentity]):
        if self.allowed_device != device:
            self.allowed_device = device
            self.refresh_layout()

    def set_allowed_module(self, module: Optional[ModuleIdentity]):
        self.allowed_module = module
        self.refresh_layout()

    def set_allowed_module_children(self, allowed: bool):
        self.allowed_module_children = allowed
        self.refresh_layout()

    def sort(self, column: int, order: Qt.SortOrder = Qt.AscendingOrder):
        self.sort_column = column
        self.sort_order = order
        self.refresh_layout()

    # ----------------------------------------

    def _grow(self, current_arr: np.ndarray, dtype) -> np.ndarray:
        new_arr = np.zeros(self.capacity, dtype=dtype)
        new_arr[: len(current_arr)] = current_arr
        return new_arr

    def _grow_buffer(self, current_arr: np.ndarray, dtype) -> np.ndarray:
        new_arr = np.zeros(self.capacity * MAX_MSG_BYTES, dtype=dtype)
        new_arr[: len(current_arr)] = current_arr
        return new_arr

    def _sync_registry(self, current_modules):
        """Ensures backend modules have UI state tracked in parallel arrays."""
        old_capacity = len(self.modules)
        n_mods = len(current_modules)

        # Build by real module.id, not by list position — module_list's
        # append order is not guaranteed to match id order.
        modules_by_id = [None] * len(current_modules)
        for module in current_modules:
            modules_by_id[module.id] = module
        self.modules = modules_by_id

        if n_mods > self.capacity:
            self.capacity = max(n_mods, self.capacity * 2)

            self.is_visible = self._grow(self.is_visible, dtype=np.bool_)
            self.painted_seqs = self._grow(self.painted_seqs, dtypes.SEQ_TYPE)
            self.painted_levels = self._grow(self.painted_levels, dtypes.LEVEL_TYPE)
            self.arrival_times = self._grow(self.arrival_times, np.float64)
            self.change_times = self._grow(self.change_times, np.float64)
            self.painted_lengths = self._grow(self.painted_lengths, np.uint16)
            self.painted_buffers = self._grow_buffer(self.painted_buffers, dtypes.BYTE)

            self.cache_seqs = self._grow(self.cache_seqs, dtypes.SEQ_TYPE)

            self.cache_strings.extend([None] * (self.capacity - len(self.cache_strings)))

            self._init_memoryviews()

        for mod_id in range(old_capacity, len(self.modules)):
            self._insert_visible_module(mod_id)

    def refresh_layout(self):
        """Applies filters to the id tracking array and executes sorting."""
        self.beginResetModel()

        filtered_ids = []
        for mod_id, module in enumerate(self.modules):
            # Essential Filter
            if module.device is self.system_device:
                if not self.show_non_essential and not module.is_essential:
                    continue

            if self.allowed_device is not None and module.device != self.allowed_device:
                continue

            if self.allowed_module is not None:
                if module.device != self.allowed_module.device:
                    continue

                if self.allowed_module_children:
                    curr = module
                    found = False
                    while curr is not None:
                        if curr == self.allowed_module:
                            found = True
                            break
                        curr = curr.parent
                    if not found:
                        continue
                else:
                    parent = self.allowed_module.parent
                    if parent is not None:
                        curr = module
                        found = False
                        while curr is not None:
                            if curr == parent:
                                found = True
                                break
                            curr = curr.parent
                        if not found:
                            continue

            if self._positive_groups or self._global_negatives:
                row_content = f"{module.name} {module.device.name}".lower()

                if self._global_negatives and any(neg in row_content for neg in self._global_negatives):
                    continue

                if self._positive_groups:
                    passed_pos = False
                    for group in self._positive_groups:
                        if all(term in row_content for term in group if term):
                            passed_pos = True
                            break
                    if not passed_pos:
                        continue

            # Read from memoryview instead of numpy array
            if self.hide_empty and self.seqs_mv[mod_id] == 0:
                continue

            filtered_ids.append(mod_id)

        reverse = self.sort_order == Qt.DescendingOrder
        if self.sort_column == TelemetryCol.DEVICE:
            filtered_ids.sort(
                key=lambda m_id: (self.modules[m_id].device.name, self.modules[m_id].name), reverse=reverse
            )
        elif self.sort_column == TelemetryCol.NAME:
            filtered_ids.sort(key=lambda m_id: self.modules[m_id].name, reverse=reverse)

        self.visible_mod_ids = np.array(filtered_ids, dtype=np.int32)

        self._visible_sort_keys = [self._sort_key(m) for m in filtered_ids]

        self.is_visible[:] = False
        if self.visible_mod_ids.size > 0:
            self.is_visible[self.visible_mod_ids] = True

        self.endResetModel()
        self.layout_changed.emit()

    def apply_updates(self, force: bool = False):
        """High-frequency vectorized pull from the module_value_tracker."""
        now = perf_counter()
        if not force and now - self.prev_apply < 0.1:  # Target ~10Hz limit
            return
        self.prev_apply = now

        start_time = perf_counter_ns()

        current_modules = self.context.id_registry.module_list
        if len(current_modules) != len(self.modules):
            self._sync_registry(current_modules)

        tracker = self.context.registry.module_value_tracker
        theme = self.context.theme
        fade_dur = theme.fade_duration
        stale_limit = theme.stale_threshold
        data_changed_emit = self.dataChanged.emit
        buffer_time = 0.02

        newly_active_ids = np.zeros(len(self.modules), dtype=np.int32)

        # While the global playback clock is scrubbing REPLAY, show the latest-per-module
        # message as of the playhead instead of the tracker's "latest ever" snapshot - unless
        # force_live pins this table to live data while other windows scrub. Routed through
        # PlaybackFollowMachine (LIVE/FOLLOWING only - see plans/playback-follow-state-machine.md)
        # rather than reading clock.mode directly, so this table's follow logic stays structurally
        # identical to LogViewerWidget/LogTableViewerWidget's.
        action = self._playback.handle(FollowEvent.Tick(), self._clock_snapshot())
        if action.kind is FollowActionKind.FETCH_FOLLOWING:
            snapshot_ctx = tracker.build_snapshot_as_of(action.anchor_ts_ns)
        else:
            snapshot_ctx = tracker.get_snapshot()

        with snapshot_ctx as snap:
            b = snap.bundle()
            sequences = b.sequence_ids
            levels = b.levels
            b_lengths = b.lengths
            b_buffer = b.buffer

            # --- REPLACE THE OLD PYTHON LOOP WITH THIS ---
            new_count = nb_initialize_new_modules(
                len(self.modules),
                sequences,
                self.painted_seqs,
                self.arrival_times,
                self.change_times,
                levels,
                self.painted_levels,
                b_lengths,
                self.painted_lengths,
                b_buffer,
                self.painted_buffers,
                now,
                MAX_MSG_BYTES,
                newly_active_ids,  # Pass the new output buffer
                self.is_visible,
            )

            if new_count > 0:
                for i in range(new_count):
                    self._insert_visible_module(newly_active_ids[i])

            if len(self.visible_mod_ids) == 0:
                return

            # Execute Numba kernel computation and perform state updates in-place
            needs_update = nb_update_visible_state(
                self.visible_mod_ids,
                sequences,
                self.painted_seqs,
                levels,  # Pass levels in
                self.painted_levels,  # Pass painted_levels in
                self.arrival_times,
                self.change_times,
                now,
                fade_dur,
                stale_limit,
                buffer_time,
                b_buffer,
                b_lengths,
                self.painted_buffers,
                self.painted_lengths,
                MAX_MSG_BYTES,
            )

            # The only thing left in Python is the Qt UI boundary
            # indices_to_update_np = np.nonzero(needs_update)[0]
            # indices_to_update = memoryview(indices_to_update_np)
            # for i in indices_to_update:
            #     # Int casting keeps Qt index creation clean of np.int64 types
            #     idx = self.index(int(i), TelemetryCol.VALUE)
            #     data_changed_emit(idx, idx)
            indices_to_update_np = np.nonzero(needs_update)[0]

            if indices_to_update_np.size > 0:
                # Grab the highest and lowest modified rows
                min_row = int(indices_to_update_np[0])
                max_row = int(indices_to_update_np[-1])

                # Create a bounding box covering the changed area in the VALUE column
                top_idx = self.index(min_row, TelemetryCol.VALUE)
                bottom_idx = self.index(max_row, TelemetryCol.VALUE)

                # Emit ONE signal. Qt will clip this to the visible viewport automatically.
                data_changed_emit(top_idx, bottom_idx, [Qt.DisplayRole, Qt.BackgroundRole, Qt.ForegroundRole])

        end_time = perf_counter_ns()

        self.logger_total.debug(f"{(end_time - start_time) / 1_000_000:.6f}")
        self.logger_count.debug(f"{len(self.modules)}")

    def rowCount(self, parent=None):
        # Table models must return 0 for child nodes, or Qt gets confused
        if parent and parent.isValid():
            return 0
        return len(self.visible_mod_ids)

    def columnCount(self, parent=None):
        if parent and parent.isValid():
            return 0
        # Return 4 to account for Device, Name, Value, AND Actions
        return len(TelemetryCol)

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        # Cast to integers to prevent strict PySide enum comparison failures
        if orientation == Qt.Horizontal and int(role) == int(Qt.DisplayRole):
            section_int = int(section)
            if section_int == int(TelemetryCol.NAME):
                return "Module"
            elif section_int == int(TelemetryCol.VALUE):
                return "Value"
            elif section_int == int(TelemetryCol.DEVICE):
                return "Device"
            elif section_int == int(TelemetryCol.ACTIONS):
                return "Actions"
        return super().headerData(section, orientation, role)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        row = index.row()
        if row >= len(self.visible_mod_ids):
            return None

        mod_id = self.visible_mod_ids[row]

        if role == Qt.DisplayRole:
            col = index.column()
            if col == TelemetryCol.DEVICE:
                return self.modules[mod_id].device.name
            elif col == TelemetryCol.NAME:
                return str(self.modules[mod_id].name)
            elif col == TelemetryCol.VALUE:
                current_seq = self.seqs_mv[mod_id]
                if current_seq == 0:
                    return "---"

                # 1. Fast Cache Hit
                if self.cache_seqs[mod_id] == current_seq:
                    return self.cache_strings[mod_id]

                # 2. Optimized Decode
                length = self.len_mv[mod_id]
                off = mod_id * MAX_MSG_BYTES

                # Use tobytes() only on the slice - this is the bottleneck
                val = self.buf_mv[off : off + length].tobytes().decode("utf-8", errors="replace")

                # 3. Cache Update
                self.cache_strings[mod_id] = val
                self.cache_seqs[mod_id] = current_seq

                return val

        return None


@register_widget_factory(WidgetName.TELEMETRY_TABLE)
class TelemetryTable(QWidget):
    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)
        self.gui_context: GUIContext = gui_context

        self.tab_name = ""

        self.show_device_column = True
        self.filtered_device: Optional[DeviceIdentity] = None
        self.filtered_module: Optional[ModuleIdentity] = None
        self.filtered_module_children = False

        self.sort_column = TelemetryCol.DEVICE
        self.sort_order = 0

        self.hide_empty = True
        self.show_non_essential = False
        self.force_live = False

        self._set_defaults()

        self.drag_start_pos = None
        self.hovered_row = -1

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(4)

        # --- CREATE LOCAL TOOLBAR ---
        self.toolbar = QToolBar()
        self.toolbar.setIconSize(QSize(16, 16))
        self.toolbar.setMovable(False)

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Filter (Space=OR, +=AND, -=NOT)...")
        self.search_box.setClearButtonEnabled(True)
        self.search_box.textChanged.connect(self._on_search_changed)
        self.toolbar.addWidget(self.search_box)

        self.action_toggle_module = QAction("Device", self)
        self.action_toggle_module.setCheckable(True)
        self.action_toggle_module.setChecked(True)
        self.action_toggle_module.triggered.connect(self._toggle_device_column)
        self.toolbar.addAction(self.action_toggle_module)

        self.action_force_live = QAction("Live", self)
        self.action_force_live.setCheckable(True)
        self.action_force_live.setChecked(self.force_live)
        self.action_force_live.setToolTip(
            "Always show live values in this table, even while other windows are scrubbing REPLAY"
        )
        self.action_force_live.triggered.connect(self._toggle_force_live)
        self.toolbar.addAction(self.action_force_live)

        self.options_button = QToolButton()
        self.options_button.setText("⚙ Options")
        self.options_button.setPopupMode(QToolButton.InstantPopup)
        self.options_button.setToolButtonStyle(Qt.ToolButtonTextBesideIcon)

        # 2. Assign the menu to the button
        self.options_menu = QMenu(self)

        self.action_hide_empty = QAction("Hide Empty", self)
        self.action_hide_empty.setCheckable(True)
        self.action_hide_empty.setChecked(self.hide_empty)
        self.action_hide_empty.triggered.connect(self._toggle_hide_empty)
        self.options_menu.addAction(self.action_hide_empty)

        self.action_show_non_essential = QAction("Show Non-Essential", self)
        self.action_show_non_essential.setCheckable(True)
        self.action_show_non_essential.setChecked(self.show_non_essential)
        self.action_show_non_essential.triggered.connect(self._toggle_show_non_essential)
        self.options_menu.addAction(self.action_show_non_essential)

        self.options_button.setMenu(self.options_menu)

        self.toolbar.addWidget(self.options_button)

        self.layout.addWidget(self.toolbar)

        # --- SETUP LOCAL MODEL ---
        self.model = TelemetryTableModel(self.gui_context, self)

        # --- SETUP THE VIEW ---
        self.view = QTableView()
        self.view.setModel(self.model)
        self.view.setStyleSheet("""
            QTableView::item {
                padding-top: 0px;
                padding-bottom: 0px;
                margin: 0px;
                border: none;
            }
        """)

        font = self.view.font()
        font.setFamily("Segoe UI, Roboto, sans-serif")
        font.setBold(True)
        font.setWeight(QFont.Weight.Bold)
        self.view.setFont(font)

        # Sorting is now handled directly by our model hook
        self.view.setSortingEnabled(True)

        self.view.clicked.connect(
            lambda index: self._trigger_module_action("view_logs", self._get_module_at_index(index))
        )
        self.view.doubleClicked.connect(self._on_double_clicked)

        v_header = self.view.verticalHeader()
        v_header.hide()
        v_header.setSectionResizeMode(QHeaderView.Fixed)
        v_header.setDefaultSectionSize(10)

        h_header = self.view.horizontalHeader()
        h_header.setFont(font)
        h_header.sortIndicatorChanged.connect(self._on_sort_indicator_changed)
        h_header.setSectionResizeMode(TelemetryCol.VALUE, QHeaderView.Stretch)
        h_header.setSectionResizeMode(TelemetryCol.ACTIONS, QHeaderView.Fixed)

        self.view.setColumnWidth(TelemetryCol.ACTIONS, 100)
        self.view.hideColumn(TelemetryCol.ACTIONS)

        self.view.setSelectionMode(QTableView.NoSelection)
        self.view.setShowGrid(False)

        self.view.setMouseTracking(True)
        self.view.entered.connect(self._on_mouse_entered)
        self.view.viewport().installEventFilter(self)

        self.view.setContextMenuPolicy(Qt.NoContextMenu)
        self.view.customContextMenuRequested.connect(self._show_context_menu)

        self.view.setItemDelegateForColumn(TelemetryCol.VALUE, TelemetryDelegate(self.gui_context.theme, self))

        self.layout.addWidget(self.view)

        self.model.layout_changed.connect(self.auto_size_columns_delayed)

        self.model.rowsInserted.connect(self.auto_size_columns_delayed)

        self.resize_timer = QTimer(self)
        self.resize_timer.setSingleShot(True)
        self.resize_timer.timeout.connect(self.auto_size_columns)

        # Hook into global updates
        self.gui_context.add_updatable(self)

        if state:
            self.restore(state)
        else:
            self.auto_size_columns_delayed()

        self._sync_force_live_visibility()

    def closeEvent(self, event):
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)

    def apply_updates(self, force: bool = False):
        """Pass update execution to our isolated table model."""
        self._sync_force_live_visibility()
        self.model.apply_updates(force=force)

    def _sync_force_live_visibility(self):
        """The Live override only means something while the global transport is actually
        scrubbing REPLAY - hidden the rest of the time. force_live itself is untouched by this,
        so re-entering REPLAY later remembers whatever this table had it set to."""
        clock = self.model._clock()
        self.action_force_live.setVisible(clock is not None and clock.mode is PlaybackMode.REPLAY)

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__
        self.allowed_device = None
        self.filtered_module = None
        self.filtered_module_children = False
        self.log_level = None
        self.show_filter_sidebar = None
        self.sort_order = 0
        self.sort_column = TelemetryCol.DEVICE
        self.hide_empty = True
        self.show_non_essential = False
        self.force_live = False

    def restore(self, state: dict):
        self.tab_name = state.get("tab_name", self.tab_name)

        self.filtered_device = self.gui_context.id_registry.resolve_device(
            state.get("filtered_device", self.filtered_device)
        )
        self.model.set_allowed_device(self.filtered_device)

        self.filtered_module = self.gui_context.id_registry.resolve_module(
            state.get("filtered_module", self.filtered_module)
        )
        self.model.set_allowed_module(self.filtered_module)

        if "show_device_column" in state:
            self.show_device_column = state["show_device_column"]
        else:
            if self.filtered_module is not None:
                self.show_device_column = False
            elif self.filtered_device is not None:
                self.show_device_column = False
            else:
                self.show_device_column = True

        self.filtered_module_children = state.get("filtered_module_children", self.filtered_module_children)
        self.model.set_allowed_module_children(self.filtered_module_children)

        self.hide_empty = state.get("hide_empty", self.hide_empty)
        self.action_hide_empty.setChecked(self.hide_empty)
        self.model.set_hide_empty(self.hide_empty)

        self.show_non_essential = state.get("show_non_essential", self.show_non_essential)
        self.action_show_non_essential.setChecked(self.show_non_essential)
        self.model.set_show_non_essential(self.show_non_essential)

        self.force_live = state.get("force_live", self.force_live)
        self.action_force_live.setChecked(self.force_live)
        self.model.set_force_live(self.force_live)

        self.action_toggle_module.setChecked(self.show_device_column)
        self._toggle_device_column(self.show_device_column)

        filter_pattern = state.get("filter_pattern")
        if filter_pattern:
            self.search_box.setText(filter_pattern)
            self.model.set_filter_text(filter_pattern)

        self.sort_column = state.get("sort_column", self.sort_column)
        self.sort_order = state.get("sort_order", self.sort_order)
        order = Qt.SortOrder(self.sort_order)

        self.view.sortByColumn(self.sort_column, order)
        self.auto_size_columns_delayed()

    def get_state(self) -> dict:
        return {
            "filter_pattern": self.search_box.text(),
            "show_device_column": self.show_device_column,
            "filtered_device": self.filtered_device.name if self.filtered_device else None,
            "filtered_module": self.filtered_module.name_with_device() if self.filtered_module else None,
            "filtered_module_children": self.filtered_module_children,
            "sort_column": self.sort_column,
            "sort_order": self.sort_order,
            "hide_empty": self.hide_empty,
            "show_non_essential": self.show_non_essential,
            "force_live": self.force_live,
        }

    def auto_size_columns_delayed(self):
        self.resize_timer.start(50)

    def auto_size_columns(self):
        header = self.view.horizontalHeader()

        self.view.resizeColumnToContents(TelemetryCol.DEVICE)
        if header.sectionSize(TelemetryCol.DEVICE) < 70:
            header.resizeSection(TelemetryCol.DEVICE, 70)
        if header.sectionSize(TelemetryCol.DEVICE) > 200:
            header.resizeSection(TelemetryCol.DEVICE, 200)

        self.view.resizeColumnToContents(TelemetryCol.NAME)
        if header.sectionSize(TelemetryCol.NAME) < 100:
            header.resizeSection(TelemetryCol.NAME, 100)
        if header.sectionSize(TelemetryCol.NAME) > 400:
            header.resizeSection(TelemetryCol.NAME, 400)

    def _toggle_device_column(self, visible: bool):
        self.show_device_column = visible
        self.view.setColumnHidden(TelemetryCol.DEVICE, not visible)
        if visible:
            self.auto_size_columns()

    def _on_search_changed(self, text):
        self.model.set_filter_text(text)
        self.auto_size_columns_delayed()

    def _on_mouse_entered(self, index):
        if not index.isValid():
            return

        if index.column() == TelemetryCol.NAME:
            self.view.setCursor(Qt.PointingHandCursor)
        else:
            self.view.unsetCursor()

        self._set_hovered_row(index.row())

    def eventFilter(self, source, event):
        if source is not self.view.viewport():
            return super().eventFilter(source, event)

        match event.type():
            case QEvent.MouseButtonPress:
                match event.button():
                    case Qt.LeftButton:
                        self.drag_start_pos = event.pos()
                    case Qt.RightButton:
                        self._show_context_menu(event.pos())
                        return True

            case QEvent.MouseMove:
                if not (event.buttons() & Qt.LeftButton) or self.drag_start_pos is None:
                    return False
                if (event.pos() - self.drag_start_pos).manhattanLength() < QApplication.startDragDistance():
                    return False

                index = self.view.indexAt(self.drag_start_pos)
                if index.isValid() and index.column() == TelemetryCol.NAME:
                    self._perform_drag(index)
                    self.drag_start_pos = None
                    return True

            case QEvent.Leave:
                self._set_hovered_row(-1)

        return super().eventFilter(source, event)

    def _get_module_at_index(self, index):
        if not index.isValid() or index.column() != TelemetryCol.NAME:
            return None
        # Convert index to backend mapping
        mod_id = self.model.visible_mod_ids[index.row()]
        return self.model.modules[mod_id]

    def open_log_viewer(self, module, include_children=False):
        if not module:
            return

        title = f"Logs: {module.device.name}.{module.name}"
        if include_children:
            title += " (+ Children)"

        self.gui_context.create_widget(
            WidgetName.LOG_VIEWER,
            title,
            as_window=True,
            params={"filtered_module": module, "include_children": include_children, "force_live": self.force_live},
        )

    def sort_by_device(self):
        header = self.view.horizontalHeader()
        current_order = header.sortIndicatorOrder()
        new_order = Qt.DescendingOrder if current_order == Qt.AscendingOrder else Qt.AscendingOrder
        self.view.sortByColumn(TelemetryCol.DEVICE, new_order)

    def sort_by_module(self):
        self.view.sortByColumn(TelemetryCol.NAME, Qt.AscendingOrder)

    def _on_sort_indicator_changed(self, column, order):
        self.sort_column = column
        self.sort_order = order.value

    def _trigger_module_action(self, action_id, module):
        if not module:
            return

        self._set_hovered_row(-1)

        match action_id:
            case "view_logs" | "view_logs_children" | "view_logs_table" | "view_logs_children_table":
                with_children = action_id in ("view_logs_children", "view_logs_children_table")
                as_table = action_id in ("view_logs_table", "view_logs_children_table")

                title = f"Logs: {module.name_with_device()}"
                if with_children:
                    title += " (+Children)"

                show_hidden = module.device == self.gui_context.registry.system_device

                self.gui_context.create_widget(
                    WidgetName.LOG_TABLE_VIEWER if as_table else WidgetName.LOG_VIEWER,
                    title,
                    as_window=True,
                    params={
                        "filtered_module": module,
                        "filtered_module_children": with_children,
                        "show_hidden": show_hidden,
                        "force_live": self.force_live,
                    },
                )

            case "copy_name":
                QApplication.clipboard().setText(module.name)

            case "copy_value":
                for row, mod_id in enumerate(self.model.visible_mod_ids):
                    if self.model.modules[mod_id] == module:
                        idx = self.model.index(row, TelemetryCol.VALUE)
                        val = idx.data(Qt.DisplayRole)
                        if val and val != "---":
                            QApplication.clipboard().setText(str(val))
                        break

            case "view_graph" | "view_graph_with_children":
                self.gui_context.create_widget(
                    WidgetName.TELEMETRY_PLOTTER,
                    f"Graph: {module.name}",
                    as_window=True,
                    params={"modules": [module] if action_id == "view_graph" else module.get_all_descendants()},
                )

    def _show_context_menu(self, pos):
        index = self.view.indexAt(pos)
        module = self._get_module_at_index(index)
        if not module:
            return

        self._set_hovered_row(-1)

        menu = QMenu(self)
        menu.setToolTipsVisible(True)

        title = menu.addAction(f"Module: {module.name}")
        title.setEnabled(False)
        font = title.font()
        font.setBold(True)
        title.setFont(font)
        menu.addSeparator()

        actions = [
            ("View Logs", "view_logs", False, None),
            ("View Logs with Children", "view_logs_children", False, None),
            ("View Logs (Table)", "view_logs_table", False, None),
            ("View Logs with Children (Table)", "view_logs_children_table", False, None),
            (None, None, False, None),
            ("View Graph", "view_graph", False, None),
            ("View Graph with Children", "view_graph_with_children", False, None),
            (None, None, False, None),
            ("Copy Module Name", "copy_name", False, None),
            ("Copy Value", "copy_value", False, None),
        ]

        for label, action_id, is_wip, issue_no in actions:
            if label is None:
                menu.addSeparator()
                continue

            action = QAction(label, self)

            if is_wip:
                set_as_in_development(action, self, feature_name=label, issue_no=issue_no)
            else:
                action.triggered.connect(lambda checked=False, aid=action_id: self._trigger_module_action(aid, module))

            menu.addAction(action)

        parent_module = module.parent
        if parent_module is not None:
            menu.addSeparator()
            parent_menu = menu.addMenu(f"Parent ({parent_module.name})")

            # Action 1: View Parent Logs
            view_parent_logs = QAction("View Logs", self)
            view_parent_logs.triggered.connect(
                lambda checked=False: self._trigger_module_action("view_logs", parent_module)
            )
            parent_menu.addAction(view_parent_logs)

            # Action 2: View Parent Logs with Children
            view_parent_children_logs = QAction("View Logs with Children", self)
            view_parent_children_logs.triggered.connect(
                lambda checked=False: self._trigger_module_action("view_logs_children", parent_module)
            )
            parent_menu.addAction(view_parent_children_logs)

        menu.exec_(self.view.viewport().mapToGlobal(pos))

    def _on_double_clicked(self, index):
        if index.column() == TelemetryCol.VALUE:
            val = index.data()
            QApplication.clipboard().setText(str(val))

    def _perform_drag(self, index):
        module = self._get_module_at_index(index)
        if not module:
            return

        mime_data = QMimeData()
        mime_data.setText(module.name_with_device())

        padding = 10
        font_metrics = self.view.fontMetrics()
        text_width = font_metrics.horizontalAdvance(module.name)
        pixmap = QPixmap(text_width + (padding * 2), 24)
        pixmap.fill(Qt.transparent)

        painter = QPainter(pixmap)
        painter.setRenderHint(QPainter.Antialiasing)
        painter.setBrush(QColor(60, 60, 60, 200))
        painter.setPen(QColor(100, 100, 255))
        painter.drawRoundedRect(pixmap.rect().adjusted(1, 1, -1, -1), 5, 5)

        painter.setPen(Qt.white)
        painter.drawText(pixmap.rect(), Qt.AlignCenter, module.name)
        painter.end()

        drag = QDrag(self)
        drag.setMimeData(mime_data)
        drag.setPixmap(pixmap)
        drag.setHotSpot(pixmap.rect().center())

        drag.exec_(Qt.CopyAction)

    def _set_hovered_row(self, row: int):
        if self.hovered_row == row:
            return

        old_row = self.hovered_row
        self.hovered_row = row

        for r in (old_row, self.hovered_row):
            if r != -1:
                for col in (TelemetryCol.VALUE, TelemetryCol.ACTIONS):
                    idx = self.model.index(r, col)
                    if idx.isValid():
                        self.view.update(idx)

    def _toggle_hide_empty(self, checked: bool):
        self.hide_empty = checked
        self.model.set_hide_empty(checked)
        self.auto_size_columns_delayed()

    def _toggle_show_non_essential(self, checked: bool):
        self.show_non_essential = checked
        self.model.set_show_non_essential(checked)
        self.auto_size_columns_delayed()

    def _toggle_force_live(self, checked: bool):
        self.force_live = checked
        self.model.set_force_live(checked)

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


from collections import deque
from enum import Enum
from typing import TYPE_CHECKING

import numpy as np
from qtpy.QtCore import Qt, QTimer
from qtpy.QtGui import QAction
from qtpy.QtWidgets import QComboBox, QLineEdit, QSizePolicy, QSplitter, QToolBar, QVBoxLayout, QWidget

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.types.formatting import FormattingConfig
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.formatting import nb_segment_estimate_out_size, nb_segment_format
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS
from blinkview.ops.segments import segment_filter, segment_filter_reversed
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.log_velocity_tracker import LogVelocityTracker
from blinkview.ui.widgets.kv_filter_line_edit import KvFilterLineEdit
from blinkview.ui.widgets.log_highlighter import LogHighlighter
from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.ui.widgets.searchable_log_area import SearchableLogArea
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from blinkview.utils.utc_offset import get_local_utc_offset_seconds

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper


class LogViewMode(Enum):
    LIVE = "live"
    HISTORY = "history"


class LogViewerWidget(QWidget):
    # Live mode keeps only a small tail of blocks for cheap paint/scroll cost; history mode
    # (entered when the user scrolls away from the tail) fetches a bounded window of rows before
    # and after an anchor sequence, mirroring LogTableStore's live/history split (see
    # qt-log-table-viewer skill Sec 1/14) but producing pre-formatted text lines instead of
    # columnar rows, since SearchableLogArea is a QPlainTextEdit rather than a direct-paint grid.
    LIVE_MAX_BLOCKS = 200
    HISTORY_BEFORE = 500
    HISTORY_AFTER = 500

    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)

        self.gui_context: GUIContext = gui_context

        self.setStyleSheet("""QToolButton {
    border-radius: 4px;
    padding: 2px;
}

/* Auto-Pause Highlight */
QToolButton[autoPaused="true"] {
    background-color: #882222; /* Deep Red */
    color: white;
    border: 1px solid #ff4444;
}

/* Optional: Manual Pause Highlight (Amber) */
QToolButton[manualPaused="true"] {
    background-color: #886622; 
    color: white;
}

QToolButton[filterEnabled="true"] {
    border: 2px solid #ff4444;
}
""")

        self.tab_name = ""
        self.allowed_device = None
        self.filtered_module = None
        self.filtered_module_children = False
        self.log_level = LogLevel.ALL.name_conf
        self.filter_sidebar_state = None

        self.show_telemetry = False
        self.show_module_filter = False
        self.show_ts = True
        self.show_dev = True
        self.show_lvl = True
        self.show_mod = True
        self.show_date = False
        self.show_rx_ts = False
        self.saved_sizes = None

        self.show_hidden = False

        self.ts_precision = 3
        self.kv_filter_text = ""
        self.search_text = ""

        self._set_defaults()

        if state:
            self.restore(state)

        self.logger = gui_context.logger.child("log_viewer")

        self.latest_seq_manual = SEQ_NONE
        self.latest_seq_seen = SEQ_NONE

        self.prev_apply = 0  # Timestamp of the last apply_updates call for throttling

        self.max_rows = self.LIVE_MAX_BLOCKS  # Per-tick fetch cap + live tail size

        # Live/history mode state (see class docstring comment above LIVE_MAX_BLOCKS).
        self.view_mode = LogViewMode.LIVE
        self.history_anchor_seq = None
        self.history_oldest_seq = None
        self.history_newest_seq = None
        self.history_reached_start = False
        self._programmatic_scroll = False

        # Sequence id for each currently-retained live-mode block, in the same order/length as
        # the text area's blocks (both bounded to LIVE_MAX_BLOCKS) - lets history mode anchor on
        # the exact row the user was looking at without the text widget itself tracking per-line
        # metadata.
        self._live_seqs = deque(maxlen=self.LIVE_MAX_BLOCKS)

        # Main layout
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        # Toolbar
        self.toolbar = QToolBar("Log Viewer Toolbar", self)
        self.toolbar.setMovable(False)
        self.layout.addWidget(self.toolbar)

        print(
            f"[LogViewer] Initializing allowed_device={self.allowed_device} filtered_module={self.filtered_module} children={self.filtered_module_children} log_level={self.log_level}"
        )
        self.action_toggle_filter = QAction("⧨ Filter", self)
        self.action_toggle_filter.setCheckable(True)
        self.action_toggle_filter.setChecked(self.show_module_filter)

        self.action_toggle_filter.setToolTip("Toggle Module Filter Sidebar")

        self.action_toggle_filter.toggled.connect(self._toggle_module_filter)
        self.toolbar.addAction(self.action_toggle_filter)

        self.toolbar.addSeparator()

        self.level_combo = QComboBox()

        for lvl in LogLevel.LIST_UI:
            self.level_combo.addItem(lvl.name_conf, lvl)  # lvl is the LevelIdentity object

        self.toolbar.addWidget(self.level_combo)

        self.level_combo.currentIndexChanged.connect(self._handle_level_change)

        self.toolbar.addSeparator()
        # --- SHIFT TOGGLES ---
        self.column_actions = {}

        # Add the Master "ALL" Toggle
        self.action_all = QAction("ALL", self)
        self.action_all.setCheckable(True)
        self.action_all.setChecked(True)
        self.action_all.toggled.connect(self._toggle_all_columns)
        self.toolbar.addAction(self.action_all)

        self.column_actions["show_ts"] = self._add_toggle(
            "Time", self.show_ts, lambda c: self._toggle_col("show_ts", c)
        )

        self.column_actions["show_ts"].setToolTip("Toggle Time (Right-click for Date & Precision)")

        time_button = self.toolbar.widgetForAction(self.column_actions["show_ts"])
        time_button.setContextMenuPolicy(Qt.ActionsContextMenu)

        # 2. Add the Date toggle
        action_date = QAction("Show Date", self)
        action_date.setCheckable(True)
        action_date.setChecked(self.show_date)
        action_date.toggled.connect(lambda c: self._toggle_col("show_date", c))
        self.column_actions["show_date"] = action_date
        time_button.addAction(action_date)

        action_rx_ts = QAction("Show Receive Time", self)
        action_rx_ts.setCheckable(True)
        action_rx_ts.setChecked(self.show_rx_ts)
        action_rx_ts.toggled.connect(lambda c: self._toggle_col("show_rx_ts", c))
        self.column_actions["show_rx_ts"] = action_rx_ts
        time_button.addAction(action_rx_ts)

        # 3. Add a visual separator
        separator = QAction(self)
        separator.setSeparator(True)
        time_button.addAction(separator)

        # 4. Add the Precision Radio Group
        from qtpy.QtWidgets import QActionGroup

        self.precision_group = QActionGroup(self)
        self.precision_group.setExclusive(True)  # Acts like radio buttons

        precisions = [("Seconds (s)", 0), ("Milliseconds (ms)", 3), ("Microseconds (us)", 6), ("Nanoseconds (ns)", 9)]

        for label, prec_val in precisions:
            act = QAction(label, self)
            act.setCheckable(True)
            act.setChecked(self.ts_precision == prec_val)

            # The lambda captures the current `prec_val` during loop iteration
            act.triggered.connect(lambda checked, p=prec_val: self._set_ts_precision(p))

            self.precision_group.addAction(act)
            time_button.addAction(act)

        self.column_actions["show_dev"] = self._add_toggle(
            "DEV", self.show_dev, lambda c: self._toggle_col("show_dev", c)
        )
        self.column_actions["show_lvl"] = self._add_toggle(
            "LVL", self.show_lvl, lambda c: self._toggle_col("show_lvl", c)
        )
        self.column_actions["show_mod"] = self._add_toggle(
            "MOD", self.show_mod, lambda c: self._toggle_col("show_mod", c)
        )

        self.toolbar.addSeparator()

        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Filter device/module/message...")
        self.search_box.setClearButtonEnabled(True)
        self.search_box.setText(self.search_text)
        self.search_box.setMaximumWidth(240)
        self.toolbar.addWidget(self.search_box)

        # Debounced and baked into the same row-level Numba filter kernels as the kv filter
        # (LogFilter.set_text_filter/bake_text_search), rather than filtering the already-
        # rendered text area - so a redraw actually re-scans the backend for matches.
        self._search_timer = QTimer(self)
        self._search_timer.setSingleShot(True)
        self._search_timer.setInterval(200)
        self.search_box.textChanged.connect(lambda _text: self._search_timer.start())

        self.toolbar.addSeparator()

        self.kv_filter_box = KvFilterLineEdit()
        self.kv_filter_box.setMaximumWidth(240)
        self.kv_filter_box.setText(self.kv_filter_text)
        self.toolbar.addWidget(self.kv_filter_box)

        self.toolbar.addSeparator()

        self.action_clear = QAction("Clear", self)
        self.action_clear.triggered.connect(self.clear_logs)
        self.toolbar.addAction(self.action_clear)

        self.is_paused = False
        self.auto_paused = False
        self._is_catching_up = True

        # Velocity Tracking
        self.velocity_tracker = LogVelocityTracker(limit_per_sec=1000)

        # Add Pause Action to Toolbar
        self.action_pause = QAction("⏸ Pause", self)
        self.action_pause.setCheckable(True)
        self.action_pause.toggled.connect(self._toggle_pause)
        # Place it before the Clear button
        self.toolbar.insertAction(self.action_clear, self.action_pause)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Preferred)

        # Add it to the toolbar (this pushes everything following it to the right)
        self.toolbar.addWidget(spacer)

        self.action_telemetry = QAction("Telemetry Table", self)
        self.action_telemetry.setCheckable(True)
        self.action_telemetry.setChecked(self.show_telemetry)
        self.action_telemetry.toggled.connect(self._toggle_telemetry_sidebar)
        self.toolbar.addAction(self.action_telemetry)

        self.splitter = QSplitter(Qt.Horizontal, self)
        self.layout.addWidget(self.splitter)

        self._prev_total_module_count = None
        self._filter_cache = None  # Allowed IDs for this tab
        self._effective_mask = None  # The final baked Numba mask

        self.log_filter = LogFilter(
            self.gui_context.id_registry,
            self.allowed_device,
            self.filtered_module,
            log_level=self.log_level,
            filtered_module_children=self.filtered_module_children,
        )
        self.log_filter.set_kv_filter(self.kv_filter_text)
        self.kv_filter_box.filterTextCommitted.connect(self._apply_kv_filter_text)

        self.log_filter.set_text_filter(self.search_text)
        self._search_timer.timeout.connect(self._apply_search_text)

        self.filter_sidebar = ModuleFilterSidebar(
            gui_context=self.gui_context, target_filter=self.log_filter, parent=self, show_hidden=self.show_hidden
        )

        self.filter_sidebar.restore_state(self.filter_sidebar_state)
        self.filter_sidebar.log_filter.filter_changed.connect(self.reload_and_redraw)

        if self.filter_sidebar_state is not None:
            self._filter_enable_toggled(self.filter_sidebar_state.get("enabled", False))

        self.filter_sidebar.action_enable.toggled.connect(self._filter_enable_toggled)

        self.filter_sidebar.setMinimumWidth(200)
        self.splitter.addWidget(self.filter_sidebar)
        self.filter_sidebar.setVisible(self.show_module_filter)

        # Text Area
        self.text_area = SearchableLogArea(self, maxlen=self.max_rows)

        self.text_area.setMinimumWidth(300)

        self.text_area.verticalScrollBar().valueChanged.connect(self._on_scroll_value_changed)

        self.splitter.addWidget(self.text_area)

        self.highlighter = LogHighlighter(self.text_area.document())

        self.set_log_index()

        self.telemetry_sidebar = TelemetryTable(
            gui_context=self.gui_context,
            state={
                "tab_name": f"{self.tab_name}_sidebar",
                "filtered_device": self.allowed_device,
                "filtered_module": self.filtered_module,
                "filtered_module_children": self.filtered_module_children,
                "show_non_essential": self.show_hidden,
            },
            parent=self,
        )

        self.telemetry_sidebar.setMinimumWidth(250)

        self.splitter.addWidget(self.telemetry_sidebar)

        self.telemetry_sidebar.setVisible(self.show_telemetry)

        self.splitter.setStretchFactor(0, 2)  # Filter
        self.splitter.setStretchFactor(1, 6)  # Logs
        self.splitter.setStretchFactor(2, 4)  # Telemetry

        if self.saved_sizes and len(self.saved_sizes) == 3:
            if any(size <= 100 for size in self.saved_sizes):
                print(f"[LogViewer] Warning: Invalid splitter sizes in view state: {self.saved_sizes}. Using defaults.")
            else:
                self.splitter.setSizes(self.saved_sizes)

        show_filter_btn = self.filtered_module is None or self.filtered_module_children
        self.action_toggle_filter.setVisible(show_filter_btn)

        idx = self.level_combo.findData(LogLevel.from_string(self.log_level))
        if idx != -1:
            self.level_combo.setCurrentIndex(idx)

        self.gui_context.add_updatable(self)

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__
        self.allowed_device = None
        self.filtered_module = None
        self.filtered_module_children = False
        self.log_level = None
        self.show_filter_sidebar = None

        self.show_date = False
        self.show_rx_ts = False
        self.ts_precision = 3

    def restore(self, state: dict):
        self.tab_name = state.get("tab_name", self.tab_name)

        self.show_hidden = state.get("show_hidden", self.show_hidden)

        self.allowed_device = self.gui_context.id_registry.resolve_device(
            state.get("allowed_device", self.allowed_device)
        )

        self.filtered_module = self.gui_context.id_registry.resolve_module(
            state.get("filtered_module", self.filtered_module)
        )

        self.filtered_module_children = state.get("filtered_module_children", self.filtered_module_children)

        default_show_dev = self.show_dev
        if self.filtered_module is not None or self.allowed_device is not None:
            default_show_dev = False  # Hide Device column if constrained to a module or device

        default_show_mod = self.show_mod
        if self.filtered_module is not None and not self.filtered_module_children:
            default_show_mod = False  # Hide Module column if constrained to a SINGLE module (no children)

        self.log_level = state.get("log_level", self.log_level)

        view_state = state.get("view_state", {})
        self.show_ts = view_state.get("show_ts", self.show_ts)
        self.show_dev = view_state.get("show_dev", default_show_dev)
        self.show_lvl = view_state.get("show_lvl", self.show_lvl)
        self.show_mod = view_state.get("show_mod", default_show_mod)
        self.show_date = view_state.get("show_date", self.show_date)
        self.show_rx_ts = view_state.get("show_rx_ts", self.show_rx_ts)
        self.ts_precision = view_state.get("ts_precision", self.ts_precision)

        self.show_telemetry = view_state.get("show_telemetry", self.show_telemetry)
        self.show_module_filter = view_state.get("show_module_filter", self.show_module_filter)
        self.kv_filter_text = view_state.get("kv_filter_text", self.kv_filter_text)
        self.search_text = view_state.get("search_text", self.search_text)
        self.filter_sidebar_state = state.get("filter_sidebar", self.filter_sidebar_state)

        self.saved_sizes = view_state.get("splitter_sizes")

    def get_state(self):
        return {
            "allowed_device": self.allowed_device.name if self.allowed_device else None,
            "filtered_module": f"{self.filtered_module.name_with_device()}" if self.filtered_module else None,
            "filtered_module_children": self.filtered_module_children,
            "view_state": {
                "show_ts": self.show_ts,
                "show_dev": self.show_dev,
                "show_lvl": self.show_lvl,
                "show_mod": self.show_mod,
                "show_date": self.show_date,
                "show_rx_ts": self.show_rx_ts,
                "ts_precision": self.ts_precision,
                "show_module_filter": self.show_module_filter,
                "show_telemetry": self.show_telemetry,
                "kv_filter_text": self.log_filter.kv_filter_text,
                "search_text": self.log_filter.text_filter_text,
                "splitter_sizes": self.splitter.sizes(),
            },
            "log_level": self.log_filter.log_level.name_conf,
            "filter_sidebar": self.filter_sidebar.get_state(),
            "show_hidden": self.filter_sidebar.action_show_non_essential.isChecked(),
        }

    def _handle_level_change(self, index):
        # Retrieve the LevelIdentity object from the userData
        level_identity = self.level_combo.itemData(index)
        self.log_filter.set_level(level_identity.name_conf)

        self._effective_mask = None  # Invalidate cache

        self._refresh_view()

    def _apply_kv_filter_text(self, text):
        self.log_filter.set_kv_filter(text)
        self._refresh_view()

    def _apply_search_text(self):
        self.log_filter.set_text_filter(self.search_box.text())
        self._refresh_view()

    def set_log_index(self):
        """Updates the syntax highlighter's index based on which columns are active."""
        # The level is always at a fixed position based on which columns are shown
        idx = 0
        if self.show_ts:
            if self.show_date:
                idx += 1
            idx += 1
        if self.show_rx_ts:
            if self.show_date:
                idx += 1
            idx += 1
        if self.show_dev:
            idx += 1
        if self.show_lvl:
            self.highlighter.set_index(idx)
            return
        if self.show_mod:
            idx += 1

        # If level column is hidden, set to an invalid index to avoid formatting
        self.highlighter.set_index(-1)

    def _add_toggle(self, text, initial_state, slot):
        """Updated helper to respect the initial logic state."""
        action = QAction(text, self)
        action.setCheckable(True)
        action.setChecked(initial_state)
        action.toggled.connect(slot)
        self.toolbar.addAction(action)
        return action

    def _toggle_all_columns(self, is_checked):
        """Sets all column toggles to applicable core columns, or clears all when unchecked."""
        # Block signals temporarily so we don't trigger a redraw 4 times
        self.blockSignals(True)

        # Base columns we always want when enabled
        applicable_cols = {"show_ts", "show_lvl"}

        # Add Device only if we aren't already restricted to a specific device or module
        if self.filtered_module is None and self.allowed_device is None:
            applicable_cols.add("show_dev")

        # Add Module only if we aren't restricted to a single module (no children)
        if self.filtered_module is None or self.filtered_module_children:
            applicable_cols.add("show_mod")

        for attr_name, action in self.column_actions.items():
            # If checking 'ALL', enable only if it's in our applicable set (disables date/rx_ts).
            # If unchecking 'ALL', target_state is False for everything.
            target_state = is_checked and (attr_name in applicable_cols)

            action.setChecked(target_state)
            setattr(self, attr_name, target_state)

        self.blockSignals(False)

        self.set_log_index()

        # Now trigger a single refresh for the whole batch
        self._refresh_view()

    def _filter_enable_toggled(self, checked):
        button = self.toolbar.widgetForAction(self.action_toggle_filter)
        if button:
            button.setProperty("filterEnabled", checked)
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

        self._effective_mask = None  # Invalidate cache

    def _toggle_module_filter(self, checked):
        """Toggles the visibility of the surgical Module Filter sidebar."""
        self.show_module_filter = checked
        self.filter_sidebar.setVisible(checked)

    def _toggle_col(self, attr_name, is_checked):
        """Updates individual flag and handles the 'ALL' button state."""
        setattr(self, attr_name, is_checked)

        # Base columns we expect to be checked for "ALL" to be active
        applicable_cols = {"show_ts", "show_lvl"}

        # Add Device only if we aren't already restricted to a specific device or module
        if self.filtered_module is None and self.allowed_device is None:
            applicable_cols.add("show_dev")

        # Add Module only if we aren't restricted to a single module (no children)
        if self.filtered_module is None or self.filtered_module_children:
            applicable_cols.add("show_mod")

        # Check if ALL applicable columns are currently checked
        all_active = all(self.column_actions[col].isChecked() for col in applicable_cols)

        # Block signals so checking the 'ALL' button doesn't trigger _toggle_all_columns
        self.action_all.blockSignals(True)
        self.action_all.setChecked(all_active)
        self.action_all.blockSignals(False)

        self.set_log_index()

        self._refresh_view()

    def reload_and_redraw(self):
        """Public method to clear current logs and reload from the source with current filters."""
        self._effective_mask = None  # Invalidate cache

        self._refresh_view()

    def _ensure_effective_mask(self):
        """Bakes self._effective_mask/self._filter_cache from the current module registry and
        filter sidebar state. Shared by apply_updates' per-tick live fetch and
        _fetch_history_window's static window fetch so both see identical filtering."""
        reg = self.gui_context.id_registry
        f = self.log_filter

        if self._prev_total_module_count != (mod_count := reg.module_count()) or self._filter_cache is None:
            self._prev_total_module_count = mod_count
            self._effective_mask = None  # Registry grew, invalidate the mask

            if m := f.filtered_module:
                t_list = (
                    reg.get_descendant_ids(m.id)
                    if f.filtered_module_children
                    else np.array([m.id], dtype=dtypes.ID_TYPE)
                )
            elif dev := f.allowed_device:
                # Tab is restricted to a device (No specific module)
                t_list = f.allowed_device.get_all_module_ids()
            else:
                # Global 'All Logs' view
                t_list = None

            self._filter_cache = t_list

        # --- Bake Effective Mask (ONLY IF INVALID) ---
        if self._effective_mask is None or len(self._effective_mask) < mod_count:
            filter_enabled, sidebar_mask = self.filter_sidebar.get_filter()
            global_threshold = dtypes.LEVEL_TYPE(f.log_level.value)

            if filter_enabled:
                # Path 1: Surgical Mode
                mask_to_use = sidebar_mask[:mod_count] if len(sidebar_mask) >= mod_count else sidebar_mask
                raw_effective = np.maximum(mask_to_use, global_threshold)
                if self._filter_cache is not None:
                    self._effective_mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
                    self._effective_mask[self._filter_cache] = raw_effective[self._filter_cache]
                else:
                    self._effective_mask = raw_effective
            else:
                # Path 2: Tab Fallback Mode
                show_hidden = self.filter_sidebar.action_show_non_essential.isChecked()

                if show_hidden:
                    # Show everything up to the global threshold
                    self._effective_mask = np.full(mod_count, global_threshold, dtype=dtypes.LEVEL_TYPE)
                else:
                    essential_mask = reg._essential_array[:mod_count]
                    # Apply threshold only to essential modules
                    self._effective_mask = np.where(essential_mask, global_threshold, LogLevel.OFF.value).astype(
                        dtypes.LEVEL_TYPE
                    )

                if self._filter_cache is not None:
                    # Constrain to the tab's allowed cache
                    mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
                    mask[self._filter_cache] = self._effective_mask[self._filter_cache]
                    self._effective_mask = mask

        return mod_count

    def apply_updates(self):
        # Pause (manual or clog-triggered) always routes through view_mode == "history" (see
        # _set_pause_ui/_enter_history_mode) - this is the single freeze gate. is_paused is kept
        # as a belt-and-suspenders fallback for the rare case a pause is requested with no live
        # data yet to build a browsable history window from (_toggle_pause's fallback branch).
        if self.is_paused or self.view_mode != LogViewMode.LIVE:
            return

        import time  # Ensure this is imported at the top of your file ideally

        time_ns = time.time_ns

        t_start_total = time_ns()

        # Existing throttling logic uses registry.now_ns, keeping it intact
        now_ns = self.gui_context.registry.now_ns
        t_start = now_ns()

        if t_start - self.prev_apply < 100_000_000:
            return

        # 1. Profile Sidebar Sync
        t_sidebar_start = time_ns()
        self.filter_sidebar.sync_modules()
        t_sidebar_end = time_ns()
        # self.logger.debug(f"[Profile] sync_modules: {(t_sidebar_end - t_sidebar_start) / 1_000_000:.3f} ms")

        self.prev_apply = t_start

        array_pool = self.gui_context.registry.system_ctx.array_pool
        f = self.log_filter
        reg = self.gui_context.id_registry
        pool = self.gui_context.registry.central.log_pool

        tz_offset_sec = get_local_utc_offset_seconds()

        self._ensure_effective_mask()

        total_new_rows = 0
        string_batches = []
        # Sequence ids for each matched row, in the same per-segment chunking as string_batches,
        # so self._live_seqs can be kept aligned 1:1 with the text area's blocks (see class
        # docstring above LIVE_MAX_BLOCKS) - lets history mode anchor on the exact row the user
        # was looking at without SearchableLogArea needing to track any per-line metadata itself.
        seq_batches = []
        format_cfg = FormattingConfig(
            self.show_ts,
            self.show_dev,
            self.show_lvl,
            self.show_mod,
            self.ts_precision,
            show_date=self.show_date,
            show_rx_ts=self.show_rx_ts,
        )

        reached_live_edge = True

        # Track the absolute newest sequence we evaluate so we can "jump" the backlog
        highest_seq_seen_this_tick = self.latest_seq_seen
        first_segment = True

        kv = f.bake_kv_arrays()
        text = f.bake_text_search()

        # filter_debug = self.logger.child("filter").debug
        # estimate_debug = self.logger.child("estimate").debug
        # format_debug = self.logger.child("format").debug

        reg_bundle = reg.bundle()
        # 4. Profile Segment Filtering & Formatting (REVERSED)
        t_segments_start = time.time_ns()
        with pool.get_reversed_snapshot() as segments, pool.acquire_indices_buffer() as indices:
            for segment in segments:
                segment_last_sequence_id = segment.last_sequence_id

                # Because we are iterating backwards (newest to oldest segments),
                # if a segment's LAST sequence is <= our tracker, ALL remaining segments
                # are guaranteed to be older. We can safely abort the loop entirely.
                if segment.size == 0 or segment_last_sequence_id <= self.latest_seq_seen:
                    break

                if first_segment:
                    highest_seq_seen_this_tick = segment_last_sequence_id
                    first_segment = False

                allowed_matches = self.max_rows - total_new_rows
                # t_filter_start = time_ns()
                match_count = segment_filter_reversed(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    max_matches=allowed_matches,
                    start_seq=self.latest_seq_seen,
                    kv=kv,
                    text=text,
                )
                # t_step = time_ns()
                #
                # filter_debug(str(t_step - t_filter_start))

                if match_count > 0:
                    # t_estimate_start = time_ns()
                    req_bytes = nb_segment_estimate_out_size(
                        indices.array, match_count, segment.bundle, reg_bundle, format_cfg
                    )

                    # t_step = time_ns()
                    # estimate_debug(str(t_step - t_estimate_start))

                    with array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                        # t_format_start = time_ns()
                        bytes_written = nb_segment_format(
                            handle.array,
                            indices.array,
                            match_count,
                            segment.bundle,
                            reg_bundle,
                            format_cfg,
                            tz_offset_sec,
                        )

                        # format_debug(str(time_ns() - t_format_start))
                        decoded_str = handle.array[:bytes_written].tobytes().decode("utf-8", errors="replace")
                        string_batches.append(decoded_str)

                    # indices.array is a pooled buffer reused next iteration, so copy the matched
                    # rows' sequence ids out now rather than keeping a view into it.
                    seq_batches.append(segment.bundle.sequences[indices.array[:match_count]].copy())

                    total_new_rows += match_count

                if total_new_rows >= self.max_rows:
                    reached_live_edge = False
                    break
        t_segments_end = time.time_ns()

        # Update tracker to the absolute newest log evaluated to drop the unprocessed backlog
        self.latest_seq_seen = max(self.latest_seq_seen, highest_seq_seen_this_tick)

        # if total_new_rows > 0:

        # Catch-up logic ...
        was_catching_up = self._is_catching_up
        if self._is_catching_up and reached_live_edge:
            self._is_catching_up = False

        if total_new_rows > 0:
            if was_catching_up:
                is_clogged = False
                self.velocity_tracker.reset()
            else:
                is_clogged = self.velocity_tracker.update_and_check(total_new_rows)

            # t_ui_start = time_ns()
            if is_clogged:
                # Freeze into a browsable history window instead of just dropping this batch
                # silently - the guards above already guarantee we were live/unpaused, so this
                # is the only place a clog can be discovered.
                self._enter_history_mode(auto=True)
            else:
                # We processed the newest segments first, so they are at the front of the list.
                # Reversing makes the older segments render first, yielding perfect chronological order.
                string_batches.reverse()
                full_string_batch = "".join(string_batches)

                self.text_area.append_log(full_string_batch)

                seq_batches.reverse()
                if seq_batches:
                    self._live_seqs.extend(np.concatenate(seq_batches).tolist())
            # t_ui_end = time_ns()

            # self.logger.debug(f"[Profile] UI Text Append: {(t_ui_end - t_ui_start) / 1_000_000:.3f} ms")
        # t_func_end = time_ns()
        # self.logger.child("total").debug(f"{t_func_end - t_start_total} rows={total_new_rows}")

    def _refresh_view(self):
        """Reapplies the current filter/level/kv/search/column-visibility settings to whatever's
        currently on screen, without forcing a return to live mode. If paused/browsing history,
        re-fetches the same anchor's window under the new settings and stays put; if live,
        refills from the tail as before. Every settings-change handler should call this instead
        of _redraw_history() directly, so adjusting a filter never silently un-pauses you."""
        if self.view_mode == LogViewMode.HISTORY:
            self._reanchor_history(self.history_anchor_seq)
        else:
            self._redraw_history()

    def _redraw_history(self):
        """
        Clears the screen and triggers a full re-fetch from the central memory pool
        using the updated column visibility toggles. Also the mechanism used to explicitly
        return to live mode (unpausing, or catching up while paging history forward) - it always
        refills from the live tail, so any caller effectively resumes tailing. Settings-change
        handlers should call _refresh_view() instead, which only falls back to this when already
        live.

        Wrapped in the _programmatic_scroll guard like _reanchor_history: clear() and
        append_log()'s auto-scroll-to-bottom both move the scrollbar and fire valueChanged just
        like a user scroll would, so without the guard a rapid pause/resume cycle can recurse
        back into _on_scroll_value_changed and overflow the stack (qt-log-table-viewer skill
        Sec 8) - this isn't a catchable Python exception, it crashes the process outright.
        """
        self._programmatic_scroll = True
        try:
            # Detach highlighter to prevent synchronous freezing during bulk insert
            self.highlighter.setDocument(None)
            self.text_area.clear()

            self.view_mode = LogViewMode.LIVE
            self.history_anchor_seq = None
            self.history_oldest_seq = None
            self.history_newest_seq = None
            self._live_seqs.clear()
            self.text_area.set_max_block_count(self.LIVE_MAX_BLOCKS)
            self._set_pause_ui(False)

            # Reset trackers so apply_updates fetches everything again
            self.latest_seq_seen = self.latest_seq_manual
            self.velocity_tracker.reset()
            self._is_catching_up = True

            # Force an immediate UI update rather than waiting for the next timer tick
            self.apply_updates()

            # Reattach highlighter (Qt will now highlight lazily)
            self.highlighter.setDocument(self.text_area.document())
        finally:
            self._programmatic_scroll = False

    def clear_logs(self):
        # clear() moves the scrollbar and fires valueChanged just like a user scroll would -
        # guard it the same way _redraw_history/_reanchor_history do, so a Clear click while
        # browsing history can't reenter _on_scroll_value_changed mid-reset.
        self._programmatic_scroll = True
        try:
            self.text_area.clear()

            self.view_mode = LogViewMode.LIVE
            self.history_anchor_seq = None
            self.history_oldest_seq = None
            self.history_newest_seq = None
            self._live_seqs.clear()
            self.text_area.set_max_block_count(self.LIVE_MAX_BLOCKS)
        finally:
            self._programmatic_scroll = False

        log_pool = self.gui_context.registry.central.log_pool

        self.latest_seq_manual = self.latest_seq_seen = log_pool.latest_sequence()

        self.velocity_tracker.reset()
        self._is_catching_up = True

    # --- Live/history mode transitions -----------------------------------------------------

    def _fetch_history_window(self, anchor_seq):
        """Fetches a bounded window of formatted lines before/after anchor_seq, mirroring
        LogTableStore._fetch_history (qt-log-table-viewer skill Sec 1/14) but producing
        already-formatted text instead of columnar rows, since SearchableLogArea is a
        QPlainTextEdit rather than a direct-paint grid.

        Returns (before_count, after_count, oldest_seq, newest_seq, reached_start, text).
        """
        self._ensure_effective_mask()

        reg = self.gui_context.id_registry
        array_pool = self.gui_context.registry.system_ctx.array_pool
        pool = self.gui_context.registry.central.log_pool
        f = self.log_filter
        tz_offset_sec = get_local_utc_offset_seconds()
        format_cfg = FormattingConfig(
            self.show_ts,
            self.show_dev,
            self.show_lvl,
            self.show_mod,
            self.ts_precision,
            show_date=self.show_date,
            show_rx_ts=self.show_rx_ts,
        )
        kv = f.bake_kv_arrays()
        text = f.bake_text_search()
        reg_bundle = reg.bundle()

        def _format_matches(segment, indices_array, match_count):
            req_bytes = nb_segment_estimate_out_size(
                indices_array, match_count, segment.bundle, reg_bundle, format_cfg
            )
            with array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                bytes_written = nb_segment_format(
                    handle.array, indices_array, match_count, segment.bundle, reg_bundle, format_cfg, tz_offset_sec
                )
                return handle.array[:bytes_written].tobytes().decode("utf-8", errors="replace")

        # --- "Before" set: matches with seq <= anchor_seq - 1, scanning newest-to-oldest ---
        before_count = 0
        oldest_seq = anchor_seq
        before_batches = []

        if anchor_seq is not None and anchor_seq > dtypes.SEQ_START:
            with pool.get_reversed_snapshot() as segments, pool.acquire_indices_buffer() as indices:
                for segment in segments:
                    if segment.size == 0:
                        continue

                    allowed = self.HISTORY_BEFORE - before_count
                    if allowed <= 0:
                        break

                    match_count = segment_filter_reversed(
                        segment.bundle,
                        effective_mask=self._effective_mask,
                        out_indices=indices.array,
                        max_matches=allowed,
                        end_seq=anchor_seq - 1,
                        kv=kv,
                        text=text,
                    )

                    if match_count > 0:
                        before_batches.append(_format_matches(segment, indices.array, match_count))
                        oldest_seq = int(segment.bundle.sequences[indices.array[0]])
                        before_count += match_count

        # A single segment holding more than HISTORY_BEFORE matching rows would previously read
        # as "reached the start" just because the deque happened to run out right there - it hit
        # the cap within that segment via max_matches, not because there's genuinely nothing
        # older. Only treat this as the true start when the fetch came back under quota; hitting
        # quota exactly always defers to the next backward page, which self-corrects (an empty
        # follow-up fetch then legitimately reports reached_start).
        reached_start = before_count < self.HISTORY_BEFORE

        # Segments were scanned newest-to-oldest, so the collected batches need reversing to
        # read oldest-first, same technique apply_updates uses for its reversed live scan.
        before_batches.reverse()

        # --- "After" set: matches with seq >= anchor_seq, scanning oldest-to-newest ---
        after_count = 0
        newest_seq = anchor_seq
        after_batches = []
        start_seq_lower_bound = SEQ_NONE if anchor_seq is None else anchor_seq - 1

        with pool.get_snapshot() as segments, pool.acquire_indices_buffer() as indices:
            for segment in segments:
                if segment.size == 0:
                    continue

                allowed = self.HISTORY_AFTER - after_count
                if allowed <= 0:
                    break

                match_count = segment_filter(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    max_matches=allowed,
                    start_seq=start_seq_lower_bound,
                    kv=kv,
                    text=text,
                )

                if match_count > 0:
                    after_batches.append(_format_matches(segment, indices.array, match_count))
                    newest_seq = int(segment.bundle.sequences[indices.array[match_count - 1]])
                    after_count += match_count

        text_result = "".join(before_batches) + "".join(after_batches)

        return before_count, after_count, oldest_seq, newest_seq, reached_start, text_result

    def _enter_history_mode(self, auto: bool = False):
        """Called the moment the user scrolls away from the live tail, or when clog protection
        (auto=True) or the manual Pause button needs to freeze the view. Anchors the new history
        window on whichever live line is currently at the top of the viewport, so the view
        doesn't visually jump."""
        if not self._live_seqs:
            return
        idx = max(0, min(self.text_area.first_visible_block(), len(self._live_seqs) - 1))
        self._reanchor_history(self._live_seqs[idx], auto=auto)

    def _reanchor_history(self, anchor_seq, auto: bool = False):
        """Rebuilds the history window around anchor_seq and repositions the view on it. Both
        callers below change the scrollbar's value programmatically, which would otherwise
        re-fire valueChanged and recurse straight back into _on_scroll_value_changed - the guard
        flag suppresses that re-entrancy (qt-log-table-viewer skill Sec 8)."""
        was_live = self.view_mode != LogViewMode.HISTORY
        self._programmatic_scroll = True
        try:
            before_count, after_count, oldest_seq, newest_seq, reached_start, text_result = (
                self._fetch_history_window(anchor_seq)
            )

            if before_count == 0 and after_count == 0:
                return

            self.view_mode = LogViewMode.HISTORY
            self.history_anchor_seq = anchor_seq
            self.history_oldest_seq = oldest_seq
            self.history_newest_seq = newest_seq
            self.history_reached_start = reached_start
            if was_live:
                # Pause and history mode are the same state (see _set_pause_ui) - only sync the
                # button on the live->history transition edge, not on every page-through reanchor
                # while already browsing history.
                self._set_pause_ui(True, auto=auto)

            self.highlighter.setDocument(None)
            self.text_area.set_max_block_count(before_count + after_count)
            self.text_area.setPlainText(text_result)
            self.highlighter.setDocument(self.text_area.document())

            self.text_area.scroll_to_block(before_count)

            # If this fetch's "after" set already reaches the backend's latest row, there may be
            # too little remaining content to fill the viewport - the scrollbar's range can
            # collapse to "nothing to scroll", meaning valueChanged will never fire again to
            # tell us we've caught up. Check eagerly here instead of only reacting to future
            # scrolls (qt-log-table-viewer skill Sec 9).
            #
            # Only applies when paging forward within an already-open history window
            # (was_live is False) - on the initial live->history transition (Pause click or
            # scrolling away from the tail), the anchor is deliberately right at/near the tail,
            # so this would otherwise immediately bounce straight back to live and undo the
            # pause the user just asked for.
            if not was_live:
                pool = self.gui_context.registry.central.log_pool
                if newest_seq is not None and newest_seq >= pool.latest_sequence():
                    self._redraw_history()
        finally:
            self._programmatic_scroll = False

    def _on_scroll_value_changed(self, value):
        if self._programmatic_scroll:
            return

        scrollbar = self.text_area.verticalScrollBar()

        if self.view_mode == LogViewMode.LIVE:
            # Any scroll away from the tail - not just reaching the very top of the small live
            # buffer - should drop out of live-follow mode, matching a user's expectation that
            # scrolling up stops the view out from under them.
            if value < scrollbar.maximum() - 1:
                self._enter_history_mode()
            return

        if value <= scrollbar.minimum() + 1:
            if not self.history_reached_start and self.history_oldest_seq is not None:
                self._reanchor_history(self.history_oldest_seq)
        elif value >= scrollbar.maximum() - 1:
            if self.history_newest_seq is not None and self.history_newest_seq != self.history_anchor_seq:
                self._reanchor_history(self.history_newest_seq)

    def _toggle_telemetry_sidebar(self, checked):
        """Toggles the visibility of the Telemetry sidebar."""
        self.show_telemetry = checked
        self.telemetry_sidebar.setVisible(checked)
        # Update tab_params so the state is saved

    def _toggle_pause(self, checked):
        """Pause and history mode are the same state from the user's perspective: pausing means
        freezing on a browsable window, and scrolling away from the tail already does that.
        Fires from the user clicking the Pause button (checked=Qt's new state)."""
        if checked:
            if self.view_mode == LogViewMode.LIVE:
                self._enter_history_mode(auto=False)
                if self.view_mode != LogViewMode.HISTORY:
                    # No live data yet to build a window from - still honor the pause request.
                    self._set_pause_ui(True)
            # else: already in history (e.g. from a prior scroll-away) - the button just
            # catches up to that; _set_pause_ui was already applied on that transition.
        else:
            self._redraw_history()

    def _set_pause_ui(self, paused: bool, auto: bool = False):
        """Syncs self.is_paused/self.auto_paused and the Pause button's text/checked/style to
        match. Every path that freezes (manual click, clog protection, scrolling away from the
        tail) or thaws (unpausing, scroll back to the live edge, filter changes) routes through
        here or through _redraw_history() so the button never drifts out of sync with view_mode."""
        self.is_paused = paused
        self.auto_paused = paused and auto

        self.action_pause.blockSignals(True)
        self.action_pause.setChecked(paused)
        self.action_pause.blockSignals(False)

        self.action_pause.setText(("▶ Resume (AUTO)" if self.auto_paused else "▶ Resume") if paused else "⏸ Pause")

        if not paused:
            self.velocity_tracker.reset()

        # Update the Stylesheet Property
        # We need to find the widget associated with the action in the toolbar
        button = self.toolbar.widgetForAction(self.action_pause)
        if button:
            # Set the properties defined in our CSS
            button.setProperty("autoPaused", self.auto_paused)
            button.setProperty("manualPaused", paused and not self.auto_paused)

            # Force Qt to re-evaluate the stylesheet (required for dynamic properties)
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

    def closeEvent(self, event):
        """Clean up by unregistering from the GUI context."""
        self.gui_context.deregister_log_target(self)
        self.gui_context.remove_updatable(self)

        self.gui_context.remove_updatable(self.telemetry_sidebar)
        super().closeEvent(event)

    def _set_ts_precision(self, precision: int):
        if self.ts_precision != precision:
            self.ts_precision = precision
            self._refresh_view()

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for log filtering/formatting kernels (nb_filter_segment,
        nb_segment_filter_reversed, nb_segment_estimate_out_size, nb_segment_format). Requires
        data in the pool, provided by NumbaWarmupHelper.exercise_logging_kernels().

        kv/text are NamedTuples of numpy arrays, and Numba types an array's read-only-ness as
        part of its signature (see numba-njit skill §3/§9). EMPTY_KV_CONDITIONS/EMPTY_TEXT_SEARCH
        (ops/kv_filter.py, ops/text_filter.py) are deliberately built from
        np.frombuffer(b"", ...) rather than np.empty(...) so their buffer fields are already
        read-only - the same type Numba sees for a real, non-empty query built via
        build_kv_condition_arrays/build_text_search_arrays. That means this single EMPTY_* call
        below covers the "real kv/text present" case too; no need to separately warm every
        real/empty combination (see numba-njit skill §15)."""

        print("[Warmup] LogViewerWidget ...")

        s_seq = 0

        # Build the Unified Effective Mask for Warmup
        mod_count = helper.registry.module_count()
        safe_capacity = max(10, mod_count)  # Ensure it's large enough for dummy IDs

        effective_mask = np.full(safe_capacity, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)

        effective_mask[helper.floats_mod.id] = LogLevel.ALL.value
        effective_mask[helper.warmup_mod.id] = LogLevel.ALL.value

        format_cfg = FormattingConfig(True, True, True, True)

        with helper.log_pool.get_snapshot() as segments, helper.log_pool.acquire_indices_buffer() as indices:
            for segment in segments:
                match_count = segment_filter(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=1000,
                    start_seq=s_seq,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )

                _ = segment_filter_reversed(
                    segment.bundle,
                    effective_mask=effective_mask,
                    out_indices=indices.array,
                    max_matches=1000,
                    start_seq=s_seq,
                    kv=EMPTY_KV_CONDITIONS,
                    text=EMPTY_TEXT_SEARCH,
                )

                if match_count > 0:
                    req_bytes = nb_segment_estimate_out_size(
                        indices.array, match_count, segment.bundle, helper.registry.bundle(), format_cfg
                    )
                    with helper.array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                        nb_segment_format(
                            handle.array,
                            indices.array,
                            match_count,
                            segment.bundle,
                            helper.registry.bundle(),
                            format_cfg,
                            0,
                        )

        print("[Warmup] LogViewerWidget ... done")

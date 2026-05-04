# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from datetime import datetime
from typing import Iterable

import numpy as np
from qtpy.QtCore import Qt
from qtpy.QtGui import QAction
from qtpy.QtWidgets import QComboBox, QSizePolicy, QSplitter, QToolBar, QVBoxLayout, QWidget

from blinkview.core import dtypes
from blinkview.core.dtypes import ID_UNSPECIFIED, LEVEL_UNSPECIFIED, SEQ_NONE
from blinkview.core.types.empty import EMPTY_ID
from blinkview.core.types.formatting import FormattingConfig
from blinkview.ops.formatting import estimate_log_batch_size, format_log_batch
from blinkview.ops.segments import filter_segment
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.log_velocity_tracker import LogVelocityTracker
from blinkview.ui.widgets.log_highlighter import LogHighlighter
from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.ui.widgets.searchable_log_area import SearchableLogArea
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from blinkview.utils.time_utils import ConsoleTimestampFormatter
from blinkview.utils.utc_offset import get_local_utc_offset_seconds


class LogViewerWidget(QWidget):
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
        self.saved_sizes = None

        self._set_defaults()

        if state:
            self.restore(state)

        self.logger = gui_context.logger.child(f"log_viewer_{id(self):x}")

        self.latest_seq_seen = SEQ_NONE

        self.prev_apply = 0  # Timestamp of the last apply_updates call for throttling

        self.max_rows = 100_000  # Max rows to keep in the text area for performance

        # --- HISTORY BUFFER ---
        # Stores the raw message objects so we can instantly redraw when a toggle changes

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
        self.action_toggle_filter = QAction("Filter", self)
        self.action_toggle_filter.setCheckable(True)
        self.action_toggle_filter.setChecked(self.show_module_filter)
        self.action_toggle_filter.toggled.connect(self._toggle_module_filter)
        self.toolbar.addAction(self.action_toggle_filter)

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
        self.column_actions["show_dev"] = self._add_toggle(
            "Device", self.show_dev, lambda c: self._toggle_col("show_dev", c)
        )
        self.column_actions["show_lvl"] = self._add_toggle(
            "Level", self.show_lvl, lambda c: self._toggle_col("show_lvl", c)
        )
        self.column_actions["show_mod"] = self._add_toggle(
            "Module", self.show_mod, lambda c: self._toggle_col("show_mod", c)
        )

        self.toolbar.addSeparator()

        self.action_clear = QAction("Clear Logs", self)
        self.action_clear.triggered.connect(self.clear_logs)
        self.toolbar.addAction(self.action_clear)

        self.action_end = QAction("Scroll to End", self)
        self.action_end.setToolTip("Scroll to the latest logs")
        self.toolbar.addAction(self.action_end)

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

        self.filter_sidebar = ModuleFilterSidebar(
            gui_context=self.gui_context, target_filter=self.log_filter, parent=self
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

        self.action_end.triggered.connect(self.text_area.scroll_to_end)

        self.splitter.addWidget(self.text_area)

        self.highlighter = LogHighlighter(self.text_area.document())

        self.timestamp_formatter = ConsoleTimestampFormatter()

        self.set_log_index()

        self.telemetry_sidebar = TelemetryTable(
            gui_context=self.gui_context,
            state={
                "tab_name": f"{self.tab_name}_sidebar",
                "filtered_device": self.allowed_device,
                "filtered_module": self.filtered_module,
                "filtered_module_children": self.filtered_module_children,
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

    def restore(self, state: dict):
        self.tab_name = state.get("tab_name", self.tab_name)

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

        self.show_telemetry = view_state.get("show_telemetry", self.show_telemetry)
        self.show_module_filter = view_state.get("show_module_filter", self.show_module_filter)
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
                "show_module_filter": self.show_module_filter,
                "show_telemetry": self.show_telemetry,
                "splitter_sizes": self.splitter.sizes(),
            },
            "log_level": self.log_filter.log_level.name_conf,
            "filter_sidebar": self.filter_sidebar.get_state(),
        }

    def _handle_level_change(self, index):
        # Retrieve the LevelIdentity object from the userData
        level_identity = self.level_combo.itemData(index)
        self.log_filter.set_level(level_identity.name_conf)

        self._effective_mask = None  # Invalidate cache

        self.clear_logs()

    def set_log_index(self):
        """Updates the syntax highlighter's index based on which columns are active."""
        # The level is always at a fixed position based on which columns are shown
        idx = 0
        if self.show_ts:
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
        """Sets all column toggles to match the 'ALL' button state."""
        # Block signals temporarily so we don't trigger a redraw 4 times
        self.blockSignals(True)

        for attr_name, action in self.column_actions.items():
            action.setChecked(is_checked)
            setattr(self, attr_name, is_checked)

        self.blockSignals(False)

        self.set_log_index()

        # Now trigger a single redraw for the whole batch
        self._redraw_history()

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

        # UI Polish: If all individual columns are checked, 'ALL' should be checked.
        # If any are unchecked, 'ALL' should probably be unchecked.
        all_active = all(action.isChecked() for action in self.column_actions.values())

        # Block signals so checking the 'ALL' button doesn't trigger _toggle_all_columns
        self.action_all.blockSignals(True)
        self.action_all.setChecked(all_active)
        self.action_all.blockSignals(False)

        self._redraw_history()

    def reload_and_redraw(self):
        """Public method to clear current logs and reload from the source with current filters."""
        self._effective_mask = None  # Invalidate cache

        self.clear_logs()
        self.latest_seq_seen = SEQ_NONE  # Reset sequence tracker to ensure we load all relevant logs

    def apply_updates(self):
        if self.is_paused or self.auto_paused:
            return

        now_ns = self.gui_context.registry.now_ns
        t_start = now_ns()

        if t_start - self.prev_apply < 100_000_000:
            return

        self.filter_sidebar.sync_modules()
        self.prev_apply = t_start

        array_pool = self.gui_context.registry.system_ctx.array_pool
        f = self.log_filter
        reg = self.gui_context.id_registry
        pool = self.gui_context.registry.central.log_pool

        tz_offset_sec = get_local_utc_offset_seconds()

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
                t_list = np.array([mod.id for mod in reg.get_all_modules() if mod.device == dev], dtype=dtypes.ID_TYPE)
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
                self._effective_mask = np.maximum(mask_to_use, global_threshold)
            else:
                # Path 2: Tab Fallback Mode
                self._effective_mask = np.full(mod_count, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)

                if self._filter_cache is not None:
                    self._effective_mask[self._filter_cache] = global_threshold
                else:
                    self._effective_mask[:] = global_threshold

        total_new_rows = 0
        full_string_batch = ""
        format_cfg = FormattingConfig(self.show_ts, self.show_dev, self.show_lvl, self.show_mod, 3)

        # Flag to track if we successfully consumed all segments without breaking
        reached_live_edge = True

        with pool.get_snapshot() as segments, pool.acquire_indices_buffer() as indices:
            for segment in segments:
                segment_last_sequence_id = segment.last_sequence_id
                if segment.size == 0 or segment_last_sequence_id <= self.latest_seq_seen:
                    continue

                # print(
                #     f"logviewer_filter_segment("
                #     f"bundle={type(segment.bundle)}, "
                #     f"tm_arr={tm_arr.dtype}, "
                #     f"indices={type(indices.array)}, "
                #     f"filter_mask={type(filter_mask)}, "
                #     f"filter_enabled={type(filter_enabled)}, "
                #     f"s_seq={type(self.latest_seq_seen)}, "
                #     f"t_lvl={type(target_level)}, "
                #     f"t_dev={type(t_device)}, "
                # )
                match_count = filter_segment(
                    segment.bundle,
                    effective_mask=self._effective_mask,
                    out_indices=indices.array,
                    start_seq=self.latest_seq_seen,
                )

                if match_count > 0:
                    req_bytes = estimate_log_batch_size(
                        indices.array, match_count, segment.bundle, reg.bundle(), format_cfg
                    )

                    with array_pool.get(req_bytes, dtype=dtypes.BYTE) as handle:
                        bytes_written = format_log_batch(
                            handle.array,
                            indices.array,
                            match_count,
                            segment.bundle,
                            reg.bundle(),
                            format_cfg,
                            tz_offset_sec,
                        )
                        full_string_batch += handle.array[:bytes_written].tobytes().decode("utf-8", errors="replace")

                    total_new_rows += match_count

                # Even if 0 matches, we have "seen" this segment up to its last sequence.
                self.latest_seq_seen = max(self.latest_seq_seen, segment_last_sequence_id)

                if total_new_rows >= self.max_rows:
                    reached_live_edge = False
                    break

        # CRITICAL FIX 2: Velocity / Auto-Pause Catch-up Logic
        was_catching_up = self._is_catching_up
        # Update catch-up state: If we cleared all segments, we are now live.
        if self._is_catching_up and reached_live_edge:
            self._is_catching_up = False

        if total_new_rows > 0:
            # 2. CHECK the remembered state, not the newly updated one
            if was_catching_up:
                # Bypass velocity tracking while paging in historical logs
                is_clogged = False
                self.velocity_tracker.reset()
            else:
                # Only track velocity for live incoming logs
                is_clogged = self.velocity_tracker.update_and_check(total_new_rows)

            if is_clogged and not self.is_paused:
                self.auto_paused = True
                self.action_pause.setChecked(True)
            elif not (self.is_paused or self.auto_paused):
                self.text_area.append_log(full_string_batch)

    def _redraw_history(self):
        """
        Clears the screen and triggers a full re-fetch from the central memory pool
        using the updated column visibility toggles.
        """
        self.text_area.clear()

        # Reset trackers so apply_updates fetches everything again
        self.latest_seq_seen = SEQ_NONE
        self.velocity_tracker.reset()
        self._is_catching_up = True

        # Force an immediate UI update rather than waiting for the next timer tick
        self.apply_updates()

    def clear_logs(self):
        self.text_area.clear()
        self.latest_seq_seen = SEQ_NONE  # Reset tracker
        self.velocity_tracker.reset()
        self._is_catching_up = True

    def _toggle_telemetry_sidebar(self, checked):
        """Toggles the visibility of the Telemetry sidebar."""
        self.show_telemetry = checked
        self.telemetry_sidebar.setVisible(checked)
        # Update tab_params so the state is saved

    def _toggle_pause(self, checked):
        self.is_paused = checked

        # Update the Text
        if checked:
            text = "▶ Resume (AUTO)" if self.auto_paused else "▶ Resume"
        else:
            text = "⏸ Pause"
            self.auto_paused = False  # Reset auto-flag on manual resume
            self.velocity_tracker.reset()

        self.action_pause.setText(text)

        # Update the Stylesheet Property
        # We need to find the widget associated with the action in the toolbar
        button = self.toolbar.widgetForAction(self.action_pause)
        if button:
            # Set the properties defined in our CSS
            button.setProperty("autoPaused", self.auto_paused)
            button.setProperty("manualPaused", checked and not self.auto_paused)

            # Force Qt to re-evaluate the stylesheet (required for dynamic properties)
            button.style().unpolish(button)
            button.style().polish(button)
            button.update()

        # Handle data catch-up
        if not checked:
            self._redraw_history()

    def closeEvent(self, event):
        """Clean up by unregistering from the GUI context."""
        self.gui_context.deregister_log_target(self)
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)

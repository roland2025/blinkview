# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from typing import Iterable

from qtpy.QtGui import QAction, Qt
from qtpy.QtWidgets import QComboBox, QSizePolicy, QSplitter, QToolBar, QVBoxLayout, QWidget

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.batched_logrows import BatchedLogRows
from blinkview.core.numpy_log import filter_segment, format_log_batch_numba, query_pool
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.log_velocity_tracker import LogVelocityTracker
from blinkview.ui.widgets.log_highlighter import LogHighlighter
from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.ui.widgets.searchable_log_area import SearchableLogArea
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from blinkview.utils.level_map import LogLevel
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.time_utils import ConsoleTimestampFormatter


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
        import numpy as np

        self._np = np

        self._set_defaults()

        if state:
            self.restore(state)

        self.logger = gui_context.logger.child(f"log_viewer_{id(self):x}")

        self.latest_seq_seen = -1

        self.max_rows = 100_000  # Max rows to keep in the text area for performance

        # --- HISTORY BUFFER ---
        # Stores the raw message objects so we can instantly redraw when a toggle changes
        # self.log_history = deque(maxlen=self.max_rows)
        self.log_history = BatchedLogRows(maxlen=self.max_rows)

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

        for lvl in LogLevel.LIST:
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

        self.highlighter = LogHighlighter(self.text_area.document(), self.gui_context.id_registry.level_map)

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

        show_dev_btn = True
        show_mod_btn = True
        if self.allowed_device is not None or self.filtered_module is not None:
            show_dev_btn = False  # If we're filtering to a specific device, the device column is redundant

        if self.filtered_module is not None and not self.filtered_module_children:
            show_mod_btn = False  # If we're filtering to a specific module, the module column is redundant
            show_dev_btn = False

        self.column_actions["show_dev"].setVisible(show_dev_btn)
        self.column_actions["show_mod"].setVisible(show_mod_btn)

        self.action_toggle_filter.setVisible(self.filtered_module is None)

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
        self.log_level = state.get("log_level", self.log_level)

        view_state = state.get("view_state", {})
        self.show_ts = view_state.get("show_ts", self.show_ts)
        self.show_dev = view_state.get("show_dev", self.show_dev)
        self.show_lvl = view_state.get("show_lvl", self.show_lvl)
        self.show_mod = view_state.get("show_mod", self.show_mod)

        if self.allowed_device is not None:
            self.show_dev = False
        if self.filtered_module is not None:
            self.show_mod = False
            self.show_dev = False

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
        self.clear_logs()
        self.latest_seq_seen = -1  # Reset sequence tracker to ensure we load all relevant logs

    def _apply_updates(self):
        # get messages from central storage and show them on the screen

        now_ns = self.gui_context.registry.now_ns
        fetch_start = now_ns()
        batches, self.latest_seq_seen = self.gui_context.registry.central.get_batches(
            self.log_filter, total=self.max_rows, start_seq=self.latest_seq_seen
        )
        fetch_end = now_ns()
        # print(f"[LogViewer] Fetched batches={len(batches)} duration={(fetch_end - fetch_start) / 1e6:.3f} ms")

        first_data = len(self.log_history)

        formatted_rows = []

        for batch in batches:
            self.log_history.put(batch)

            if self.is_paused or self.auto_paused:
                continue

            if first_data > 0 and self.velocity_tracker.update_and_check(len(batch)):
                if not self.is_paused:
                    self.auto_paused = True
                    self.action_pause.setChecked(True)
                    continue

            formatted_rows = self._format_messages(batch, formatted_rows)

        self.text_area.append_log(formatted_rows)

        end = now_ns()
        self.logger.debug(
            f"Fetched batches={len(batches)} total={(end - fetch_start) / 1e6:.3f} ms fetch={(fetch_end - fetch_start) / 1e6:.3f} ms print={(end - fetch_end) / 1e6:.3f} ms rows={len(formatted_rows)}"
        )

    def ____apply_updates(self):
        """
        Drop-in replacement using CircularLogPool with Numba-accelerated filtering.
        """
        now_ns = self.gui_context.registry.now_ns
        t_start = now_ns()

        # 1. Extract Filter Params
        f = self.log_filter
        # Assuming your LogFilter now holds a 'target_modules' list or we use the singular
        t_modules = getattr(f, "target_modules", None)
        t_module = f.filtered_module.id if f.filtered_module else 0xFFFF
        t_device = f.allowed_device.id if f.allowed_device else 0xFFFF

        # 2. Fetch Stage (Accelerated)
        t_fetch_start = now_ns()

        # msg_query_args = f"t_modules={t_modules}, t_module={t_module}, t_device={t_device}, level={f.log_level.value}, start_seq={self.latest_seq_seen}"
        # print(msg_query_args)

        new_batch = []
        # Note: We pass start_seq to the query_pool to let Numba skip old data
        log_gen = query_pool(
            self.gui_context.id_registry,
            self.gui_context.registry.central.log_pool,
            target_modules=t_modules,
            target_module=t_module,
            target_device=t_device,
            target_level=f.log_level.value if f.log_level.value else 0xFF,
            start_seq=self.latest_seq_seen,  # Numba-optimized skip
        )

        for row in log_gen:
            # Update sequence tracker
            if row.seq > self.latest_seq_seen:
                self.latest_seq_seen = row.seq

            new_batch.append(row)

            # Safety cap to prevent UI locking on massive bursts
            if len(new_batch) >= self.max_rows:
                break

        t_fetch_end = now_ns()
        fetch_ms = (t_fetch_end - t_fetch_start) / 1e6

        # 3. Processing Stage
        t_format_ms = 0.0
        t_ui_ms = 0.0
        formatted_count = 0

        if new_batch:
            # Update underlying history storage
            # self.log_history.put(new_batch)

            # Check for Auto-Pause / Velocity logic
            if not (self.is_paused or self.auto_paused):
                is_clogged = len(self.log_history) > 0 and self.velocity_tracker.update_and_check(len(new_batch))

                if is_clogged:
                    if not self.is_paused:
                        self.auto_paused = True
                        self.action_pause.setChecked(True)
                else:
                    # 4. Formatting Stage
                    t_format_start = now_ns()
                    formatted_rows = []
                    formatted_rows = self._format_messages(new_batch, formatted_rows)
                    formatted_count = len(formatted_rows)
                    t_format_end = now_ns()
                    t_format_ms = (t_format_end - t_format_start) / 1e6

                    # 5. UI Append Stage
                    t_ui_start = now_ns()
                    self.text_area.append_log(formatted_rows)
                    t_ui_end = now_ns()
                    t_ui_ms = (t_ui_end - t_ui_start) / 1e6

        t_total_end = now_ns()
        total_ms = (t_total_end - t_start) / 1e6

        # 6. Detailed Performance Telemetry
        # if new_batch or total_ms > 10:  # Only log if there's work or a stutter
        msg = (
            f"LogUpdate | Total: {total_ms:6.2f}ms | "
            f"Fetch: {fetch_ms:6.2f}ms ({len(new_batch)} rows) | "
            f"Format: {t_format_ms:6.2f}ms | "
            f"UI: {t_ui_ms:6.2f}ms | "
            # f"Seq: {self.latest_seq_seen}"
        )
        self.logger.debug(msg)
        # print(msg)

    def apply_updates(self):
        """
        Zero-hydration replacement. Blasts raw bytes from the pool into a single
        formatted string for the UI, bypassing LogRow object creation entirely.
        """
        now_ns = self.gui_context.registry.now_ns
        t_start = now_ns()

        # 1. Setup Filters & Registry Maps
        f = self.log_filter
        reg = self.gui_context.id_registry
        pool = self.gui_context.registry.central.log_pool

        # Get Numba-friendly byte maps

        l_params = reg.level_map_bytes.get_numba_params()
        m_params = reg.module_map.get_numba_params()
        d_params = reg.device_map.get_numba_params()

        # Prepare module whitelist for Numba
        t_modules = getattr(f, "target_modules", None)
        if t_modules:
            tm_arr = self._np.array(t_modules, dtype=self._np.uint16)
        else:
            tm_arr = self._np.empty(0, dtype=self._np.uint16)

        t_module = f.filtered_module.id if f.filtered_module else 0xFFFF
        t_device = f.allowed_device.id if f.allowed_device else 0xFFFF
        target_level = f.log_level.value if f.log_level.value else 0xFF

        # 2. Fetch & Format Stage (Numba Accelerated)
        t_fetch_start = now_ns()

        total_new_rows = 0
        full_string_batch = ""

        # Iterate segments chronologically
        for segment in pool.get_ordered_segments():
            # Quick skip: segment is empty or already fully seen
            if segment.count == 0 or segment.seqs[segment.count - 1] <= self.latest_seq_seen:
                continue

            # Find matching indices in this segment
            indices = filter_segment(
                segment.count,
                segment.timestamps,
                segment.levels,
                segment.modules,
                segment.devices,
                segment.seqs,
                target_modules_arr=tm_arr,
                start_seq=self.latest_seq_seen,
                target_level=target_level,
                target_module=t_module,
                target_device=t_device,
            )

            if len(indices) > 0:
                # Pass the toggles directly to Numba
                raw_bytes = format_log_batch_numba(
                    indices,
                    segment.timestamps,
                    segment.levels,
                    segment.modules,
                    segment.devices,  # segment.devices is the device_id array
                    segment.msg_offsets,
                    segment.msg_lens,
                    segment.msg_buffer,
                    l_params,
                    m_params,
                    d_params,
                    self.show_ts,
                    self.show_dev,
                    self.show_lvl,
                    self.show_mod,
                )

                full_string_batch += raw_bytes.tobytes().decode("utf-8", errors="replace")
                total_new_rows += len(indices)
                self.latest_seq_seen = max(self.latest_seq_seen, segment.seqs[indices[-1]])

            if total_new_rows >= self.max_rows:
                break

        t_fetch_end = now_ns()
        fetch_and_format_ms = (t_fetch_end - t_fetch_start) / 1e6

        # 3. UI Append Stage
        t_ui_ms = 0.0
        if total_new_rows > 0:
            # Velocity / Auto-Pause Logic
            # (Note: we use total_new_rows as the count since we don't have row objects)
            is_ui_empty = self.text_area.document().isEmpty()

            is_clogged = self.velocity_tracker.update_and_check(total_new_rows)

            if is_ui_empty:
                is_clogged = False

            if is_clogged and not self.is_paused:
                self.auto_paused = True
                self.action_pause.setChecked(True)
            elif not (self.is_paused or self.auto_paused):
                t_ui_start = now_ns()
                # Pass the single giant string to your smart append_log
                self.text_area.append_log(full_string_batch)
                t_ui_end = now_ns()
                t_ui_ms = (t_ui_end - t_ui_start) / 1e6

        t_total_end = now_ns()
        total_ms = (t_total_end - t_start) / 1e6

        # 4. Telemetry
        if total_new_rows:
            msg = f"rows={total_new_rows} total={total_ms:.2f}ms fetch={fetch_and_format_ms:.2f}ms ui={t_ui_ms:.2f}ms"
            self.logger.debug(msg)
            # print(msg)

    def _format_messages(self, messages: Iterable, rows=None) -> list:
        """Dynamically builds the string based on the active toggles."""
        format_ts = self.timestamp_formatter.format
        rows = rows or []
        append = rows.append

        # Local cache for speed
        show_ts = self.show_ts
        show_dev = self.show_dev
        show_lvl = self.show_lvl
        show_mod = self.show_mod

        for msg in messages:
            parts = []
            if show_ts:
                parts.append(format_ts(msg.timestamp_ns))
            if show_dev:
                parts.append(msg.module.device.name)
            if show_lvl:
                parts.append(msg.level.name)
            if show_mod:
                parts.append(f"{msg.module.name}:")

            parts.append(msg.message)

            # Join the active parts with a space
            append(" ".join(parts))

        return rows

    def _redraw_history(self):
        """Instantly clears the screen and redraws all historical logs with the new layout."""
        self.text_area.clear()
        if self.log_history:
            # Re-sync the high watermark to the last item in our history buffer
            self.latest_seq_seen = self.log_history[-1][-1].seq
            formatted_rows = []
            for batch in self.log_history:
                formatted_rows.extend(self._format_messages(batch))

            print(
                f"[BlinkMainWindow] Redrawing historical logs batches={len(self.log_history)} rows={len(formatted_rows)}"
            )

            self.text_area.setPlainText("\n".join(formatted_rows))

            # Scroll to the bottom
            self.text_area.scroll_to_end()

    def clear_logs(self):
        self.log_history.clear()
        self.text_area.clear()
        self.latest_seq_seen = -1  # Reset tracker

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

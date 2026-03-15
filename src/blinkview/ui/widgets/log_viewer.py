# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter

from PySide6.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QToolBar, QSplitter, QSizePolicy, QComboBox
from PySide6.QtGui import QFont, QAction, QSyntaxHighlighter, QTextCharFormat, QColor, QTextCursor, Qt

from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.id_registry import IDRegistry
from blinkview.core.system_context import SystemContext
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.log_velocity_tracker import LogVelocityTracker
from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.ui.widgets.module_filter_table import ModuleFilterTable, TempLogFilter
from blinkview.ui.widgets.telemetry_model import TelemetryModel
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from blinkview.utils.level_map import LevelMap, LogLevel
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.time_utils import ConsoleTimestampFormatter


from collections import deque
from PySide6.QtWidgets import QWidget, QVBoxLayout, QPlainTextEdit, QToolBar
from PySide6.QtGui import QFont, QAction


class LogHighlighter(QSyntaxHighlighter):
    def __init__(self, parent, level_map: LevelMap):
        super().__init__(parent)

        self.level_map: LevelMap = level_map

        # Define formats for each level
        # self.formats = {
        #     'I': self._create_format("#808080"),  # Gray for Info
        #     'W': self._create_format("#FFCC00", bold=True),  # Amber for Warning
        #     'E': self._create_format("#FF3333", bold=True),  # Red for Error
        # }
        self.formats = {}

        self.level_index = 0

        for level in level_map.levels():
            self.formats[level.name] = self._create_format(level.color, bold=level >= LogLevel.WARN)

    def _create_format(self, color_hex, bold=False):
        fmt = QTextCharFormat()
        fmt.setForeground(QColor(color_hex))
        if bold:
            fmt.setFontWeight(QFont.Bold)
        return fmt

    def set_index(self, idx):
        self.level_index = idx

    def highlightBlock(self, text):
        """Called automatically by Qt when a line needs rendering."""
        try:
            idx = self.level_index
            # Assuming the level is the 3rd 'word' in your string:
            # "17:28:35.459 ABC E asi: ..."
            parts = text.split(maxsplit=idx+1)  # Split into at most idx+1 parts to avoid unnecessary splitting
            # if len(parts) > idx:
            fmt = self.formats[parts[idx]]
            self.setFormat(0, len(text), fmt)
        except (KeyError, IndexError):
            # If the expected level part is missing or not recognized, we can skip formatting
            pass


class LogViewerWidget(QWidget):
    def __init__(self, gui_context, tab_name, allowed_device=None, filtered_module=None, view_state=None, log_level=None, filter_sidebar=None, parent=None):
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

        self.show_telemetry = False
        self.show_module_filter = False
        self.show_ts = True
        self.show_dev = True
        self.show_lvl = True
        self.show_mod = True
        saved_sizes = None
        # 1. Define Defaults / Restore View State
        if view_state is not None:
            self.show_ts = view_state.get("show_ts", self.show_ts)
            self.show_dev = view_state.get("show_dev", self.show_dev)
            self.show_lvl = view_state.get("show_lvl", self.show_lvl)
            self.show_mod = view_state.get("show_mod", self.show_mod)
            self.show_telemetry = view_state.get("show_telemetry", self.show_telemetry)
            self.show_module_filter = view_state.get("show_module_filter", self.show_module_filter)
            saved_sizes = view_state.get("splitter_sizes")

        if allowed_device is not None or filtered_module is not None:
            self.show_dev = False  # If we're filtering to a specific device, the device column is redundant

        if filtered_module is not None:
            self.show_mod = False  # If we're filtering to a specific module, the module column is redundant
            self.show_dev = False

        if log_level is None:
            log_level = LogLevel.ALL.name_conf

        self.tab_name = tab_name

        self.latest_seq_seen = -1

        self.max_rows = 100_000  # Max rows to keep in the text area for performance

        # --- HISTORY BUFFER ---
        # Stores the raw message objects so we can instantly redraw when a toggle changes
        self.log_history = deque(maxlen=self.max_rows)

        # Main layout
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        # Toolbar
        self.toolbar = QToolBar("Log Viewer Toolbar", self)
        self.toolbar.setMovable(False)
        self.layout.addWidget(self.toolbar)

        print(f"[LogViewer] Initializing with allowed_device={allowed_device}, filtered_module={filtered_module}, log_level={log_level}")
        self.action_toggle_filter = QAction("Filter", self)
        self.action_toggle_filter.setCheckable(True)
        self.action_toggle_filter.setChecked(self.show_module_filter)
        self.action_toggle_filter.toggled.connect(self._toggle_module_filter)
        self.action_toggle_filter.setVisible(filtered_module is None)
        self.toolbar.addAction(self.action_toggle_filter)

        self.level_combo = QComboBox()

        for lvl in LogLevel.LIST:
            self.level_combo.addItem(lvl.name_conf, lvl)  # lvl is the LevelIdentity object

        self.toolbar.addWidget(self.level_combo)

        # select and set the current index based on the log_level in tab_params
        idx = self.level_combo.findData(LogLevel.from_string(log_level))
        if idx != -1:
            self.level_combo.setCurrentIndex(idx)

        self.level_combo.currentIndexChanged.connect(self._handle_level_change)

        self.toolbar.addSeparator()
        # --- SHIFT TOGGLES ---
        self.column_actions = {}

        # 1. Add the Master "ALL" Toggle
        self.action_all = QAction("ALL", self)
        self.action_all.setCheckable(True)
        self.action_all.setChecked(True)
        self.action_all.toggled.connect(self._toggle_all_columns)
        self.toolbar.addAction(self.action_all)

        self.column_actions['show_ts'] = self._add_toggle("Time", self.show_ts, lambda c: self._toggle_col('show_ts', c))
        if self.show_dev:
            self.column_actions['show_dev'] = self._add_toggle("Device", self.show_dev, lambda c: self._toggle_col('show_dev', c))
        self.column_actions['show_lvl'] = self._add_toggle("Level", self.show_lvl, lambda c: self._toggle_col('show_lvl', c))
        if self.show_mod:
            self.column_actions['show_mod'] = self._add_toggle("Module", self.show_mod, lambda c: self._toggle_col('show_mod', c))

        self.toolbar.addSeparator()

        self.action_clear = QAction("Clear Logs", self)
        self.action_clear.triggered.connect(self.clear_logs)
        self.toolbar.addAction(self.action_clear)

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

        # 2. Add it to the toolbar (this pushes everything following it to the right)
        self.toolbar.addWidget(spacer)

        self.action_telemetry = QAction("Telemetry Table", self)
        self.action_telemetry.setCheckable(True)
        self.action_telemetry.setChecked(self.show_telemetry)
        self.action_telemetry.toggled.connect(self._toggle_telemetry_sidebar)
        self.toolbar.addAction(self.action_telemetry)

        self.splitter = QSplitter(Qt.Orientation.Horizontal, self)
        self.layout.addWidget(self.splitter)

        self.log_filter = LogFilter(self.gui_context.id_registry, allowed_device, filtered_module, log_level=log_level)

        self.filter_sidebar = ModuleFilterSidebar(
            gui_context=self.gui_context,
            target_filter=self.log_filter,
            parent=self
        )

        self.filter_sidebar.restore_state(filter_sidebar)
        self.filter_sidebar.log_filter.filter_changed.connect(self.reload_and_redraw)

        if filter_sidebar is not None:
            self._filter_enable_toggled(filter_sidebar.get("enabled", False))

        self.filter_sidebar.action_enable.toggled.connect(self._filter_enable_toggled)

        self.filter_sidebar.setMinimumWidth(200)
        self.splitter.addWidget(self.filter_sidebar)
        self.filter_sidebar.setVisible(self.show_module_filter)

        # Text Area
        self.text_area = QPlainTextEdit(self)
        self.text_area.setReadOnly(True)
        self.text_area.setFont(QFont("Consolas", 10))
        self.text_area.setMaximumBlockCount(self.max_rows)
        self.text_area.setUndoRedoEnabled(False)
        self.text_area.setLineWrapMode(QPlainTextEdit.NoWrap)
        self.text_area.setMinimumWidth(300)

        self.splitter.addWidget(self.text_area)

        self.highlighter = LogHighlighter(self.text_area.document(), self.gui_context.id_registry.level_map)

        self.timestamp_formatter = ConsoleTimestampFormatter()

        self.set_log_index()

        self.telemetry_sidebar = TelemetryTable(
            gui_context=self.gui_context,
            tab_name=f"{tab_name}_sidebar",
            filtered_device=allowed_device,
            parent=self
        )

        self.telemetry_sidebar.setMinimumWidth(250)

        self.splitter.addWidget(self.telemetry_sidebar)

        self.telemetry_sidebar.setVisible(self.show_telemetry)

        self.splitter.setStretchFactor(0, 2)  # Filter
        self.splitter.setStretchFactor(1, 6)  # Logs
        self.splitter.setStretchFactor(2, 4)  # Telemetry

        if saved_sizes and len(saved_sizes) == 3:
            if any(size <= 100 for size in saved_sizes):
                print(f"[LogViewer] Warning: Invalid splitter sizes in view state: {saved_sizes}. Using defaults.")
            else:
                self.splitter.setSizes(saved_sizes)

        self.load_history()
        self.gui_context.register_log_target(self)

    def get_state(self):
        return {
            "allowed_device": self.log_filter.allowed_device.name if self.log_filter.allowed_device else None,
            "filtered_module": f"{self.log_filter.filtered_module.device.name}.{self.log_filter.filtered_module.name}" if self.log_filter.filtered_module else None,
            "view_state": {
                "show_ts": self.show_ts,
                "show_dev": self.show_dev,
                "show_lvl": self.show_lvl,
                "show_mod": self.show_mod,
                "show_module_filter": self.show_module_filter,
                "show_telemetry": self.show_telemetry,
                "splitter_sizes": self.splitter.sizes()
            },
            "log_level": self.log_filter.log_level.name_conf,
            "filter_sidebar": self.filter_sidebar.get_state()
        }

    def _fast_append(self, text: str):
        cursor = self.text_area.textCursor()
        cursor.movePosition(QTextCursor.End)

        # This is a 'silent' insert that doesn't trigger as many UI events
        cursor.insertText(text + "\n")

    def _handle_level_change(self, index):
        # Retrieve the LevelIdentity object from the userData
        level_identity = self.level_combo.itemData(index)
        self.tab_params['log_level'] = level_identity.name_conf
        self.log_filter.set_level(level_identity.name_conf)

        self.clear_logs()
        self.load_history()

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

        self.reload_and_redraw()

    def reload_and_redraw(self):
        """Public method to clear current logs and reload from the source with current filters."""
        self.clear_logs()
        self.load_history()

    def process_log_batch(self, batch: list, load_history=False):
        """
        Ingests logs into the view.
        - If load_history: Bypasses filters and velocity checks (assumed pre-filtered).
        - If Live: Applies full Baked Filter and Velocity Throttling.
        """
        # 1. BATCH FILTERING & DE-DUPLICATION
        if load_history:
            # Optimization: History from get_rows() is already pre-filtered.
            # We only do a quick sequence check to ensure zero overlap.
            # batch = [msg for msg in batch if msg.seq > self.latest_seq_seen]
            pass
        else:
            # Live path: Apply the 'Baked' metadata + sequence filter in one pass.
            batch = self.log_filter.filter_batch(batch, after_seq=self.latest_seq_seen)

        if not batch:
            return

        # 2. UPDATE HIGH WATER MARK
        # Crucial to do this BEFORE the pause check so the next batch knows where we left off.
        self.latest_seq_seen = max(self.latest_seq_seen, batch[-1].seq)

        # 3. BACKGROUND STORAGE
        # Deque handles 'max_rows' via its maxlen automatically.
        self.log_history.extend(batch)

        # 4. VELOCITY MONITOR (Throttling)
        # Only run for live logs; historical loads are bursts by nature.
        if not load_history:
            if self.velocity_tracker.update_and_check(len(batch)):
                if not self.is_paused:
                    self.auto_paused = True
                    self.action_pause.setChecked(True)

        # 5. UI UPDATE GATE
        # Pause blocks the text area update, but not the background buffer (Step 3).
        if self.is_paused and not load_history:
            return

        # 6. FORMATTING & RENDERING
        formatted_rows = self._format_messages(batch)
        if not formatted_rows:
            return

        # --- UI RENDER BLOCK ---
        scrollbar = self.text_area.verticalScrollBar()
        was_at_bottom = scrollbar.value() >= (scrollbar.maximum() - 20)

        # Use 'UpdatesEnabled' and 'blockSignals' to stop the UI from flickering
        # and preventing the highlighter from running per-line during large batches.
        self.text_area.setUpdatesEnabled(False)
        self.text_area.blockSignals(True)
        try:
            # Join batch into a single string for one single layout update in QPlainTextEdit
            self._fast_append("\n".join(formatted_rows))
        finally:
            self.text_area.setUpdatesEnabled(True)
            self.text_area.blockSignals(False)

        if was_at_bottom:
            scrollbar.setValue(scrollbar.maximum())

    def _toggle_pause(self, checked):
        self.is_paused = checked
        self.action_pause.setText("▶ Resume" if checked else "⏸ Pause")

        if not checked:
            # When unpausing, catch up the UI with everything missed
            self.auto_paused = False
            self._redraw_history()

    def _format_messages(self, messages: list) -> list:
        """Dynamically builds the string based on the active toggles."""
        format_ts = self.timestamp_formatter.format
        rows = []
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
                parts.append(str(msg.module.device))
            if show_lvl:
                parts.append(str(msg.level))
            if show_mod:
                parts.append(f"{msg.module.name}:")

            parts.append(str(msg.message))

            # Join the active parts with a space
            append(" ".join(parts))

        return rows

    def _redraw_history(self):
        """Instantly clears the screen and redraws all historical logs with the new layout."""
        self.text_area.clear()
        self.text_area.clear()
        if self.log_history:
            # Re-sync the high water mark to the last item in our history buffer
            self.latest_seq_seen = self.log_history[-1].seq

            formatted_rows = self._format_messages(self.log_history)
            self.text_area.setPlainText("\n".join(formatted_rows))

            # Scroll to the bottom
            scrollbar = self.text_area.verticalScrollBar()
            scrollbar.setValue(scrollbar.maximum())

    def clear_logs(self):
        self.log_history.clear()
        self.text_area.clear()
        self.latest_seq_seen = -1  # Reset tracker

    def load_history(self):
        """Special one-time call for the initial historical load."""
        try:
            history = self.gui_context.registry.central.get_rows(
                self.log_filter,
                total=self.max_rows,
                after_seq=self.latest_seq_seen
            )

            self.process_log_batch(history, load_history=True)
        finally:
            self.text_area.verticalScrollBar().setValue(self.text_area.verticalScrollBar().maximum())

    def _toggle_telemetry_sidebar(self, checked):
        """Toggles the visibility of the Telemetry sidebar."""
        self.show_telemetry = checked
        self.telemetry_sidebar.setVisible(checked)
        # Update tab_params so the state is saved

    def _toggle_pause(self, checked):
        self.is_paused = checked

        # 1. Update the Text
        if checked:
            text = "▶ Resume (AUTO)" if self.auto_paused else "▶ Resume"
        else:
            text = "⏸ Pause"
            self.auto_paused = False  # Reset auto-flag on manual resume

        self.action_pause.setText(text)

        # 2. Update the Stylesheet Property
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

        # 3. Handle data catch-up
        if not checked:
            self._redraw_history()

    def closeEvent(self, event):
        """Clean up by unregistering from the GUI context."""
        self.gui_context.deregister_log_target(self)
        super().closeEvent(event)

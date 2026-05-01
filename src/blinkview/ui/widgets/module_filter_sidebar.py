# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import QSize, Qt
from qtpy.QtWidgets import QComboBox, QHeaderView, QLabel, QTableView, QToolBar, QVBoxLayout, QWidget

from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.module_filter_table import ModuleFilterTable, TempLogFilter
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel


class ModuleFilterSidebar(QWidget):
    """
    A self-contained sidebar that wraps the ModuleFilterTable and adds a toolbar.
    """

    def __init__(self, gui_context: GUIContext, target_filter: LogFilter, parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self.log_filter = TempLogFilter(gui_context, target_filter)

        # Main Layout
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        # Setup Toolbar
        self.toolbar = QToolBar(self)
        self.toolbar.setMovable(False)
        self.toolbar.setIconSize(QSize(16, 16))
        self.layout.addWidget(self.toolbar)

        # Add the Master "Enable" Toggle
        # We use a checkable action for that 'pushed-in' look
        self.action_enable = self.toolbar.addAction("Enable Filter")
        self.action_enable.setCheckable(True)
        # Match the initial state of your log filter
        self.action_enable.setChecked(self.log_filter.enabled)
        self.action_enable.toggled.connect(self._on_enable_toggled)

        self.toolbar.addSeparator()

        # Add the Global Level Selector to the Toolbar
        self.toolbar.addWidget(QLabel(" Min: "))
        self.level_combo = QComboBox()
        for lvl in LogLevel.LIST:
            self.level_combo.addItem(lvl.name_conf, lvl)

        self.toolbar.addWidget(self.level_combo)
        self.level_combo.currentIndexChanged.connect(self._on_global_level_changed)

        self.toolbar.addSeparator()

        # Add Pause Indicator
        self.pause_label = QLabel(" ⏸ Sync Paused ")
        # self.pause_label.setStyleSheet("color: #888; font-style: italic; font-size: 10px;")
        # self.pause_label.setVisible(True)ÄÖÖ
        self.pause_action = self.toolbar.addWidget(self.pause_label)
        self.pause_action.setVisible(False)

        # Add the Table (The existing logic)
        self.table = ModuleFilterTable(gui_context, self.log_filter, self)
        self.table.setEnabled(self.action_enable.isChecked())
        self.layout.addWidget(self.table)

        self.table.sync_paused.connect(self.pause_action.setVisible)

    def _on_enable_toggled(self, checked: bool):
        """Toggle whether this tab uses surgical filtering or pass-through."""
        self.log_filter.set_enabled(checked)

        # Visually dim the table when disabled to prevent confusion
        self.table.setEnabled(checked)

    def _on_global_level_changed(self, index):
        """Mass-update the index-based filter for this tab."""
        level_identity = self.level_combo.itemData(index)

        self.log_filter.set_level(level_identity)

        # Trigger the table and log view to refresh
        self.table.fast_model.layoutChanged.emit()

    def get_state(self):
        return {
            "enabled": self.log_filter.enabled,
            "global_level": self.level_combo.itemData(self.level_combo.currentIndex()).name_conf,
            "module_filters": self.log_filter.get_state(),
        }

    def restore_state(self, state):
        if state is None:
            return

        self.action_enable.setChecked(state.get("enabled", False))
        global_level_name = state.get("global_level", "INFO")
        index = self.level_combo.findData(LogLevel.from_string(global_level_name))
        if index != -1:
            self.level_combo.setCurrentIndex(index)

        self.log_filter.restore_state(state.get("module_filters", {}))

    def sync_modules(self):
        """Ensures the underlying Numba masks are sized for all known modules."""
        self.log_filter.sync_modules()

    def get_filter(self):
        return self.log_filter.get_filter()

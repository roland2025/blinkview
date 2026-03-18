# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtGui import QAction, QFont
from PySide6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QTableView,
                               QHeaderView, QLineEdit, QPushButton, QToolBar)
from PySide6.QtCore import Qt, QSortFilterProxyModel, QEvent, QSize, QTimer

from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.action_button_delegate import TelemetryDelegate, TelemetryCol
from blinkview.ui.widgets.config.style_config import StyleConfig
from blinkview.ui.widgets.telemetry_model import TelemetryModel, TelemetryRowState


class MultiColumnFilterProxyModel(QSortFilterProxyModel):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._positive_groups = []
        self._global_negatives = []
        # Store the allowed device name/id (None means allow all)
        self.allowed_device = None

    def setFilterText(self, text):
        """Space = OR, + = AND, - = Global NOT"""
        clean_text = text.lower().strip()
        if not clean_text:
            self._positive_groups = []
            self._global_negatives = []
            self.invalidateFilter()
            return

        chunks = clean_text.split()
        self._global_negatives = [c[1:] for c in chunks if c.startswith('-') and len(c) > 1]
        pos_chunks = [c for c in chunks if not c.startswith('-')]
        self._positive_groups = [c.split('+') for c in pos_chunks if c]

        self.invalidateFilter()

    def setAllowedDevice(self, device_name: str | None):
        """Sets a strict device filter. Only rows from this device will be shown."""
        if self.allowed_device != device_name:
            self.allowed_device = device_name
            # Re-run the filter over all rows
            self.invalidateFilter()

    def filterAcceptsRow(self, source_row, source_parent):
        # --- DIRECT MODEL ACCESS (High Performance) ---
        model: 'TelemetryModel' = self.sourceModel()
        state = model._row_states[source_row]

        # if state.module.latest_row is None:
        #     return False

        # 1. Strict Device Filter Check
        # If a device is specified, reject anything that doesn't match immediately
        if self.allowed_device is not None and state.module.device != self.allowed_device:
            return False

        # 2. Empty Search Filter = Show everything (that passed the device check)
        if not self._positive_groups and not self._global_negatives:
            return True

        # Pre-compute the search string.
        # Accessing state.module directly avoids QModelIndex and QVariant overhead.
        row_content = f"{state.module.name} {state.module.device.name}".lower()

        # 3. Check Global Negatives (NOT)
        if self._global_negatives:
            if any(neg in row_content for neg in self._global_negatives):
                return False

        # 4. Check Positive Groups (OR / AND)
        if not self._positive_groups:
            return True

        for group in self._positive_groups:
            if all(term in row_content for term in group if term):
                return True

        return False


class TelemetryTable(QWidget):
    def __init__(self, gui_context, tab_name, filter_pattern=None, show_device_column=True, filtered_device=None, sort_column=TelemetryCol.DEVICE, sort_order=0, parent=None):
        super().__init__(parent)
        self.gui_context: GUIContext = gui_context

        self.tab_name = tab_name

        self.tab_params = {
            "filter_pattern": filter_pattern,
            "show_device_column": show_device_column,
            "filtered_device": filtered_device,
            "sort_column": sort_column,
            "sort_order": sort_order
        }
        filtered_device = self.tab_params.get("filtered_device", filtered_device)
        show_device_column = self.tab_params.get("show_device_column", show_device_column)

        self.hovered_row = -1

        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(4)  # Small gap between toolbar and table

        # --- 1. CREATE LOCAL TOOLBAR ---
        self.toolbar = QToolBar()
        self.toolbar.setIconSize(QSize(16, 16))
        self.toolbar.setMovable(False)
        # self.toolbar.setStyleSheet("QToolBar { border: none; background: transparent; }")

        # Add Search Box to Toolbar
        self.search_box = QLineEdit()
        self.search_box.setPlaceholderText("Filter (Space=OR, +=AND, -=NOT)...")
        # self.search_box.setFixedWidth(200)
        self.search_box.setClearButtonEnabled(True)
        self.search_box.textChanged.connect(self._on_search_changed)

        self.toolbar.addWidget(self.search_box)

        # Add Module Toggle Action
        self.action_toggle_module = QAction("Device", self)
        self.action_toggle_module.setCheckable(True)
        self.action_toggle_module.setChecked(True)
        self.action_toggle_module.triggered.connect(self._toggle_device_column)
        self.toolbar.addAction(self.action_toggle_module)

        # Add Settings Action
        self.action_settings = QAction("⚙ Options", self)
        self.toolbar.addAction(self.action_settings)

        self.layout.addWidget(self.toolbar)

        # --- 2. SETUP PROXY MODEL ---
        self.proxy_model = MultiColumnFilterProxyModel(self)
        self.proxy_model.setSourceModel(self.gui_context.telemetry_model)

        if filtered_device is not None:
            self.proxy_model.setAllowedDevice(self.gui_context.registry.get_device(filtered_device))
            show_device_column = False  # If we're filtering by device, we can hide the device column for cleaner UI

        # --- 3. SETUP THE VIEW ---
        self.view = QTableView()
        self.view.setModel(self.proxy_model)

        self.view.setStyleSheet("""
            QTableView::item {
                padding-top: 0px;
                padding-bottom: 0px;
                margin: 0px;
                border: none;
            }
        """)

        # --- FORCE BOLD FONT WITH FALLBACKS ---
        font = self.view.font()
        # Set the family string (Qt handles comma-separated fallbacks)
        font.setFamily("Segoe UI, Roboto, sans-serif")

        # Force Boldness
        font.setBold(True)
        # Using Weight 700 (Bold) for better rendering on High-DPI
        font.setWeight(QFont.Weight.Bold)

        # Apply to the TableView
        self.view.setFont(font)

        # Also apply to the Horizontal Header specifically

        # ENABLE SORTING
        self.view.setSortingEnabled(True)

        self.view.clicked.connect(self._on_cell_clicked)

        # Performance & Appearance
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

        # Hover Tracking
        self.view.setMouseTracking(True)
        self.view.entered.connect(self._on_mouse_entered)
        self.view.viewport().installEventFilter(self)

        # Delegate
        self.view.setItemDelegateForColumn(TelemetryCol.VALUE, TelemetryDelegate(self.gui_context.theme, self))

        # Add table below the toolbar
        self.layout.addWidget(self.view)

        # Apply initial filter if one was passed in
        if filter_pattern:
            self.search_box.setText(filter_pattern)
            self.proxy_model.setFilterText(filter_pattern)  # Initialize the filter

        self.action_toggle_module.setChecked(show_device_column)
        self._toggle_device_column(show_device_column)
        self.apply_saved_sort()

        self.gui_context.telemetry_model.layout_changed.connect(self.auto_size_columns_delayed)

        self.auto_size_columns_delayed()

    def auto_size_columns_delayed(self):
        QTimer.singleShot(50, lambda: self.auto_size_columns())

    def auto_size_columns(self):
        """
        Resizes the Name/Module column to fit content.
        Note: We call this on self.view because self is a QWidget.
        """
        # Ensure the view exists and has a header
        header = self.view.horizontalHeader()

        # Resize specifically the Name column (Column 0)

        self.view.resizeColumnToContents(TelemetryCol.DEVICE)

        if header.sectionSize(TelemetryCol.DEVICE) < 70:
            header.resizeSection(TelemetryCol.DEVICE, 70)

        if header.sectionSize(TelemetryCol.DEVICE) > 200:
            header.resizeSection(TelemetryCol.DEVICE, 200)

        self.view.resizeColumnToContents(TelemetryCol.NAME)

        # Enforce a reasonable minimum so the UI stays grounded
        if header.sectionSize(TelemetryCol.NAME) < 100:
            header.resizeSection(TelemetryCol.NAME, 100)

        # Optional: Limit the maximum width so a giant string doesn't
        # push the Value column off-screen
        if header.sectionSize(TelemetryCol.NAME) > 400:
            header.resizeSection(TelemetryCol.NAME, 400)

    def _toggle_device_column(self, visible: bool):
        """Hides or shows the module name column based on button state."""
        # Use our constant for the Name/Module columnmsg
        column_idx = TelemetryCol.DEVICE

        self.view.setColumnHidden(column_idx, not visible)

        # If we just enabled it, make sure it has a reasonable default width
        if visible:
            h_header = self.view.horizontalHeader()
            # If it's not set to stretch, give it a fixed starting width
            if h_header.sectionResizeMode(column_idx) != QHeaderView.Stretch:
                self.view.setColumnWidth(column_idx, 180)

        # Update tab_params so the UIStateHandler saves this preference
        self.tab_params["show_device_column"] = visible

    def _on_search_changed(self, text):
        """Pass the text to our custom proxy model."""
        self.tab_params["filter_pattern"] = text
        self.proxy_model.setFilterText(text)
        self.gui_context.telemetry_model.refresh_active_cache()
        self.auto_size_columns_delayed()

    def _on_mouse_entered(self, index):
        """Update which row shows buttons as the mouse moves."""
        if not index.isValid():
            return

        old_row = self.hovered_row
        self.hovered_row = index.row()

        # Trigger repaint only for the action column (Column 2)
        if old_row != -1:
            self.view.update(self.proxy_model.index(old_row, TelemetryCol.ACTIONS))
        self.view.update(self.proxy_model.index(self.hovered_row, TelemetryCol.ACTIONS))

    def eventFilter(self, source, event):
        """Detect mouse leaving the table to hide all buttons."""
        if event.type() == QEvent.Leave and source is self.view.viewport():
            if self.hovered_row != -1:
                row = self.hovered_row
                self.hovered_row = -1
                self.view.update(self.proxy_model.index(row, TelemetryCol.ACTIONS))
        return super().eventFilter(source, event)

    def on_action_clicked(self, module, action_name):
        """Route the button click back up to the main application."""
        print(f"Action '{action_name}' triggered for {module.name}")
        # Typically you'd emit a signal here or call a method on MainWindow

    def get_active_indices(self) -> list:
        """Called by the Model during refresh_active_cache."""
        if self.isHidden():
            return []

        indices = []
        for proxy_row in range(self.proxy_model.rowCount()):
            source_idx = self.proxy_model.mapToSource(self.proxy_model.index(proxy_row, 0))
            if source_idx.isValid():
                indices.append(source_idx.row())
        return indices

    def showEvent(self, event):
        super().showEvent(event)
        self.gui_context.telemetry_model.register_view(self)
        self.gui_context.telemetry_model.refresh_active_cache()

    def hideEvent(self, event):
        self.gui_context.telemetry_model.unregister_view(self)
        super().hideEvent(event)

    def closeEvent(self, event):
        self.gui_context.telemetry_model.unregister_view(self)
        super().closeEvent(event)

    def _on_cell_clicked(self, proxy_index):
        # 1. Only trigger if the user clicked the NAME column
        if proxy_index.column() != TelemetryCol.NAME:
            return

        # 2. Map to source model index
        source_index = self.proxy_model.mapToSource(proxy_index)
        row_idx = source_index.row()

        # 3. Retrieve the module from our state list
        # Assuming TelemetryModel stores states in self._row_states
        state: TelemetryRowState = self.gui_context.telemetry_model._row_states[row_idx]
        module = state.module

        self.gui_context.create_widget("LogViewerWidget", f"Logs: {module.device.name}.{module.name}", as_window=True, filtered_module=module)

    def sort_by_device(self):
        header = self.view.horizontalHeader()
        # If already sorting by Device, toggle the order
        current_order = header.sortIndicatorOrder()
        new_order = Qt.DescendingOrder if current_order == Qt.AscendingOrder else Qt.AscendingOrder

        self.view.sortByColumn(TelemetryCol.DEVICE, new_order)

    def sort_by_module(self):
        # Sorts by the Module Name (TelemetryCol.NAME)
        self.view.sortByColumn(TelemetryCol.NAME, Qt.AscendingOrder)

    def _on_sort_indicator_changed(self, column, order):
        """Saves the current sort state whenever the user clicks a header."""
        self.tab_params["sort_column"] = column
        self.tab_params["sort_order"] = order.value

    def apply_saved_sort(self):
        """Restores the sort from tab_params."""
        col = self.tab_params.get("sort_column", TelemetryCol.NAME)

        # Get the integer (default to 0 for Ascending)
        order_val = self.tab_params.get("sort_order", 0)

        # Convert integer back to the Enum type
        order = Qt.SortOrder(order_val)

        self.view.sortByColumn(col, order)

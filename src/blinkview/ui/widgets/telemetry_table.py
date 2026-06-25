# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import dataclass
from time import perf_counter
from typing import Optional

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

from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.utils.in_development import set_as_in_development
from blinkview.ui.widgets.action_button_delegate import TelemetryCol, TelemetryDelegate


@dataclass(slots=True)
class TelemetryRowState:
    module: ModuleIdentity
    last_painted_seq: int = 0
    last_painted_msg: str = ""
    last_painted_level: int = 0
    last_change_time: float = 0.0
    last_arrival_time: float = 0.0


class TelemetryTableModel(QAbstractTableModel):
    """
    A per-table model that handles its own filtering and sorting directly,
    pulling high-frequency updates straight from the module_value_tracker.
    """

    layout_changed = Signal()

    def __init__(self, gui_context, parent=None):
        super().__init__(parent)
        self.context: GUIContext = gui_context

        # Global cache to preserve state (like flash timings) across filter changes
        self._all_states: dict[int, TelemetryRowState] = {}
        # The actual rows presented to the View
        self._visible_states: list[TelemetryRowState] = []

        # Filter settings
        self._positive_groups: list[list[str]] = []
        self._global_negatives: list[str] = []
        self.allowed_device: Optional[DeviceIdentity] = None
        self.allowed_module: Optional[ModuleIdentity] = None
        self.allowed_module_children = False

        self.hide_empty = False

        # Sort settings
        self.sort_column = TelemetryCol.DEVICE
        self.sort_order = Qt.AscendingOrder

        self.prev_apply = perf_counter()

    def set_hide_empty(self, hide: bool):
        self.hide_empty = hide
        self.refresh_layout()

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

    def _sync_registry(self, current_modules):
        """Ensures all backend modules have a UI state object."""
        for m in current_modules:
            if m.id not in self._all_states:
                self._all_states[m.id] = TelemetryRowState(module=m)
        self.refresh_layout()

    def refresh_layout(self):
        """Applies filters and sorts the visible cache. Emits a full reset."""
        self.beginResetModel()

        filtered = []
        for state in self._all_states.values():
            module = state.module

            # 1. Device Filter
            if self.allowed_device is not None and module.device != self.allowed_device:
                continue

            # 2. Module/Hierarchy Filter
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

            # 3. Text Filter
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

            if self.hide_empty and state.last_painted_seq == 0:
                continue

            filtered.append(state)

        # 4. Sorting
        reverse = self.sort_order == Qt.DescendingOrder
        if self.sort_column == TelemetryCol.DEVICE:
            filtered.sort(key=lambda s: (s.module.device.name, s.module.name), reverse=reverse)
        elif self.sort_column == TelemetryCol.NAME:
            filtered.sort(key=lambda s: s.module.name, reverse=reverse)
        # Value and Actions columns are generally not sorted in real-time displays

        self._visible_states = filtered

        self.endResetModel()
        self.layout_changed.emit()

    def apply_updates(self):
        """High-frequency pull from the module_value_tracker."""
        now = perf_counter()
        if now - self.prev_apply < 0.1:  # Target ~10-30Hz
            return
        self.prev_apply = now

        current_modules = self.context.id_registry.module_list
        if len(current_modules) != len(self._all_states):
            self._sync_registry(current_modules)

        tracker = self.context.registry.module_value_tracker
        theme = self.context.theme
        fade_dur = theme.fade_duration
        stale_limit = theme.stale_threshold
        data_changed_emit = self.dataChanged.emit
        buffer = 0.02

        # 1. Grab lock-free snapshot from backend
        with tracker.get_snapshot() as snap:
            sequences = snap._seq_mv
            levels = snap._lvl_mv
            try:
                # Intercept hidden empty rows receiving their first payloads
                if getattr(self, "hide_empty", False) and len(self._visible_states) < len(self._all_states):
                    needs_refresh = False
                    for state in self._all_states.values():
                        if state.last_painted_seq == 0:
                            mod_id = state.module.id
                            if sequences[mod_id] > 0:
                                # Pre-populate so it passes the empty filter during refresh
                                state.last_painted_seq = sequences[mod_id]
                                state.last_arrival_time = now
                                state.last_change_time = now
                                state.last_painted_msg = snap.get_message(mod_id)
                                state.last_painted_level = levels[mod_id]
                                needs_refresh = True
                    if needs_refresh:
                        self.refresh_layout()
                        # View fully updates during refresh layout; we can return for this tick
                        return

                # Check for visible states AFTER checking for new arrivals
                if not self._visible_states:
                    return

                for row_idx, state in enumerate(self._visible_states):
                    mod_id = state.module.id
                    current_seq = sequences[mod_id]

                    if current_seq == 0:
                        continue

                    # --- ARRIVAL CHECK ---
                    if current_seq > state.last_painted_seq:
                        state.last_arrival_time = now
                        msg = snap.get_message(mod_id)
                        level = levels[mod_id]

                        if msg != state.last_painted_msg:
                            state.last_change_time = now
                            state.last_painted_msg = msg

                        state.last_painted_seq = current_seq
                        state.last_painted_level = level

                        idx = self.index(row_idx, TelemetryCol.VALUE)
                        data_changed_emit(idx, idx)
                        continue

                    # --- ANIMATION/STALE CHECK ---
                    elapsed_flash = now - state.last_change_time
                    elapsed_stale = now - state.last_arrival_time

                    if elapsed_flash <= (fade_dur + buffer) or (stale_limit <= elapsed_stale <= stale_limit + buffer):
                        idx = self.index(row_idx, TelemetryCol.VALUE)
                        data_changed_emit(idx, idx)
            finally:
                sequences = levels = None

    def rowCount(self, parent=QModelIndex()):
        return len(self._visible_states)

    def columnCount(self, parent=QModelIndex()):
        return 3

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        state = self._visible_states[index.row()]

        if role == Qt.DisplayRole:
            col = index.column()
            if col == TelemetryCol.DEVICE:
                return state.module.device.name
            elif col == TelemetryCol.NAME:
                return str(state.module.name)
            elif col == TelemetryCol.VALUE:
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

        self.action_hide_empty = QAction("Hide Empty", self)
        self.action_hide_empty.setCheckable(True)
        self.action_hide_empty.setChecked(self.hide_empty)
        self.action_hide_empty.triggered.connect(self._toggle_hide_empty)
        self.toolbar.addAction(self.action_hide_empty)

        self.action_settings = QAction("⚙ Options", self)
        self.toolbar.addAction(self.action_settings)

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

        self.resize_timer = QTimer(self)
        self.resize_timer.setSingleShot(True)
        self.resize_timer.timeout.connect(self.auto_size_columns)

        # Hook into global updates
        self.gui_context.add_updatable(self)

        if state:
            self.restore(state)
        else:
            self.auto_size_columns_delayed()

    def closeEvent(self, event):
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)

    def apply_updates(self):
        """Pass update execution to our isolated table model."""
        self.model.apply_updates()

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
        return self.model._visible_states[index.row()].module

    def open_log_viewer(self, module, include_children=False):
        if not module:
            return

        title = f"Logs: {module.device.name}.{module.name}"
        if include_children:
            title += " (+ Children)"

        self.gui_context.create_widget(
            "LogViewerWidget",
            title,
            as_window=True,
            params={"filtered_module": module, "include_children": include_children},
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
            case "view_logs" | "view_logs_children":
                with_children = action_id == "view_logs_children"
                title = f"Logs: {module.name_with_device()}"
                if with_children:
                    title += " (+Children)"

                self.gui_context.create_widget(
                    "LogViewerWidget",
                    title,
                    as_window=True,
                    params={"filtered_module": module, "filtered_module_children": with_children},
                )

            case "copy_name":
                QApplication.clipboard().setText(module.name)

            case "copy_value":
                for row, state in enumerate(self.model._visible_states):
                    if state.module == module:
                        idx = self.model.index(row, TelemetryCol.VALUE)
                        val = idx.data(Qt.DisplayRole)
                        if val and val != "---":
                            QApplication.clipboard().setText(str(val))
                        break

            case "view_graph" | "view_graph_with_children":
                self.gui_context.create_widget(
                    "TelemetryPlotter",
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

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass, field
from typing import List, Optional, Union

from PySide6.QtWidgets import (QWidget, QVBoxLayout, QFormLayout, QHBoxLayout,
                               QLabel, QFrame, QScrollArea, QPushButton, QInputDialog, QSizePolicy, QToolBar, QLineEdit,
                               QGridLayout)
from PySide6.QtCore import Qt, QMimeData

from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from blinkview.ui.gui_context import GUIContext
from builtins import print as builtin_print
from PySide6.QtGui import QDrag, QAction, QFont, QPixmap

from blinkview.utils.generate_id import generate_id


def add_custom_print(cls):
    def custom_print(self, *args):
        builtin_print(f"[{self.__class__.__name__}] {self.tab_name}:", *args)

    cls.print = custom_print
    return cls


class DragHandle(QLabel):
    """A small button-like handle that triggers the drag for a specific index."""

    def __init__(self, index, parent_view):
        super().__init__(" ⁝⁝ ")
        self.index = index
        self.view = parent_view
        self.setFixedWidth(30)
        self.setAlignment(Qt.AlignCenter)
        self.setCursor(Qt.SizeAllCursor)
        self.setStyleSheet("color: #888; background: #333; border-radius: 3px; margin: 2px;")

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            drag = QDrag(self)
            mime = QMimeData()
            mime.setText(str(self.index))
            drag.setMimeData(mime)

            # Simple ghosting effect
            pix = QPixmap(self.size())
            pix.fill(Qt.transparent)
            drag.setPixmap(self.grab())
            drag.exec_(Qt.MoveAction)


@dataclass
class TelemetryEntry:
    """Base class for items in the telemetry list."""

    def to_dict(self) -> dict:
        raise NotImplementedError


@dataclass
class SectionEntry(TelemetryEntry):
    label: str
    type: str = "section"

    def to_dict(self) -> dict:
        return {"type": self.type, "label": self.label}

    def update(self):
        pass


@dataclass(slots=True)
class RowEntry(TelemetryEntry):
    label: str
    key: str = field(default_factory=lambda: generate_id("row"))
    modules: List[ModuleIdentity] = field(default_factory=list)

    # UI/Runtime State
    value_label: Optional[QLabel] = None
    latest: Optional[LogRow] = None
    type: str = "row"

    def update(self):
        """Logic to check for new telemetry data."""
        if not self.value_label:
            return

        best_row = self.latest
        best_seq = best_row.seq if best_row else -1

        for m in self.modules:
            if (row := m.latest_row) and row.seq > best_seq:
                best_row = row
                best_seq = row.seq

        if best_row is not self.latest:
            self.latest = best_row
            self.value_label.setText(best_row.message)

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "key": self.key,
            "label": self.label,
            "modules": [m.name_with_device() for m in self.modules]
        }


@add_custom_print
class TelemetryWatch(QScrollArea):
    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)
        self.setWidgetResizable(True)
        self.setAcceptDrops(True)

        self.container = QWidget()
        self.font = QFont()
        self.font.setBold(True)
        self.container.setFont(self.font)

        # Use a single QGridLayout for the whole container
        self.layout = QGridLayout(self.container)
        self.layout.setContentsMargins(0, 0, 0, 0)
        # self.layout.setSpacing(5)
        self.layout.setAlignment(Qt.AlignTop)

        # Configure columns
        # self.layout.setColumnMinimumWidth(0, 30)  # Handle
        # self.layout.setColumnMinimumWidth(1, 150)  # Label
        self.layout.setColumnStretch(2, 1)  # Content (Fills space)
        # self.layout.setColumnMinimumWidth(3, 80)  # Delete Button

        # --- Toolbar Setup ---
        self.edit_mode = False
        self.toolbar = QToolBar()
        self.toolbar.setMovable(False)

        # Edit Toggle Action
        self.edit_action = QAction("✎ Edit Layout", self)
        self.edit_action.setCheckable(True)
        self.edit_action.triggered.connect(self.toggle_edit_mode)
        self.toolbar.addAction(self.edit_action)

        # Spacer to push "Add" buttons to the right
        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.toolbar.addWidget(spacer)

        # Add Actions (Hidden by default)
        self.add_section_action = QAction("+ Section", self)
        self.add_section_action.triggered.connect(self.prompt_add_section)
        self.add_section_action.setVisible(False)

        self.add_row_action = QAction("+ Row", self)
        self.add_row_action.triggered.connect(self.prompt_add_row)
        self.add_row_action.setVisible(False)

        self.toolbar.addAction(self.add_section_action)
        self.toolbar.addAction(self.add_row_action)

        self.outer_layout = QVBoxLayout()
        self.outer_layout.setContentsMargins(0, 0, 0, 0)
        self.outer_layout.addWidget(self.toolbar)
        self.outer_layout.addWidget(self.container)  # The grid container
        self.outer_layout.addStretch()

        # We need a dummy widget to hold the outer_layout
        self.main_widget = QWidget()
        self.main_widget.setLayout(self.outer_layout)
        self.setWidget(self.main_widget)

        self.entries: List[Union[SectionEntry, RowEntry]] = []

        self.gui_context: GUIContext = gui_context
        self.tab_name = ""
        self.name = None
        self.watch_id = generate_id("watch")
        self._set_defaults()

        self.node = None

        if state:
            self.restore(state)
        else:
            self.node = self.gui_context.gui_config_manager.create_node("watches", self.watch_id,
                                                                        received=self.update_config_schema)
            self.rebuild_ui()

        self.node.fetch()
        self.gui_context.add_updatable(self)

    def __del__(self):
        self.gui_context.remove_updatable(self)

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__

    def dragMoveEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event):
        mime_text = event.mimeData().text()
        pos = event.pos()

        if mime_text.isdigit():
            self._handle_internal_move(int(mime_text), pos)
        else:
            self._handle_external_drop(mime_text, pos)

        event.acceptProposedAction()

    def _handle_internal_move(self, old_index, pos):
        new_index = self._calculate_drop_index(pos)
        if old_index != new_index:
            item = self.entries.pop(old_index)
            # If the item was originally before the target, the list shifted.
            if new_index > old_index:
                new_index -= 1
            self.entries.insert(max(0, new_index), item)
            self.save_config()

    def _handle_external_drop(self, module_id: str, pos):
        target_index = self._calculate_drop_index(pos)
        module = self.gui_context.id_registry.resolve_module(module_id)

        if target_index < len(self.entries):
            target = self.entries[target_index]
            if isinstance(target, RowEntry):
                if module not in target.modules:
                    target.modules.append(module)

                    self.save_config()
                return

        # Fallback: Create new row
        new_row = RowEntry(
            label=module.name,
            modules=[module]
        )
        self.entries.insert(target_index, new_row)
        # self.rebuild_ui()

        self.save_config()

    def _calculate_drop_index(self, pos):
        # Map global drop position to the container's coordinate system
        local_pos = self.container.mapFrom(self.viewport(), pos)

        # Loop through the rows in the grid
        for i in range(len(self.entries)):
            # Check the geometry of the widget in Column 1 for that row
            item = self.layout.itemAtPosition(i, 1)
            if item and item.widget():
                if local_pos.y() < item.widget().geometry().center().y():
                    return i
        return len(self.entries)

    def toggle_edit_mode(self):
        self.edit_mode = self.edit_action.isChecked()
        self.edit_action.setText("✓ Done Editing" if self.edit_mode else "✎ Edit Layout")
        self.add_section_action.setVisible(self.edit_mode)
        self.add_row_action.setVisible(self.edit_mode)
        if not self.edit_mode:
            self.save_config()
        self.rebuild_ui()

    def rebuild_ui(self):
        # 1. Clear the layout completely
        self._clear_layout()

        # 2. Populate the Grid
        for row, entry in enumerate(self.entries):

            # Col 0: Drag Handle
            if self.edit_mode:
                handle = DragHandle(row, self)
                self.layout.addWidget(handle, row, 0)

            # Col 1: Label or Editor
            if self.edit_mode:
                lbl = QLineEdit(entry.label)
                lbl.textEdited.connect(lambda text, e=entry: setattr(e, 'label', text))
            else:
                txt = entry.label.upper() if isinstance(entry, SectionEntry) else entry.label
                lbl = QLabel(txt)
                lbl.setFont(self.font)
                if isinstance(entry, SectionEntry):
                    lbl.setStyleSheet("color: #55aaff;")

            self.layout.addWidget(lbl, row, 1)

            # Col 2: Content (Value or Section Line)
            if isinstance(entry, SectionEntry):
                content = QFrame()
                content.setFrameShape(QFrame.HLine)
                content.setFrameShadow(QFrame.Plain)
            else:
                msg = entry.latest.message if entry.latest else "---"
                content = QLabel(msg)
                content.setFont(self.font)
                entry.value_label = content

            self.layout.addWidget(content, row, 2)

            # Col 3: Remove Button
            if self.edit_mode:
                btn = QPushButton("Remove")
                btn.clicked.connect(lambda checked, r=row: self.remove_item(r))
                self.layout.addWidget(btn, row, 3)

    def _clear_layout(self):
        while self.layout.count() > 0:
            item = self.layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

    def remove_item(self, index):
        self.entries.pop(index)
        self.rebuild_ui()

    # --- State ---

    def get_state(self) -> dict:
        return {
            "tab_name": self.tab_name,
            "id": self.watch_id
        }

    def restore(self, state: dict):
        self.print(f"restore: {state}")
        self.tab_name = state.get("tab_name", self.tab_name)
        self.watch_id = state.get("id") or self.watch_id

        self.node = self.gui_context.gui_config_manager.create_node(f"/watches/{self.watch_id}", on_update=self.update_config_schema)

        self.rebuild_ui()

    def update_config_schema(self, config_: dict, schema_: dict):
        self.print(f"update_config_schema: {config_}")
        raw_entries = config_.get("entries", [])
        self.print(f"id: {self.watch_id}")
        self.name = config_.get("name")
        self.entries = []
        for e in raw_entries:
            if e.get("type") == "section":
                self.entries.append(SectionEntry(e["label"]))
            else:
                mods = self.gui_context.id_registry.resolve_modules(e.get("modules", []))
                self.entries.append(RowEntry(
                    key=e["key"],
                    label=e["label"],
                    modules=mods
                ))

        self.rebuild_ui()
    # --- Prompts ---

    def save_config(self):
        self.node.send_config({
            "name": self.name,
            "tab_name": self.tab_name,
            "id": self.watch_id,
            "entries": [e.to_dict() for e in self.entries]
        })

    def prompt_add_row(self):
        # We don't even need a dialog anymore if we don't want to!
        # Just add a generic row and let the user type the name in the QLineEdit.
        self.entries.append(RowEntry(label="New Metric"))
        self.save_config()
        # self.rebuild_ui()

    def prompt_add_section(self):
        self.entries.append(SectionEntry(label="NEW SECTION"))
        # self.rebuild_ui()
        self.save_config()

    def apply_updates(self):
        for entry in self.entries:
            entry.update()

    @classmethod
    def new_watch(cls, name, parent: dict = None):
        parent = parent or {}
        id_ = generate_id("watch", list(parent.keys()))
        conf = {
            "id": id_,
            "name": name,
        }
        return id_, conf

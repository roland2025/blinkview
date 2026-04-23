# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from builtins import print as builtin_print
from dataclasses import dataclass, field
from time import perf_counter
from typing import List, Optional, Union

from PyQt6.QtCore import QTimer
from qtpy.QtCore import QMimeData, Qt, Signal
from qtpy.QtGui import QAction, QDrag, QFont, QPixmap
from qtpy.QtWidgets import (
    QComboBox,
    QFormLayout,
    QFrame,
    QGridLayout,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QLineEdit,
    QPushButton,
    QScrollArea,
    QSizePolicy,
    QStackedWidget,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from blinkview.core import dtypes
from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.message_box import MessageBox
from blinkview.utils.generate_id import generate_id


def add_custom_print(cls):
    def custom_print(self, *args):
        builtin_print(f"[{self.__class__.__name__}] {self.tab_name}:", *args)

    cls.print = custom_print
    return cls


class HistoryComboBox(QComboBox):
    """A ComboBox that allows cycling through items with the scroll wheel."""

    def __init__(self, parent=None):
        super().__init__(parent)
        # We need to catch wheel events even if they happen over the text area
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)

    def wheelEvent(self, event):
        if self.view().isVisible():
            super().wheelEvent(event)
            return

        # angleDelta().y() > 0 is scrolling UP (usually meant to go to 'previous/newer' items)
        # angleDelta().y() < 0 is scrolling DOWN (usually meant to go to 'next/older' items)
        delta = 1 if event.angleDelta().y() < 0 else -1
        current = self.currentIndex()
        count = self.count()

        if count == 0:
            return

        # Handle the transition from typing (index -1) to scrolling
        if current == -1:
            new_index = 0 if delta > 0 else count - 1
        else:
            new_index = current + delta

        # Clamp index within bounds
        if 0 <= new_index < count:
            self.setCurrentIndex(new_index)
            event.accept()


class ShiftingStackedWidget(QStackedWidget):
    """A StackedWidget that shrinks/grows to fit only the current widget."""

    def sizeHint(self):
        if self.currentWidget():
            return self.currentWidget().sizeHint()
        return super().sizeHint()

    def minimumSizeHint(self):
        if self.currentWidget():
            return self.currentWidget().minimumSizeHint()
        return super().minimumSizeHint()


class RequestComboBox(QComboBox):
    """A ComboBox that signals when the dropdown is about to be shown."""

    aboutToShowPopup = Signal(object)

    def showPopup(self):
        # Trigger the fetch before showing the list
        self.aboutToShowPopup.emit(self)
        super().showPopup()


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


@dataclass(slots=True)
class TelemetryEntry:
    """Base class for items in the telemetry list."""

    def to_dict(self) -> dict:
        raise NotImplementedError

    def clear_widgets(self):
        """Reset UI references to prevent updating deleted widgets."""
        pass


@dataclass(slots=True)
class SectionEntry(TelemetryEntry):
    label: str
    collapsed: bool = False
    type: str = "section"

    def to_dict(self) -> dict:
        return {"type": self.type, "label": self.label, "collapsed": self.collapsed}

    def clear_widgets(self):
        # Sections currently don't hold runtime widget refs,
        # but we implement it for consistency.
        pass


@dataclass(slots=True)
class RowEntry(TelemetryEntry):
    label: str
    key: str = field(default_factory=lambda: generate_id("row"))
    modules: List[ModuleIdentity] = field(default_factory=list)

    # UI/Runtime State
    value_label: Optional[QLabel] = None
    last_painted_seq: dtypes.SEQ_TYPE = dtypes.SEQ_NONE
    last_painted_msg: str = ""
    type: str = "row"

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "key": self.key,
            "label": self.label,
            "modules": [m.name_with_device() for m in self.modules],
        }

    def clear_widgets(self):
        """Release the reference to the QLabel."""
        self.value_label = None


@dataclass(slots=True)
class ButtonEntry(TelemetryEntry):
    label: str
    command_payload: str = ""
    target_device: str = ""  # Now a string to hold the selected device ID/name
    key: str = field(default_factory=lambda: generate_id("btn"))

    # UI Reference
    button_widget: Optional[QPushButton] = None
    type: str = "button"

    def to_dict(self) -> dict:
        return {
            "type": self.type,
            "key": self.key,
            "label": self.label,
            "command_payload": self.command_payload,
            "target_device": self.target_device,
        }

    def clear_widgets(self):
        self.button_widget = None


@dataclass(slots=True)
class GroupStartEntry(TelemetryEntry):
    label: str = "Button Group"
    key: str = field(default_factory=lambda: generate_id("grp_start"))
    type: str = "group_start"

    def to_dict(self) -> dict:
        return {"type": self.type, "key": self.key, "label": self.label}

    def clear_widgets(self):
        pass


@dataclass(slots=True)
class GroupEndEntry(TelemetryEntry):
    key: str = field(default_factory=lambda: generate_id("grp_end"))
    type: str = "group_end"

    def to_dict(self) -> dict:
        return {"type": self.type, "key": self.key}

    def clear_widgets(self):
        pass


@add_custom_print
class TelemetryWatch(QWidget):
    signal_destroy = Signal(QWidget)

    signal_devices_updated = Signal(list)

    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)

        self.default_target = ""

        self.device_cache = {}

        self._stashed_scroll_pos = 0
        self.command_history = []

        self.setAcceptDrops(True)

        self.container = QWidget()
        self.font = QFont()
        self.font.setBold(True)
        self.container.setFont(self.font)

        # Use a single QGridLayout for the whole container
        self.layout = QGridLayout(self.container)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setVerticalSpacing(1)  # Gap between rows (try 0 for ultra-dense)
        self.layout.setHorizontalSpacing(10)  # Keep some horizontal breathing room
        # self.layout.setSpacing(5)
        self.layout.setAlignment(Qt.AlignTop)

        # --- Drag and Drop Visual Indicator ---
        self.drop_indicator = QFrame(self.container)
        self.drop_indicator.hide()
        # Keep it on top of other widgets
        self.drop_indicator.raise_()

        # Configure columns
        # self.layout.setColumnMinimumWidth(0, 30)  # Handle
        # self.layout.setColumnMinimumWidth(1, 150)  # Label
        self.layout.setColumnStretch(2, 1)  # Content (Fills space)
        # self.layout.setColumnMinimumWidth(3, 80)  # Delete Button

        # --- Toolbar Setup ---
        self.edit_mode = False
        self.toolbar = QToolBar()
        # self.toolbar.setStyleSheet("QWidget { border: 1px solid red; }")
        self.toolbar.setMovable(False)

        # Create a Stacked Widget to hold the Name Label and Name Edit
        self.name_stack = ShiftingStackedWidget()

        self.name_stack.setMaximumWidth(300)  # Prevent it from growing too large

        # Normal Mode Label
        self.name_label = QLabel("WATCH")
        self.name_label.setStyleSheet("font-weight: bold; color: #55aaff; margin-left: 5px; font-size: 14px;")
        self.name_stack.addWidget(self.name_label)
        self.name_label.setSizePolicy(QSizePolicy.Policy.Minimum, QSizePolicy.Policy.Preferred)

        # Edit Mode LineEdit
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Watch Name...")
        self.name_edit.setMinimumWidth(200)  # Minimum instead of Fixed
        self.name_edit.textEdited.connect(self._handle_rename)
        self.name_stack.addWidget(self.name_edit)

        # Add the STACK to the toolbar instead of the individual widgets
        self.toolbar.addWidget(self.name_stack)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.toolbar_delete_spacer = self.toolbar.addWidget(spacer)

        self.delete_watch_action = QAction("🗑 Delete Watch", self)
        self.delete_watch_action.triggered.connect(self.prompt_delete_watch)
        self.delete_watch_action.setVisible(False)
        # Optional: Make it look destructive
        self.delete_watch_action.setToolTip("Permanently delete this entire watch tab")

        # Add it to the toolbar
        self.toolbar.addAction(self.delete_watch_action)

        # Spacer to push "Add" buttons to the right
        spacer_2 = QWidget()
        spacer_2.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.toolbar_delete_spacer_2 = self.toolbar.addWidget(spacer_2)

        self.toolbar_delete_spacer_2.setVisible(False)

        self.default_target_label = QLabel(" Default Target: ")
        self.default_target_label.setVisible(False)

        self.default_target_combo = RequestComboBox()
        self.default_target_combo.setFixedWidth(120)

        # Connect the "Show" signal to your fetch method
        self.default_target_combo.aboutToShowPopup.connect(self._trigger_device_fetch)

        # Connect the existing text change handler
        self.default_target_combo.currentTextChanged.connect(self._handle_default_target_change)

        self.default_target_combo.currentIndexChanged.connect(self._handle_default_target_change)
        self.default_target_combo.setVisible(False)

        self.default_target_label_action = self.toolbar.addWidget(self.default_target_label)
        self.default_target_combo_action = self.toolbar.addWidget(self.default_target_combo)

        self.default_target_label_action.setVisible(False)
        self.default_target_combo_action.setVisible(False)

        self.command_input = QComboBox()
        self.command_input.setEditable(True)
        self.command_input.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        self.command_input.lineEdit().setPlaceholderText("Send generic command...")
        self.command_input.setMinimumWidth(150)
        self.command_input.setMaximumWidth(250)
        self.command_input.setSizePolicy(QSizePolicy.Policy.MinimumExpanding, QSizePolicy.Policy.Fixed)

        # Connect the Enter key
        self.command_input.lineEdit().returnPressed.connect(self._handle_generic_command)

        self.command_input_combo_action = self.toolbar.addWidget(self.command_input)

        self.send_command_btn = QPushButton("➔")
        self.send_command_btn.setFixedWidth(30)
        self.send_command_btn.setToolTip("Send Command")
        self.send_command_btn.setStyleSheet("""
            QPushButton { 
                background-color: #444; 
                color: #55aaff; 
                border: 1px solid #55aaff;
                border-radius: 3px;
                font-weight: bold;
            }
            QPushButton:hover { background-color: #55aaff; color: white; }
            QPushButton:pressed { background-color: #3388dd; }
        """)
        self.send_command_btn.clicked.connect(self._handle_generic_command)

        # Add to toolbar and store the action reference for visibility toggling
        self.send_command_btn_action = self.toolbar.addWidget(self.send_command_btn)

        # Add Actions (Hidden by default)
        self.add_section_action = QAction("+ Section", self)
        self.add_section_action.triggered.connect(self.prompt_add_section)
        self.add_section_action.setVisible(False)

        self.add_row_action = QAction("+ Row", self)
        self.add_row_action.triggered.connect(self.prompt_add_row)
        self.add_row_action.setVisible(False)

        self.add_button_action = QAction("+ Button", self)
        self.add_button_action.triggered.connect(self.prompt_add_button)
        self.add_button_action.setVisible(False)

        self.add_button_group_action = QAction("+ Button Group", self)
        self.add_button_group_action.triggered.connect(self.prompt_add_button_group)
        self.add_button_group_action.setVisible(False)

        self.toolbar.addAction(self.add_section_action)
        self.toolbar.addAction(self.add_row_action)
        self.toolbar.addAction(self.add_button_action)
        self.toolbar.addAction(self.add_button_group_action)

        # Edit Toggle Action
        self.edit_action = QAction("✎ Edit", self)
        self.edit_action.setCheckable(True)
        self.edit_action.triggered.connect(self.toggle_edit_mode)
        self.toolbar.addAction(self.edit_action)

        # Create the Scroll Area for the container
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFrameShape(QFrame.NoFrame)
        self.scroll_area.setWidget(self.container)

        # The main layout for the TelemetryWatch widget itself
        self.main_layout = QVBoxLayout(self)
        self.main_layout.setContentsMargins(0, 0, 0, 0)
        self.main_layout.setSpacing(0)
        self.main_layout.addWidget(self.toolbar)
        self.main_layout.addWidget(self.scroll_area)

        self.entries: List = []

        self.prev_apply = 0.0

        self.gui_context: GUIContext = gui_context
        self.tab_name = ""
        self.name = None
        self.watch_id = generate_id("watch")
        self._set_defaults()

        self.node = None

        if state:
            self.restore(state)
        else:
            self.node = self.gui_context.gui_config_manager.create_node(
                "watches", self.watch_id, on_update=self.update_config_schema
            )
            self.rebuild_ui()

        self.node.signal_deleted.connect(lambda: self.signal_destroy.emit(self))

        self.gui_context.add_updatable(self)
        self.auto_scroll_timer = QTimer(self)
        self.auto_scroll_timer.timeout.connect(self._handle_auto_scroll)
        self.scroll_direction = 0
        self.last_drag_pos = None
        self.last_drag_is_internal = False

    def closeEvent(self, event):
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__

    def dragEnterEvent(self, event):
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dragMoveEvent(self, event):
        mime_text = event.mimeData().text()
        if not mime_text:
            event.ignore()
            return

        self.last_drag_pos = event.pos()
        self.last_drag_is_internal = mime_text.isdigit()

        # --- Handle Auto-scroll ---
        # Map the position relative to the scroll area to check boundaries
        scroll_pos = self.scroll_area.mapFrom(self, self.last_drag_pos)
        margin = 40

        if 0 <= scroll_pos.x() <= self.scroll_area.width():
            if scroll_pos.y() < margin:
                self.scroll_direction = -1
                if not self.auto_scroll_timer.isActive():
                    self.auto_scroll_timer.start(30)  # 30ms tick rate
            elif scroll_pos.y() > self.scroll_area.viewport().height() - margin:
                self.scroll_direction = 1
                if not self.auto_scroll_timer.isActive():
                    self.auto_scroll_timer.start(30)
            else:
                self.scroll_direction = 0
                self.auto_scroll_timer.stop()
        else:
            self.scroll_direction = 0
            self.auto_scroll_timer.stop()

        # --- Update Indicator ---
        index, action = self._calculate_drop_state(self.last_drag_pos, self.last_drag_is_internal)
        self._update_drop_indicator(index, action)
        event.acceptProposedAction()

    def dropEvent(self, event):
        self.auto_scroll_timer.stop()
        self.scroll_direction = 0

        mime_text = event.mimeData().text()
        pos = event.pos()

        # Clear the visual indicator
        self._update_drop_indicator(None, None)

        is_internal = mime_text.isdigit()
        index, action = self._calculate_drop_state(pos, is_internal)

        if is_internal:
            self._handle_internal_move(int(mime_text), index, action)
        else:
            self._handle_external_drop(mime_text, index, action)

        event.acceptProposedAction()

    def dragLeaveEvent(self, event):
        """Hide the indicator and stop scrolling if the user drags outside the widget."""
        self.auto_scroll_timer.stop()
        self.scroll_direction = 0
        self._update_drop_indicator(None, None)
        super().dragLeaveEvent(event)

    def _handle_auto_scroll(self):
        """Called by the timer to continuously scroll while dragging near edges."""
        if self.scroll_direction == 0:
            self.auto_scroll_timer.stop()
            return

        scrollbar = self.scroll_area.verticalScrollBar()
        old_val = scrollbar.value()

        # Scroll by 15 pixels per tick (adjust for speed)
        scrollbar.setValue(old_val + (self.scroll_direction * 15))

        # Re-calculate drop indicator since the container shifted under the mouse
        if scrollbar.value() != old_val and self.last_drag_pos is not None:
            index, action = self._calculate_drop_state(self.last_drag_pos, self.last_drag_is_internal)
            self._update_drop_indicator(index, action)

    def _calculate_drop_state(self, pos, is_internal):
        local_pos = self.container.mapFrom(self, pos)

        if not self.entries:
            return 0, "below"

        for i, entry in enumerate(self.entries):
            item = self.layout.itemAtPosition(i, 1)
            if item and item.widget():
                rect = item.widget().geometry()

                if local_pos.y() <= rect.bottom():
                    # --- FIXED AUTO EXPAND ---
                    # Only auto-expand in normal mode. In edit mode,
                    # widgets aren't hidden, so there's no need to expand.
                    if not self.edit_mode and isinstance(entry, SectionEntry) and entry.collapsed:
                        entry.collapsed = False
                        self.rebuild_ui()
                        # Exit immediately! The current layout is now invalid.
                        # The next dragMoveEvent will re-calculate correctly.
                        return i, "above"
                    # -------------------------

                    if is_internal:
                        if local_pos.y() < rect.center().y():
                            return i, "above"
                        else:
                            return i, "below"
                    else:
                        is_row = isinstance(entry, RowEntry)
                        if local_pos.y() < rect.top() + rect.height() * 0.25:
                            return i, "above"
                        elif local_pos.y() > rect.bottom() - rect.height() * 0.25:
                            return i, "below"
                        elif is_row:
                            return i, "into"
                        else:
                            return i, "above"

        return len(self.entries) - 1, "below"

    def _update_drop_indicator(self, index, action):
        """Draws the visual feedback line or box over the layout."""
        if action is None or index is None or index >= len(self.entries):
            self.drop_indicator.hide()
            return

        item_label = self.layout.itemAtPosition(index, 1)
        item_content = self.layout.itemAtPosition(index, 2)

        if not item_label or not item_label.widget():
            self.drop_indicator.hide()
            return

        # Calculate bounding rect for the entire row
        rect = item_label.widget().geometry()
        if item_content and item_content.widget():
            rect = rect.united(item_content.widget().geometry())

        # Expand rect to container width
        rect.setX(0)
        rect.setWidth(self.container.width())

        self.drop_indicator.raise_()
        self.drop_indicator.show()

        if action == "into":
            # Highlight box for merging
            self.drop_indicator.setStyleSheet(
                "background-color: rgba(85, 170, 255, 60); border: 2px solid #55aaff; border-radius: 4px;"
            )
            self.drop_indicator.setGeometry(rect)
        elif action == "above":
            # Thin line above
            self.drop_indicator.setStyleSheet("background-color: #55aaff; border: none;")
            self.drop_indicator.setGeometry(rect.x(), rect.top() - 2, rect.width(), 4)
        elif action == "below":
            # Thin line below
            self.drop_indicator.setStyleSheet("background-color: #55aaff; border: none;")
            self.drop_indicator.setGeometry(rect.x(), rect.bottom() - 2, rect.width(), 4)

    def _handle_internal_move(self, old_index, target_index, action):
        if old_index == target_index and action == "above":
            return

        source_item = self.entries[old_index]

        # --- BLOCK MOVE LOGIC (Sections & Button Groups) ---
        if isinstance(source_item, (SectionEntry, GroupStartEntry)):
            start = old_index
            end = start + 1

            if isinstance(source_item, SectionEntry):
                # Section block: move until the next SectionEntry
                while end < len(self.entries) and not isinstance(self.entries[end], SectionEntry):
                    end += 1
            else:
                # Button Group block: move until the matching GroupEndEntry
                while end < len(self.entries) and not isinstance(self.entries[end], GroupEndEntry):
                    end += 1
                # Include the End marker itself in the block
                if end < len(self.entries):
                    end += 1

            # 2. Extract the whole block
            block = [self.entries.pop(start) for _ in range(end - start)]
            block_size = len(block)

            # 3. Adjust target_index because the list shrank
            if start < target_index:
                target_index -= block_size

            # 4. Smart Target Positioning
            if action == "below":
                target_item = self.entries[target_index] if target_index < len(self.entries) else None

                # If dropping below a Section, move to the end of that section's children
                if isinstance(target_item, SectionEntry):
                    target_index += 1
                    while target_index < len(self.entries) and not isinstance(self.entries[target_index], SectionEntry):
                        target_index += 1

                # If dropping below a Group Start, move to the end of that group (after the End marker)
                elif isinstance(target_item, GroupStartEntry):
                    target_index += 1
                    while target_index < len(self.entries) and not isinstance(
                        self.entries[target_index], GroupEndEntry
                    ):
                        target_index += 1
                    if target_index < len(self.entries):  # Skip over the End marker
                        target_index += 1
                else:
                    target_index += 1

            # 5. Re-insert the whole block
            for i, item in enumerate(block):
                self.entries.insert(max(0, target_index + i), item)

        else:
            # --- STANDARD SINGLE ROW MOVE ---
            item = self.entries.pop(old_index)

            if old_index < target_index:
                target_index -= 1

            if action == "below":
                target_index += 1

            self.entries.insert(max(0, target_index), item)

        self.save_config()

    def _handle_external_drop(self, module_id: str, target_index, action):
        module = self.gui_context.id_registry.resolve_module(module_id)

        if action == "into" and target_index < len(self.entries):
            target = self.entries[target_index]
            if isinstance(target, RowEntry) and module not in target.modules:
                target.modules.append(module)
                self.save_config()
            return

        # If action is "above" or "below", create a new row
        new_row = RowEntry(label=module.name, modules=[module])

        insert_idx = target_index
        if action == "below":
            insert_idx += 1

        self.entries.insert(insert_idx, new_row)
        self.save_config()
        # self.rebuild_ui(scroll_to_bottom=insert_idx >= len(self.entries) - 1)

    def _handle_rename(self, new_name):
        """Update local state and sync to config."""
        self.name = new_name
        # Optional: update tab_name if you want them linked
        # self.tab_name = new_name
        self.save_config()

    def toggle_edit_mode(self):
        edit_mode = self.edit_mode = self.edit_action.isChecked()

        self.edit_action.setText("✓ Done Editing" if edit_mode else "✎ Edit")

        self.name_stack.setCurrentIndex(1 if edit_mode else 0)

        if edit_mode:
            self.name_edit.setText(self.name or "")
            self.name_edit.setFocus()
            self.name_edit.selectAll()
        else:
            self.name_label.setText(self.name.upper() if self.name else "UNNAMED WATCH")

        self.add_section_action.setVisible(edit_mode)
        self.add_row_action.setVisible(edit_mode)
        self.add_button_action.setVisible(edit_mode)
        self.add_button_group_action.setVisible(edit_mode)

        self.default_target_label_action.setVisible(edit_mode)
        self.default_target_combo_action.setVisible(edit_mode)

        self.command_input_combo_action.setVisible(not edit_mode)
        self.send_command_btn_action.setVisible(not edit_mode)

        self.toolbar_delete_spacer_2.setVisible(edit_mode)

        self.delete_watch_action.setVisible(edit_mode)
        if not edit_mode:
            self.save_config()
        self.rebuild_ui()

    def toggle_section(self, section_entry):
        section_entry.collapsed = not section_entry.collapsed
        self.save_config()
        self.rebuild_ui()

    def rebuild_ui(self, scroll_to_bottom=False):
        self.container.setUpdatesEnabled(False)
        scrollbar = self.scroll_area.verticalScrollBar()
        if scrollbar.maximum() > 0:
            self._stashed_scroll_pos = scrollbar.value()

        self._clear_layout()

        current_section_active = False
        is_hidden = False

        # State tracking for flat button groups
        in_button_group = False
        current_btn_layout = None

        self.command_input_combo_action.setVisible(
            len(self.default_target) and self.default_target != "None" and not self.edit_mode
        )

        for row, entry in enumerate(self.entries):
            # --- Section Logic ---
            if isinstance(entry, (SectionEntry, RowEntry)):
                in_button_group = False
                current_btn_layout = None

            if isinstance(entry, SectionEntry):
                current_section_active = True
                is_hidden = False

                # Col 0: Drag Handle
                if self.edit_mode:
                    handle = DragHandle(row, self)
                    self.layout.addWidget(handle, row, 0)

                # Col 1: Section Title
                if self.edit_mode:
                    lbl = QLineEdit(entry.label)
                    # Styling sections to stand out in Edit Mode
                    lbl.setStyleSheet("""
                        QLineEdit { 
                            font-weight: bold; 
                            color: #55aaff; 
                            background-color: #2a2a2a; 
                            border: 1px solid #444;
                            padding: 2px;
                        }
                    """)
                    lbl.textEdited.connect(lambda text, e=entry: setattr(e, "label", text))
                else:
                    lbl = QLabel(f"{entry.label.upper()}  {'▾' if not entry.collapsed else '▸'}")
                    lbl.setCursor(Qt.PointingHandCursor)
                    lbl.setStyleSheet("color: #55aaff; font-weight: bold;")
                    lbl.mousePressEvent = lambda event, e=entry: self.toggle_section(e)

                self.layout.addWidget(lbl, row, 1)

                # Col 2: Section Divider Line
                line = QFrame()
                line.setFrameShape(QFrame.HLine)
                line.setFrameShadow(QFrame.Plain)
                line.setStyleSheet("color: #55aaff;" if self.edit_mode else "color: #333;")
                self.layout.addWidget(line, row, 2)

                # Col 3: Remove Button (ADDED FOR SECTIONS)
                if self.edit_mode:
                    del_btn = QPushButton("Remove")
                    del_btn.setStyleSheet("color: #ff5555; font-weight: bold;")
                    del_btn.clicked.connect(lambda checked, r=row: self.remove_item(r))
                    self.layout.addWidget(del_btn, row, 3)

                if entry.collapsed and not self.edit_mode:
                    is_hidden = True
                continue

            # --- Group Start Marker ---
            elif isinstance(entry, GroupStartEntry):
                in_button_group = True

                if is_hidden:
                    continue

                if self.edit_mode:
                    handle = DragHandle(row, self)
                    self.layout.addWidget(handle, row, 0)

                    lbl = QLineEdit(entry.label)
                    lbl.setStyleSheet("font-weight: bold; color: #aaa; background: #222; border: 1px dashed #55aaff;")
                    lbl.textEdited.connect(lambda text, e=entry: setattr(e, "label", text))
                    self.layout.addWidget(lbl, row, 1)

                    line = QLabel("┌───────── GROUP START ─────────")
                    line.setStyleSheet("color: #55aaff;")
                    self.layout.addWidget(line, row, 2)

                    del_btn = QPushButton("Remove")
                    del_btn.setStyleSheet("color: #ff5555;")
                    del_btn.clicked.connect(lambda checked, r=row: self.remove_item(r))
                    self.layout.addWidget(del_btn, row, 3)
                else:
                    # Normal Mode: Add Label, create the horizontal container
                    lbl = QLabel(entry.label)
                    lbl.setFont(self.font)
                    indent = 20 if current_section_active else 5
                    lbl.setContentsMargins(indent, 0, 0, 0)
                    self.layout.addWidget(lbl, row, 1)

                    container = QWidget()
                    current_btn_layout = QHBoxLayout(container)
                    current_btn_layout.setContentsMargins(0, 0, 0, 0)
                    current_btn_layout.setSpacing(5)
                    current_btn_layout.setAlignment(Qt.AlignLeft)
                    self.layout.addWidget(container, row, 2)
                continue

            # --- Group End Marker ---
            elif isinstance(entry, GroupEndEntry):
                in_button_group = False
                current_btn_layout = None

                if is_hidden:
                    continue

                if self.edit_mode:
                    # handle = DragHandle(row, self)
                    # self.layout.addWidget(handle, row, 0)

                    lbl = QLabel("└─")
                    lbl.setStyleSheet("color: #55aaff; font-weight: bold;")
                    lbl.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
                    self.layout.addWidget(lbl, row, 1)

                    line = QLabel("└───────── GROUP END ─────────")
                    line.setStyleSheet("color: #55aaff;")
                    self.layout.addWidget(line, row, 2)

                    del_btn = QPushButton("Remove")
                    del_btn.setStyleSheet("color: #ff5555;")
                    del_btn.clicked.connect(lambda checked, r=row: self.remove_item(r))
                    self.layout.addWidget(del_btn, row, 3)
                # In Normal mode, the Group End does absolutely nothing visually, it just resets the state.
                continue

            # --- Button Logic ---
            elif isinstance(entry, ButtonEntry):
                if is_hidden:
                    continue

                if self.edit_mode:
                    # Col 0: Drag Handle
                    handle = DragHandle(row, self)
                    self.layout.addWidget(handle, row, 0)

                    # Col 1: Action Label
                    lbl = QLineEdit(entry.label)
                    # Use a distinct color (Amber) and styling for "Actions"
                    lbl.setStyleSheet(
                        """
                                    QLineEdit {
                                        color: #ffaa00; 
                                        font-weight: bold;
                                        background: transparent;
                                        border: none;
                                        border-bottom: 1px solid #444;
                                        margin-left: """
                        + ("15px;" if in_button_group else "0px;")
                        + """
                                    }
                                    QLineEdit:focus { border-bottom: 1px solid #ffaa00; }
                                """
                    )
                    lbl.textEdited.connect(lambda text, e=entry: setattr(e, "label", text))
                    self.layout.addWidget(lbl, row, 1)

                    # Col 2: Content (Styled Command Card)
                    content = QWidget()
                    # Apply a distinct background to the config area
                    content.setStyleSheet("""
                                    QWidget#CommandCard {
                                        background-color: #2b2b2b;
                                        border: 1px solid #3d3d3d;
                                        border-radius: 4px;
                                    }
                                """)
                    content.setObjectName("CommandCard")

                    btn_layout = QHBoxLayout(content)
                    btn_layout.setContentsMargins(8, 4, 8, 4)
                    btn_layout.setSpacing(8)

                    # Device Dropdown
                    device_combo = RequestComboBox()
                    device_combo.aboutToShowPopup.connect(self._trigger_device_fetch)
                    # Add a "Default" option to the button's own dropdown
                    if entry.target_device:
                        device_combo.addItem(f"ID: {entry.target_device}", entry.target_device)
                    else:
                        device_combo.addItem("None", "")

                        # Save the ID (userData), not the display text
                    device_combo.currentIndexChanged.connect(
                        lambda idx, e=entry, cb=device_combo: self._handle_button_target_change(cb, e)
                    )

                    # Command Payload (Monospace/Code Style)
                    payload_input = QLineEdit(entry.command_payload)
                    payload_input.setPlaceholderText("Command (JSON/Raw)...")
                    payload_input.setStyleSheet("""
                                    QLineEdit {
                                        font-family: 'Consolas', 'Monaco', monospace;
                                        background-color: #1e1e1e;
                                        color: #9cdcfe;
                                        border: 1px solid #333;
                                        padding: 2px 5px;
                                    }
                                """)
                    payload_input.textEdited.connect(lambda text, e=entry: setattr(e, "command_payload", text))

                    btn_layout.addWidget(QLabel("🎯"))  # Target Icon
                    btn_layout.addWidget(device_combo)
                    btn_layout.addWidget(QLabel("⌨"))  # Payload Icon
                    btn_layout.addWidget(payload_input)

                    self.layout.addWidget(content, row, 2)

                    # Col 3: Remove Row Button
                    del_btn = QPushButton("Remove")
                    del_btn.setStyleSheet("color: #ff5555; padding: 4px;")
                    del_btn.clicked.connect(lambda checked, r=row: self.remove_item(r))
                    self.layout.addWidget(del_btn, row, 3)

                else:
                    # Normal Mode: (Keep your current Button styling)
                    content = QPushButton(entry.label)
                    content.setStyleSheet("""
                                    QPushButton {
                                        background-color: #444; color: #eee; 
                                        border-radius: 4px; padding: 4px 12px; font-weight: bold;
                                    }
                                    QPushButton:hover { background-color: #555; }
                                    QPushButton:pressed { background-color: #333; }
                                """)
                    content.clicked.connect(lambda checked, e=entry: self.execute_button_command(e))
                    entry.button_widget = content

                    if in_button_group and current_btn_layout is not None:
                        current_btn_layout.addWidget(content)
                    else:
                        lbl = QLabel("")
                        self.layout.addWidget(lbl, row, 1)
                        self.layout.addWidget(content, row, 2)

                continue

            # --- Row Logic ---
            if is_hidden:
                continue

            # Col 0: Drag Handle
            if self.edit_mode:
                handle = DragHandle(row, self)
                self.layout.addWidget(handle, row, 0)

            # Col 1: Item Name
            if self.edit_mode:
                lbl = QLineEdit(entry.label)
                lbl.textEdited.connect(lambda text, e=entry: setattr(e, "label", text))
            else:
                lbl = QLabel(entry.label)
                lbl.setFont(self.font)

            # If the row is under a section, indent it 20px
            indent = 20 if current_section_active else 5
            lbl.setContentsMargins(indent, 0, 0, 0)
            self.layout.addWidget(lbl, row, 1)

            # Col 2: Content (Values or Module Pills)
            if self.edit_mode:
                content = QWidget()
                mod_layout = QHBoxLayout(content)
                mod_layout.setContentsMargins(5, 0, 0, 0)  # Slight indent
                mod_layout.setSpacing(5)
                mod_layout.setAlignment(Qt.AlignLeft)

                for mod_idx, mod in enumerate(entry.modules):
                    pill = QFrame()
                    pill.setStyleSheet("background-color: #333; border-radius: 4px;")
                    pill_layout = QHBoxLayout(pill)
                    pill_layout.setContentsMargins(6, 2, 4, 2)
                    pill_layout.setSpacing(4)

                    mod_lbl = QLabel(mod.name)
                    mod_lbl.setStyleSheet("color: #ccc; font-weight: normal;")

                    btn = QPushButton("✖")
                    btn.setFixedSize(16, 16)
                    btn.setStyleSheet("border: none; color: #ff5555; font-weight: bold;")
                    btn.clicked.connect(lambda checked, r=row, m=mod_idx: self.remove_module_from_row(r, m))

                    pill_layout.addWidget(mod_lbl)
                    pill_layout.addWidget(btn)
                    mod_layout.addWidget(pill)
                mod_layout.addStretch()
            else:
                msg = entry.last_painted_msg if entry.last_painted_msg else "---"
                content = QLabel(msg)
                content.setFont(self.font)
                entry.value_label = content

            self.layout.addWidget(content, row, 2)

            # Col 3: Remove Row Button
            if self.edit_mode:
                del_btn = QPushButton("Remove")
                del_btn.clicked.connect(lambda checked, r=row: self.remove_item(r))
                self.layout.addWidget(del_btn, row, 3)

        self.container.setUpdatesEnabled(True)

        if scroll_to_bottom:
            self._stashed_scroll_pos = 0  # Clear stash for next time
            QTimer.singleShot(50, lambda: scrollbar.setValue(scrollbar.maximum()))
        else:
            # Restore from the class-level stash to survive the "Double Rebuild" race
            QTimer.singleShot(50, lambda: scrollbar.setValue(self._stashed_scroll_pos))

    def _clear_layout(self):
        for entry in self.entries:
            entry.clear_widgets()

        while self.layout.count() > 0:
            item = self.layout.takeAt(0)
            if item.widget():
                item.widget().deleteLater()

    def remove_item(self, index):
        self.entries.pop(index)
        self.save_config()

    # --- State ---

    def get_state(self) -> dict:
        return {"tab_name": self.tab_name, "id": self.watch_id}

    def restore(self, state: dict):
        self.print(f"restore: {state}")
        self.tab_name = state.get("tab_name", self.tab_name)
        self.watch_id = state.get("id") or self.watch_id

        self.node = self.gui_context.gui_config_manager.create_node(
            f"/watches/{self.watch_id}", on_update=self.update_config_schema
        )

        self.rebuild_ui()

    def update_config_schema(self, config_: dict, schema_: dict):
        self.print(f"update_config_schema: {config_}")
        raw_entries = config_.get("entries", [])
        self.print(f"id: {self.watch_id}")
        self.name = config_.get("name")

        self.default_target = config_.get("default_target", "")
        self.command_history = list(config_.get("command_history", []))
        self.device_cache = dict(config_.get("device_cache", {}))

        self.command_input.blockSignals(True)
        # Store current text in case the user was in the middle of typing
        current_typing = self.command_input.currentText()

        self.command_input.clear()
        self.command_input.addItems(self.command_history)

        # Only restore the text if the user was actually typing something
        # that ISN'T already in the history list (or if the input is empty)
        if not self.command_input.lineEdit().hasFocus():
            self.command_input.setEditText("")
        else:
            self.command_input.setEditText(current_typing)

        self.command_input.blockSignals(False)

        # Update the UI selector without triggering the change signal
        self.default_target_combo.blockSignals(True)
        self.default_target_combo.clear()
        display_name = self._get_device_display_name(self.default_target)
        self.default_target_combo.addItem(display_name, self.default_target)
        self.default_target_combo.blockSignals(False)

        display_name = self.name.upper() if self.name else "UNNAMED WATCH"
        self.name_label.setText(display_name)

        if not self.name_edit.hasFocus():
            self.name_edit.setText(self.name or "")

        self.entries = []
        for e in raw_entries:
            if e.get("type") == "section":
                self.entries.append(SectionEntry(label=e["label"], collapsed=e.get("collapsed", False)))
            elif e.get("type") == "group_start":
                self.entries.append(GroupStartEntry(key=e["key"], label=e.get("label", "Group")))
            elif e.get("type") == "group_end":
                self.entries.append(GroupEndEntry(key=e["key"]))
            elif e.get("type") == "button":
                self.entries.append(
                    ButtonEntry(
                        key=e["key"],
                        label=e["label"],
                        command_payload=e.get("command_payload", ""),
                        target_device=e.get("target_device", ""),
                    )
                )
            else:
                mods = self.gui_context.id_registry.resolve_modules(e.get("modules", []))
                self.entries.append(RowEntry(key=e["key"], label=e["label"], modules=mods))

        self.rebuild_ui()

    # --- Prompts ---

    def save_config(self):
        print(f"command_history: {self.command_history}")
        self.node.send_config(
            {
                "name": self.name,
                "tab_name": self.tab_name,
                "id": self.watch_id,
                "default_target": self.default_target,
                "command_history": self.command_history,
                "device_cache": self.device_cache,
                "entries": [e.to_dict() for e in self.entries],
            }
        )

    def _handle_default_target_change(self, index):
        # Get the ID (userData) instead of the text
        self.default_target = self.default_target_combo.currentData() or ""
        self.save_config()

    def prompt_add_row(self):
        # We don't even need a dialog anymore if we don't want to!
        # Just add a generic row and let the user type the name in the QLineEdit.
        self.entries.append(RowEntry(label="New Metric"))
        self.save_config()

    def prompt_add_section(self):
        self.entries.append(SectionEntry(label="NEW SECTION"))
        self.save_config()

    def apply_updates(self):
        now = perf_counter()
        # 100ms = 10Hz refresh. Perfect for human reading.
        if now - self.prev_apply < 0.1:
            return
        self.prev_apply = now

        tracker = self.gui_context.registry.module_value_tracker
        if not tracker:
            return

        # Acquire a single snapshot for the entire batch of entries
        with tracker.get_snapshot() as snap:
            for entry in self.entries:
                if not isinstance(entry, RowEntry) or not entry.value_label:
                    continue

                # Check all modules assigned to this row (for multi-module aggregation)
                best_seq = entry.last_painted_seq
                new_msg = None

                for m in entry.modules:
                    current_seq = snap.get_sequence(m.id)

                    # If this specific module has a newer sequence than what we've ever seen
                    if current_seq > best_seq:
                        best_seq = current_seq
                        new_msg = snap.get_message(m.id)

                # Only hit the heavy QLabel.setText if we actually found newer data
                if new_msg is not None:
                    entry.last_painted_seq = best_seq
                    entry.last_painted_msg = new_msg
                    entry.value_label.setText(new_msg)

    @classmethod
    def new_watch(cls, name, parent: dict = None):
        parent = parent or {}
        id_ = generate_id("watch", list(parent.keys()))
        conf = {
            "id": id_,
            "name": name,
        }
        return id_, conf

    def prompt_delete_watch(self):
        """Confirm and delete the entire watch node."""
        watch_name = self.name or "this watch"
        reply = MessageBox.question(
            self,
            "Delete Watch?",
            f"Are you sure you want to permanently delete '{watch_name}'?\nThis cannot be undone.",
        )

        if reply == MessageBox.Btn.Yes:
            self.node.delete()

    def remove_module_from_row(self, row_index, mod_index):
        """Removes a specific sub-module from a RowEntry."""
        entry = self.entries[row_index]
        if isinstance(entry, RowEntry):
            entry.modules.pop(mod_index)
            self.save_config()
            self.rebuild_ui()

    def execute_button_command(self, entry: ButtonEntry):
        # RESOLUTION HIERARCHY:
        # 1. Button specific target
        # 2. Watch default target
        target = entry.target_device or self.default_target

        if not target or not entry.command_payload:
            self.print(f"Skipping command: No target resolved for '{entry.label}'")
            return

        self.print(f"Sending command to {target}: {entry.command_payload}")
        self._send_command_to_target(entry.command_payload, target)

    def prompt_add_button(self):
        self.entries.append(ButtonEntry(label="New Command"))
        self.save_config()

    def prompt_add_button_group(self):
        """Injects a group start and end pair."""
        self.entries.append(GroupStartEntry())
        # Optionally pre-fill it with a blank button so they see how it works
        self.entries.append(ButtonEntry(label="Cmd 1"))
        self.entries.append(GroupEndEntry())

        self.save_config()

    def _handle_generic_command(self):
        cmd = self.command_input.currentText().strip()
        if not cmd:
            return

        target = self.default_target
        if not target:
            self.print("Cannot send command: No Default Target set.")
            return

        self._send_command_to_target(cmd, target)

        # Update history list
        if cmd in self.command_history:
            self.command_history.remove(cmd)

        # Newest item at the top (index 0)
        self.command_history.insert(0, cmd)
        self.command_history = self.command_history[:20]

        # Refresh the UI items immediately
        self.command_input.blockSignals(True)
        self.command_input.clear()
        self.command_input.addItems(self.command_history)
        self.command_input.setEditText("")  # Ready for next command
        self.command_input.clearFocus()  # Optional: helps visually confirm 'sent'
        self.command_input.blockSignals(False)

        self.save_config()

    def _trigger_device_fetch(self, combo: RequestComboBox):
        self.print(f"Fetch triggered by widget: {combo}")

        # Optional: Clear the temporary 'ID: xxx' item to show progress
        combo.blockSignals(True)
        combo.setPlaceholderText("Fetching...")
        combo.blockSignals(False)

        self.signal_devices_updated.connect(
            lambda devices: self._update_single_combo(combo, devices), Qt.ConnectionType.SingleShotConnection
        )

        results = self.gui_context.registry.get_reference_values("/sources")
        self._handle_backend_response(results)

    def _handle_backend_response(self, new_devices: List[tuple]):
        """
        new_devices: List of (id, human_readable) tuples
        """
        self.print(f"Backend returned {len(new_devices)} devices. Updating cache...")

        # Update local cache
        for dev_id, dev_name in new_devices:
            self.device_cache[dev_id] = dev_name

        # Notify all waiting single-shot listeners
        self.save_config()
        self.signal_devices_updated.emit(new_devices)

    def _update_single_combo(self, combo: QComboBox, device_data: List[tuple]):
        """
        device_data: List of (id, human_readable) tuples
        """
        try:
            if not combo or combo.parent() is None:
                return

            combo.blockSignals(True)

            # 1. Store the CURRENT ID (not text) so we can restore it
            # .currentData() retrieves what we previously stored in Qt.UserRole
            previous_id = combo.currentData()

            combo.clear()

            # 2. Add the "None" option
            # Text: "None", Data: "" (empty string or None)
            combo.addItem("None", "")

            # 3. Populate with backend data
            for dev_id, dev_name in device_data:
                combo.addItem(dev_name, dev_id)

            combo.setPlaceholderText("")

            # 4. Restore selection by ID
            idx = combo.findData(previous_id)
            if idx >= 0:
                combo.setCurrentIndex(idx)
            else:
                # If the ID no longer exists, default to "None"
                combo.setCurrentIndex(0)

            combo.blockSignals(False)
        except RuntimeError:
            pass

    def _handle_button_target_change(self, combo: QComboBox, entry: ButtonEntry):
        """Saves the machine-readable ID from a button's dropdown."""
        # currentData() grabs the 'id' string stored in UserRole
        entry.target_device = combo.currentData() or ""
        self.save_config()

    def _get_device_display_name(self, device_id: str) -> str:
        """Returns human readable name from cache, or fallback if unknown."""
        if not device_id or device_id == "None":
            return "None"
        return self.device_cache.get(device_id, f"ID: {device_id}")

    def _send_command_to_target(self, command, target):
        val_with_newline = f"{command}\n"
        try:
            tasks = self.gui_context.registry.system_ctx.tasks
            devices = self.gui_context.registry.sources
            tasks.run_task(devices.send_command, target, val_with_newline)
        except Exception as e:
            print(f"Error sending to '{target}': {e}")

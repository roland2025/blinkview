# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from copy import deepcopy

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QListWidget, QToolBar, QMenu, QInputDialog
)
from PySide6.QtGui import QAction, QCursor
from PySide6.QtCore import Qt, Signal, QTimer
from PySide6.QtWidgets import (
    QWidget, QHBoxLayout, QLabel, QCheckBox, QPushButton, QListWidgetItem
)

from blinkview.ui.utils.config_node import ConfigNode
from blinkview.ui.utils.in_development import set_as_in_development
from blinkview.utils.generate_id import generate_id


class PipelineListItemWidget(QWidget):
    def __init__(self, config_node, gui_context, parent=None):  # Note: Removed type hint for ConfigNode assuming it's imported
        super().__init__(parent)
        self.device_name = "Loading..."
        self.config_node = config_node
        self.device_type = config_node.get("type", "Unknown")

        self.gui_context = gui_context

        # Horizontal layout for the row
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        # 1. Left Side: Device Name & Type
        self.lbl_info = QLabel(
            f"<b>{self.device_name}</b><br><small style='color: gray;'>{config_node.get('type', 'Loading...')}</small>")
        layout.addWidget(self.lbl_info, stretch=1)

        # 2. Right Side: Enable Checkbox
        self.chk_enable = QCheckBox("Active")
        self.chk_enable.setChecked(True)
        self.chk_enable.toggled.connect(self._enable_clicked)  # Hooked up your missing connection!
        layout.addWidget(self.chk_enable)

        # 3. Right Side: Log Button
        self.btn_log = QPushButton("📄")  # Or "📝"
        self.btn_log.setFixedSize(28, 28)
        self.btn_log.setToolTip("Open Log")
        self.btn_log.clicked.connect(self._on_log_clicked)

        # Setup Right-Click Menu for Log Button
        self.btn_log.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.btn_log.customContextMenuRequested.connect(self._show_log_context_menu)
        layout.addWidget(self.btn_log)

        # 4. Right Side: Config Button
        self.btn_config = QPushButton("⚙️")
        self.btn_config.setFixedSize(28, 28)
        self.btn_config.setToolTip("Open Configuration")
        self.btn_config.clicked.connect(lambda: self.config_node.show())
        layout.addWidget(self.btn_config)

        # 5. Right Side: Hamburger Menu Button
        self.btn_menu = QPushButton("⋮")  # Vertical ellipsis acts as a great hamburger menu
        self.btn_menu.setFixedSize(28, 28)

        # Create the dropdown menu for the hamburger button
        self.hamburger_menu = QMenu(self)
        self.action_remove = QAction("Remove", self)
        self.action_remove.triggered.connect(self._on_remove_clicked)
        self.hamburger_menu.addAction(self.action_remove)

        set_as_in_development(self.action_remove, self, "Remove pipeline")

        # self.action_other = QAction("Something else", self)
        # self.action_other.triggered.connect(self._on_something_else_clicked)
        # self.hamburger_menu.addAction(self.action_other)

        # Attach the menu to the button natively
        self.btn_menu.setMenu(self.hamburger_menu)
        # Optional: Hide the default dropdown arrow Qt adds to menu buttons
        self.btn_menu.setStyleSheet("QPushButton::menu-indicator { image: none; }")

        layout.addWidget(self.btn_menu)

        # Connections
        self.config_node.signal_received.connect(self._on_config_update)
        self.setEnabled(False)

    # --- Button Handlers ---

    def _on_log_clicked(self):
        """Triggered on standard left click of the Log button."""
        print(f"Opening log for {self.device_name} in current view...")
        self.gui_context.create_widget("LogViewerWidget", f"Logs: {self.device_name}", allowed_device=self.device_name)
        # Emit a signal or call your main window logic here

    def _show_log_context_menu(self, pos):
        """Triggered on right click of the Log button."""
        context_menu = QMenu(self)
        action_new_window = QAction("Open in new window", self)
        action_new_window.triggered.connect(self._on_log_new_window)
        context_menu.addAction(action_new_window)

        # Map the widget's local position to the global screen position
        global_pos = self.btn_log.mapToGlobal(pos)
        context_menu.exec(global_pos)

    def _on_log_new_window(self):
        print(f"Opening log for {self.device_name} in NEW window...")

    def _on_remove_clicked(self):
        print(f"Removing device: {self.device_name}")
        # Logic to remove the pipeline item

    def _on_something_else_clicked(self):
        print("Doing something else...")

    # --- Existing Handlers ---

    def _enable_clicked(self, checked):
        current_config = deepcopy(self.config_node.config)
        current_config["enabled"] = checked
        self.config_node.send_config(current_config)

    def _on_config_update(self, device: dict, schema: dict):
        enabled = device.get("enabled", True)
        self.device_name = device.get('name', 'Unknown')
        self.lbl_info.setText(
            f"<b>{self.device_name}</b><br><small style='color: gray;'>{device.get('type', 'Unknown')}</small>")

        # Block signals temporarily so updating the checkbox doesn't re-trigger _enable_clicked
        self.chk_enable.blockSignals(True)
        self.chk_enable.setChecked(enabled)
        self.chk_enable.blockSignals(False)

        self.setEnabled(True)


from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QListWidget, QToolBar, QMenu, QInputDialog
)
from PySide6.QtGui import QAction, QCursor
from PySide6.QtCore import Qt, Signal, QThread
import time


class PipelinesSidebarWidget(QWidget):
    signal_added = Signal(str, str)
    # device_toggled = Signal(str, bool)
    signal_config_requested = Signal(str)
    signal_types_fetched = Signal(list)

    def __init__(self, config_node: ConfigNode, gui_context):
        super().__init__()

        self._active_menu = None  # Track the currently open menu
        self.config_node = config_node
        self.gui_context = gui_context

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Toolbar
        self.toolbar = QToolBar("Pipeline Actions")
        self.toolbar.setStyleSheet("QToolBar { border-bottom: 1px solid #444; }")

        self.btn_add = QAction("➕ Add pipeline", self)
        self.toolbar.addAction(self.btn_add)
        layout.addWidget(self.toolbar)

        # Device List
        self.list_widget = QListWidget()
        self.list_widget.setSpacing(2)
        layout.addWidget(self.list_widget)

        # self.factory_fetch_fn = None  # Placeholder for the actual fetch function you'll inject

        self.signal_types_fetched.connect(self._on_fetch_complete)

        # Connect the click directly to the pop-up logic
        self.btn_add.triggered.connect(self.fetch_types_and_show_menu)
        self.config_node.signal_received.connect(self._on_config_update)

    def fetch_types_and_show_menu(self):
        """Creates the menu instantly, starts the fetch, and blocks on the menu."""

        # 1. Create the menu and add the loading placeholder
        self._active_menu = QMenu(self)

        self._active_menu.setToolTipsVisible(True)

        loading_action = self._active_menu.addAction("⏳ Loading...")
        loading_action.setEnabled(False)  # Users can't click the loading text

        # 3. Find screen position
        button = self.toolbar.widgetForAction(self.btn_add)
        if button:
            pos = button.mapToGlobal(button.rect().bottomLeft())
        else:
            pos = QCursor.pos()

        QTimer.singleShot(0, lambda: self._on_fetch_complete(self.config_node.factory_types("parser")))

        # 4. Show the menu.
        # Note: .exec() pauses this specific function's execution here until the menu closes,
        # but Qt's background event loop continues running, which allows signals to still fire!
        self._active_menu.exec(pos)

        # 5. Cleanup when the user closes the menu
        self._active_menu = None

    def _on_fetch_complete(self, types: list):
        """Triggered when the background network call finishes."""

        # If the user got impatient and clicked away, the menu is gone. We just ignore the data.
        if self._active_menu is None or not self._active_menu.isVisible():
            return

        # 2. INSTANT REDIRECT: If only 1 item, close and call add_item
        if len(types) == 1:
            dev_type, _ = types[0]
            self._active_menu.close()  # Removes the menu from screen
            self.add_item(dev_type)
            return

        # Clear the "⏳ Loading..." action
        self._active_menu.clear()

        # Handle potential network failures safely
        if not types:
            err_action = self._active_menu.addAction("❌ Failed to fetch types")
            err_action.setEnabled(False)
            return

        # Populate the real items

        for dev_type, description in types:
            print(f"Adding menu item for {dev_type} - {description}")
            action = self._active_menu.addAction(dev_type)

            action.setToolTip(description)
            action.setStatusTip(description)
            action.triggered.connect(
                lambda checked=False, dtype=dev_type: self.add_item(dtype)
            )

    def add_item(self, device_type: str):
        """Prompts for a name and updates the list widget."""
        name, ok = QInputDialog.getText(
            self, "Pipeline Name", f"Enter a name for the new '{device_type}':"
        )

        name = name.strip()

        if not ok or not name:
            return  # User cancelled or entered an empty name

        config = self.config_node.get_copy()
        source_id = generate_id("pipe")
        config[source_id] = {
            "enabled": True,
            "type": device_type,
            "name": name
        }

        self.config_node.send_config(config)
        self.config_node.show(source_id, name)

    def _on_config_update(self, items: dict, schema: dict):
        """Listens for config updates to keep the sidebar in sync with external changes."""
        self.list_widget.clear()

        # devices is a dict of source_id: source_config
        for item_id, item_config in items.items():
            item = QListWidgetItem(self.list_widget)
            node = self.config_node.create_child(f"{item_id}", name=f"Pipeline - {item_config.get('name', item_id)}")
            row_widget = PipelineListItemWidget(node, gui_context=self.gui_context)
            node.fetch()

            item.setSizeHint(row_widget.sizeHint())
            self.list_widget.setItemWidget(item, row_widget)

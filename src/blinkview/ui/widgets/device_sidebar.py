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
from blinkview.utils.generate_id import generate_id


class DeviceListItemWidget(QWidget):
    def __init__(self, config_node: ConfigNode, gui_context, parent=None):
        super().__init__(parent)
        self.device_name = "Loading..."
        self.config_node = config_node
        self.device_type = config_node.get("type", "Unknown")
        self.gui_context = gui_context
        # self.device_type = "unknown"

        # self.node_enabled = config_node.create_child("enabled")
        # self.node_enabled.signal_received.connect(self._on_enabled_update)
        # self.node_enabled.fetch()

        # Horizontal layout for the row
        layout = QHBoxLayout(self)
        layout.setContentsMargins(5, 5, 5, 5)

        # 1. Left Side: Device Name & Type (Using basic HTML for styling)
        self.lbl_info = QLabel(f"<b>{self.device_name}</b><br><small style='color: gray;'>{config_node.get('type', 'Loading...')}</small>")  # Placeholder until we fetch the type
        layout.addWidget(self.lbl_info, stretch=1)  # Stretch pushes buttons to the right

        # 2. Right Side: Enable Checkbox
        self.chk_enable = QCheckBox("Active")
        self.chk_enable.setChecked(True)
        layout.addWidget(self.chk_enable)

        # self.chk_enable.clicked.connect(lambda: self.node_enabled.send(not self.node_enabled.get()))  # This will trigger the node to send the updated config back to the backend
        self.chk_enable.clicked.connect(self._enable_clicked)
        # 3. Right Side: Config Button
        self.btn_config = QPushButton("⚙️")
        self.btn_config.setFixedSize(28, 28) # Keep it small and square
        self.btn_config.setToolTip("Open Configuration")
        self.btn_config.clicked.connect(lambda: self.config_node.show())
        layout.addWidget(self.btn_config)
        self.config_node.signal_received.connect(self._on_config_update)

        self.setEnabled(False)

    def _enable_clicked(self, checked):
        """Sends the updated 'enabled' state back to the backend when the checkbox is toggled."""
        current_config = deepcopy(self.config_node.config)
        current_config["enabled"] = checked
        self.config_node.send_config(current_config)

    # def _on_enabled_update(self, enabled, schema: dict):
    #     """Listens for updates to the 'enabled' field to keep the checkbox in sync."""
    #     self.chk_enable.setChecked(enabled)

    def _on_config_update(self, device: dict, schema: dict):
        """Listens for config updates to keep the row in sync with external changes."""
        # This is where you'd update the row's display based on changes to its config.
        # For example, if the device gets disabled externally, you might want to uncheck the box:
        enabled = device.get("enabled", True)
        self.lbl_info.setText(f"<b>{device.get('name', 'Unknown')}</b><br><small style='color: gray;'>{device.get('type', 'Unknown')}</small>")
        self.chk_enable.setChecked(enabled)

        self.setEnabled(True)


from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QListWidget, QToolBar, QMenu, QInputDialog
)
from PySide6.QtGui import QAction, QCursor
from PySide6.QtCore import Qt, Signal, QThread
import time


class DeviceSidebarWidget(QWidget):
    device_added = Signal(str, str)
    device_toggled = Signal(str, bool)
    device_config_requested = Signal(str)
    device_fetch_complete = Signal(list)

    def __init__(self, config_node: ConfigNode, gui_context):
        super().__init__()

        self._active_menu = None  # Track the currently open menu
        self.config_node = config_node
        self.gui_context = gui_context

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Toolbar
        self.toolbar = QToolBar("Device Actions")
        self.toolbar.setStyleSheet("QToolBar { border-bottom: 1px solid #444; }")

        self.btn_add_device = QAction("➕ Add Source", self)
        self.toolbar.addAction(self.btn_add_device)
        layout.addWidget(self.toolbar)

        # Device List
        self.list_widget = QListWidget()
        self.list_widget.setSpacing(2)
        layout.addWidget(self.list_widget)

        # self.factory_fetch_fn = None  # Placeholder for the actual fetch function you'll inject

        self.device_fetch_complete.connect(self._on_fetch_complete)

        # Connect the click directly to the pop-up logic
        self.btn_add_device.triggered.connect(self.start_device_fetch_and_show_menu)
        self.config_node.signal_received.connect(self._on_config_update)

    def start_device_fetch_and_show_menu(self):
        """Creates the menu instantly, starts the fetch, and blocks on the menu."""

        # 1. Create the menu and add the loading placeholder
        self._active_menu = QMenu(self)

        self._active_menu.setToolTipsVisible(True)

        loading_action = self._active_menu.addAction("⏳ Loading...")
        loading_action.setEnabled(False)  # Users can't click the loading text

        # 3. Find screen position
        button = self.toolbar.widgetForAction(self.btn_add_device)
        if button:
            pos = button.mapToGlobal(button.rect().bottomLeft())
        else:
            pos = QCursor.pos()

        QTimer.singleShot(0, lambda: self._on_fetch_complete(self.config_node.factory_types("source")))

        # 4. Show the menu.
        # Note: .exec() pauses this specific function's execution here until the menu closes,
        # but Qt's background event loop continues running, which allows signals to still fire!
        self._active_menu.exec(pos)

        # 5. Cleanup when the user closes the menu
        self._active_menu = None

    def _on_fetch_complete(self, device_types: list):
        """Triggered when the background network call finishes."""

        # If the user got impatient and clicked away, the menu is gone. We just ignore the data.
        if self._active_menu is None or not self._active_menu.isVisible():
            return

        # Clear the "⏳ Loading..." action
        self._active_menu.clear()

        # Handle potential network failures safely
        if not device_types:
            err_action = self._active_menu.addAction("❌ Failed to fetch types")
            err_action.setEnabled(False)
            return

        # Populate the real items

        for dev_type, description in device_types:
            print(f"Adding menu item for {dev_type} - {description}")
            action = self._active_menu.addAction(dev_type)

            action.setToolTip(description)
            action.setStatusTip(description)
            action.triggered.connect(
                lambda checked=False, dtype=dev_type: self.add_source(dtype)
            )

    def add_source(self, device_type: str):
        """Prompts for a name and updates the list widget."""
        name, ok = QInputDialog.getText(
            self, "Source Name", f"Enter a name for the new '{device_type}':"
        )

        name = name.strip()

        if not ok or not name:
            return  # User cancelled or entered an empty name

        config = deepcopy(self.config_node.config)
        source_id = generate_id("src")
        config[source_id] = {
            "enabled": True,
            "type": device_type,
            "name": name
        }

        self.config_node.send_config(config)
        self.config_node.show(source_id, name)

    def _on_config_update(self, sources: dict, schema: dict):
        """Listens for config updates to keep the sidebar in sync with external changes."""
        self.list_widget.clear()

        # devices is a dict of source_id: source_config
        for source_id, source_config in sources.items():
            item = QListWidgetItem(self.list_widget)
            node = self.config_node.create_child(f"{source_id}", name=f"Source - {source_config.get('name', source_id)}")
            row_widget = DeviceListItemWidget(node, gui_context=self.gui_context)
            node.fetch()

            item.setSizeHint(row_widget.sizeHint())
            self.list_widget.setItemWidget(item, row_widget)

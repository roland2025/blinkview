# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from copy import deepcopy

from qtpy.QtCore import QPoint, Qt
from qtpy.QtGui import QAction
from qtpy.QtWidgets import (
    QCheckBox,
    QHBoxLayout,
    QLabel,
    QMenu,
    QPushButton,
    QWidget,
)

from blinkview.ui.utils.in_development import set_as_in_development
from blinkview.ui.widgets.message_box import MessageBox


class BaseListItemWidget(QWidget):
    def __init__(self, config_node, gui_context, parent=None):
        super().__init__(parent)
        self.config_node = config_node
        self.gui_context = gui_context

        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self._show_context_menu)

        # Main Layout
        self.layout = QHBoxLayout(self)
        self.layout.setContentsMargins(5, 5, 5, 5)

        # Info Label (Name & Type)
        self.lbl_info = QLabel()
        self.update_label_info()
        self.layout.addWidget(self.lbl_info, stretch=1)

        # Active Checkbox
        self.chk_enable = QCheckBox()
        self.chk_enable.setChecked(True)
        self.chk_enable.toggled.connect(self._enable_clicked)
        self.layout.addWidget(self.chk_enable)

        # --- Hook for Subclasses ---
        self._setup_custom_controls()

        # Config Button (Common to all)
        self.btn_config = QPushButton("⚙️")
        self.btn_config.setFixedSize(28, 28)
        self.btn_config.setToolTip("Open Configuration")
        self.btn_config.clicked.connect(lambda: self.config_node.show())
        self.layout.addWidget(self.btn_config)

        # Setup update listener
        self.config_node.on_update(self._on_config_update)
        self.setEnabled(False)

    def _setup_custom_controls(self):
        """Override this in subclasses to add specific buttons to the layout."""
        pass

    def _add_context_menu_actions(self, menu: QMenu):
        """Override in subclasses to add specific menu items."""
        pass

    def _enable_clicked(self, checked):
        """Standardized enable logic."""
        current_config = deepcopy(self.config_node.config)
        current_config["enabled"] = checked
        self.config_node.send_config(current_config)

    def update_label_info(self):

        device_name = self.config_node.get("name", "Unknown")
        dev_type = self.config_node.get("type", "Unknown")
        self.lbl_info.setText(f"<b>{device_name}</b><br><small style='color: gray;'>{dev_type}</small>")

    def _on_config_update(self, device: dict, schema: dict):
        """Standardized UI refresh logic."""
        enabled = device.get("enabled", True)

        self.update_label_info()

        # Update checkbox without triggering the toggle signal
        self.chk_enable.blockSignals(True)
        self.chk_enable.setChecked(enabled)
        self.chk_enable.blockSignals(False)
        self.lbl_info.updateGeometry()
        self.setEnabled(True)

    def _show_context_menu(self, position: QPoint):
        """Creates and executes the right-click menu."""
        menu = QMenu(self)
        menu.setAttribute(Qt.WA_DeleteOnClose)

        # Standard Action: Configure
        config_action = QAction("Configure...", self)
        config_action.triggered.connect(self.config_node.show)
        menu.addAction(config_action)

        # Standard Action: Toggle State
        is_enabled = self.chk_enable.isChecked()
        toggle_label = "Disable" if is_enabled else "Enable"
        toggle_action = QAction(f"{toggle_label}", self)
        toggle_action.triggered.connect(lambda: self.chk_enable.toggle())
        menu.addAction(toggle_action)

        menu.addSeparator()

        action_remove = QAction("Remove", self)
        action_remove.triggered.connect(self._on_remove_clicked)

        # set_as_in_development(action_remove, self, "Remove item")

        menu.addAction(action_remove)

        menu.addSeparator()

        # --- Hook for Subclasses ---
        # This allows subclasses to inject specific actions (like 'Reset' or 'Calibrate')
        self._add_context_menu_actions(menu)

        # Display the menu at the cursor position
        menu.exec_(self.mapToGlobal(position))

    def _on_remove_clicked(self):
        name = self.config_node.get("name", "Unknown")

        # Create the confirmation dialog
        # Parent is 'self' so it centers over the widget and stays modal
        if MessageBox.Btn.Yes == MessageBox.question(
            self,
            "Confirm Removal",
            f"Are you sure you want to remove the pipeline: <b>{name}</b>?",
        ):
            print(f"Confirmed! Removing: {name}")
            self.config_node.delete()
        else:
            print("Removal cancelled by user.")

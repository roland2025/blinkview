# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import Qt, QTimer, Signal
from qtpy.QtGui import QCursor
from qtpy.QtWidgets import QInputDialog, QMenu, QToolButton

from blinkview.ui.widgets.toast import ToastManager, ToastType


class BaseToolButtonWidget(QToolButton):
    types_fetched = Signal(list)

    def __init__(self, config_node, gui_context, button_text: str, factory_key: str, input_title: str):
        super().__init__()
        self.config_node = config_node
        self.gui_context = gui_context
        self.factory_key = factory_key
        self.input_title = input_title
        self._active_menu = None

        self.setText(button_text)
        self.setCheckable(True)

        # Enable explicit custom context actions
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.show_dynamic_context_menu)
        self.types_fetched.connect(self._on_fetch_complete)

    def show_dynamic_context_menu(self, position):
        """Builds the main context menu with clean alignment."""
        menu = QMenu(self)

        config_data = self.config_node.config or {}
        if not config_data:
            # Cleanly aligned empty state
            menu.addAction("No items").setEnabled(False)
        else:
            for item_id in sorted(config_data.keys()):
                self._build_device_submenu(menu, item_id, config_data[item_id])

        menu.addSeparator()

        add_action = menu.addAction("➕ Add")
        add_action.triggered.connect(self.fetch_types_and_show_factory_menu)

        menu.exec(self.mapToGlobal(position))

    def _build_device_submenu(self, parent_menu: QMenu, item_id: str, item_config: dict):
        """Builds neatly aligned cascading submenus."""
        is_enabled = item_config.get("enabled", True)
        status_icon = "🟢" if is_enabled else "🔴"
        clean_name = item_config.get("name", item_id)

        # No leading spaces—let Qt handle the menu icon layout natively
        submenu = parent_menu.addMenu(f"{status_icon} {clean_name}")

        if is_enabled:
            try:
                # Look up the live runtime driver instance inside the backend registry
                live_device = self.gui_context.registry.get_reference_target(item_id)

                # Check if the driver instance exists and implements our command discovery method
                if live_device and hasattr(live_device, "get_commands"):
                    available_commands = live_device.get_commands()

                    if available_commands:
                        submenu.addSeparator()

                        print(f"{item_id} has available commands: {available_commands}")
                        # Create a nested cascading submenu to clean up real estate
                        for cmd_token, human_name in available_commands:
                            action = submenu.addAction(human_name)
                            action.triggered.connect(
                                lambda _, t=cmd_token, dev=live_device: self._send_command_to_target(t, dev)
                            )

            except Exception as e:
                # Shield the UI building sequence from sudden backend initialization races
                print(f"[UI Warning] Could not resolve live commands for target '{item_id}': {e}")

            submenu.addSeparator()

            cmd_action = submenu.addAction("✉️ Send Command")
            cmd_action.triggered.connect(lambda: self.send_data_prompt(item_id, clean_name))

        submenu.addSeparator()

        edit_action = submenu.addAction("⚙️ Edit Configuration")
        edit_action.triggered.connect(lambda: self.config_node.show(item_id, clean_name))

        submenu.addSeparator()

        toggle_text = "🔴 Disable Source" if is_enabled else "🟢 Enable Source"
        toggle_action = submenu.addAction(toggle_text)
        toggle_action.triggered.connect(lambda: self.toggle_item_state(item_id, is_enabled))

    def fetch_types_and_show_factory_menu(self):
        """Asynchronously requests factory signatures to populate an element generation menu."""
        self._active_menu = QMenu(self)
        loading = self._active_menu.addAction("⏳ Loading Backend Schema...")
        loading.setEnabled(False)

        # Map display boundary vectors relative to cursor coordinates
        pos = QCursor.pos()
        QTimer.singleShot(0, lambda: self.types_fetched.emit(self.config_node.factory_types(self.factory_key)))
        self._active_menu.exec(pos)
        self._active_menu = None

    def _on_fetch_complete(self, types: list):
        if self._active_menu is None or not self._active_menu.isVisible():
            return
        if len(types) == 1:
            self._active_menu.close()
            self.add_item(types[0][0])
            return

        self._active_menu.clear()
        if not types:
            self._active_menu.addAction("❌ Failed to fetch structural maps").setEnabled(False)
            return

        for dev_type, description in types:
            action = self._active_menu.addAction(dev_type)
            action.setToolTip(description)
            action.triggered.connect(lambda checked=False, dtype=dev_type: self.add_item(dtype))

    def add_item(self, item_type: str):
        name, ok = QInputDialog.getText(self, self.input_title, f"Enter name for '{item_type}':")
        name = name.strip()
        if not ok or not name:
            return

        config = self.config_node.get_copy()
        id_, conf = self.generate_config_payload(name, item_type, config)
        config[id_] = conf
        self.config_node.send_config(config)
        self.config_node.show(id_, name)

    def send_data_prompt(self, item_id: str, name: str):
        """Prompts the operator for text data and hands it to the background task executor."""
        cmd_text, ok = QInputDialog.getText(
            self,
            "Execute Runtime Target Command",
            f"Deliver message payload directly to '{name}':",
        )

        if not ok or not cmd_text.strip():
            return

        # Direct execution handover to your backend threading workflow
        self._send_data_to_target(cmd_text.strip(), item_id)

    def _send_data_to_target(self, command: str, target: str):
        """Appends proper protocol breaks and submits the task to the system architecture threads."""
        val_with_newline = f"{command}\n"
        try:
            tasks = self.gui_context.registry.system_ctx.tasks
            devices = self.gui_context.registry.sources
            tasks.run_task(devices.send_data, target, val_with_newline)
        except Exception as e:
            print(f"Error sending to '{target}': {e}")

    def generate_config_payload(self, name: str, item_type: str, parent_config: dict) -> tuple[str, dict]:
        raise NotImplementedError("Subclasses must define explicit generation config payloads.")

    def _send_command_to_target(self, command: str, live_device):
        """Dispatches an explicit protocol command token directly on the target device via the worker thread pool."""
        try:
            tasks = self.gui_context.registry.system_ctx.tasks

            # Offload the instance method call safely away from the Qt UI thread
            tasks.run_task(live_device.send_command, command)
        except Exception as e:
            print(f"Error executing command '{command}' on target driver thread: {e}")

    def toggle_item_state(self, item_id: str, current_state: bool):
        """Mutates the 'enabled' configuration field state for the targeted component."""
        # Create a deep mutation copy of the configuration node's state payload
        config = self.config_node.get_copy()

        if item_id in config:
            new_state = not current_state
            config[item_id]["enabled"] = new_state

            # Synchronize changes out to the configuration cluster node
            self.config_node.send_config(config)
        else:
            print(f"[UI Error] Cannot toggle state. ID '{item_id}' not found in active config.")


from blinkview.io.BaseReader import BaseReader


class SourcesToolButton(BaseToolButtonWidget):
    def __init__(self, config_node, gui_context):
        super().__init__(
            config_node=config_node,
            gui_context=gui_context,
            button_text="Sources",
            factory_key="source",
            input_title="Source Configuration Prompt",
        )

    def generate_config_payload(self, name: str, item_type: str, parent_config: dict):
        return BaseReader.new_daemon(name, item_type, prefix="src", parent=parent_config)

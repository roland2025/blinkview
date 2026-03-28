# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtCore import Qt, QTimer, Signal
from PySide6.QtGui import QAction, QCursor
from PySide6.QtWidgets import (
    QInputDialog,
    QListWidget,
    QListWidgetItem,
    QMenu,
    QToolBar,
    QVBoxLayout,
    QWidget,
)


class BaseSidebarWidget(QWidget):
    types_fetched = Signal(list)

    def __init__(
        self,
        config_node,
        gui_context,
        toolbar_title: str,
        add_btn_text: str,
        factory_key: str,
        input_title: str,
        item_name_prefix: str,
        list_item_class,
    ):
        super().__init__()

        self.config_node = config_node
        self.gui_context = gui_context
        self._active_menu = None

        # Subclass specific configs
        self.factory_key = factory_key
        self.input_title = input_title
        self.item_name_prefix = item_name_prefix
        self.list_item_class = list_item_class

        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # Toolbar
        self.toolbar = QToolBar(toolbar_title)
        self.toolbar.setStyleSheet("QToolBar { border-bottom: 1px solid #444; }")

        self.btn_add = QAction(add_btn_text, self)
        self.toolbar.addAction(self.btn_add)
        layout.addWidget(self.toolbar)

        # Device List
        self.list_widget = QListWidget()
        self.list_widget.setSpacing(2)
        layout.addWidget(self.list_widget)

        self.types_fetched.connect(self._on_fetch_complete)
        self.btn_add.triggered.connect(self.fetch_types_and_show_menu)
        self.config_node.on_update(self._on_config_update)

    def fetch_types_and_show_menu(self):
        """Creates the menu instantly, starts the fetch, and blocks on the menu."""
        self._active_menu = QMenu(self)
        self._active_menu.setToolTipsVisible(True)

        loading_action = self._active_menu.addAction("⏳ Loading...")
        loading_action.setEnabled(False)

        button = self.toolbar.widgetForAction(self.btn_add)
        pos = (
            button.mapToGlobal(button.rect().bottomLeft()) if button else QCursor.pos()
        )

        QTimer.singleShot(
            0,
            lambda: self.types_fetched.emit(
                self.config_node.factory_types(self.factory_key)
            ),
        )

        self._active_menu.exec(pos)
        self._active_menu = None

    def _on_fetch_complete(self, types: list):
        """Triggered when the background network call finishes."""
        if self._active_menu is None or not self._active_menu.isVisible():
            return

        if len(types) == 1:
            dev_type, _ = types[0]
            self._active_menu.close()
            self.add_item(dev_type)
            return

        self._active_menu.clear()

        if not types:
            err_action = self._active_menu.addAction("❌ Failed to fetch types")
            err_action.setEnabled(False)
            return

        for dev_type, description in types:
            action = self._active_menu.addAction(dev_type)
            action.setToolTip(description)
            action.setStatusTip(description)
            action.triggered.connect(
                lambda checked=False, dtype=dev_type: self.add_item(dtype)
            )

    def add_item(self, item_type: str):
        """Prompts for a name and updates the list widget."""
        name, ok = QInputDialog.getText(
            self, self.input_title, f"Enter a name for the new '{item_type}':"
        )
        name = name.strip()

        if not ok or not name:
            return

        config = self.config_node.get_copy()

        # Call the subclass-specific implementation to generate the config payload
        id_, conf = self.generate_daemon_config(name, item_type, config)

        config[id_] = conf
        self.config_node.send_config(config)
        self.config_node.show(id_, name)

    def generate_daemon_config(
        self, name: str, item_type: str, parent_config: dict
    ) -> tuple[str, dict]:
        """Must be implemented by subclasses to define how the specific daemon is created."""
        raise NotImplementedError("Subclasses must implement generate_daemon_config")

    def _on_config_update(self, items: dict, schema: dict):
        """Listens for config updates to keep the sidebar in sync with external changes."""
        self.list_widget.clear()

        for item_id, item_config in items.items():
            item = QListWidgetItem(self.list_widget)

            node_name = f"{self.item_name_prefix} - {item_config.get('name', item_id)}"
            node = self.config_node.create_child(f"{item_id}", name=node_name)

            # Instantiate the specific row widget class passed by the subclass
            row_widget = self.list_item_class(node, gui_context=self.gui_context)

            item.setSizeHint(row_widget.sizeHint())
            self.list_widget.setItemWidget(item, row_widget)

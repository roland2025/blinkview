# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtCore import Qt
from PySide6.QtGui import QAction
from PySide6.QtWidgets import (
    QMenu,
    QPushButton,
)

from blinkview.parsers.parser import ParserThread
from blinkview.ui.widgets.base_list_item import BaseListItemWidget
from blinkview.ui.widgets.base_sidebar_widget import BaseSidebarWidget


class PipelineListItemWidget(BaseListItemWidget):
    def _setup_custom_controls(self):
        # 1. Log Button
        self.btn_log = QPushButton("📄")
        self.btn_log.setFixedSize(28, 28)
        self.btn_log.setToolTip("Open Log")
        self.btn_log.clicked.connect(self._on_log_clicked)
        self.btn_log.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.btn_log.customContextMenuRequested.connect(self._show_log_context_menu)
        self.layout.addWidget(self.btn_log)

    def _on_log_clicked(self):
        name = self.config_node.get("name")
        if not name:
            return
        self.gui_context.create_widget("LogViewerWidget", f"Logs: {name}", params={"allowed_device": name})

    def _show_log_context_menu(self, pos):
        context_menu = QMenu(self)
        action_new_window = QAction("Open in new window", self)
        action_new_window.triggered.connect(lambda: print("New window logic here"))
        context_menu.addAction(action_new_window)
        context_menu.exec(self.btn_log.mapToGlobal(pos))


class PipelinesSidebarWidget(BaseSidebarWidget):
    def __init__(self, config_node, gui_context):
        super().__init__(
            config_node=config_node,
            gui_context=gui_context,
            toolbar_title="Pipeline Actions",
            add_btn_text="➕ Add pipeline",
            factory_key="parser",
            input_title="Pipeline Name",
            item_name_prefix="Pipeline",
            list_item_class=PipelineListItemWidget,  # Your existing widget class
        )

    def generate_daemon_config(self, name: str, item_type: str, parent_config: dict):
        return ParserThread.new_daemon(name, item_type, prefix="pipe", parent=parent_config)

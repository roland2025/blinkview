# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.pipelines_sidebar import PipelineListItemWidget, PipelinesSidebarWidget


class FakeGuiContext:
    def __init__(self):
        self.created = []

    def create_widget(self, cls_name, title, params=None):
        self.created.append((cls_name, title, params))


class FakeConfigNode:
    def __init__(self, config=None):
        self.config = config or {"name": "mypipeline", "type": "parser", "enabled": True}
        self._update_cb = None
        self.shown = None
        self.sent_configs = []

    def get(self, key, default=None):
        return self.config.get(key, default)

    def on_update(self, callback):
        self._update_cb = callback

    def fire_update(self, items, schema=None):
        self._update_cb(items, schema or {})

    def factory_types(self, key):
        return [("parser", "Generic Parser")]

    def get_copy(self):
        return {}

    def send_config(self, config):
        self.sent_configs.append(config)

    def show(self, id_=None, name=None):
        self.shown = (id_, name)

    def create_child(self, item_id, name=None):
        return FakeConfigNode({"name": name, "type": "parser", "enabled": True})


@pytest.fixture
def gui_context():
    return FakeGuiContext()


@pytest.fixture
def config_node():
    return FakeConfigNode()


@pytest.fixture
def sidebar(qapp, qtbot, config_node, gui_context):
    w = PipelinesSidebarWidget(config_node, gui_context=gui_context)
    qtbot.addWidget(w)
    return w


class TestPipelinesSidebarWidget:
    def test_add_button_uses_pipeline_wording(self, sidebar):
        assert sidebar.btn_add.text() == "➕ Add pipeline"

    def test_list_item_class_is_pipeline_list_item_widget(self, sidebar):
        assert sidebar.list_item_class is PipelineListItemWidget

    def test_generate_daemon_config_uses_pipe_prefix(self, sidebar):
        id_, conf = sidebar.generate_daemon_config("MyPipeline", "parser", {})

        assert id_.startswith("pipe")
        assert conf["name"] == "MyPipeline"
        assert conf["type"] == "parser"

    def test_fire_update_builds_real_pipeline_list_item_rows_with_log_button(self, sidebar, config_node):
        config_node.fire_update({"p1": {"name": "mypipeline"}})

        assert sidebar.list_widget.count() == 1
        row_widget = sidebar.list_widget.itemWidget(sidebar.list_widget.item(0))
        assert isinstance(row_widget, PipelineListItemWidget)
        assert row_widget.btn_log.toolTip() == "Open Log"


@pytest.fixture
def pipeline_item(qapp, qtbot, config_node, gui_context):
    w = PipelineListItemWidget(config_node, gui_context)
    qtbot.addWidget(w)
    return w


class TestPipelineListItemWidget:
    def test_log_button_click_opens_log_viewer_for_the_named_device(self, qapp, qtbot, pipeline_item, gui_context):
        from qtpy.QtCore import Qt

        pipeline_item.setEnabled(True)  # starts disabled until the first config update
        qtbot.mouseClick(pipeline_item.btn_log, Qt.LeftButton)

        assert gui_context.created == [("LogViewerWidget", "Logs: mypipeline", {"allowed_device": "mypipeline"})]

    def test_log_button_click_is_a_no_op_without_a_name(self, qapp, qtbot, gui_context):
        from qtpy.QtCore import Qt

        item = PipelineListItemWidget(FakeConfigNode({"name": None, "type": "parser"}), gui_context)
        qtbot.addWidget(item)
        item.setEnabled(True)

        qtbot.mouseClick(item.btn_log, Qt.LeftButton)

        assert gui_context.created == []

    def test_log_context_menu_opens_table_view_for_the_named_device(self, qapp, qtbot, pipeline_item, gui_context):
        from qtpy.QtCore import QPoint, QTimer
        from qtpy.QtWidgets import QApplication, QMenu

        def _pick_table_view():
            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMenu) and w.isVisible():
                    for action in w.actions():
                        if action.text() == "Open as Table":
                            action.trigger()
                            w.close()
                            return

        QTimer.singleShot(50, _pick_table_view)
        pipeline_item._show_log_context_menu(QPoint(5, 5))

        assert gui_context.created == [("LogTableViewerWidget", "Logs: mypipeline", {"allowed_device": "mypipeline"})]

    def test_log_context_menu_is_a_no_op_without_a_name(self, qapp, qtbot, gui_context):
        from qtpy.QtCore import QPoint

        item = PipelineListItemWidget(FakeConfigNode({"name": None, "type": "parser"}), gui_context)
        qtbot.addWidget(item)

        item._show_log_context_menu(QPoint(5, 5))  # must not raise or open a menu

        assert gui_context.created == []

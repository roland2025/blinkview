# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest
from qtpy.QtWidgets import QWidget

from blinkview.ui.widgets.base_sidebar_widget import BaseSidebarWidget


class FakeConfigNode:
    def __init__(self, types=None):
        self._update_cb = None
        self._types = types if types is not None else [("adb", "Android Debug Bridge")]
        self.shown = None
        self.created_children = []
        self.sent_configs = []

    def on_update(self, callback):
        self._update_cb = callback

    def fire_update(self, items, schema=None):
        self._update_cb(items, schema or {})

    def factory_types(self, key):
        return self._types

    def get_copy(self):
        return {}

    def send_config(self, config):
        self.sent_configs.append(config)

    def show(self, id_, name):
        self.shown = (id_, name)

    def create_child(self, item_id, name=None):
        self.created_children.append((item_id, name))
        return FakeConfigNode()


class DummyListItem(QWidget):
    """Minimal stand-in for a list_item_class - a real QWidget (setItemWidget requires one)
    that just needs to be constructible without dragging in BaseListItemWidget's own
    config_node.get()/checkbox setup."""

    def __init__(self, config_node, gui_context=None):
        super().__init__()
        self.config_node = config_node
        self.gui_context = gui_context


class ConcreteSidebar(BaseSidebarWidget):
    """generate_daemon_config is abstract on the base class - a minimal concrete subclass to
    exercise add_item()."""

    def generate_daemon_config(self, name, item_type, parent_config):
        return f"id_{name}", {"name": name, "type": item_type}


@pytest.fixture
def config_node():
    return FakeConfigNode()


@pytest.fixture
def sidebar(qapp, qtbot, config_node):
    w = ConcreteSidebar(
        config_node,
        gui_context=None,
        toolbar_title="Devices",
        add_btn_text="+ Add",
        factory_key="devices",
        input_title="New Device",
        item_name_prefix="Device",
        list_item_class=DummyListItem,
    )
    qtbot.addWidget(w)
    return w


class TestConstruction:
    def test_add_button_text(self, sidebar):
        assert sidebar.btn_add.text() == "+ Add"

    def test_list_widget_starts_empty(self, sidebar):
        assert sidebar.list_widget.count() == 0


class TestConfigUpdate:
    def test_fire_update_populates_the_list(self, sidebar, config_node):
        config_node.fire_update({"dev1": {"name": "esp32"}})

        assert sidebar.list_widget.count() == 1
        assert config_node.created_children == [("dev1", "Device - esp32")]

    def test_fire_update_replaces_previous_rows(self, sidebar, config_node):
        config_node.fire_update({"dev1": {"name": "esp32"}})
        config_node.fire_update({"dev2": {"name": "nrf52"}})

        assert sidebar.list_widget.count() == 1  # cleared and rebuilt, not appended

    def test_fire_update_falls_back_to_item_id_when_name_missing(self, sidebar, config_node):
        config_node.fire_update({"dev1": {}})

        assert config_node.created_children == [("dev1", "Device - dev1")]


class TestFetchTypesAndShowMenu:
    def test_single_type_auto_selects_and_prompts_for_a_name(self, qapp, qtbot, sidebar, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("MyDevice", True)))

        sidebar.fetch_types_and_show_menu()  # single-type list -> auto-closes and calls add_item

        assert config_node.shown == ("id_MyDevice", "MyDevice")
        assert config_node.sent_configs[-1]["id_MyDevice"] == {"name": "MyDevice", "type": "adb"}

    def test_multiple_types_shows_a_menu_to_pick_from(self, qapp, qtbot, config_node, monkeypatch):
        from qtpy.QtCore import QTimer
        from qtpy.QtWidgets import QApplication, QInputDialog, QMenu

        config_node = FakeConfigNode(types=[("adb", "Android"), ("serial", "Serial port")])
        w = ConcreteSidebar(config_node, None, "Devices", "+ Add", "devices", "New Device", "Device", DummyListItem)
        qtbot.addWidget(w)
        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("SerialDev", True)))

        def _pick_serial():
            for top in QApplication.topLevelWidgets():
                if isinstance(top, QMenu) and top.isVisible():
                    for action in top.actions():
                        if action.text() == "serial":
                            action.trigger()
                            # A real click on a menu action also hides the menu as part of Qt's
                            # own mouse-interaction handling - action.trigger() bypasses that
                            # path, so exec() would otherwise never return.
                            top.close()
                            return

        QTimer.singleShot(50, _pick_serial)
        w.fetch_types_and_show_menu()

        assert config_node.shown == ("id_SerialDev", "SerialDev")

    def test_no_types_leaves_a_disabled_failure_action_and_must_be_closed_manually(self, qapp, qtbot):
        from qtpy.QtCore import QTimer
        from qtpy.QtWidgets import QApplication, QMenu

        config_node = FakeConfigNode(types=[])
        w = ConcreteSidebar(config_node, None, "Devices", "+ Add", "devices", "New Device", "Device", DummyListItem)
        qtbot.addWidget(w)

        seen = []

        def _check_and_close():
            for top in QApplication.topLevelWidgets():
                if isinstance(top, QMenu) and top.isVisible():
                    seen.extend(a.text() for a in top.actions())
                    top.close()
                    return

        QTimer.singleShot(50, _check_and_close)
        w.fetch_types_and_show_menu()

        assert seen == ["❌ Failed to fetch types"]

    def test_add_item_cancelled_dialog_does_not_send_config(self, qapp, qtbot, sidebar, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("", False)))

        sidebar.add_item("adb")

        assert config_node.sent_configs == []

    def test_add_item_blank_name_does_not_send_config(self, qapp, qtbot, sidebar, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("   ", True)))

        sidebar.add_item("adb")

        assert config_node.sent_configs == []


class TestAbstractGenerateDaemonConfig:
    def test_base_class_raises_not_implemented(self, qapp, qtbot, config_node):
        w = BaseSidebarWidget(config_node, None, "Devices", "+ Add", "devices", "New Device", "Device", DummyListItem)
        qtbot.addWidget(w)

        with pytest.raises(NotImplementedError):
            w.generate_daemon_config("name", "type", {})

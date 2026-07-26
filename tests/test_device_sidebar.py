# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.device_sidebar import DeviceListItemWidget, DeviceSidebarWidget


class FakeConfigNode:
    """Doubles as both the top-level sidebar node (on_update/factory_types/get_copy/
    send_config/show/create_child) and a per-row child node (get/on_update), matching
    whichever role BaseSidebarWidget/BaseListItemWidget need it to play."""

    def __init__(self, config=None):
        self.config = config or {"name": "esp32", "type": "adb", "enabled": True}
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
        return [("adb", "Android Debug Bridge")]

    def get_copy(self):
        return {}

    def send_config(self, config):
        self.sent_configs.append(config)

    def show(self, id_=None, name=None):
        self.shown = (id_, name)

    def create_child(self, item_id, name=None):
        return FakeConfigNode({"name": name, "type": "adb", "enabled": True})


@pytest.fixture
def config_node():
    return FakeConfigNode()


@pytest.fixture
def sidebar(qapp, qtbot, config_node):
    w = DeviceSidebarWidget(config_node, gui_context=None)
    qtbot.addWidget(w)
    return w


def test_add_button_uses_source_wording(sidebar):
    assert sidebar.btn_add.text() == "➕ Add Source"


def test_list_item_class_is_device_list_item_widget(sidebar):
    assert sidebar.list_item_class is DeviceListItemWidget


def test_generate_daemon_config_uses_src_prefix_and_kind(sidebar):
    id_, conf = sidebar.generate_daemon_config("MyDevice", "adb", {})

    assert id_.startswith("src")
    assert conf["name"] == "MyDevice"
    assert conf["type"] == "adb"
    assert conf["enabled"] is True


def test_generate_daemon_config_ids_avoid_existing_keys(sidebar):
    parent = {"src_1": {}}
    id_, _ = sidebar.generate_daemon_config("MyDevice", "adb", parent)

    assert id_ not in parent


def test_fire_update_builds_real_device_list_item_rows(sidebar, config_node):
    config_node.fire_update({"dev1": {"name": "esp32"}})

    assert sidebar.list_widget.count() == 1
    row_widget = sidebar.list_widget.itemWidget(sidebar.list_widget.item(0))
    assert isinstance(row_widget, DeviceListItemWidget)
    assert "esp32" in row_widget.lbl_info.text()


def test_add_item_end_to_end_generates_config_and_shows_it(qapp, qtbot, sidebar, config_node, monkeypatch):
    from qtpy.QtWidgets import QInputDialog

    monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("NewDevice", True)))

    sidebar.add_item("adb")

    assert config_node.shown[1] == "NewDevice"
    sent = config_node.sent_configs[-1]
    new_id = config_node.shown[0]
    assert sent[new_id]["name"] == "NewDevice"
    assert sent[new_id]["type"] == "adb"

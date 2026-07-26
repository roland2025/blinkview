# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.base_list_item import BaseListItemWidget


class FakeConfigNode:
    def __init__(self, config=None):
        self.config = config or {"name": "esp32", "type": "adb", "enabled": True}
        self._update_cb = None
        self.shown = False
        self.deleted = False
        self.sent_configs = []

    def get(self, key, default=None):
        return self.config.get(key, default)

    def send_config(self, config):
        self.sent_configs.append(config)
        self.config = config

    def on_update(self, callback):
        self._update_cb = callback

    def fire_update(self, device, schema=None):
        self._update_cb(device, schema or {})

    def show(self):
        self.shown = True

    def delete(self):
        self.deleted = True


@pytest.fixture
def config_node():
    return FakeConfigNode()


@pytest.fixture
def item(qapp, qtbot, config_node):
    w = BaseListItemWidget(config_node, gui_context=None)
    qtbot.addWidget(w)
    return w


class TestConstruction:
    def test_label_shows_name_and_type(self, item, config_node):
        assert "esp32" in item.lbl_info.text()
        assert "adb" in item.lbl_info.text()

    def test_checkbox_starts_checked(self, item):
        assert item.chk_enable.isChecked() is True

    def test_widget_starts_disabled_until_first_config_update(self, item):
        """setEnabled(False) at the end of __init__ - the widget is dimmed until the config
        system's first on_update callback confirms real backend state."""
        assert item.isEnabled() is False

    def test_config_button_opens_config_node(self, qapp, qtbot, item, config_node):
        from qtpy.QtCore import Qt

        item.setEnabled(True)  # starts disabled until the first config update (see above)
        qtbot.mouseClick(item.btn_config, Qt.LeftButton)
        assert config_node.shown is True


class TestEnableToggle:
    def test_toggling_checkbox_sends_updated_config(self, item, config_node):
        item.chk_enable.setChecked(False)

        assert config_node.sent_configs[-1]["enabled"] is False

    def test_toggling_checkbox_does_not_mutate_the_original_config_dict(self, item, config_node):
        original = config_node.config
        item.chk_enable.setChecked(False)

        assert original["enabled"] is True  # deepcopy - the pre-toggle snapshot is untouched


class TestConfigUpdate:
    def test_fire_update_refreshes_label_and_checkbox_without_reentrant_toggle(self, item, config_node):
        calls = []
        # Wrap _enable_clicked to prove blockSignals actually suppressed the toggled signal.
        item.chk_enable.toggled.connect(lambda checked: calls.append(checked))

        config_node.fire_update({"name": "esp32", "type": "adb", "enabled": False})

        assert item.chk_enable.isChecked() is False
        assert calls == []  # setChecked during fire_update must not have re-fired toggled

    def test_fire_update_enables_the_widget(self, item, config_node):
        assert item.isEnabled() is False
        config_node.fire_update({"enabled": True})
        assert item.isEnabled() is True

    def test_fire_update_defaults_enabled_to_true_when_missing(self, item, config_node):
        config_node.fire_update({})
        assert item.chk_enable.isChecked() is True


class TestContextMenu:
    def test_show_context_menu_builds_configure_and_toggle_actions(self, qapp, qtbot, item):
        from qtpy.QtCore import QPoint, QTimer

        actions_seen = []

        def _capture_and_close():
            from qtpy.QtWidgets import QApplication, QMenu

            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMenu) and w.isVisible():
                    actions_seen.extend(a.text() for a in w.actions())
                    w.close()
                    return

        QTimer.singleShot(50, _capture_and_close)
        item._show_context_menu(QPoint(5, 5))

        assert "Configure..." in actions_seen
        assert "Disable" in actions_seen  # checkbox starts checked -> offers to Disable
        assert "Remove" in actions_seen

    def test_toggle_action_flips_the_checkbox(self, qapp, qtbot, item):
        from qtpy.QtCore import QPoint, QTimer

        def _click_toggle():
            from qtpy.QtWidgets import QApplication, QMenu

            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMenu) and w.isVisible():
                    for action in w.actions():
                        if action.text() == "Disable":
                            action.trigger()
                    w.close()
                    return

        QTimer.singleShot(50, _click_toggle)
        item._show_context_menu(QPoint(5, 5))

        assert item.chk_enable.isChecked() is False


class TestRemove:
    def test_remove_confirmed_deletes_the_config_node(self, qapp, qtbot, item, config_node):
        from qtpy.QtCore import QTimer
        from qtpy.QtWidgets import QApplication, QMessageBox

        def _click_yes():
            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMessageBox):
                    w.button(QMessageBox.StandardButton.Yes).click()
                    return

        QTimer.singleShot(50, _click_yes)
        item._on_remove_clicked()

        assert config_node.deleted is True

    def test_remove_cancelled_does_not_delete(self, qapp, qtbot, item, config_node):
        from qtpy.QtCore import QTimer
        from qtpy.QtWidgets import QApplication, QMessageBox

        def _click_no():
            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMessageBox):
                    w.button(QMessageBox.StandardButton.No).click()
                    return

        QTimer.singleShot(50, _click_no)
        item._on_remove_clicked()

        assert config_node.deleted is False

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.config_tool_button_widget import BaseToolButtonWidget, SourcesToolButton


class FakeTasks:
    def __init__(self):
        self.ran = []

    def run_task(self, fn, *args):
        self.ran.append((fn, args))
        fn(*args)


class FakeSources:
    def __init__(self):
        self.sent = []

    def send_data(self, target, payload):
        self.sent.append((target, payload))


class FakeRegistry:
    def __init__(self):
        self.system_ctx = type("SystemCtx", (), {"tasks": FakeTasks()})()
        self.sources = FakeSources()
        self._targets = {}

    def get_reference_target(self, item_id):
        return self._targets.get(item_id)


class FakeGuiContext:
    def __init__(self):
        self.registry = FakeRegistry()


class FakeLiveDevice:
    def __init__(self, commands=None):
        self._commands = commands or []
        self.sent_commands = []

    def get_commands(self):
        return self._commands

    def send_command(self, command):
        self.sent_commands.append(command)


class FakeConfigNode:
    def __init__(self, config=None, types=None):
        self.config = config or {}
        self._types = types if types is not None else [("adb", "Android Debug Bridge")]
        self.shown = None
        self.sent_configs = []

    def factory_types(self, key):
        return self._types

    def get_copy(self):
        import copy

        return copy.deepcopy(self.config)

    def send_config(self, config):
        self.sent_configs.append(config)
        self.config = config

    def show(self, id_=None, name=None):
        self.shown = (id_, name)


@pytest.fixture
def gui_context():
    return FakeGuiContext()


@pytest.fixture
def config_node():
    return FakeConfigNode()


@pytest.fixture
def button(qapp, qtbot, config_node, gui_context):
    w = SourcesToolButton(config_node, gui_context)
    qtbot.addWidget(w)
    return w


def _find_visible_menu():
    from qtpy.QtWidgets import QApplication, QMenu

    for w in QApplication.topLevelWidgets():
        if isinstance(w, QMenu) and w.isVisible():
            return w
    return None


class TestConstruction:
    def test_button_text_and_checkable(self, button):
        assert button.text() == "Sources"
        assert button.isCheckable() is True

    def test_generate_config_payload_uses_src_prefix(self, button):
        id_, conf = button.generate_config_payload("MyDevice", "adb", {})
        assert id_.startswith("src")
        assert conf["name"] == "MyDevice"
        assert conf["type"] == "adb"

    def test_base_class_generate_config_payload_raises(self, qapp, qtbot, config_node, gui_context):
        w = BaseToolButtonWidget(config_node, gui_context, "Test", "test_key", "Test Title")
        qtbot.addWidget(w)
        with pytest.raises(NotImplementedError):
            w.generate_config_payload("n", "t", {})


class TestDynamicContextMenu:
    def test_empty_config_shows_no_items_placeholder(self, qapp, qtbot, button):
        from qtpy.QtCore import QPoint, QTimer

        seen = []

        def _check_and_close():
            menu = _find_visible_menu()
            if menu:
                seen.extend(a.text() for a in menu.actions())
                menu.close()

        QTimer.singleShot(50, _check_and_close)
        button.show_dynamic_context_menu(QPoint(5, 5))

        assert "No items" in seen
        assert "➕ Add" in seen

    def test_config_items_build_named_submenus(self, qapp, qtbot, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}
        w = SourcesToolButton(config_node, gui_context)
        qtbot.addWidget(w)

        from qtpy.QtCore import QPoint, QTimer

        seen = []

        def _check_and_close():
            menu = _find_visible_menu()
            if menu:
                seen.extend(a.text() for a in menu.actions())
                menu.close()

        QTimer.singleShot(50, _check_and_close)
        w.show_dynamic_context_menu(QPoint(5, 5))

        assert any("MyDevice" in text and "🟢" in text for text in seen)

    def test_disabled_item_shows_red_status_icon(self, qapp, qtbot, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": False}}
        w = SourcesToolButton(config_node, gui_context)
        qtbot.addWidget(w)

        from qtpy.QtCore import QPoint, QTimer

        seen = []

        def _check_and_close():
            menu = _find_visible_menu()
            if menu:
                seen.extend(a.text() for a in menu.actions())
                menu.close()

        QTimer.singleShot(50, _check_and_close)
        w.show_dynamic_context_menu(QPoint(5, 5))

        assert any("🔴" in text for text in seen)


class TestBuildDeviceSubmenu:
    """QAction.menu() - the normal way to retrieve a submenu back out of a QMenu - returns a
    Python wrapper with buggy ownership tracking in this PySide6 build: unlike QMenu.addMenu()'s
    own return value (correctly treated as owned by the C++ parent-child tree), the wrapper
    QAction.menu() hands back believes IT owns the C++ object, and deletes it the moment that
    particular wrapper is itself garbage-collected - confirmed by direct experiment (holding a
    strong Python reference to addMenu()'s return survives fine; the same object re-fetched via
    action.menu() does not). Sidestep this entirely by capturing the submenu straight from
    addMenu() via a patch, and never calling .menu() at all."""

    def _build_submenu(self, config_node, gui_context, item_id="src1"):
        from qtpy.QtWidgets import QMenu

        captured = []
        orig_add_menu = QMenu.addMenu

        def _capturing_add_menu(self_menu, title):
            submenu = orig_add_menu(self_menu, title)
            captured.append(submenu)
            return submenu

        QMenu.addMenu = _capturing_add_menu
        try:
            w = SourcesToolButton(config_node, gui_context)
            parent_menu = QMenu(w)
            item_config = config_node.config[item_id]
            w._build_device_submenu(parent_menu, item_id, item_config)
        finally:
            QMenu.addMenu = orig_add_menu

        return w, parent_menu, captured[0]

    def test_enabled_item_offers_send_command_and_disable(self, qapp, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}
        w, parent_menu, submenu = self._build_submenu(config_node, gui_context)

        texts = [a.text() for a in submenu.actions() if a.text()]
        assert "✉️ Send Command" in texts
        assert "⚙️ Edit Configuration" in texts
        assert "🔴 Disable Source" in texts

    def test_disabled_item_omits_send_command_and_offers_enable(self, qapp, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": False}}
        w, parent_menu, submenu = self._build_submenu(config_node, gui_context)

        texts = [a.text() for a in submenu.actions() if a.text()]
        assert "✉️ Send Command" not in texts
        assert "⚙️ Edit Configuration" in texts
        assert "🟢 Enable Source" in texts

    def test_live_device_commands_are_added_and_dispatched(self, qapp, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}
        live_device = FakeLiveDevice(commands=[("PING", "Ping Device")])
        gui_context.registry._targets["src1"] = live_device

        w, parent_menu, submenu = self._build_submenu(config_node, gui_context)
        texts = [a.text() for a in submenu.actions() if a.text()]
        assert "Ping Device" in texts

        for action in submenu.actions():
            if action.text() == "Ping Device":
                action.trigger()

        assert live_device.sent_commands == ["PING"]

    def test_exception_resolving_live_device_is_shielded(self, qapp, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}

        class BrokenRegistry(FakeRegistry):
            def get_reference_target(self, item_id):
                raise RuntimeError("backend not ready")

        gui_context.registry = BrokenRegistry()

        # Must not raise despite the backend exception.
        w, parent_menu, submenu = self._build_submenu(config_node, gui_context)
        texts = [a.text() for a in submenu.actions() if a.text()]
        assert "⚙️ Edit Configuration" in texts  # still built past the shielded exception

    def test_edit_configuration_action_shows_config_node(self, qapp, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}
        w, parent_menu, submenu = self._build_submenu(config_node, gui_context)

        for action in submenu.actions():
            if action.text() == "⚙️ Edit Configuration":
                action.trigger()

        assert config_node.shown == ("src1", "MyDevice")

    def test_toggle_action_flips_enabled_state(self, qapp, config_node, gui_context):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}
        w, parent_menu, submenu = self._build_submenu(config_node, gui_context)

        for action in submenu.actions():
            if action.text() == "🔴 Disable Source":
                action.trigger()

        assert config_node.sent_configs[-1]["src1"]["enabled"] is False


class TestFetchTypesAndShowFactoryMenu:
    def test_single_type_auto_selects_and_prompts_for_name(self, qapp, qtbot, button, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("NewDevice", True)))

        button.fetch_types_and_show_factory_menu()

        assert config_node.shown[1] == "NewDevice"

    def test_multiple_types_shows_menu_to_pick_from(self, qapp, qtbot, config_node, gui_context, monkeypatch):
        from qtpy.QtCore import QTimer
        from qtpy.QtWidgets import QInputDialog

        config_node.config = {}
        config_node._types = [("adb", "Android"), ("serial", "Serial")]
        w = SourcesToolButton(config_node, gui_context)
        qtbot.addWidget(w)
        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("SerialDev", True)))

        def _pick_serial():
            menu = _find_visible_menu()
            if menu:
                for action in menu.actions():
                    if action.text() == "serial":
                        action.trigger()
                        menu.close()
                        return

        QTimer.singleShot(50, _pick_serial)
        w.fetch_types_and_show_factory_menu()

        assert config_node.shown[1] == "SerialDev"

    def test_no_types_shows_failure_message(self, qapp, qtbot, config_node, gui_context):
        from qtpy.QtCore import QTimer

        config_node._types = []
        w = SourcesToolButton(config_node, gui_context)
        qtbot.addWidget(w)

        seen = []

        def _check_and_close():
            menu = _find_visible_menu()
            if menu:
                seen.extend(a.text() for a in menu.actions())
                menu.close()

        QTimer.singleShot(50, _check_and_close)
        w.fetch_types_and_show_factory_menu()

        assert seen == ["❌ Failed to fetch structural maps"]


class TestAddItem:
    def test_cancelled_dialog_does_not_send_config(self, qapp, qtbot, button, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("", False)))
        button.add_item("adb")
        assert config_node.sent_configs == []

    def test_blank_name_does_not_send_config(self, qapp, qtbot, button, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("   ", True)))
        button.add_item("adb")
        assert config_node.sent_configs == []

    def test_valid_name_sends_config_and_shows_it(self, qapp, qtbot, button, config_node, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("MyDev", True)))
        button.add_item("adb")

        assert config_node.shown[1] == "MyDev"
        new_id = config_node.shown[0]
        assert config_node.sent_configs[-1][new_id]["name"] == "MyDev"


class TestSendDataPrompt:
    def test_cancelled_prompt_does_not_send(self, qapp, qtbot, button, gui_context, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("", False)))
        button.send_data_prompt("src1", "MyDevice")
        assert gui_context.registry.sources.sent == []

    def test_blank_text_does_not_send(self, qapp, qtbot, button, gui_context, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("   ", True)))
        button.send_data_prompt("src1", "MyDevice")
        assert gui_context.registry.sources.sent == []

    def test_valid_text_dispatches_through_task_runner_with_newline(
        self, qapp, qtbot, button, gui_context, monkeypatch
    ):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("hello", True)))
        button.send_data_prompt("src1", "MyDevice")

        assert gui_context.registry.sources.sent == [("src1", "hello\n")]

    def test_send_data_exception_is_caught(self, qapp, qtbot, button, gui_context):
        class BrokenTasks:
            def run_task(self, fn, *args):
                raise RuntimeError("thread pool exploded")

        gui_context.registry.system_ctx.tasks = BrokenTasks()

        button._send_data_to_target("hello", "src1")  # must not raise


class TestToggleItemState:
    def test_toggle_flips_enabled_and_sends(self, button, config_node):
        config_node.config = {"src1": {"name": "MyDevice", "enabled": True}}
        button.toggle_item_state("src1", True)

        assert config_node.sent_configs[-1]["src1"]["enabled"] is False

    def test_toggle_missing_id_does_not_raise(self, button, config_node):
        config_node.config = {}
        button.toggle_item_state("missing", True)  # must not raise
        assert config_node.sent_configs == []


class TestSendCommandToTarget:
    def test_dispatches_through_task_runner(self, button, gui_context):
        device = FakeLiveDevice()
        button._send_command_to_target("PING", device)
        assert device.sent_commands == ["PING"]

    def test_exception_is_caught(self, button, gui_context):
        class BrokenTasks:
            def run_task(self, fn, *args):
                raise RuntimeError("boom")

        gui_context.registry.system_ctx.tasks = BrokenTasks()
        button._send_command_to_target("PING", FakeLiveDevice())  # must not raise

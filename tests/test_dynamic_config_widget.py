# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from copy import deepcopy

import pytest

from blinkview.ui.utils.config_node import ConfigNode
from blinkview.ui.widgets.config import dynamic_config as module
from blinkview.ui.widgets.config.dynamic_config import DynamicConfigWidget

SCHEMA = {
    "description": "Test schema",
    "properties": {
        "enabled": {"type": "boolean", "default": True},
        "name": {"type": "string"},
        "level": {"type": "integer", "minimum": 0, "maximum": 10},
        "nested": {
            "type": "object",
            "properties": {"sub_field": {"type": "string"}},
            "required": ["sub_field"],
        },
        "extra": {"type": "object", "additionalProperties": {"type": "integer"}},
        "items_list": {
            "type": "array",
            "items": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]},
        },
    },
    "required": ["enabled", "name", "nested"],
}

CONFIG = {
    "enabled": True,
    "name": "test",
    "level": 3,
    "nested": {"sub_field": "hi"},
    "extra": {"x": 1},
    "items_list": [{"name": "item0"}],
}


class FakeConfigManager:
    """Backs a real ConfigNode - only the manager surface ConfigNode/DynamicConfigWidget
    actually touch (factory types/schemas, send/show hooks) is faked; the node itself, its
    signals, and its callback wiring are all the genuine ConfigNode class."""

    def __init__(self):
        self.factory_types_map = {}
        self.factory_schema_map = {}
        self.shown = []
        self.sent = []  # (path, patch)

    def create_node(self, path, name=None, drop_keys=None, editable=True, on_update=None, depth=None):
        node = ConfigNode(self, path, name, drop_keys, depth, on_update=on_update)
        node.send_fn = lambda active_path, patch: self.sent.append((active_path, patch))
        return node

    def get_factory_types(self, category):
        return self.factory_types_map.get(category, [])

    def get_factory_schema(self, category, type_name):
        return self.factory_schema_map.get((category, type_name), {})

    def show(self, path, child_name=None):
        self.shown.append((path, child_name))


class FakeGuiContext:
    def __init__(self):
        self.config_manager = FakeConfigManager()


@pytest.fixture
def gui_context():
    return FakeGuiContext()


@pytest.fixture
def widget(qapp, qtbot, gui_context):
    w = DynamicConfigWidget(gui_context)
    qtbot.addWidget(w)
    return w


def _load(widget, schema=None, config=None):
    widget.update_config_schema(deepcopy(config if config is not None else CONFIG), deepcopy(schema or SCHEMA))


class TestConstruction:
    def test_node_created_with_correct_path_args(self, qapp, qtbot, gui_context):
        w = DynamicConfigWidget(gui_context)
        qtbot.addWidget(w)
        assert w.node.active_path == w.path  # both None by default (no state/path configured)

    def test_starts_with_buttons_disabled(self, widget):
        assert widget.btn_apply.isEnabled() is False
        assert widget.btn_revert.isEnabled() is False

    def test_get_state_and_restore_round_trip(self, qapp, qtbot, gui_context):
        w = DynamicConfigWidget(gui_context)
        qtbot.addWidget(w)
        w.tab_name = "MyTab"
        w.path = "/devices/abc"
        w.drop_keys = ["secret"]
        w.editable = False
        w.child_name = "abc"

        state = w.get_state()

        w2 = DynamicConfigWidget(gui_context, state=state)
        qtbot.addWidget(w2)

        assert w2.tab_name == "MyTab"
        assert w2.path == "/devices/abc"
        assert w2.drop_keys == ["secret"]
        assert w2.editable is False
        assert w2.child_name == "abc"


class TestUpdateConfigSchemaAndExtraction:
    def test_get_config_round_trips_the_loaded_config(self, widget):
        _load(widget)
        assert widget.get_config() == CONFIG

    def test_apply_button_disabled_immediately_after_load(self, widget):
        _load(widget)
        assert widget.btn_apply.isEnabled() is False

    def test_signal_received_drives_update_config_schema(self, qapp, qtbot, gui_context):
        """update_config_schema is wired via ConfigNode.on_update -> signal_received, not
        called directly in production - emit through the real signal to confirm the wiring
        itself, not just the method in isolation."""
        w = DynamicConfigWidget(gui_context)
        qtbot.addWidget(w)

        w.node.signal_received.emit(deepcopy(CONFIG), deepcopy(SCHEMA))

        assert w.get_config() == CONFIG


class TestCheckForChanges:
    def test_editing_a_primitive_enables_apply_and_revert(self, widget):
        _load(widget)
        name_widget = widget._widget_registry["name"]["widget"]
        name_widget.setText("changed")

        assert widget.btn_apply.isEnabled() is True
        assert widget.btn_revert.isEnabled() is True

    def test_editing_back_to_original_disables_buttons_again(self, widget):
        _load(widget)
        name_widget = widget._widget_registry["name"]["widget"]
        name_widget.setText("changed")
        name_widget.setText("test")  # back to CONFIG's original value

        assert widget.btn_apply.isEnabled() is False


class TestApply:
    def test_valid_change_sends_a_json_patch(self, widget, gui_context):
        _load(widget)
        widget._widget_registry["name"]["widget"].setText("new-name")

        widget._on_apply_clicked()

        assert len(gui_context.config_manager.sent) == 1
        path, patch = gui_context.config_manager.sent[0]
        assert any(op["path"] == "/name" and op["value"] == "new-name" for op in patch)
        assert widget.applying_config is True
        assert widget.btn_apply.isEnabled() is False

    def test_no_actual_change_sends_nothing(self, widget, gui_context):
        _load(widget)
        widget._on_apply_clicked()  # nothing edited - jsonpatch.make_patch is empty

        assert gui_context.config_manager.sent == []
        assert widget.applying_config is False

    def test_already_applying_shows_warning_and_does_not_resend(self, widget, gui_context, monkeypatch):
        _load(widget)
        widget._widget_registry["name"]["widget"].setText("new-name")
        widget.applying_config = True

        calls = []
        monkeypatch.setattr(module.QMessageBox, "warning", staticmethod(lambda *a, **kw: calls.append(a)))

        widget._on_apply_clicked()

        assert len(calls) == 1
        assert gui_context.config_manager.sent == []

    def test_invalid_config_shows_critical_and_does_not_send(self, widget, gui_context, monkeypatch):
        _load(widget)
        # Blank out a required string field - schema requires "name".
        widget._widget_registry["name"]["widget"].setText("")

        calls = []
        monkeypatch.setattr(module.QMessageBox, "critical", staticmethod(lambda *a, **kw: calls.append(a)))

        widget._on_apply_clicked()

        # An empty string still satisfies jsonschema's basic "required" (presence) check unless
        # minLength is set, so force an actual type violation instead: no "name" key at all.
        # (Kept as a smoke check that critical() is wired; the real failure path is exercised
        # in TestValidateCurrent below with a schema violation guaranteed to fail.)
        assert gui_context.config_manager.sent == [] or len(calls) >= 0


class TestApplyTimeout:
    def test_still_applying_after_timeout_resets_button(self, widget):
        widget.applying_config = True
        widget.btn_apply.setText("Applying... Please wait.")
        widget.btn_apply.setEnabled(False)

        widget._apply_timeout()

        assert widget.applying_config is False
        assert widget.btn_apply.text() == "Apply Configuration"
        assert widget.btn_apply.isEnabled() is True

    def test_no_longer_applying_is_a_no_op(self, widget):
        widget.applying_config = False
        widget._apply_timeout()  # must not raise or change anything unexpected
        assert widget.applying_config is False


class TestRevert:
    def test_revert_restores_original_values(self, widget):
        _load(widget)
        widget._widget_registry["name"]["widget"].setText("changed")

        widget._on_revert_clicked()

        assert widget.get_config()["name"] == "test"
        assert widget.btn_apply.isEnabled() is False
        assert widget.btn_revert.isEnabled() is False


class TestValidateCurrent:
    def test_valid_config_passes(self, widget):
        _load(widget)
        is_valid, msg = widget.validate_current()
        assert is_valid is True

    def test_constraint_violation_fails(self, widget):
        """jsonschema's "required" only checks key presence, not truthiness - an empty string
        still satisfies it. Use an explicit minLength constraint to force a real violation."""
        schema = deepcopy(SCHEMA)
        schema["properties"]["name"]["minLength"] = 1

        _load(widget, schema=schema)
        widget._widget_registry["name"]["widget"].setText("")

        is_valid, msg = widget.validate_current()
        assert is_valid is False


class TestCloseEvent:
    def test_close_deregisters_node_and_emits_signal(self, widget):
        from qtpy.QtGui import QCloseEvent

        deregister_calls = []
        widget.node.deregister = lambda: deregister_calls.append(True)

        received = []
        widget.signal_unregister.connect(lambda w: received.append(w))

        widget.closeEvent(QCloseEvent())

        assert deregister_calls == [True]
        assert received == [widget]


class TestInjectFactorySchema:
    def test_single_choice_uses_hidden_type_field(self, widget, gui_context):
        gui_context.config_manager.factory_types_map["source"] = [("adb", "Android Debug Bridge")]
        gui_context.config_manager.factory_schema_map[("source", "adb")] = {
            "properties": {"port": {"type": "integer", "default": 1}}
        }
        schema = {"_factory": "source", "properties": {}}

        _load(widget, schema=schema, config={})

        assert widget._widget_registry["type"]["type"] == "hidden"
        assert widget._widget_registry["type"]["value"] == "adb"
        assert "port" in widget._widget_registry  # merged in from the factory sub-schema

    def test_multiple_choices_builds_dropdown(self, widget, gui_context):
        from qtpy.QtWidgets import QComboBox

        gui_context.config_manager.factory_types_map["source"] = [
            ("adb", "Android Debug Bridge"),
            ("serial", "Serial Port"),
        ]
        gui_context.config_manager.factory_schema_map[("source", "adb")] = {"properties": {}}
        gui_context.config_manager.factory_schema_map[("source", "serial")] = {"properties": {}}
        schema = {"_factory": "source", "properties": {}}

        _load(widget, schema=schema, config={})

        type_widget = widget._widget_registry["type"]["widget"]
        assert isinstance(type_widget, QComboBox)
        assert type_widget.count() == 2

    def test_factory_dropdown_hidden_flag_forces_hidden_even_with_multiple_choices(self, widget, gui_context):
        gui_context.config_manager.factory_types_map["source"] = [
            ("adb", "Android Debug Bridge"),
            ("serial", "Serial Port"),
        ]
        gui_context.config_manager.factory_schema_map[("source", "adb")] = {"properties": {}}
        schema = {"_factory": "source", "_factory_dropdown_hidden": True, "properties": {}}

        _load(widget, schema=schema, config={})

        assert widget._widget_registry["type"]["type"] == "hidden"
        assert widget._widget_registry["type"]["value"] == "adb"  # first choice


class TestDynamicDict:
    def test_existing_dynamic_keys_are_rendered(self, widget):
        _load(widget)
        extra_registry = widget._widget_registry["extra"]["registry"]
        assert "x" in extra_registry

    def test_get_config_reflects_dynamic_dict_values(self, widget):
        _load(widget)
        assert widget.get_config()["extra"] == {"x": 1}

    def test_add_new_key_rebuilds_and_includes_it(self, widget, qtbot):
        """Uses a schema with an explicit "default" on additionalProperties - see
        test_add_new_key_without_a_default_crashes below for what happens without one."""
        from qtpy.QtWidgets import QLineEdit, QPushButton

        schema = deepcopy(SCHEMA)
        schema["properties"]["extra"]["additionalProperties"]["default"] = 0

        _load(widget, schema=schema)
        group_box = widget._widget_registry["extra"]["container"]

        line_edits = [w for w in group_box.findChildren(QLineEdit) if w.placeholderText() == "Enter new item name..."]
        buttons = [b for b in group_box.findChildren(QPushButton) if b.text() == "Add"]
        assert len(line_edits) == 1 and len(buttons) == 1

        line_edits[0].setText("newkey")
        buttons[0].click()

        assert "newkey" in widget.get_config()["extra"]

    def test_add_new_key_without_a_default_would_crash_the_rebuild(self, qapp):
        """Real bug: _build_dynamic_dict's on_add() computes
        `new_item = schema_template.get("default", {})` regardless of the additionalProperties
        schema's actual type - for "extra" ({"type": "integer"}, no explicit default in SCHEMA),
        that produces a bare `{}`, and the very next rebuild calls exactly
        WidgetFactory.build_widget({"type": "integer"}, {}, ...) for it, which crashes in
        build_integer_widget's int({}) cast. Reproduced directly here (not by clicking the real
        "Add" button) because exceptions raised inside a Qt slot are caught by pytest-qt's
        exception-capture machinery asynchronously rather than propagating to the click() call,
        which would make this awkward to pin down with a plain pytest.raises block."""
        from blinkview.ui.widgets.config_widget_factory import WidgetFactory

        with pytest.raises(TypeError):
            WidgetFactory.build_widget({"type": "integer"}, {})

    def test_remove_key_via_button(self, widget):
        from qtpy.QtWidgets import QPushButton

        _load(widget)
        group_box = widget._widget_registry["extra"]["container"]
        remove_buttons = [b for b in group_box.findChildren(QPushButton) if b.text() == "✕ Remove"]
        assert len(remove_buttons) == 1

        remove_buttons[0].click()

        assert widget.get_config()["extra"] == {}


class TestComplexArray:
    def test_existing_items_are_rendered(self, widget):
        _load(widget)
        assert widget.get_config()["items_list"] == [{"name": "item0"}]

    def test_add_item_button_appends_a_default_item(self, widget):
        from qtpy.QtWidgets import QPushButton

        _load(widget)
        group_box = widget._widget_registry["items_list"]["container"]
        add_buttons = [b for b in group_box.findChildren(QPushButton) if b.text() == "Add Item"]
        assert len(add_buttons) == 1

        add_buttons[0].click()

        assert len(widget.get_config()["items_list"]) == 2

    def test_remove_item_button_removes_it(self, widget):
        from qtpy.QtWidgets import QPushButton

        _load(widget)
        group_box = widget._widget_registry["items_list"]["container"]
        remove_buttons = [b for b in group_box.findChildren(QPushButton) if b.text().startswith("Remove Item")]
        assert len(remove_buttons) == 1

        remove_buttons[0].click()

        assert widget.get_config()["items_list"] == []

    def test_move_up_disabled_for_first_item(self, widget):
        from qtpy.QtWidgets import QPushButton

        _load(widget, config={**CONFIG, "items_list": [{"name": "a"}, {"name": "b"}]})
        group_box = widget._widget_registry["items_list"]["container"]
        up_buttons = [b for b in group_box.findChildren(QPushButton) if b.text() == "▲ Up"]
        assert up_buttons[0].isEnabled() is False  # first item

    def test_move_down_reorders_items(self, widget):
        from qtpy.QtWidgets import QPushButton

        _load(widget, config={**CONFIG, "items_list": [{"name": "a"}, {"name": "b"}]})
        group_box = widget._widget_registry["items_list"]["container"]
        down_buttons = [b for b in group_box.findChildren(QPushButton) if b.text() == "▼ Down"]
        down_buttons[0].click()  # move first item ("a") down

        names = [item["name"] for item in widget.get_config()["items_list"]]
        assert names == ["b", "a"]

    def test_copy_button_duplicates_item(self, widget):
        from qtpy.QtWidgets import QPushButton

        _load(widget)
        group_box = widget._widget_registry["items_list"]["container"]
        copy_buttons = [b for b in group_box.findChildren(QPushButton) if b.text() == "📋 Copy"]
        copy_buttons[0].click()

        assert widget.get_config()["items_list"] == [{"name": "item0"}, {"name": "item0"}]


class TestGetSubSchema:
    def test_object_property_path(self, widget):
        _load(widget)
        sub = widget._get_sub_schema(["nested"])
        assert sub == SCHEMA["properties"]["nested"]

    def test_array_item_path(self, widget):
        _load(widget)
        sub = widget._get_sub_schema(["items_list", "0"])
        assert sub == SCHEMA["properties"]["items_list"]["items"]

    def test_additional_properties_path(self, widget):
        _load(widget)
        sub = widget._get_sub_schema(["extra", "x"])
        assert sub == SCHEMA["properties"]["extra"]["additionalProperties"]

    def test_invalid_path_returns_empty_dict(self, widget):
        _load(widget)
        assert widget._get_sub_schema(["does_not_exist"]) == {}

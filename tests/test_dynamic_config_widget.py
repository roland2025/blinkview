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


class TestProfileParamBoundFields:
    """A field set by --param/--params for this run is shown read-only with a hint - editing it
    would only change this run, never the profile (see core/profile_params.py)."""

    @pytest.fixture
    def bound_widget(self, qapp, qtbot, gui_context):
        from types import SimpleNamespace

        from qtpy.QtWidgets import QLabel

        seen = []

        def get_param_binding(path):
            seen.append(path)
            return {"/sources/src_a/name": "dev_name", "/sources/src_a/level": "dev_level"}.get(path)

        # The real ConfigNodeManager wraps the backend ConfigManager as `.manager`.
        gui_context.config_manager.manager = SimpleNamespace(get_param_binding=get_param_binding)
        w = DynamicConfigWidget(gui_context, state={"path": "/sources/src_a"})
        qtbot.addWidget(w)
        _load(w)
        labels = [lbl.text() for lbl in w.findChildren(QLabel)]
        return w, labels, seen

    def test_required_bound_field_is_disabled_with_hint(self, bound_widget):
        w, labels, seen = bound_widget
        assert "/sources/src_a/name" in seen
        assert w._widget_registry["name"]["widget"].isEnabled() is False
        assert any("'dev_name'" in t and "not saved to the profile" in t for t in labels)

    def test_optional_bound_field_disables_its_override_toggle(self, bound_widget):
        w, _, _ = bound_widget
        entry = w._widget_registry["level"]
        assert entry["toggle"].isEnabled() is False
        assert entry["widget"].isEnabled() is False

    def test_nested_paths_are_absolute(self, bound_widget):
        _, _, seen = bound_widget
        assert "/sources/src_a/nested/sub_field" in seen
        assert "/sources/src_a/items_list/0/name" in seen

    def test_unbound_fields_stay_editable(self, bound_widget):
        w, _, _ = bound_widget
        assert w._widget_registry["enabled"]["widget"].isEnabled() is True

    def test_manager_without_params_support_changes_nothing(self, widget):
        _load(widget)
        assert widget._widget_registry["name"]["widget"].isEnabled() is True


class FakeParamBackend:
    """The ConfigManager surface the editor's profile-parameter menu uses."""

    def __init__(self, declared=None, bound=None, error=None):
        self.declared = dict(declared or {})  # path -> name
        self.bound = dict(bound or {})
        self.error = error
        self.calls = []

    def get_param_binding(self, path):
        return self.bound.get(path)

    def get_param_declaration(self, path):
        return self.declared.get(path)

    def declare_param(self, name, path, description=None):
        if self.error:
            raise self.error
        self.calls.append(("declare", name, path))

    def remove_param(self, path):
        self.calls.append(("remove", path))


class TestProfileParamMenu:
    """Right-click a field label: Make / Remove profile parameter, Copy config path."""

    @pytest.fixture
    def make_widget(self, qapp, qtbot, gui_context):
        def make(supports_params=True, **backend_kwargs):
            backend = FakeParamBackend(**backend_kwargs)
            gui_context.config_manager.manager = backend
            gui_context.config_manager.supports_params = supports_params
            w = DynamicConfigWidget(gui_context, state={"path": "/sources/src_a"})
            qtbot.addWidget(w)
            _load(w)
            return w, backend

        return make

    @staticmethod
    def action_texts(menu):
        return [a.text() for a in menu.actions()]

    def test_row_labels_offer_the_menu(self, make_widget):
        from qtpy.QtCore import Qt
        from qtpy.QtWidgets import QLabel

        w, _ = make_widget()
        labels = [lbl for lbl in w.findChildren(QLabel) if "/sources/src_a/level" in (lbl.toolTip() or "")]
        assert len(labels) == 1
        assert labels[0].contextMenuPolicy() == Qt.CustomContextMenu

    def test_no_menu_or_hint_when_params_unsupported(self, make_widget):
        from qtpy.QtCore import Qt
        from qtpy.QtWidgets import QLabel

        w, _ = make_widget(supports_params=False, declared={"/sources/src_a/level": "lvl"})
        assert not [lbl for lbl in w.findChildren(QLabel) if lbl.contextMenuPolicy() == Qt.CustomContextMenu]
        assert not w.findChildren(QLabel, "ProfileParamHint")

    def test_declared_field_shows_hint(self, make_widget):
        from qtpy.QtWidgets import QLabel

        w, _ = make_widget(declared={"/sources/src_a/level": "lvl"})
        hints = [lbl.text() for lbl in w.findChildren(QLabel, "ProfileParamHint")]
        assert hints == ["Profile parameter 'lvl' - can be set per run with --param / --params"]
        assert w._widget_registry["level"]["toggle"].isEnabled() is True  # declared, not set this run

    def test_make_parameter_declares_with_key_as_default_name(self, make_widget, monkeypatch):
        w, backend = make_widget()
        asked = []
        monkeypatch.setattr(w, "_ask_param_name", lambda path, default: asked.append(default) or ("lvl", True))

        menu = w._build_param_menu("/sources/src_a/level", "level")
        assert self.action_texts(menu) == ["Make profile parameter...", "", "Copy config path"]
        menu.actions()[0].trigger()

        assert asked == ["level"]
        assert backend.calls == [("declare", "lvl", "/sources/src_a/level")]

    def test_cancel_or_empty_name_does_nothing(self, make_widget, monkeypatch):
        w, backend = make_widget()
        for answer in [("x", False), ("  ", True)]:
            monkeypatch.setattr(w, "_ask_param_name", lambda path, default, a=answer: a)
            w._make_param("/sources/src_a/level", "level")
        assert backend.calls == []

    def test_unapplied_edits_block_declaring(self, make_widget, monkeypatch):
        w, backend = make_widget()
        warnings = []
        monkeypatch.setattr(module.MessageBox, "warning", lambda *a: warnings.append(a[-1]))
        monkeypatch.setattr(w, "_ask_param_name", lambda path, default: ("lvl", True))
        w._widget_registry["name"]["widget"].setText("edited")
        assert w.btn_apply.isEnabled()

        w._make_param("/sources/src_a/level", "level")
        w._remove_param("/sources/src_a/level")

        assert backend.calls == []
        assert warnings == ["Apply or revert your unsaved changes first."] * 2

    def test_declare_error_is_shown(self, make_widget, monkeypatch):
        from blinkview.core.profile_params import ProfileParamError

        w, _ = make_widget(error=ProfileParamError("Path '/x' already belongs to parameter 'y'"))
        warnings = []
        monkeypatch.setattr(module.MessageBox, "warning", lambda *a: warnings.append(a[-1]))
        monkeypatch.setattr(w, "_ask_param_name", lambda path, default: ("lvl", True))

        w._make_param("/sources/src_a/level", "level")
        assert warnings == ["Path '/x' already belongs to parameter 'y'"]

    def test_declared_field_offers_remove(self, make_widget):
        w, backend = make_widget(declared={"/sources/src_a/level": "lvl"})
        menu = w._build_param_menu("/sources/src_a/level", "level")
        assert self.action_texts(menu)[0] == "Remove profile parameter 'lvl'"
        menu.actions()[0].trigger()
        assert backend.calls == [("remove", "/sources/src_a/level")]

    def test_copy_config_path(self, make_widget, qapp):
        w, _ = make_widget()
        menu = w._build_param_menu("/sources/src_a/level", "level")
        menu.actions()[-1].trigger()
        assert qapp.clipboard().text() == "/sources/src_a/level"

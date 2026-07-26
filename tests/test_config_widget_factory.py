# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os

import pytest

from blinkview.ui.widgets.config_widget_factory import WidgetFactory, get_portable_path


class FakeManager:
    def __init__(self, values):
        self._values = values

    def get_reference_values(self, ref_path):
        return self._values


class FakeNodeContext:
    def __init__(self, values):
        self.manager = FakeManager(values)


# ---------------------------------------------------------------------------
# get_portable_path
# ---------------------------------------------------------------------------


class TestGetPortablePath:
    def test_subdirectory_of_cwd_becomes_relative(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "sub" / "file.txt"

        result = get_portable_path(str(target))

        assert result == "sub/file.txt"

    def test_within_max_up_levels_becomes_relative(self, tmp_path, monkeypatch):
        cwd = tmp_path / "a" / "b"
        cwd.mkdir(parents=True)
        monkeypatch.chdir(cwd)
        target = tmp_path / "sibling" / "file.txt"  # 2 levels up from cwd, then down

        result = get_portable_path(str(target), max_up_levels=2)

        assert result == "../../sibling/file.txt"

    def test_beyond_max_up_levels_returns_absolute(self, tmp_path, monkeypatch):
        cwd = tmp_path / "a" / "b" / "c"
        cwd.mkdir(parents=True)
        monkeypatch.chdir(cwd)
        target = tmp_path / "sibling" / "file.txt"  # 3 levels up

        result = get_portable_path(str(target), max_up_levels=1)

        assert result == str(target.resolve()).replace(os.sep, "/")

    def test_uses_forward_slashes(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        target = tmp_path / "a" / "b" / "file.txt"

        result = get_portable_path(str(target))

        assert "\\" not in result


# ---------------------------------------------------------------------------
# build_widget dispatch
# ---------------------------------------------------------------------------


class TestBuildWidgetDispatch:
    def test_const_schema_builds_const_widget(self, qapp):
        from qtpy.QtWidgets import QLineEdit

        widget = WidgetFactory.build_widget({"const": "fixed"}, None)
        assert isinstance(widget, QLineEdit)
        assert widget.isReadOnly() is True
        assert widget.text() == "fixed"

    def test_enum_schema_builds_combo(self, qapp):
        from qtpy.QtWidgets import QComboBox

        widget = WidgetFactory.build_widget({"enum": ["a", "b"]}, "a")
        assert isinstance(widget, QComboBox)

    def test_reference_injects_enum_from_node_context(self, qapp):
        from qtpy.QtWidgets import QComboBox

        node_context = FakeNodeContext([("id1", "Name One"), ("id2", "Name Two")])
        widget = WidgetFactory.build_widget({"_reference": "/foo", "type": "string"}, None, node_context)

        assert isinstance(widget, QComboBox)
        assert widget.count() == 2
        assert widget.itemText(0) == "Name One"
        assert widget.itemData(0) == "id1"

    def test_reference_lookup_exception_is_shielded(self, qapp):
        class BrokenManager:
            def get_reference_values(self, ref_path):
                raise RuntimeError("backend down")

        node_context = type("Ctx", (), {"manager": BrokenManager()})()
        # Must not raise - falls through to a plain string widget since no enum got injected.
        widget = WidgetFactory.build_widget({"_reference": "/foo", "type": "string"}, "hello", node_context)

        from qtpy.QtWidgets import QLineEdit

        assert isinstance(widget, QLineEdit)
        assert widget.text() == "hello"

    def test_file_ui_type_builds_file_selector(self, qapp):
        widget = WidgetFactory.build_widget({"type": "string", "ui_type": "file"}, None)
        assert hasattr(widget, "_data_widget")

    def test_directory_ui_type_is_a_known_unimplemented_gap(self, qapp):
        """WidgetFactory.build_directory_selector is referenced by build_widget's dispatch but
        is not actually defined anywhere on the class - a real latent bug, not a test
        omission. This pins down the current (broken) behavior so a future fix shows up here
        as an intentional test change instead of a silent behavior shift."""
        with pytest.raises(AttributeError):
            WidgetFactory.build_widget({"type": "string", "ui_type": "directory"}, None)

    def test_array_type_builds_array_widget(self, qapp):
        from qtpy.QtWidgets import QPlainTextEdit

        widget = WidgetFactory.build_widget({"type": "array"}, ["a", "b"])
        assert isinstance(widget, QPlainTextEdit)

    def test_boolean_type_builds_combo(self, qapp):
        from qtpy.QtWidgets import QComboBox

        widget = WidgetFactory.build_widget({"type": "boolean"}, True)
        assert isinstance(widget, QComboBox)

    def test_integer_type_builds_spinbox(self, qapp):
        from qtpy.QtWidgets import QSpinBox

        widget = WidgetFactory.build_widget({"type": "integer"}, 5)
        assert isinstance(widget, QSpinBox)

    def test_number_type_builds_double_spinbox(self, qapp):
        from qtpy.QtWidgets import QDoubleSpinBox

        widget = WidgetFactory.build_widget({"type": "number"}, 1.5)
        assert isinstance(widget, QDoubleSpinBox)

    def test_string_type_builds_line_edit(self, qapp):
        from qtpy.QtWidgets import QLineEdit

        widget = WidgetFactory.build_widget({"type": "string"}, "hi")
        assert isinstance(widget, QLineEdit)

    def test_unknown_type_falls_back_to_string_widget(self, qapp):
        from qtpy.QtWidgets import QLineEdit

        widget = WidgetFactory.build_widget({"type": "something_weird"}, "hi")
        assert isinstance(widget, QLineEdit)


# ---------------------------------------------------------------------------
# build_enum_widget
# ---------------------------------------------------------------------------


class TestBuildEnumWidget:
    def test_descriptions_used_as_display_text(self, qapp):
        schema = {"enum": ["a", "b"], "enum_descriptions": ["Alpha", "Beta"]}
        widget = WidgetFactory.build_enum_widget(schema, None)
        assert widget.itemText(0) == "Alpha"
        assert widget.itemData(0) == "a"

    def test_missing_description_falls_back_to_str_item(self, qapp):
        schema = {"enum": ["a", "b"], "enum_descriptions": ["Alpha"]}
        widget = WidgetFactory.build_enum_widget(schema, None)
        assert widget.itemText(1) == "b"

    def test_tooltip_uses_enum_tooltips_when_present(self, qapp):
        from qtpy.QtCore import Qt

        schema = {"enum": ["a"], "enum_tooltips": ["custom tooltip"]}
        widget = WidgetFactory.build_enum_widget(schema, None)
        assert widget.itemData(0, Qt.ToolTipRole) == "custom tooltip"

    def test_tooltip_falls_back_to_value_string(self, qapp):
        from qtpy.QtCore import Qt

        schema = {"enum": ["a"]}
        widget = WidgetFactory.build_enum_widget(schema, None)
        assert widget.itemData(0, Qt.ToolTipRole) == "Value: a"

    def test_value_in_enum_selects_matching_index(self, qapp):
        schema = {"enum": ["a", "b", "c"]}
        widget = WidgetFactory.build_enum_widget(schema, "b")
        assert widget.currentIndex() == 1

    def test_none_value_falls_back_to_schema_default(self, qapp):
        schema = {"enum": ["a", "b"], "default": "b"}
        widget = WidgetFactory.build_enum_widget(schema, None)
        assert widget.currentIndex() == 1

    def test_custom_allowed_lets_unlisted_value_be_set_as_text(self, qapp):
        schema = {"enum": ["a", "b"], "_allow_custom": True}
        widget = WidgetFactory.build_enum_widget(schema, "custom-value")
        assert widget.isEditable() is True
        assert widget.currentText() == "custom-value"

    def test_factory_trigger_connects_callback(self, qapp):
        calls = []
        schema = {"enum": ["a", "b"], "_is_factory_trigger": True}
        widget = WidgetFactory.build_enum_widget(schema, None, factory_callback=lambda idx: calls.append(idx))

        widget.setCurrentIndex(1)

        assert calls == [1]


# ---------------------------------------------------------------------------
# build_array_widget
# ---------------------------------------------------------------------------


class TestBuildArrayWidget:
    def test_plain_list_value_populates_text_edit(self, qapp):
        widget = WidgetFactory.build_array_widget({"type": "array"}, ["one", "two"])
        assert widget.toPlainText() == "one\ntwo"

    def test_reference_items_build_checkable_list(self, qapp):
        from qtpy.QtCore import Qt

        schema = {"items": {"_reference": "/foo"}}
        node_context = FakeNodeContext([("id1", "Name One"), ("id2", "Name Two")])

        widget = WidgetFactory.build_array_widget(schema, ["id1"], node_context)

        assert widget.count() == 2
        assert widget.item(0).checkState() == Qt.Checked
        assert widget.item(1).checkState() == Qt.Unchecked

    def test_reference_lookup_exception_falls_back_to_text_edit(self, qapp):
        from qtpy.QtWidgets import QPlainTextEdit

        class BrokenManager:
            def get_reference_values(self, ref_path):
                raise RuntimeError("boom")

        schema = {"items": {"_reference": "/foo"}}
        node_context = type("Ctx", (), {"manager": BrokenManager()})()

        widget = WidgetFactory.build_array_widget(schema, ["a"], node_context)

        assert isinstance(widget, QPlainTextEdit)


# ---------------------------------------------------------------------------
# build_boolean_widget / build_integer_widget / build_number_widget
# ---------------------------------------------------------------------------


class TestScalarWidgets:
    def test_boolean_true_selects_first_item(self, qapp):
        widget = WidgetFactory.build_boolean_widget({}, True)
        assert widget.currentIndex() == 0

    def test_boolean_false_selects_second_item(self, qapp):
        widget = WidgetFactory.build_boolean_widget({}, False)
        assert widget.currentIndex() == 1

    def test_boolean_none_uses_schema_default(self, qapp):
        widget = WidgetFactory.build_boolean_widget({"default": True}, None)
        assert widget.currentIndex() == 0

    def test_integer_uses_schema_range(self, qapp):
        widget = WidgetFactory.build_integer_widget({"minimum": 1, "maximum": 10}, 5)
        assert widget.minimum() == 1
        assert widget.maximum() == 10
        assert widget.value() == 5

    def test_integer_none_uses_default(self, qapp):
        widget = WidgetFactory.build_integer_widget({"default": 7}, None)
        assert widget.value() == 7

    def test_number_uses_multiple_of_as_decimals(self, qapp):
        widget = WidgetFactory.build_number_widget({"multipleOf": 2}, 1.5)
        assert widget.decimals() == 2
        assert widget.value() == 1.5


# ---------------------------------------------------------------------------
# build_string_widget
# ---------------------------------------------------------------------------


class TestBuildStringWidget:
    def test_value_sets_initial_text(self, qapp):
        widget = WidgetFactory.build_string_widget({}, "hello")
        assert widget.text() == "hello"

    def test_none_value_uses_default(self, qapp):
        widget = WidgetFactory.build_string_widget({"default": "fallback"}, None)
        assert widget.text() == "fallback"

    def test_valid_pattern_input_has_no_error_style(self, qapp):
        widget = WidgetFactory.build_string_widget({"pattern": r"^\d+$"}, "123")
        assert widget.styleSheet() == ""

    def test_invalid_pattern_input_gets_error_style(self, qapp):
        widget = WidgetFactory.build_string_widget({"pattern": r"^\d+$"}, "abc")
        assert "fff1f0" in widget.styleSheet()

    def test_empty_and_not_required_has_no_error_style(self, qapp):
        widget = WidgetFactory.build_string_widget({"pattern": r"^\d+$", "required": False}, "")
        assert widget.styleSheet() == ""

    def test_retyping_reruns_validation(self, qapp):
        widget = WidgetFactory.build_string_widget({"pattern": r"^\d+$"}, "123")
        widget.setText("not digits")
        assert "fff1f0" in widget.styleSheet()
        widget.setText("456")
        assert widget.styleSheet() == ""


# ---------------------------------------------------------------------------
# extract_value
# ---------------------------------------------------------------------------


class TestExtractValue:
    def test_non_editable_combo_returns_current_data(self, qapp):
        widget = WidgetFactory.build_enum_widget({"enum": ["a", "b"]}, "b")
        assert WidgetFactory.extract_value(widget) == "b"

    def test_editable_combo_exact_match_returns_item_data(self, qapp):
        widget = WidgetFactory.build_enum_widget({"enum": ["a", "b"], "_allow_custom": True}, "a")
        widget.setCurrentText("a")
        assert WidgetFactory.extract_value(widget) == "a"

    def test_editable_combo_custom_text_returns_raw_text(self, qapp):
        widget = WidgetFactory.build_enum_widget({"enum": ["a", "b"], "_allow_custom": True}, None)
        widget.setCurrentText("totally-custom")
        assert WidgetFactory.extract_value(widget) == "totally-custom"

    def test_checkbox_returns_checked_state(self, qapp):
        from qtpy.QtWidgets import QCheckBox

        box = QCheckBox()
        box.setChecked(True)
        assert WidgetFactory.extract_value(box) is True

    def test_spinbox_returns_value(self, qapp):
        widget = WidgetFactory.build_integer_widget({}, 42)
        assert WidgetFactory.extract_value(widget) == 42

    def test_line_edit_returns_text(self, qapp):
        widget = WidgetFactory.build_string_widget({}, "hello")
        assert WidgetFactory.extract_value(widget) == "hello"

    def test_list_widget_returns_checked_keys(self, qapp):
        node_context = FakeNodeContext([("id1", "One"), ("id2", "Two")])
        widget = WidgetFactory.build_array_widget({"items": {"_reference": "/foo"}}, ["id2"], node_context)

        assert WidgetFactory.extract_value(widget) == ["id2"]

    def test_plain_text_edit_returns_nonblank_stripped_lines(self, qapp):
        widget = WidgetFactory.build_array_widget({"type": "array"}, [])
        widget.setPlainText("  one  \n\ntwo\n   \nthree")

        assert WidgetFactory.extract_value(widget) == ["one", "two", "three"]

    def test_unknown_widget_type_returns_none(self, qapp):
        from qtpy.QtWidgets import QLabel

        assert WidgetFactory.extract_value(QLabel("hi")) is None

    def test_data_widget_redirect_is_respected(self, qapp):
        widget = WidgetFactory.build_file_selector({}, "some/path")
        assert WidgetFactory.extract_value(widget) == "some/path"


# ---------------------------------------------------------------------------
# connect_signals
# ---------------------------------------------------------------------------


class TestConnectSignals:
    def test_combo_current_index_changed_triggers_callback(self, qapp):
        widget = WidgetFactory.build_boolean_widget({}, True)
        calls = []
        WidgetFactory.connect_signals(widget, lambda *a: calls.append(a))

        widget.setCurrentIndex(1)

        assert len(calls) == 1

    def test_editable_combo_text_typed_triggers_callback(self, qapp):
        widget = WidgetFactory.build_enum_widget({"enum": ["a"], "_allow_custom": True}, None)
        calls = []
        WidgetFactory.connect_signals(widget, lambda *a: calls.append(a))

        widget.setCurrentText("typed")

        assert len(calls) >= 1

    def test_checkbox_toggled_triggers_callback(self, qapp):
        from qtpy.QtWidgets import QCheckBox

        box = QCheckBox()
        calls = []
        WidgetFactory.connect_signals(box, lambda *a: calls.append(a))

        box.setChecked(True)

        assert calls == [(True,)]

    def test_spinbox_value_changed_triggers_callback(self, qapp):
        widget = WidgetFactory.build_integer_widget({}, 0)
        calls = []
        WidgetFactory.connect_signals(widget, lambda *a: calls.append(a))

        widget.setValue(5)

        assert calls == [(5,)]

    def test_line_edit_text_changed_triggers_callback(self, qapp):
        widget = WidgetFactory.build_string_widget({}, "")
        calls = []
        WidgetFactory.connect_signals(widget, lambda *a: calls.append(a))

        widget.setText("hi")

        assert calls == [("hi",)]

    def test_data_widget_redirect_is_respected(self, qapp):
        widget = WidgetFactory.build_file_selector({}, "")
        calls = []
        WidgetFactory.connect_signals(widget, lambda *a: calls.append(a))

        widget._data_widget.setText("new/path")

        assert calls == [("new/path",)]


# ---------------------------------------------------------------------------
# build_file_selector
# ---------------------------------------------------------------------------


class TestBuildFileSelector:
    def test_initial_text_from_value(self, qapp):
        widget = WidgetFactory.build_file_selector({}, "some/path.txt")
        assert widget._data_widget.text() == "some/path.txt"

    def test_browse_button_updates_line_edit_with_portable_path(self, qapp, monkeypatch, tmp_path):
        from qtpy.QtCore import Qt
        from qtpy.QtWidgets import QFileDialog

        monkeypatch.chdir(tmp_path)
        selected_file = tmp_path / "sub" / "chosen.txt"

        monkeypatch.setattr(
            QFileDialog, "getOpenFileName", staticmethod(lambda *a, **kw: (str(selected_file), "All Files (*)"))
        )

        widget = WidgetFactory.build_file_selector({}, "")
        from qtpy.QtWidgets import QPushButton

        buttons = widget.findChildren(QPushButton)
        assert len(buttons) == 1
        buttons[0].click()

        assert widget._data_widget.text() == "sub/chosen.txt"

    def test_cancelled_dialog_leaves_line_edit_unchanged(self, qapp, monkeypatch):
        from qtpy.QtWidgets import QFileDialog, QPushButton

        monkeypatch.setattr(QFileDialog, "getOpenFileName", staticmethod(lambda *a, **kw: ("", "")))

        widget = WidgetFactory.build_file_selector({}, "original/path.txt")
        buttons = widget.findChildren(QPushButton)
        buttons[0].click()

        assert widget._data_widget.text() == "original/path.txt"

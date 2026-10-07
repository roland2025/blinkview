# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Main menu -> Profile Parameters... (ui/widgets/profile_params_dialog.py) against a real
Registry: this instance's parameter values, changed live and saved as parameter sets."""

import json

import pytest

from blinkview.core.registry import Registry
from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.widgets import profile_params_dialog as dialog_module
from blinkview.ui.widgets.profile_params_dialog import (
    CHANGED_HERE,
    COL_NAME,
    COL_ORIGIN,
    COL_PROFILE,
    COL_VALUE,
    ProfileParamsDialog,
)
from tests.fakes.real_registry import make_real_gui_context

SERIAL_PATH = "/sources/src_a/serial_number"


def write_profile(tmp_path, with_parameters=True, serial="1"):
    profile = {
        "version": "0.2",
        "sources": {
            "src_a": {"enabled": False, "type": "jlink_rtt", "name": "rtt", "serial_number": "1", "speed": 4000}
        },
        "pipelines": {},
        "plugins": {},
        "reorder": {"enabled": True, "type": "default"},
        "central": {"enabled": True, "type": "default"},
    }
    if serial is None:
        del profile["sources"]["src_a"]["serial_number"]  # left empty: not in the JSON at all
    if with_parameters:
        profile["parameters"] = {
            "rtt_serial": {"paths": [SERIAL_PATH], "description": "J-Link serial"},
            "speed": {"paths": ["/sources/src_a/speed"]},
        }
    (tmp_path / "rtt.json").write_text(json.dumps(profile))
    (tmp_path / "rtt.left.params.json").write_text(
        json.dumps({"session": "Left board", "params": {"rtt_serial": "51024923"}})
    )


@pytest.fixture
def make_dialog(qapp, qtbot, tmp_path):
    registries = []

    def make(with_parameters=True, node_manager=False, serial="1", **registry_kwargs):
        write_profile(tmp_path, with_parameters, serial)
        registry_kwargs.setdefault("params_files", ["left"])
        reg = Registry(log_dir=tmp_path / "logs", config_path=tmp_path / "rtt.json", **registry_kwargs)
        registries.append(reg)
        gui_context = make_real_gui_context(reg)
        if node_manager:
            gui_context.set_config_manager(ConfigNodeManager(gui_context))
        dlg = ProfileParamsDialog(gui_context)
        qtbot.addWidget(dlg)
        return dlg, reg

    yield make
    for reg in registries:
        reg.stop()


@pytest.fixture
def warnings(monkeypatch):
    shown = []
    monkeypatch.setattr(dialog_module.MessageBox, "warning", lambda parent, title, text: shown.append(text))
    return shown


def rows(dlg):
    return {
        dlg.table.item(r, COL_NAME).text(): {
            "value": dlg.table.item(r, COL_VALUE).text(),
            "profile": dlg.table.item(r, COL_PROFILE).text(),
            "origin": dlg.table.item(r, COL_ORIGIN).text(),
        }
        for r in range(dlg.table.rowCount())
    }


def row_of(dlg, name):
    return next(r for r in range(dlg.table.rowCount()) if dlg.table.item(r, COL_NAME).text() == name)


def set_cell(dlg, name, text):
    dlg.table.item(row_of(dlg, name), COL_VALUE).setText(text)


def apply_and_wait(dlg, qtbot):
    future = dlg.apply()
    assert future is not None
    future.result(timeout=10)
    qtbot.waitUntil(lambda: not dlg._applying, timeout=5000)


class TestShows:
    def test_rows_info_and_buttons(self, make_dialog):
        dlg, _ = make_dialog()
        assert rows(dlg) == {
            "rtt_serial": {"value": "51024923", "profile": "1", "origin": "set 'left'"},
            "speed": {"value": "", "profile": "4000", "origin": "(profile value)"},
        }
        assert "rtt.left.params.json" in dlg.info_label.text()
        assert "session 'Left board'" in dlg.info_label.text()  # not the folder-safe "Left_board"
        assert dlg.session_edit.text() == "Left board"
        assert dlg.btn_save.text() == "Save to 'rtt.left.params.json'"
        assert dlg.btn_save.isEnabled()
        assert not dlg.btn_apply.isEnabled()  # nothing edited yet
        assert dlg.table.item(row_of(dlg, "rtt_serial"), COL_NAME).toolTip() == "J-Link serial"

    def test_only_the_value_column_is_editable(self, make_dialog):
        from qtpy.QtCore import Qt

        dlg, _ = make_dialog()
        r = row_of(dlg, "rtt_serial")
        editable = [bool(dlg.table.item(r, c).flags() & Qt.ItemIsEditable) for c in range(dlg.table.columnCount())]
        assert editable == [False, True, False, False, False]

    def test_profile_without_parameters(self, make_dialog):
        dlg, _ = make_dialog(with_parameters=False, params_files=None)
        assert dlg.table.rowCount() == 0
        assert not dlg.empty_label.isHidden()
        assert dlg.table.isHidden()
        assert not dlg.btn_save_as.isEnabled()
        assert not dlg.btn_save.isEnabled()
        assert "Started without --params" in dlg.info_label.text()


class TestEditing:
    def test_enter_applies_rather_than_clearing(self, make_dialog):
        dlg, _ = make_dialog()
        assert dlg.btn_apply.isDefault()
        assert not dlg.btn_reset.autoDefault()
        assert not dlg.btn_reset.isDefault()

    def test_pending_edit_is_marked_until_reverted(self, make_dialog):
        from blinkview.ui.widgets.profile_params_dialog import PENDING_COLOR

        dlg, _ = make_dialog()
        set_cell(dlg, "speed", "8000")
        r = row_of(dlg, "speed")
        assert dlg.table.item(r, COL_VALUE).foreground().color() == PENDING_COLOR
        assert rows(dlg)["speed"]["origin"] == "(not applied)"

        set_cell(dlg, "speed", "")
        assert rows(dlg)["speed"]["origin"] == "(profile value)"
        assert dlg.table.item(r, COL_VALUE).foreground().color() != PENDING_COLOR

    def test_session_label_falls_back_to_folder_name(self, make_dialog):
        dlg, _ = make_dialog(session_name="bench")
        assert "session 'bench'" in dlg.info_label.text()


class TestApply:
    def test_edit_applies_live_and_records(self, make_dialog, qtbot):
        dlg, reg = make_dialog()
        set_cell(dlg, "speed", "12000")
        set_cell(dlg, "rtt_serial", "99")
        assert dlg.btn_apply.isEnabled()

        apply_and_wait(dlg, qtbot)

        assert reg.config.get_by_path("/sources/src_a/speed") == 12000
        assert reg.config.get_by_path(SERIAL_PATH) == "99"
        assert rows(dlg)["speed"] == {"value": "12000", "profile": "4000", "origin": CHANGED_HERE}
        meta = json.loads((reg.file_manager.session_dir / "metadata.json").read_text())
        assert meta["config"]["params"] == {"rtt_serial": "99", "speed": 12000}
        # The profile itself is untouched.
        profile = json.loads((reg.file_manager.config_dir / "rtt.json").read_text())
        assert profile["sources"]["src_a"]["serial_number"] == "1"

    def test_cleared_value_goes_back_to_profile_value(self, make_dialog, qtbot):
        dlg, reg = make_dialog()
        dlg.table.selectRow(row_of(dlg, "rtt_serial"))
        dlg.reset_selected()
        assert dlg.pending_changes() == {"rtt_serial": None}

        apply_and_wait(dlg, qtbot)
        assert reg.config.get_by_path(SERIAL_PATH) == "1"
        assert rows(dlg)["rtt_serial"]["origin"] == "(profile value)"

    def test_invalid_value_warns_and_changes_nothing(self, make_dialog, warnings):
        dlg, reg = make_dialog()
        set_cell(dlg, "speed", "fast")
        assert dlg.apply() is None
        assert len(warnings) == 1 and "speed" in warnings[0]
        assert reg.config.get_by_path("/sources/src_a/speed") == 4000

    def test_nothing_changed_is_a_no_op(self, make_dialog):
        dlg, _ = make_dialog()
        assert dlg.apply() is None

    def test_live_source_gets_the_new_serial(self, make_dialog, qtbot):
        dlg, reg = make_dialog()
        reg.configure_system()
        source = reg.sources.get("src_a")
        assert source.serial_number == "51024923"

        set_cell(dlg, "rtt_serial", "77")
        apply_and_wait(dlg, qtbot)
        assert reg.sources.get("src_a").serial_number == "77"


class TestUnsetField:
    def test_set_and_clear_serial_the_profile_leaves_empty(self, make_dialog, qtbot, tmp_path):
        """Reported bug: with serial_number empty (absent from the JSON) the value couldn't be
        applied."""
        dlg, reg = make_dialog(serial=None, params_files=None)
        assert rows(dlg)["rtt_serial"] == {"value": "", "profile": "(not set)", "origin": "(profile value)"}

        set_cell(dlg, "rtt_serial", "51024923")
        apply_and_wait(dlg, qtbot)
        assert reg.config.get_by_path(SERIAL_PATH) == "51024923"
        assert rows(dlg)["rtt_serial"]["profile"] == "(not set)"

        set_cell(dlg, "rtt_serial", "")
        apply_and_wait(dlg, qtbot)
        assert "serial_number" not in reg.config.get_by_path("/sources/src_a")
        assert "serial_number" not in json.loads((tmp_path / "rtt.json").read_text())["sources"]["src_a"]


class TestSave:
    def test_save_to_current_set_with_session(self, make_dialog, tmp_path):
        dlg, _ = make_dialog()
        set_cell(dlg, "speed", "8000")  # pending edits are saved too
        dlg.session_edit.setText("Left bench")
        assert dlg.save_to_current_set() is True
        assert json.loads((tmp_path / "rtt.left.params.json").read_text()) == {
            "session": "Left bench",
            "params": {"rtt_serial": "51024923", "speed": 8000},
        }

    def test_save_as_new_set(self, make_dialog, tmp_path, monkeypatch):
        dlg, _ = make_dialog()
        dlg.session_edit.setText("")
        monkeypatch.setattr(dlg, "_ask_set_name", lambda default: ("right", True))
        assert dlg.save_as_new_set() == "right"
        assert json.loads((tmp_path / "rtt.right.params.json").read_text()) == {"rtt_serial": "51024923"}

    def test_save_as_existing_set_asks_before_overwriting(self, make_dialog, tmp_path, monkeypatch):
        dlg, _ = make_dialog()
        before = (tmp_path / "rtt.left.params.json").read_text()
        monkeypatch.setattr(dlg, "_ask_set_name", lambda default: ("left", True))
        monkeypatch.setattr(dlg, "_confirm_overwrite", lambda path: False)
        assert dlg.save_as_new_set() is None
        assert (tmp_path / "rtt.left.params.json").read_text() == before

    def test_save_as_rejects_bad_name_and_cancel(self, make_dialog, monkeypatch, warnings):
        dlg, _ = make_dialog()
        monkeypatch.setattr(dlg, "_ask_set_name", lambda default: ("bad name!", True))
        assert dlg.save_as_new_set() is None
        assert warnings
        monkeypatch.setattr(dlg, "_ask_set_name", lambda default: ("x", False))
        assert dlg.save_as_new_set() is None

    def test_nothing_to_save(self, make_dialog, tmp_path, monkeypatch, warnings):
        dlg, _ = make_dialog(params_files=None)
        monkeypatch.setattr(dlg, "_ask_set_name", lambda default: ("empty", True))
        assert dlg.save_as_new_set() is None
        assert warnings == ["No parameter has a value - nothing to save."]
        assert not (tmp_path / "rtt.empty.params.json").exists()


class TestLaunchCommand:
    def test_started_from_set(self, make_dialog, tmp_path):
        dlg, _ = make_dialog()
        assert dlg.launch_command() == f"blink -c {tmp_path / 'rtt.json'} --params left"

    def test_after_live_change_lists_values(self, make_dialog, qtbot):
        dlg, _ = make_dialog()
        set_cell(dlg, "speed", "12000")
        apply_and_wait(dlg, qtbot)
        assert dlg.launch_command().endswith("--param rtt_serial=51024923 --param speed=12000")

    def test_pending_edit_is_included(self, make_dialog):
        dlg, _ = make_dialog()
        set_cell(dlg, "rtt_serial", "5 5")
        assert dlg.launch_command().endswith('--param "rtt_serial=5 5"')

    def test_copy(self, make_dialog, qapp):
        dlg, _ = make_dialog()
        dlg.copy_launch_command()
        assert qapp.clipboard().text() == dlg.launch_command()


class TestFollowsConfigChanges:
    def test_reload_from_disk_refreshes_visible_dialog(self, make_dialog, qtbot, tmp_path):
        dlg, reg = make_dialog(node_manager=True)
        dlg.show()
        profile = json.loads((tmp_path / "rtt.json").read_text())
        profile["sources"]["src_a"]["speed"] = 1000
        (tmp_path / "rtt.json").write_text(json.dumps(profile))

        reg.config.reload_from_disk()
        qtbot.waitUntil(lambda: rows(dlg)["speed"]["profile"] == "1000", timeout=5000)

    def test_pending_edit_survives_config_change(self, make_dialog, qtbot):
        dlg, reg = make_dialog(node_manager=True)
        dlg.show()
        set_cell(dlg, "speed", "123")
        reg.config.declare_param("rtt_name", "/sources/src_a/name")
        qtbot.wait(50)
        assert rows(dlg)["speed"]["value"] == "123"
        assert "rtt_name" not in rows(dlg)


@pytest.fixture
def menu_registry(tmp_path):
    from tests.fakes.real_registry import make_real_registry

    reg = make_real_registry(tmp_path, "params_menu_test")
    yield reg
    reg.stop()  # after qtbot closed the window (fixture teardown order), like tests/test_main_window.py


def test_main_menu_opens_one_dialog(qapp, qtbot, menu_registry):
    from blinkview.ui.main_window import BlinkMainWindow

    w = BlinkMainWindow(menu_registry)
    qtbot.addWidget(w)
    w.populate_main_menu()
    assert "Profile Parameters..." in [a.text() for a in w.app_menu.actions()]
    first = w.show_profile_params_dialog()
    assert w.show_profile_params_dialog() is first
    assert first.isVisible()
    first.close()

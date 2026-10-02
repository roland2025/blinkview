# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Profile parameters (core/profile_params.py): one profile, several identical devices, each run
filling in its own serial number / port via --params SET or --param NAME=VALUE - without the
value ever being written back into the shared profile file."""

import json
from argparse import ArgumentParser
from pathlib import Path

import pytest

from blinkview.core.config_manager import ConfigManager
from blinkview.core.profile_params import (
    MISSING,
    ProfileParamError,
    add_declaration,
    apply_params,
    coerce_value,
    get_declarations,
    get_pointer,
    list_params_sets,
    load_params_files,
    params_label,
    params_set_path,
    parse_param_args,
)

SERIAL_PATH = "/sources/src_a/serial_number"


def make_profile():
    return {
        "version": "0.2",
        "sources": {
            "src_a": {"type": "jlink_rtt", "name": "rtt", "serial_number": "11111111", "speed": 4000},
        },
        "pipelines": {"pipe_a": {"name": "c3x", "sources_": ["src_a"], "enabled": True}},
        "parameters": {
            "rtt_serial": {"paths": [SERIAL_PATH], "description": "J-Link serial"},
            "speed": {"path": "/sources/src_a/speed"},
        },
    }


# ==========================================
# Parsing / labels
# ==========================================
class TestParseParamArgs:
    def test_splits_on_first_equals_only(self):
        assert parse_param_args(["a=1", "b=x=y"]) == {"a": "1", "b": "x=y"}

    def test_empty_value_is_allowed(self):
        assert parse_param_args(["a="]) == {"a": ""}

    def test_later_duplicate_wins(self):
        assert parse_param_args(["a=1", "a=2"]) == {"a": "2"}

    @pytest.mark.parametrize("bad", ["novalue", "=1", " =1"])
    def test_rejects_malformed(self, bad):
        with pytest.raises(ProfileParamError):
            parse_param_args([bad])

    def test_none_is_empty(self):
        assert parse_param_args(None) == {}


class TestParamsLabel:
    def test_named_sets_joined(self):
        assert params_label(["left", "bench"]) == "left+bench"

    def test_file_path_uses_set_part_of_stem(self):
        assert params_label(["./x/rtt.params.left.json"]) == "left"
        assert params_label(["C:/cfg/board7.json"]) == "board7"

    def test_falls_back_to_cli_values(self):
        assert params_label(None, {"rtt_serial": "123"}) == "rtt_serial_123"

    def test_nothing_given(self):
        assert params_label(None, {}) is None


# ==========================================
# Declarations / pointers
# ==========================================
class TestDeclarations:
    def test_path_shorthand_is_normalized(self):
        decl = get_declarations(make_profile())
        assert decl["speed"]["paths"] == ["/sources/src_a/speed"]
        assert decl["rtt_serial"]["description"] == "J-Link serial"

    def test_no_parameters_key(self):
        assert get_declarations({"sources": {}}) == {}

    @pytest.mark.parametrize("bad", [{"x": {}}, {"x": {"paths": ["no_slash"]}}, {"x": "str"}, ["list"]])
    def test_bad_declarations_raise(self, bad):
        with pytest.raises(ProfileParamError):
            get_declarations({"parameters": bad})

    def test_add_declaration_requires_existing_parent(self):
        cfg = make_profile()
        with pytest.raises(ProfileParamError, match="does not exist"):
            add_declaration(cfg, "port", "/sources/src_missing/port")

    def test_add_declaration_allows_unset_optional_field(self):
        cfg = make_profile()  # src_a has no "port" key - an optional field left unset
        add_declaration(cfg, "port", "/sources/src_a/port")
        assert cfg["parameters"]["port"] == {"paths": ["/sources/src_a/port"]}

    def test_add_declaration_appends_paths_and_normalizes_shorthand(self):
        cfg = make_profile()
        cfg["sources"]["src_b"] = {"speed": 1}
        add_declaration(cfg, "speed", "/sources/src_b/speed", "SWD speed")
        assert cfg["parameters"]["speed"] == {
            "paths": ["/sources/src_a/speed", "/sources/src_b/speed"],
            "description": "SWD speed",
        }

    def test_add_declaration_rejects_bad_name(self):
        with pytest.raises(ProfileParamError, match="Invalid parameter name"):
            add_declaration(make_profile(), "rtt-serial", SERIAL_PATH)

    def test_pointer_into_lists_and_escapes(self):
        data = {"a": [{"x/y": 1}]}
        assert get_pointer(data, "/a/0/x~1y") == 1
        assert get_pointer(data, "/a/5") is MISSING
        assert get_pointer(data, "/a/zz") is MISSING


# ==========================================
# Coercion
# ==========================================
class TestCoerce:
    def test_string_field_accepts_number_from_json_file(self):
        assert coerce_value(51024923, "11111111") == "51024923"

    def test_int_field(self):
        assert coerce_value("4000", 1) == 4000
        assert coerce_value("0x10", 1) == 16
        assert coerce_value(3.0, 1) == 3

    def test_int_field_rejects_garbage_with_param_name(self):
        with pytest.raises(ProfileParamError, match="speed"):
            coerce_value("fast", 1, "speed", "/x")

    def test_int_field_rejects_bool_and_fraction(self):
        with pytest.raises(ProfileParamError):
            coerce_value(True, 1)
        with pytest.raises(ProfileParamError):
            coerce_value(1.5, 1)

    @pytest.mark.parametrize("text,expected", [("true", True), ("0", False), ("YES", True), (False, False)])
    def test_bool_field(self, text, expected):
        assert coerce_value(text, True) is expected

    def test_bool_field_rejects_garbage(self):
        with pytest.raises(ProfileParamError):
            coerce_value("maybe", False)

    def test_float_field(self):
        assert coerce_value("1.5", 0.0) == 1.5

    def test_list_field_parses_json(self):
        assert coerce_value("[1, 2]", [0]) == [1, 2]
        with pytest.raises(ProfileParamError):
            coerce_value('{"a": 1}', [0])

    def test_none_field_keeps_value(self):
        assert coerce_value("COM7", None) == "COM7"


# ==========================================
# apply_params
# ==========================================
class TestApplyParams:
    def test_applies_and_reports_bindings(self):
        cfg = make_profile()
        bindings, effective = apply_params(cfg, {"rtt_serial": "51024923", "speed": "12000"})
        assert cfg["sources"]["src_a"]["serial_number"] == "51024923"
        assert cfg["sources"]["src_a"]["speed"] == 12000
        assert bindings[SERIAL_PATH] == ("rtt_serial", "11111111")
        assert effective == {"rtt_serial": "51024923", "speed": 12000}

    def test_multiple_paths_get_same_value(self):
        cfg = make_profile()
        cfg["sources"]["src_b"] = {"serial_number": "22222222"}
        cfg["parameters"]["rtt_serial"]["paths"].append("/sources/src_b/serial_number")
        apply_params(cfg, {"rtt_serial": "9"})
        assert cfg["sources"]["src_a"]["serial_number"] == "9"
        assert cfg["sources"]["src_b"]["serial_number"] == "9"

    def test_unknown_parameter_lists_declared(self):
        with pytest.raises(ProfileParamError, match="rtt_serial, speed"):
            apply_params(make_profile(), {"rtt_serail": "1"})

    def test_unknown_parameter_without_declarations_hints_add_param(self):
        with pytest.raises(ProfileParamError, match="--add-param"):
            apply_params({"sources": {}}, {"x": "1"})

    def test_missing_target_parent(self):
        cfg = make_profile()
        del cfg["sources"]["src_a"]
        with pytest.raises(ProfileParamError, match="does not exist"):
            apply_params(cfg, {"rtt_serial": "1"})

    def test_failure_leaves_data_untouched(self):
        cfg = make_profile()
        before = json.dumps(cfg, sort_keys=True)
        with pytest.raises(ProfileParamError):
            apply_params(cfg, {"rtt_serial": "1", "speed": "not-a-number"})
        assert json.dumps(cfg, sort_keys=True) == before

    def test_nothing_given(self):
        assert apply_params(make_profile(), {}) == ({}, {})


# ==========================================
# Parameter files
# ==========================================
class TestParamsFiles:
    def test_named_set_resolves_next_to_profile(self, tmp_path):
        params_set_path(tmp_path, "rtt", "left").write_text(json.dumps({"rtt_serial": "1"}))
        loaded = load_params_files(["left"], tmp_path, "rtt")
        values, paths = loaded.values, loaded.paths
        assert values == {"rtt_serial": "1"}
        assert paths == [tmp_path / "rtt.params.left.json"]

    def test_explicit_path_relative_to_cwd(self, tmp_path, monkeypatch):
        (tmp_path / "bench").mkdir()
        (tmp_path / "bench" / "left.json").write_text(json.dumps({"rtt_serial": 5}))
        monkeypatch.chdir(tmp_path)
        loaded = load_params_files(["bench/left.json"], tmp_path / "elsewhere", "rtt")
        values, paths = loaded.values, loaded.paths
        assert values == {"rtt_serial": 5}
        assert paths == [(tmp_path / "bench" / "left.json").resolve()]

    def test_later_files_win(self, tmp_path):
        params_set_path(tmp_path, "rtt", "a").write_text(json.dumps({"x": 1, "y": 1}))
        params_set_path(tmp_path, "rtt", "b").write_text(json.dumps({"y": 2}))
        values = load_params_files(["a", "b"], tmp_path, "rtt").values
        assert values == {"x": 1, "y": 2}

    def test_missing_named_set_lists_available(self, tmp_path):
        params_set_path(tmp_path, "rtt", "left").write_text("{}")
        with pytest.raises(ProfileParamError, match="Available sets: left"):
            load_params_files(["right"], tmp_path, "rtt")

    def test_missing_explicit_file(self, tmp_path):
        with pytest.raises(ProfileParamError, match="not found"):
            load_params_files([str(tmp_path / "nope.json")], tmp_path, "rtt")

    @pytest.mark.parametrize("content", ["[1]", "{bad json", '{"x": {"nested": 1}}'])
    def test_bad_content(self, tmp_path, content):
        params_set_path(tmp_path, "rtt", "left").write_text(content)
        with pytest.raises(ProfileParamError, match="rtt.params.left.json"):
            load_params_files(["left"], tmp_path, "rtt")

    def test_list_sets(self, tmp_path):
        for name in ("right", "left"):
            params_set_path(tmp_path, "rtt", name).write_text("{}")
        (tmp_path / "rtt.json").write_text("{}")
        (tmp_path / "other.params.x.json").write_text("{}")
        assert list_params_sets(tmp_path, "rtt") == ["left", "right"]


# ==========================================
# ConfigManager: values never reach the profile file
# ==========================================
@pytest.fixture
def profile_file(tmp_path):
    path = tmp_path / "rtt.json"
    path.write_text(json.dumps(make_profile()))
    return path


def read(path: Path) -> dict:
    return json.loads(path.read_text())


class TestConfigManagerParams:
    def test_params_apply_in_memory_only(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "autosave.json", params={"rtt_serial": "51024923"})
        assert cm.get_by_path(SERIAL_PATH) == "51024923"
        assert cm.param_values == {"rtt_serial": "51024923"}
        assert cm.get_param_binding(SERIAL_PATH) == "rtt_serial"
        assert cm.get_param_binding("/sources/src_a/speed") is None
        assert read(profile_file)["sources"]["src_a"]["serial_number"] == "11111111"

    def test_unrelated_edit_keeps_profile_serial(self, profile_file, tmp_path):
        autosave = tmp_path / "autosave.json"
        cm = ConfigManager(profile_file, autosave, params={"rtt_serial": "51024923"})
        cm.apply_patch("/pipelines/pipe_a", [{"op": "replace", "path": "/name", "value": "c3x_new"}])

        on_disk = read(profile_file)
        assert on_disk["pipelines"]["pipe_a"]["name"] == "c3x_new"
        assert on_disk["sources"]["src_a"]["serial_number"] == "11111111"
        # Session snapshots carry the effective value, so a replay knows the real serial.
        assert read(autosave)["sources"]["src_a"]["serial_number"] == "51024923"

    def test_edit_replacing_whole_bound_parent_keeps_profile_serial(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "51024923"})
        new_source = dict(cm.get_by_path("/sources/src_a"), speed=8000)
        cm.apply_patch("/sources", [{"op": "replace", "path": "/src_a", "value": new_source}])

        on_disk = read(profile_file)["sources"]["src_a"]
        assert on_disk["speed"] == 8000
        assert on_disk["serial_number"] == "11111111"
        assert cm.get_by_path(SERIAL_PATH) == "51024923"

    def test_full_save_to_profile_swaps_values_back(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "51024923"})
        cm.save_full_config()
        assert read(profile_file)["sources"]["src_a"]["serial_number"] == "11111111"
        cm.save_full_config(tmp_path / "session.start.json")
        assert read(tmp_path / "session.start.json")["sources"]["src_a"]["serial_number"] == "51024923"

    def test_bad_param_raises(self, profile_file, tmp_path):
        with pytest.raises(ProfileParamError):
            ConfigManager(profile_file, tmp_path / "a.json", params={"nope": "1"})

    def test_two_instances_keep_each_others_edits(self, profile_file, tmp_path):
        """Two BlinkView processes on one profile (one per board): each must persist its own edit
        without reverting the other's - the old whole-dict dump did exactly that."""
        left = ConfigManager(profile_file, tmp_path / "l.json", params={"rtt_serial": "1"})
        right = ConfigManager(profile_file, tmp_path / "r.json", params={"rtt_serial": "2"})

        left.apply_patch("/pipelines/pipe_a", [{"op": "add", "path": "/kv_rule", "value": "temp="}])
        right.apply_patch("/sources/src_a", [{"op": "replace", "path": "/speed", "value": 9000}])

        on_disk = read(profile_file)
        assert on_disk["pipelines"]["pipe_a"]["kv_rule"] == "temp="
        assert on_disk["sources"]["src_a"]["speed"] == 9000
        assert on_disk["sources"]["src_a"]["serial_number"] == "11111111"

    def test_patch_that_no_longer_applies_on_disk_falls_back_to_full_write(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "9"})
        profile_file.write_text(json.dumps({"sources": {}}))  # someone else removed src_a
        cm.apply_patch("/sources/src_a", [{"op": "replace", "path": "/speed", "value": 1}])
        on_disk = read(profile_file)
        assert on_disk["sources"]["src_a"]["speed"] == 1
        assert on_disk["sources"]["src_a"]["serial_number"] == "11111111"

    def test_without_params_patch_still_saves(self, tmp_path):
        path = tmp_path / "new.json"
        cm = ConfigManager(path, tmp_path / "a.json", default_config={"sources": {}})
        cm.apply_patch("/sources", [{"op": "add", "path": "/x", "value": {"a": 1}}])
        assert read(path) == {"sources": {"x": {"a": 1}}}


# ==========================================
# `blink switch <profile> --add-param/--save-params/--show-params`
# ==========================================
@pytest.fixture
def workspace(tmp_path, monkeypatch):
    from blinkview.utils import project_settings

    ws = tmp_path / ".blinkview"
    (ws / "profiles" / "rtt").mkdir(parents=True)
    profile = make_profile()
    del profile["parameters"]
    (ws / "profiles" / "rtt" / "rtt.json").write_text(json.dumps(profile))
    monkeypatch.setattr(project_settings, "get_workspace_dir", lambda: ws)
    return ws / "profiles" / "rtt"


def run_switch(*argv):
    from blinkview.utils.project_settings import handle_profile_args, setup_project_parser

    parser = ArgumentParser()
    setup_project_parser(parser)
    handle_profile_args(parser.parse_args(list(argv)))


class TestSwitchCommand:
    def test_add_save_show_roundtrip(self, workspace, capsys):
        run_switch("rtt", "--add-param", "rtt_serial", SERIAL_PATH, "--description", "J-Link serial")
        assert read(workspace / "rtt.json")["parameters"] == {
            "rtt_serial": {"paths": [SERIAL_PATH], "description": "J-Link serial"}
        }

        run_switch("rtt", "--save-params", "left", "--param", "rtt_serial=51024923")
        assert read(workspace / "rtt.params.left.json") == {"rtt_serial": "51024923"}

        run_switch("rtt", "--save-params", "template")
        assert read(workspace / "rtt.params.template.json") == {"rtt_serial": "11111111"}

        capsys.readouterr()
        run_switch("rtt", "--show-params")
        out = capsys.readouterr().out
        assert 'rtt_serial = "11111111"' in out
        assert 'left: rtt_serial="51024923"' in out
        assert 'template: rtt_serial="11111111"' in out

    def test_add_param_path_owned_by_other_parameter_exits(self, workspace, capsys):
        run_switch("rtt", "--add-param", "rtt_serial", SERIAL_PATH)
        with pytest.raises(SystemExit):
            run_switch("rtt", "--add-param", "serial2", SERIAL_PATH)
        assert "already belongs to parameter 'rtt_serial'" in capsys.readouterr().out

    def test_add_param_bad_path_exits(self, workspace, capsys):
        with pytest.raises(SystemExit):
            run_switch("rtt", "--add-param", "port", "/sources/src_missing/port")
        assert "does not exist" in capsys.readouterr().out

    def test_add_param_on_unset_field_warns_about_spelling(self, workspace, capsys):
        run_switch("rtt", "--add-param", "port", "/sources/src_a/port")
        out = capsys.readouterr().out
        assert "Declared parameter 'port'" in out
        assert "not set in the profile yet" in out

    def test_save_params_unknown_name_exits(self, workspace, capsys):
        run_switch("rtt", "--add-param", "rtt_serial", SERIAL_PATH)
        with pytest.raises(SystemExit):
            run_switch("rtt", "--save-params", "left", "--param", "nope=1")
        assert "Unknown parameter" in capsys.readouterr().out

    def test_save_params_without_declarations_exits(self, workspace, capsys):
        with pytest.raises(SystemExit):
            run_switch("rtt", "--save-params", "left")
        assert "--add-param" in capsys.readouterr().out


# ==========================================
# Registry / FileManager wiring (real Registry, isolated config_path under tmp_path)
# ==========================================
def make_registry(tmp_path, **kwargs):
    from blinkview.core.registry import Registry

    return Registry(log_dir=tmp_path / "logs", config_path=tmp_path / "rtt.json", **kwargs)


def session_dirs(tmp_path):
    return [d for d in (tmp_path / "logs").rglob("*") if (d / "metadata.json").is_file()]


class TestRegistryParams:
    def test_params_set_and_cli_override_reach_config_and_metadata(self, profile_file, tmp_path):
        params_set_path(tmp_path, "rtt", "left").write_text(json.dumps({"rtt_serial": 51024923, "speed": 8000}))
        reg = make_registry(tmp_path, params_files=["left"], param_args=["speed=12000"])
        try:
            assert reg.config.get_by_path(SERIAL_PATH) == "51024923"
            assert reg.config.get_by_path("/sources/src_a/speed") == 12000

            fm = reg.file_manager
            assert fm.params_label == "left"
            assert fm.session_dir.name.endswith("_left")  # default session name = set name
            meta = json.loads((fm.session_dir / "metadata.json").read_text())
            assert meta["config"]["params"] == {"rtt_serial": "51024923", "speed": 12000}
            assert meta["config"]["params_files"] == [str(tmp_path / "rtt.params.left.json")]

            start = json.loads(fm.get_session_path(suffix="start").read_text())
            assert start["sources"]["src_a"]["serial_number"] == "51024923"
            assert read(profile_file)["sources"]["src_a"]["serial_number"] == "11111111"
        finally:
            reg.stop()

    def test_explicit_session_name_wins_over_label(self, profile_file, tmp_path):
        reg = make_registry(tmp_path, session_name="bench", param_args=["rtt_serial=1"])
        try:
            assert reg.file_manager.session_dir.name.endswith("_bench")
            assert reg.file_manager.params_label == "rtt_serial_1"
        finally:
            reg.stop()

    def test_bad_param_raises_and_leaves_no_session_folder(self, profile_file, tmp_path):
        with pytest.raises(ProfileParamError, match="Unknown parameter"):
            make_registry(tmp_path, param_args=["nope=1"])
        assert session_dirs(tmp_path) == []

    def test_missing_set_raises_and_leaves_no_session_folder(self, profile_file, tmp_path):
        with pytest.raises(ProfileParamError, match="not found"):
            make_registry(tmp_path, params_files=["right"])
        assert session_dirs(tmp_path) == []

    def test_no_params_metadata_is_empty(self, profile_file, tmp_path):
        reg = make_registry(tmp_path)
        try:
            meta = json.loads((reg.file_manager.session_dir / "metadata.json").read_text())
            assert meta["config"]["params"] == {}
            assert reg.config.get_by_path(SERIAL_PATH) == "11111111"
        finally:
            reg.stop()

    def test_params_survive_rotation_metadata(self, profile_file, tmp_path):
        reg = make_registry(tmp_path, param_args=["rtt_serial=7"])
        try:
            reg.file_manager.rotate()
            meta = json.loads((reg.file_manager.session_dir / "metadata.json").read_text())
            assert meta["config"]["params"] == {"rtt_serial": "7"}
        finally:
            reg.stop()


class TestUniqueStartupSessionDir:
    def test_two_instances_same_second_get_different_folders(self, profile_file, tmp_path, monkeypatch):
        """Two boards started together with one profile must not share a session folder."""
        from blinkview.storage import file_manager as fm_module

        class FrozenDatetime(fm_module.datetime):
            @classmethod
            def now(cls, tz=None):
                return fm_module.datetime(2026, 10, 1, 12, 0, 0, tzinfo=tz)

        monkeypatch.setattr(fm_module, "datetime", FrozenDatetime)
        a = make_registry(tmp_path, session_name="dev")
        b = make_registry(tmp_path, session_name="dev")
        try:
            assert a.file_manager.session_dir != b.file_manager.session_dir
            assert b.file_manager.session_dir.name == a.file_manager.session_dir.name + "_2"
        finally:
            a.stop()
            b.stop()


class TestGuiArgv:
    @pytest.mark.parametrize("command", [[], ["gui"], ["replay", "--last"]])
    def test_params_flags_parsed_for_gui_and_replay(self, command, monkeypatch):
        import sys

        from blinkview import __main__ as main_module

        calls = []
        monkeypatch.setattr(main_module, "run_gui", lambda args: calls.append(args))
        monkeypatch.setattr(main_module, "run_replay", lambda args: calls.append(args))
        argv = ["blink", *command, "-p", "rtt", "--params", "left", "--params", "x.json", "--param", "a=1"]
        monkeypatch.setattr(sys, "argv", argv)

        main_module.main()

        assert calls[0].params == ["left", "x.json"]
        assert calls[0].param == ["a=1"]

    def test_window_title_gets_params_label(self, monkeypatch):
        from blinkview.ui.utils import window_title

        monkeypatch.setattr(window_title, "_prefix", "")  # restored after the test (module global)
        window_title.set_title_prefix("proj", "rtt", "left")
        assert window_title.titled("X") == "proj / rtt [left] - X"


# ==========================================
# Live reload: another instance (or an editor) saved the profile
# ==========================================
class Subscriber:
    def __init__(self):
        self.calls = []

    def apply_config(self, config):
        self.calls.append(config)


def write_external(path: Path, mutate):
    """What another BlinkView instance / a text editor does: rewrite the file."""
    data = read(path)
    mutate(data)
    path.write_text(json.dumps(data, indent=2))


class TestExternalChanges:
    def test_nothing_changed(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        assert cm.poll_external_change() is False

    def test_own_edits_are_not_external(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "9"})
        cm.apply_patch("/sources/src_a", [{"op": "replace", "path": "/speed", "value": 1}])
        cm.declare_param("rtt_name", "/sources/src_a/name")
        assert cm.poll_external_change() is False

    def test_external_write_is_reported_until_reloaded(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        write_external(profile_file, lambda d: d["pipelines"]["pipe_a"].update(name="c3x_ext"))
        assert cm.poll_external_change() is True
        assert cm.poll_external_change() is True  # still pending
        assert cm.reload_from_disk() is True
        assert cm.poll_external_change() is False

    def test_rewrite_with_identical_content_is_not_a_change(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        write_external(profile_file, lambda d: None)  # different formatting, same data
        assert cm.poll_external_change() is False

    def test_unreadable_mid_write_is_retried_later(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        good = profile_file.read_text()
        profile_file.write_text('{"truncated": ')
        assert cm.poll_external_change() is False
        profile_file.write_text(good.replace("c3x", "c3x_ext"))
        assert cm.poll_external_change() is True

    def test_reload_applies_changes_and_notifies(self, profile_file, tmp_path):
        autosave = tmp_path / "a.json"
        cm = ConfigManager(profile_file, autosave)
        pipelines, sources = Subscriber(), Subscriber()
        cm.subscribe("/pipelines", pipelines)
        cm.subscribe("/sources", sources)
        broadcasts = []
        cm.config_changed_cb = lambda path, config, schema: broadcasts.append(path)

        write_external(profile_file, lambda d: d["pipelines"]["pipe_a"].update(kv_rule="temp="))
        assert cm.reload_from_disk() is True

        assert cm.get_by_path("/pipelines/pipe_a/kv_rule") == "temp="
        assert pipelines.calls and pipelines.calls[-1]["pipe_a"]["kv_rule"] == "temp="
        assert sources.calls == []  # untouched subtree
        assert broadcasts == ["/"]  # every open config node re-fetches
        assert read(autosave)["pipelines"]["pipe_a"]["kv_rule"] == "temp="

    def test_reload_without_changes_is_a_no_op(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        sub = Subscriber()
        cm.subscribe("/", sub)
        assert cm.reload_from_disk() is False
        assert sub.calls == []

    def test_reload_keeps_this_runs_param_and_tracks_new_profile_value(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "51024923"})

        def change(d):
            d["sources"]["src_a"]["serial_number"] = "22222222"  # someone changed the default
            d["sources"]["src_a"]["speed"] = 8000

        write_external(profile_file, change)
        cm.reload_from_disk()

        assert cm.get_by_path(SERIAL_PATH) == "51024923"
        assert cm.get_by_path("/sources/src_a/speed") == 8000
        # A later full save must keep the *new* profile default, not resurrect the old one.
        cm.save_full_config()
        assert read(profile_file)["sources"]["src_a"]["serial_number"] == "22222222"

    def test_reload_drops_binding_whose_path_disappeared(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "1"})
        write_external(profile_file, lambda d: d["sources"].pop("src_a"))
        cm.reload_from_disk()
        assert cm.get_param_binding(SERIAL_PATH) is None
        assert "src_a" not in cm.get_by_path("/sources")

    def test_external_change_merged_by_own_edit_is_still_reported(self, profile_file, tmp_path):
        """Our edit is saved on top of the newer file, so afterwards the file equals what we
        wrote - the external change must not get lost from the reload offer."""
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        write_external(profile_file, lambda d: d["pipelines"]["pipe_a"].update(kv_rule="temp="))
        cm.apply_patch("/sources/src_a", [{"op": "replace", "path": "/speed", "value": 1}])

        assert read(profile_file)["pipelines"]["pipe_a"]["kv_rule"] == "temp="
        assert cm.poll_external_change() is True
        cm.reload_from_disk()
        assert cm.get_by_path("/pipelines/pipe_a/kv_rule") == "temp="
        assert cm.get_by_path("/sources/src_a/speed") == 1

    def test_two_instances_end_to_end(self, profile_file, tmp_path):
        """Board A adds a KV rule; board B sees the offer, reloads, keeps its own serial."""
        a = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "1"})
        b = ConfigManager(profile_file, tmp_path / "b.json", params={"rtt_serial": "2"})

        a.apply_patch("/pipelines/pipe_a", [{"op": "add", "path": "/kv_rule", "value": "temp="}])

        assert a.poll_external_change() is False
        assert b.poll_external_change() is True
        b.reload_from_disk()
        assert b.get_by_path("/pipelines/pipe_a/kv_rule") == "temp="
        assert b.get_by_path(SERIAL_PATH) == "2"
        assert read(profile_file)["sources"]["src_a"]["serial_number"] == "11111111"


# ==========================================
# Declaring parameters from the running app (config editor)
# ==========================================
class TestDeclareFromApp:
    def test_declare_saves_and_is_queryable(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        cm.declare_param("rtt_name", "/sources/src_a/name")
        assert cm.get_param_declaration("/sources/src_a/name") == "rtt_name"
        assert cm.get_param_declaration("/sources/src_a/speed") == "speed"  # shorthand form too
        assert read(profile_file)["parameters"]["rtt_name"] == {"paths": ["/sources/src_a/name"]}

    def test_declare_bad_input_raises_and_saves_nothing(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        before = profile_file.read_text()
        with pytest.raises(ProfileParamError):
            cm.declare_param("port", "/sources/src_missing/port")
        with pytest.raises(ProfileParamError):
            cm.declare_param("bad name", "/sources/src_a/name")
        with pytest.raises(ProfileParamError, match="already belongs to parameter 'speed'"):
            cm.declare_param("rtt_speed", "/sources/src_a/speed")
        assert profile_file.read_text() == before

    def test_declare_on_profile_without_parameters_key(self, tmp_path):
        path = tmp_path / "p.json"
        path.write_text(json.dumps({"sources": {"s": {"port": "COM1"}}}))
        cm = ConfigManager(path, tmp_path / "a.json")
        cm.declare_param("com_port", "/sources/s/port")
        assert read(path)["parameters"] == {"com_port": {"paths": ["/sources/s/port"]}}

    def test_declare_while_running_with_params_keeps_profile_values(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "9"})
        cm.declare_param("rtt_name", "/sources/src_a/name")
        assert read(profile_file)["sources"]["src_a"]["serial_number"] == "11111111"

    def test_remove_keeps_other_paths_then_drops_parameter(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        cm.declare_param("rtt_serial", "/sources/src_a/name")  # second path for the same param
        cm.remove_param(SERIAL_PATH)
        assert read(profile_file)["parameters"]["rtt_serial"] == {
            "paths": ["/sources/src_a/name"],
            "description": "J-Link serial",
        }
        cm.remove_param("/sources/src_a/name")
        assert "rtt_serial" not in read(profile_file)["parameters"]
        assert cm.get_param_declaration(SERIAL_PATH) is None

    def test_get_declaration_unknown_and_broken(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        assert cm.get_param_declaration("/sources/src_a/name") is None
        cm.get_data()["parameters"] = "broken"
        assert cm.get_param_declaration(SERIAL_PATH) is None


class TestAtomicJsonDump:
    def test_no_temp_files_left_and_names_are_unique(self, tmp_path, monkeypatch):
        from blinkview.utils import atomic_json_dump as module

        seen = []
        real_replace = Path.replace

        def spy(self, target):
            seen.append(self.name)
            return real_replace(self, target)

        monkeypatch.setattr(Path, "replace", spy)
        module.atomic_json_dump({"a": 1}, tmp_path / "p.json")
        module.atomic_json_dump({"a": 2}, tmp_path / "p.json")

        assert len(set(seen)) == 2
        assert all(name.startswith(".p.json.") and name.endswith(".tmp") for name in seen)
        assert [f.name for f in tmp_path.iterdir()] == ["p.json"]
        assert read(tmp_path / "p.json") == {"a": 2}

    def test_retries_replace_while_target_is_briefly_locked(self, tmp_path, monkeypatch):
        from blinkview.utils import atomic_json_dump as module

        monkeypatch.setattr(module, "REPLACE_RETRY_DELAY_S", 0)
        real_replace = Path.replace
        attempts = []

        def flaky(self, target):
            attempts.append(1)
            if len(attempts) < 3:
                raise PermissionError("in use")
            return real_replace(self, target)

        monkeypatch.setattr(Path, "replace", flaky)
        module.atomic_json_dump({"a": 1}, tmp_path / "p.json")
        assert len(attempts) == 3
        assert read(tmp_path / "p.json") == {"a": 1}

    def test_gives_up_and_cleans_temp_after_retries(self, tmp_path, monkeypatch):
        from blinkview.utils import atomic_json_dump as module

        monkeypatch.setattr(module, "REPLACE_RETRY_DELAY_S", 0)

        def locked(self, target):
            raise PermissionError("in use")

        monkeypatch.setattr(Path, "replace", locked)
        with pytest.raises(PermissionError):
            module.atomic_json_dump({"a": 1}, tmp_path / "p.json")
        assert list(tmp_path.iterdir()) == []


# ==========================================
# Params file: optional default session name
# ==========================================
class TestParamsFileSession:
    def test_flat_file_has_no_session(self, tmp_path):
        from blinkview.core.profile_params import read_params_file

        path = tmp_path / "f.json"
        path.write_text(json.dumps({"rtt_serial": "1"}))
        assert read_params_file(path) == ({"rtt_serial": "1"}, None)

    def test_structured_file(self, tmp_path):
        from blinkview.core.profile_params import read_params_file

        path = tmp_path / "f.json"
        path.write_text(json.dumps({"session": " Left board ", "params": {"rtt_serial": 5}}))
        assert read_params_file(path) == ({"rtt_serial": 5}, "Left board")

    def test_structured_without_session(self, tmp_path):
        from blinkview.core.profile_params import read_params_file

        path = tmp_path / "f.json"
        path.write_text(json.dumps({"params": {"rtt_serial": 5}}))
        assert read_params_file(path) == ({"rtt_serial": 5}, None)

    @pytest.mark.parametrize(
        "content,match",
        [
            ({"session": "x", "params": {}, "extra": 1}, "Unknown key"),
            ({"session": 5, "params": {}}, "non-empty string"),
            ({"session": "  ", "params": {}}, "non-empty string"),
            ({"params": {"a": {"nested": 1}}}, "plain value"),
        ],
    )
    def test_structured_errors(self, tmp_path, content, match):
        from blinkview.core.profile_params import read_params_file

        path = tmp_path / "f.json"
        path.write_text(json.dumps(content))
        with pytest.raises(ProfileParamError, match=match):
            read_params_file(path)

    def test_later_file_session_wins_and_missing_one_keeps_earlier(self, tmp_path):
        params_set_path(tmp_path, "rtt", "a").write_text(json.dumps({"session": "A", "params": {"x": 1}}))
        params_set_path(tmp_path, "rtt", "b").write_text(json.dumps({"x": 2}))
        params_set_path(tmp_path, "rtt", "c").write_text(json.dumps({"session": "C", "params": {}}))

        assert load_params_files(["a", "b"], tmp_path, "rtt").session_name == "A"
        loaded = load_params_files(["a", "b", "c"], tmp_path, "rtt")
        assert loaded.session_name == "C"
        assert loaded.values == {"x": 2}

    def test_write_flat_and_structured(self, tmp_path):
        from blinkview.core.profile_params import read_params_file, write_params_file

        write_params_file(tmp_path / "flat.json", {"a": "1"})
        write_params_file(tmp_path / "named.json", {"a": "1"}, "Left board")
        assert read(tmp_path / "flat.json") == {"a": "1"}
        assert read(tmp_path / "named.json") == {"session": "Left board", "params": {"a": "1"}}
        assert read_params_file(tmp_path / "named.json") == ({"a": "1"}, "Left board")

    def test_switch_save_params_with_session_and_show(self, workspace, capsys):
        run_switch("rtt", "--add-param", "rtt_serial", SERIAL_PATH)
        run_switch("rtt", "--save-params", "left", "--param", "rtt_serial=7", "--session", "Left board")
        assert read(workspace / "rtt.params.left.json") == {"session": "Left board", "params": {"rtt_serial": "7"}}

        capsys.readouterr()
        run_switch("rtt", "--show-params")
        assert "left: session 'Left board'; rtt_serial=\"7\"" in capsys.readouterr().out


class TestRegistrySessionFromParamsFile:
    def test_params_file_session_names_the_session(self, profile_file, tmp_path):
        params_set_path(tmp_path, "rtt", "left").write_text(
            json.dumps({"session": "Left board", "params": {"rtt_serial": "5"}})
        )
        reg = make_registry(tmp_path, params_files=["left"])
        try:
            fm = reg.file_manager
            assert fm.session_dir.name.endswith("_Left_board")
            assert fm.params_label == "left"  # window title / layout file still use the set name
            assert reg.config.get_by_path(SERIAL_PATH) == "5"
            meta = json.loads((fm.session_dir / "metadata.json").read_text())
            assert meta["project"]["display_name"] == "Left_board"
            assert meta["config"]["params"] == {"rtt_serial": "5"}
            # The config autosave/start snapshots follow the renamed session.
            assert fm.get_session_path(suffix="start").parent == fm.session_dir
            assert fm.get_session_path(suffix="start").exists()
            assert session_dirs(tmp_path) == [fm.session_dir]  # the first folder is gone
        finally:
            reg.stop()

    def test_explicit_session_name_wins_over_params_file(self, profile_file, tmp_path):
        params_set_path(tmp_path, "rtt", "left").write_text(
            json.dumps({"session": "Left board", "params": {"rtt_serial": "5"}})
        )
        reg = make_registry(tmp_path, session_name="bench", params_files=["left"])
        try:
            assert reg.file_manager.session_dir.name.endswith("_bench")
        finally:
            reg.stop()


class TestRenameNewSession:
    def test_refused_once_something_else_was_written(self, profile_file, tmp_path):
        reg = make_registry(tmp_path, session_name="first")
        try:
            fm = reg.file_manager
            (fm.session_dir / "unified.log").write_text("x")
            old = fm.session_dir
            assert fm.rename_new_session("second") is False
            assert fm.session_dir == old and old.exists()
        finally:
            reg.stop()

    def test_same_name_is_a_no_op(self, profile_file, tmp_path):
        reg = make_registry(tmp_path, session_name="first")
        try:
            old = reg.file_manager.session_dir
            assert reg.file_manager.rename_new_session("first") is True
            assert reg.file_manager.session_dir == old
        finally:
            reg.stop()


# ==========================================
# Window layout (gui_state) per parameter set
# ==========================================
class TestGuiStatePerParamsSet:
    @pytest.fixture
    def fm(self, profile_file, tmp_path):
        reg = make_registry(tmp_path, param_args=["rtt_serial=1"])
        yield reg.file_manager
        reg.stop()

    def test_without_params_uses_shared_file(self, profile_file, tmp_path):
        reg = make_registry(tmp_path)
        try:
            fm = reg.file_manager
            assert fm.get_gui_state_path() == tmp_path / "rtt.gui_state.json"
            assert fm.get_gui_state_path(for_load=True) == tmp_path / "rtt.gui_state.json"
        finally:
            reg.stop()

    def test_own_file_missing_loads_shared_saves_own(self, fm, tmp_path):
        fm.params_label = "left"
        assert fm.get_gui_state_path(for_load=True) == tmp_path / "rtt.gui_state.json"
        assert fm.get_gui_state_path() == tmp_path / "rtt.gui_state.left.json"

    def test_own_file_present_is_loaded(self, fm, tmp_path):
        fm.params_label = "left"
        (tmp_path / "rtt.gui_state.left.json").write_text("{}")
        assert fm.get_gui_state_path(for_load=True) == tmp_path / "rtt.gui_state.left.json"

    def test_label_is_sanitized_into_the_file_name(self, fm, tmp_path):
        fm.params_label = "left+bench"
        assert fm.get_gui_state_path() == tmp_path / "rtt.gui_state.left_bench.json"

    def test_save_gui_state_writes_own_file_only(self, fm, tmp_path):
        from types import SimpleNamespace

        fm.params_label = "left"
        shared = tmp_path / "rtt.gui_state.json"
        shared.write_text(json.dumps({"layout": "shared"}))
        fm.gui_context = SimpleNamespace(gui_state=SimpleNamespace(get_data=lambda: {"layout": "left"}))

        fm.save_gui_state()

        assert read(tmp_path / "rtt.gui_state.left.json") == {"layout": "left"}
        assert read(shared) == {"layout": "shared"}
        assert read(fm.get_session_path("gui_state", "autosave")) == {"layout": "left"}

    def test_session_start_snapshot_is_the_layout_actually_loaded(self, fm, tmp_path):
        fm.params_label = "left"
        (tmp_path / "rtt.gui_state.json").write_text(json.dumps({"layout": "shared"}))
        fm.snapshot_gui_start()
        assert read(fm.get_session_path("gui_state", "start")) == {"layout": "shared"}

        (tmp_path / "rtt.gui_state.left.json").write_text(json.dumps({"layout": "left"}))
        fm.snapshot_gui_start()
        assert read(fm.get_session_path("gui_state", "start")) == {"layout": "left"}

    def test_label_comes_from_registry_params(self, fm):
        assert fm.params_label == "rtt_serial_1"
        assert fm.get_gui_state_path().name == "rtt.gui_state.rtt_serial_1.json"


# ==========================================
# Changing this run's values live (Profile Parameters dialog)
# ==========================================
class TestSetParamValues:
    def test_set_change_and_unset(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        sources = Subscriber()
        cm.subscribe("/sources", sources)

        assert cm.set_param_values({"rtt_serial": 51024923}) == {"rtt_serial": "51024923"}
        assert cm.get_by_path(SERIAL_PATH) == "51024923"
        assert cm.get_param_binding(SERIAL_PATH) == "rtt_serial"
        assert sources.calls[-1]["src_a"]["serial_number"] == "51024923"

        cm.set_param_values({"rtt_serial": "2"})
        assert cm.get_by_path(SERIAL_PATH) == "2"

        assert cm.set_param_values({"rtt_serial": None}) == {}
        assert cm.get_by_path(SERIAL_PATH) == "11111111"
        assert cm.get_param_binding(SERIAL_PATH) is None
        assert read(profile_file)["sources"]["src_a"]["serial_number"] == "11111111"

    def test_profile_file_never_gets_the_value(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        before = profile_file.read_text()
        cm.set_param_values({"rtt_serial": "9", "speed": "100"})
        assert profile_file.read_text() == before  # not even rewritten
        cm.apply_patch("/pipelines/pipe_a", [{"op": "replace", "path": "/name", "value": "n"}])
        assert read(profile_file)["sources"]["src_a"] == make_profile()["sources"]["src_a"]

    def test_startup_param_can_be_changed_and_unset(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "1"})
        cm.set_param_values({"rtt_serial": "2"})
        assert cm.get_by_path(SERIAL_PATH) == "2"
        cm.set_param_values({"rtt_serial": None})
        assert cm.get_by_path(SERIAL_PATH) == "11111111"  # the profile value, not the startup one

    def test_unchanged_value_notifies_nobody(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        sub = Subscriber()
        cm.subscribe("/sources", sub)
        cm.set_param_values({"rtt_serial": "11111111"})
        assert sub.calls == []
        assert cm.param_values == {"rtt_serial": "11111111"}

    def test_invalid_input_changes_nothing(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json", params={"rtt_serial": "1"})
        with pytest.raises(ProfileParamError):
            cm.set_param_values({"rtt_serial": "2", "speed": "fast"})
        with pytest.raises(ProfileParamError, match="Unknown"):
            cm.set_param_values({"nope": "1"})
        assert cm.get_by_path(SERIAL_PATH) == "1"
        assert cm.param_values == {"rtt_serial": "1"}

    def test_dry_run_validates_without_applying(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        assert cm.set_param_values({"speed": "12000"}, dry_run=True) == {"speed": 12000}
        assert cm.param_values == {}
        assert cm.get_by_path("/sources/src_a/speed") == 4000

    def test_reload_after_live_change_keeps_it(self, profile_file, tmp_path):
        cm = ConfigManager(profile_file, tmp_path / "a.json")
        cm.set_param_values({"rtt_serial": "7"})
        write_external(profile_file, lambda d: d["pipelines"]["pipe_a"].update(name="x"))
        cm.reload_from_disk()
        assert cm.get_by_path(SERIAL_PATH) == "7"
        assert cm.get_by_path("/pipelines/pipe_a/name") == "x"


class TestOrigins:
    def test_load_params_files_reports_origins(self, tmp_path):
        params_set_path(tmp_path, "rtt", "a").write_text(json.dumps({"x": 1, "y": 1}))
        (tmp_path / "b.json").write_text(json.dumps({"y": 2}))
        loaded = load_params_files(["a", str(tmp_path / "b.json")], tmp_path, "rtt")
        assert loaded.origins == {"x": "set 'a'", "y": "file 'b.json'"}

    def test_registry_records_origins_and_session(self, profile_file, tmp_path):
        params_set_path(tmp_path, "rtt", "left").write_text(
            json.dumps({"session": "Left board", "params": {"rtt_serial": "5"}})
        )
        reg = make_registry(tmp_path, params_files=["left"], param_args=["speed=1"])
        try:
            assert reg.file_manager.param_origins == {"rtt_serial": "set 'left'", "speed": "--param"}
            assert reg.file_manager.params_session == "Left board"
        finally:
            reg.stop()


# ==========================================
# Optional fields the profile leaves empty (absent from the JSON, or null) - e.g. an RTT source
# whose serial_number was never filled in
# ==========================================
def make_unset_profile(serial=MISSING):
    profile = make_profile()
    if serial is MISSING:
        del profile["sources"]["src_a"]["serial_number"]
    else:
        profile["sources"]["src_a"]["serial_number"] = serial
    return profile


@pytest.fixture
def unset_profile_file(tmp_path):
    path = tmp_path / "rtt.json"
    path.write_text(json.dumps(make_unset_profile()))
    return path


def schema_types(data, path):
    """Stand-in for the factory-schema lookup Registry passes in."""
    return {"/sources/src_a/serial_number": "string", "/sources/src_a/speed": "integer"}.get(path)


class TestUnsetOptionalFields:
    def test_missing_survives_deepcopy(self):
        from copy import deepcopy

        assert deepcopy(MISSING) is MISSING
        assert deepcopy({"a": ("x", MISSING)})["a"][1] is MISSING

    def test_apply_creates_the_key_and_remembers_it_was_unset(self):
        cfg = make_unset_profile()
        bindings, effective = apply_params(cfg, {"rtt_serial": "51024923"})
        assert cfg["sources"]["src_a"]["serial_number"] == "51024923"
        assert bindings[SERIAL_PATH] == ("rtt_serial", MISSING)

    def test_schema_type_used_when_there_is_no_value(self):
        for serial in (MISSING, None):
            cfg = make_unset_profile(serial)
            _, effective = apply_params(cfg, {"rtt_serial": 51024923}, schema_types)
            assert effective == {"rtt_serial": "51024923"}  # a number from a params file -> string

    def test_without_schema_value_is_kept_as_given(self):
        cfg = make_unset_profile(None)
        _, effective = apply_params(cfg, {"rtt_serial": "51024923"})
        assert effective == {"rtt_serial": "51024923"}

    def test_schema_type_only_consulted_when_needed(self):
        asked = []
        cfg = make_profile()
        apply_params(cfg, {"speed": "100"}, lambda data, path: asked.append(path))
        assert asked == []  # the current value already says "integer"

    def test_coerce_by_schema_type(self):
        assert coerce_value("8000", MISSING, schema_type="integer") == 8000
        assert coerce_value("yes", None, schema_type="boolean") is True
        with pytest.raises(ProfileParamError):
            coerce_value("fast", MISSING, "speed", "/x", schema_type="integer")

    def test_profile_file_never_gets_the_key(self, unset_profile_file, tmp_path):
        autosave = tmp_path / "a.json"
        cm = ConfigManager(unset_profile_file, autosave, params={"rtt_serial": "51024923"}, param_type_of=schema_types)
        assert cm.get_by_path(SERIAL_PATH) == "51024923"

        cm.apply_patch("/pipelines/pipe_a", [{"op": "replace", "path": "/name", "value": "x"}])
        assert "serial_number" not in read(unset_profile_file)["sources"]["src_a"]

        # Replacing the whole source (as the config editor's Apply can) mustn't leak it either.
        new_source = dict(cm.get_by_path("/sources/src_a"), speed=8000)
        cm.apply_patch("/sources", [{"op": "replace", "path": "/src_a", "value": new_source}])
        on_disk = read(unset_profile_file)["sources"]["src_a"]
        assert on_disk["speed"] == 8000 and "serial_number" not in on_disk

        cm.save_full_config()
        assert "serial_number" not in read(unset_profile_file)["sources"]["src_a"]
        assert read(autosave)["sources"]["src_a"]["serial_number"] == "51024923"
        assert cm.get_by_path(SERIAL_PATH) == "51024923"

    def test_live_set_and_unset_on_unset_field(self, unset_profile_file, tmp_path):
        """The reported bug: the Profile Parameters dialog could not set serial_number while the
        profile left it empty."""
        cm = ConfigManager(unset_profile_file, tmp_path / "a.json", param_type_of=schema_types)
        sources = Subscriber()
        cm.subscribe("/sources", sources)

        assert cm.set_param_values({"rtt_serial": 51024923}) == {"rtt_serial": "51024923"}
        assert cm.get_by_path(SERIAL_PATH) == "51024923"
        assert sources.calls[-1]["src_a"]["serial_number"] == "51024923"

        cm.set_param_values({"rtt_serial": None})
        assert "serial_number" not in cm.get_by_path("/sources/src_a")
        assert cm.get_param_binding(SERIAL_PATH) is None
        assert "serial_number" not in read(unset_profile_file)["sources"]["src_a"]

    def test_null_field_is_restored_as_null(self, tmp_path):
        path = tmp_path / "rtt.json"
        path.write_text(json.dumps(make_unset_profile(None)))
        cm = ConfigManager(path, tmp_path / "a.json", params={"rtt_serial": "5"}, param_type_of=schema_types)
        cm.apply_patch("/pipelines/pipe_a", [{"op": "replace", "path": "/name", "value": "x"}])
        assert read(path)["sources"]["src_a"]["serial_number"] is None
        cm.set_param_values({"rtt_serial": None})
        assert cm.get_by_path(SERIAL_PATH) is None

    def test_reload_keeps_binding_on_unset_field(self, unset_profile_file, tmp_path):
        cm = ConfigManager(unset_profile_file, tmp_path / "a.json", params={"rtt_serial": "5"})
        write_external(unset_profile_file, lambda d: d["sources"]["src_a"].update(speed=1))
        cm.reload_from_disk()
        assert cm.get_by_path(SERIAL_PATH) == "5"
        assert cm.get_param_binding(SERIAL_PATH) == "rtt_serial"
        cm.save_full_config()
        assert "serial_number" not in read(unset_profile_file)["sources"]["src_a"]

    def test_declare_on_unset_field(self, unset_profile_file, tmp_path):
        cm = ConfigManager(unset_profile_file, tmp_path / "a.json")
        cm.remove_param(SERIAL_PATH)
        cm.declare_param("rtt_serial", SERIAL_PATH)  # right-click on the empty field
        assert cm.get_param_declaration(SERIAL_PATH) == "rtt_serial"


class TestRegistryUnsetSerial:
    @pytest.mark.parametrize("serial", [MISSING, None])
    def test_real_schema_makes_the_serial_a_string(self, tmp_path, serial):
        (tmp_path / "rtt.json").write_text(json.dumps(make_unset_profile(serial)))
        params_set_path(tmp_path, "rtt", "left").write_text(json.dumps({"rtt_serial": 51024923}))
        reg = make_registry(tmp_path, params_files=["left"])
        try:
            assert reg.config.get_by_path(SERIAL_PATH) == "51024923"
            reg.config.set_param_values({"speed": "100"})
            assert reg.config.get_by_path("/sources/src_a/speed") == 100
        finally:
            reg.stop()

    def test_field_type_lookup(self):
        from blinkview.core.factory_category_registry import build_system_factory_registry
        from blinkview.core.registry import _param_field_type

        factories = build_system_factory_registry()
        data = make_unset_profile()
        assert _param_field_type(factories, data, SERIAL_PATH) == "string"
        assert _param_field_type(factories, data, "/sources/src_a/speed") == "integer"
        assert _param_field_type(factories, data, "/sources/src_a/no_such_field") is None
        assert _param_field_type(factories, data, "/sources/src_missing/serial_number") is None
        assert _param_field_type(factories, data, "/reorder/enabled") is None

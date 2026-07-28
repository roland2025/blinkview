# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Tests for blinkview.utils.config_handler.handle_config - the `blink config` subcommand's
git-style get/set/list/unset/keys logic. GlobalSettings/ProjectSettings are imported locally
inside handle_config, so monkeypatching blinkview.utils.global_settings.GlobalSettings /
blinkview.utils.project_settings.ProjectSettings is picked up at call time."""

import argparse
from types import SimpleNamespace

import blinkview.utils.global_settings as global_settings_module
import blinkview.utils.project_settings as project_settings_module
from blinkview.utils.config_handler import handle_config, setup_config_parser


class FakeSettings:
    """Mimics blinkview.utils.settings.Settings closely enough for handle_config's dot-notation
    get/set/unset/flatten logic, without touching any real file on disk."""

    def __init__(self, data=None, path="fake/path.json", supported=("log_dir", "update")):
        self._data = data if data is not None else {}
        self._path = path
        self._supported = supported
        self.saved = False

    def supported_keys(self):
        return list(self._supported)

    def supported_key(self, key_string):
        return key_string.split(".")[0] in self._supported

    def flattened_items(self, data=None, prefix=""):
        target = data if data is not None else self._data
        for k, v in target.items():
            key_path = f"{prefix}{k}"
            if isinstance(v, dict) and v:
                yield from self.flattened_items(v, prefix=f"{key_path}.")
            else:
                yield key_path, v

    def get(self, key_string, default=None):
        val = self._data
        for k in key_string.split("."):
            if isinstance(val, dict) and k in val:
                val = val[k]
            else:
                return default
        return val

    def set(self, key_string, value):
        keys = key_string.split(".")
        target = self._data
        for k in keys[:-1]:
            target = target.setdefault(k, {})
        target[keys[-1]] = value

    def unset_deep(self, key_string):
        keys = key_string.split(".")
        if len(keys) == 1:
            return self._data.pop(keys[0], None)
        parent = self.get(".".join(keys[:-1]))
        if isinstance(parent, dict):
            return parent.pop(keys[-1], None)
        return None

    def save(self):
        self.saved = True


def _args(**overrides):
    defaults = dict(global_scope=False, list=False, keys=False, unset=False, key=None, value=None)
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


class TestSetupConfigParser:
    def _build_parser(self):
        parser = argparse.ArgumentParser()
        setup_config_parser(parser)
        return parser

    def test_parses_key_value_and_flags(self):
        args = self._build_parser().parse_args(["--global", "log_dir", "/tmp/logs"])

        assert args.global_scope is True
        assert args.key == "log_dir"
        assert args.value == "/tmp/logs"

    def test_list_works_without_a_key(self):
        args = self._build_parser().parse_args(["--list"])

        assert args.list is True
        assert args.key is None
        assert args.value is None

    def test_unset_and_keys_and_check_updates_flags(self):
        args = self._build_parser().parse_args(["--unset", "--keys", "--check-updates", "log_dir"])

        assert args.unset is True
        assert args.keys is True
        assert args.check_updates is True
        assert args.key == "log_dir"


class TestGlobalScope:
    def test_get_existing_key_prints_value(self, monkeypatch, capsys):
        fake = FakeSettings(data={"log_dir": "/var/log/blink"})
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, key="log_dir"))

        assert capsys.readouterr().out.strip() == "/var/log/blink"

    def test_get_missing_key_prints_not_set_message(self, monkeypatch, capsys):
        fake = FakeSettings()
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, key="log_dir"))

        assert "not set in global config" in capsys.readouterr().out

    def test_set_writes_value_and_saves(self, monkeypatch, capsys):
        fake = FakeSettings()
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, key="log_dir", value="/tmp/logs"))

        assert fake._data == {"log_dir": "/tmp/logs"}
        assert fake.saved is True
        assert "Set global log_dir to: /tmp/logs" in capsys.readouterr().out

    def test_unset_existing_key_saves_and_confirms(self, monkeypatch, capsys):
        fake = FakeSettings(data={"log_dir": "/tmp/logs"})
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, unset=True, key="log_dir"))

        assert fake._data == {}
        assert fake.saved is True
        assert "Unset log_dir (global)" in capsys.readouterr().out

    def test_unset_key_not_set_does_not_save(self, monkeypatch, capsys):
        fake = FakeSettings()
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, unset=True, key="log_dir"))

        assert fake.saved is False
        assert "was not set in global config" in capsys.readouterr().out

    def test_keys_lists_supported_keys(self, monkeypatch, capsys):
        fake = FakeSettings(supported=("log_dir", "update"))
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, keys=True))

        out = capsys.readouterr().out
        assert "log_dir" in out and "update" in out

    def test_list_on_empty_settings_prints_empty_message(self, monkeypatch, capsys):
        fake = FakeSettings(data={})
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, list=True))

        assert "Global config is empty" in capsys.readouterr().out

    def test_list_with_data_prints_flattened_sorted_items(self, monkeypatch, capsys):
        fake = FakeSettings(data={"update": {"path": "/repo"}, "log_dir": "/tmp"})
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, list=True))

        lines = capsys.readouterr().out.splitlines()
        assert lines == ["log_dir=/tmp", "update.path=/repo"]

    def test_unsupported_key_prints_error_and_allowed_keys(self, monkeypatch, capsys):
        fake = FakeSettings(supported=("log_dir",))
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True, key="bogus.thing"))

        out = capsys.readouterr().out
        assert "'bogus' is not a valid global setting" in out
        assert "log_dir" in out

    def test_no_key_and_no_list_prints_key_required_error(self, monkeypatch, capsys):
        fake = FakeSettings()
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: fake)

        handle_config(_args(global_scope=True))

        assert "config key required" in capsys.readouterr().out


class TestProjectScope:
    def test_in_a_project_uses_local_scope_label(self, monkeypatch, capsys):
        fake = FakeSettings(data={"log_dir": "/proj/logs"}, path="proj/.blinkview/project.json")
        monkeypatch.setattr(project_settings_module, "ProjectSettings", lambda: fake)

        handle_config(_args(key="log_dir"))

        assert capsys.readouterr().out.strip() == "/proj/logs"

    def test_not_in_a_project_without_list_prints_error_and_does_not_touch_global(self, monkeypatch, capsys):
        fake = FakeSettings(path=None)
        monkeypatch.setattr(project_settings_module, "ProjectSettings", lambda: fake)
        global_calls = []
        monkeypatch.setattr(
            global_settings_module, "GlobalSettings", lambda: global_calls.append(True) or FakeSettings()
        )

        handle_config(_args(key="log_dir"))

        assert "Not in a BlinkView project" in capsys.readouterr().out
        assert not global_calls

    def test_not_in_a_project_with_list_falls_back_to_global_and_labels_it_correctly(self, monkeypatch, capsys):
        """Regression test: scope_name used to get unconditionally reset to 'local' right after
        this fallback assigned 'global (fallback)', so the printed scope label lied about which
        settings were actually being listed even though the correct GlobalSettings object was
        used underneath."""
        project_fake = FakeSettings(path=None)
        monkeypatch.setattr(project_settings_module, "ProjectSettings", lambda: project_fake)
        global_fake = FakeSettings(data={})
        monkeypatch.setattr(global_settings_module, "GlobalSettings", lambda: global_fake)

        handle_config(_args(list=True))

        assert "Global (fallback) config is empty" in capsys.readouterr().out

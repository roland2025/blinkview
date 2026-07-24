# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json

import pytest

from blinkview.core.config_manager import ConfigManager


class DummyCallback:
    """Minimal apply_config-carrying subscriber for exercising subscribe()/notify paths."""

    def __init__(self):
        self.apply_config_calls = []
        self.hydrate_calls = []

    def apply_config(self, config):
        self.apply_config_calls.append(config)

    def hydrate_config(self, data):
        self.hydrate_calls.append(data)
        return data


@pytest.fixture
def paths(tmp_path):
    return tmp_path / "config.json", tmp_path / "autosave.json"


class TestInitAndLoad:
    def test_missing_file_uses_default_config(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        assert cm.get_full_config() == {"a": 1}

    def test_missing_file_and_no_default_yields_empty_dict(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave)
        assert cm.get_full_config() == {}

    def test_loads_existing_valid_json(self, paths):
        filepath, autosave = paths
        filepath.write_text(json.dumps({"devices": {"x": {}}}))
        cm = ConfigManager(filepath, autosave, default_config={"should": "not appear"})
        assert cm.get_full_config() == {"devices": {"x": {}}}

    def test_corrupt_json_falls_back_to_default(self, paths):
        filepath, autosave = paths
        filepath.write_text("{not valid json")
        cm = ConfigManager(filepath, autosave, default_config={"fallback": True})
        assert cm.get_full_config() == {"fallback": True}


class TestReadAccessors:
    def test_get_device_names(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {}, "b": {}}})
        assert set(cm.get_device_names()) == {"a", "b"}

    def test_get_device_config_returns_a_copy(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cfg = cm.get_device_config("a")
        cfg["x"] = 999
        assert cm.get_device_config("a") == {"x": 1}

    def test_get_device_config_missing_device_returns_empty_dict(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={})
        assert cm.get_device_config("missing") == {}

    def test_get_plugins(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"plugins": ["p1"]})
        assert cm.get_plugins() == ["p1"]

    def test_get_reorder_config_defaults_when_absent(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={})
        assert cm.get_reorder_config() == {"enabled": True}

    def test_get_central_storage_config_defaults_when_absent(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={})
        assert cm.get_central_storage_config() == {"enabled": True}

    def test_get_full_config_returns_a_copy(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        full = cm.get_full_config()
        full["a"] = 2
        assert cm.get_full_config()["a"] == 1

    def test_get_data_returns_the_live_dict_not_a_copy(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        assert cm.get_data() is cm._data

    def test_get_by_path(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        assert cm.get_by_path("/devices/a/x") == 1
        assert cm.get_by_path("/missing", default="fallback") == "fallback"


class TestSaveFullConfig:
    def test_writes_data_to_filepath(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        cm.save_full_config()
        assert json.loads(filepath.read_text()) == {"a": 1}

    def test_writes_to_an_explicit_target(self, paths, tmp_path):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        other = tmp_path / "other.json"
        cm.save_full_config(other)
        assert json.loads(other.read_text()) == {"a": 1}

    def test_session_autosave_writes_to_the_autosave_path(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        cm.session_autosave()
        assert json.loads(autosave.read_text()) == {"a": 1}

    def test_save_failure_is_caught_and_does_not_raise(self, paths, monkeypatch):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})

        def boom(*args, **kwargs):
            raise OSError("disk full")

        monkeypatch.setattr("blinkview.core.config_manager.atomic_json_dump", boom)
        cm.save_full_config()  # must not raise


class TestSubscribe:
    def test_requires_an_apply_config_method(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave)
        with pytest.raises(ValueError):
            cm.subscribe("/devices", object())

    def test_registers_the_callback(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave)
        cb = DummyCallback()
        cm.subscribe("/devices", cb)
        assert cm._subscriptions["/devices"] == [cb]

    def test_does_not_duplicate_the_same_callback(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave)
        cb = DummyCallback()
        cm.subscribe("/devices", cb)
        cm.subscribe("/devices", cb)
        assert cm._subscriptions["/devices"] == [cb]

    def test_unsubscribe_removes_the_callback(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave)
        cb = DummyCallback()
        cm.subscribe("/devices", cb)
        cm.unsubscribe("/devices", cb)
        assert cm._subscriptions["/devices"] == []

    def test_unsubscribe_missing_path_or_callback_is_a_noop(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave)
        cm.unsubscribe("/nope", DummyCallback())  # must not raise


class TestApplyPatch:
    def test_empty_patch_is_a_noop(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        cm.apply_patch("/", [])
        assert cm.get_full_config() == {"a": 1}
        assert not filepath.exists()

    def test_add_at_root(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={})
        cm.apply_patch("/", [{"op": "add", "path": "/foo", "value": 1}])
        assert cm.get_full_config() == {"foo": 1}

    def test_relative_path_without_leading_slash_is_scoped_under_base_path(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cm.apply_patch("/devices/a", [{"op": "replace", "path": "x", "value": 2}])
        assert cm.get_by_path("/devices/a/x") == 2

    def test_relative_path_with_leading_slash_is_also_scoped_under_base_path(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cm.apply_patch("/devices/a", [{"op": "replace", "path": "/x", "value": 3}])
        assert cm.get_by_path("/devices/a/x") == 3

    def test_empty_relative_path_targets_the_base_path_itself(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cm.apply_patch("/devices/a", [{"op": "replace", "path": "", "value": {"x": 99}}])
        assert cm.get_by_path("/devices/a") == {"x": 99}

    def test_saves_full_config_and_session_autosave_after_applying(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={})
        cm.apply_patch("/", [{"op": "add", "path": "/foo", "value": 1}])
        assert json.loads(filepath.read_text()) == {"foo": 1}
        assert json.loads(autosave.read_text()) == {"foo": 1}

    def test_invalid_patch_is_caught_and_leaves_data_unchanged(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        cm.apply_patch("/", [{"op": "remove", "path": "/missing"}])  # target doesn't exist
        assert cm.get_full_config() == {"a": 1}
        assert not filepath.exists()  # save is never reached

    def test_calls_config_changed_cb_with_new_config_and_schema(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={})
        calls = []
        cm.config_changed_cb = lambda path, cfg, schema: calls.append((path, cfg, schema))
        cm.get_schema_by_path = lambda path: {"type": "object"}

        cm.apply_patch("/", [{"op": "add", "path": "/foo", "value": 1}])

        assert calls == [("/", {"foo": 1}, {"type": "object"})]


class TestNotifySubscribers:
    def test_notifies_a_subscriber_on_the_exact_changed_path(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cb = DummyCallback()
        cm.subscribe("/devices/a", cb)

        cm.apply_patch("/devices/a", [{"op": "replace", "path": "/x", "value": 2}])

        assert cb.apply_config_calls == [{"x": 2}]

    def test_notifies_a_subscriber_whose_path_is_a_parent_of_the_change(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cb = DummyCallback()
        cm.subscribe("/devices", cb)

        cm.apply_patch("/devices/a", [{"op": "replace", "path": "/x", "value": 2}])

        assert cb.apply_config_calls == [{"a": {"x": 2}}]

    def test_notifies_a_subscriber_whose_path_is_a_child_of_the_change(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cb = DummyCallback()
        cm.subscribe("/devices/a/x", cb)

        cm.apply_patch("/devices", [{"op": "replace", "path": "/a", "value": {"x": 5}}])

        assert cb.apply_config_calls == [5]

    def test_does_not_notify_unrelated_subscribers(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {}}, "plugins": []})
        cb = DummyCallback()
        cm.subscribe("/plugins", cb)

        cm.apply_patch("/devices/a", [{"op": "add", "path": "/x", "value": 1}])

        assert cb.apply_config_calls == []

    def test_root_subscriber_is_always_notified(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        cb = DummyCallback()
        cm.subscribe("/", cb)

        cm.apply_patch("/", [{"op": "add", "path": "/b", "value": 2}])

        assert cb.apply_config_calls == [{"a": 1, "b": 2}]

    def test_uses_hydrate_config_when_available(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})
        cb = DummyCallback()
        cm.subscribe("/devices/a", cb)

        cm.apply_patch("/devices/a", [{"op": "replace", "path": "/x", "value": 2}])

        assert cb.hydrate_calls == [{"x": 2}]

    def test_falls_back_to_raw_value_when_hydrate_config_raises(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {"x": 1}}})

        class BrokenHydrate(DummyCallback):
            def hydrate_config(self, data):
                raise RuntimeError("boom")

        cb = BrokenHydrate()
        cm.subscribe("/devices/a", cb)

        cm.apply_patch("/devices/a", [{"op": "replace", "path": "/x", "value": 2}])

        assert cb.apply_config_calls == [{"x": 2}]

    def test_callback_exception_is_caught_and_does_not_block_other_subscribers(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {}}})

        class BrokenApply(DummyCallback):
            def apply_config(self, config):
                raise RuntimeError("boom")

        broken = BrokenApply()
        good = DummyCallback()
        cm.subscribe("/devices/a", broken)
        cm.subscribe("/devices/a", good)

        cm.apply_patch("/devices/a", [{"op": "add", "path": "/y", "value": 1}])

        assert good.apply_config_calls == [{"y": 1}]

    def test_thread_needs_restart_triggers_restart(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"devices": {"a": {}}})

        class RestartingCallback(DummyCallback):
            thread_needs_restart = True

            def __init__(self):
                super().__init__()
                self.restarted = 0

            def restart(self):
                self.restarted += 1

        cb = RestartingCallback()
        cm.subscribe("/devices/a", cb)

        cm.apply_patch("/devices/a", [{"op": "add", "path": "/y", "value": 1}])

        assert cb.restarted == 1


class TestGetSubFilePath:
    def test_derives_a_sibling_filename_from_the_main_config_stem(self, tmp_path):
        filepath = tmp_path / "blink_config.json"
        cm = ConfigManager(filepath, tmp_path / "autosave.json")
        assert cm.get_sub_file_path("devices") == tmp_path / "blink_config_devices.json"


class TestGetConfigSchema:
    def test_returns_config_and_none_schema_when_resolver_unset(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        config, schema = cm.get_config_schema("/a")
        assert config == 1
        assert schema is None

    def test_uses_the_schema_resolver_when_set(self, paths):
        filepath, autosave = paths
        cm = ConfigManager(filepath, autosave, default_config={"a": 1})
        cm.get_schema_by_path = lambda path, drop_keys=None: {"type": "integer"}
        config, schema = cm.get_config_schema("/a")
        assert config == 1
        assert schema == {"type": "integer"}

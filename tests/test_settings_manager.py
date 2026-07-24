# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.settings_manager import SettingsManager


class FakeSettings:
    """Duck-typed stand-in for utils.settings.Settings, matching the subset of its interface
    SettingsManager actually calls (get/set/save/unset_deep/flattened_items)."""

    def __init__(self, initial=None):
        self._data = dict(initial or {})
        self.saved = 0
        self.unset_calls = []

    def get(self, key, default=None):
        return self._data.get(key, default)

    def set(self, key, value):
        self._data[key] = value

    def save(self):
        self.saved += 1

    def unset_deep(self, key):
        self.unset_calls.append(key)
        return self._data.pop(key, None)

    def flattened_items(self):
        return list(self._data.items())


def make_manager(project=None, global_=None):
    """Bypasses __init__ (which unconditionally constructs a real GlobalSettings, touching
    ~/.blinkview/settings.json, and conditionally a ProjectSettings via get_project_root()
    walking up from cwd) so tests never touch the real filesystem or a real project."""
    mgr = SettingsManager.__new__(SettingsManager)
    mgr._project = project
    mgr._global = global_ if global_ is not None else FakeSettings()
    return mgr


class TestIsProject:
    def test_true_when_project_scope_present(self):
        mgr = make_manager(project=FakeSettings())
        assert mgr.is_project is True

    def test_false_when_no_project_scope(self):
        mgr = make_manager(project=None)
        assert mgr.is_project is False


class TestGet:
    def test_returns_project_value_without_touching_global(self):
        mgr = make_manager(project=FakeSettings({"log_dir": "/proj"}), global_=FakeSettings({"log_dir": "/glob"}))
        assert mgr.get("log_dir") == "/proj"

    def test_falls_back_to_global_when_project_lacks_key(self):
        mgr = make_manager(project=FakeSettings({}), global_=FakeSettings({"log_dir": "/glob"}))
        assert mgr.get("log_dir") == "/glob"

    def test_goes_straight_to_global_when_standalone(self):
        mgr = make_manager(project=None, global_=FakeSettings({"log_dir": "/glob"}))
        assert mgr.get("log_dir") == "/glob"

    def test_returns_default_when_missing_everywhere(self):
        mgr = make_manager(project=FakeSettings({}), global_=FakeSettings({}))
        assert mgr.get("missing", default="fallback") == "fallback"


class TestSet:
    def test_project_scope_sets_and_saves_project_only(self):
        project, glob = FakeSettings(), FakeSettings()
        mgr = make_manager(project=project, global_=glob)

        mgr.set("k", "v", scope="project")

        assert project.get("k") == "v"
        assert project.saved == 1
        assert glob.saved == 0

    def test_project_scope_without_a_project_raises(self):
        mgr = make_manager(project=None, global_=FakeSettings())
        with pytest.raises(RuntimeError):
            mgr.set("k", "v", scope="project")

    def test_global_scope_sets_and_saves_global_only(self):
        project, glob = FakeSettings(), FakeSettings()
        mgr = make_manager(project=project, global_=glob)

        mgr.set("k", "v", scope="global")

        assert glob.get("k") == "v"
        assert glob.saved == 1
        assert project.saved == 0

    def test_invalid_scope_raises_value_error(self):
        mgr = make_manager(project=FakeSettings(), global_=FakeSettings())
        with pytest.raises(ValueError):
            mgr.set("k", "v", scope="nonsense")


class TestUnset:
    def test_project_scope_unsets_and_saves_project(self):
        project = FakeSettings({"k": "v"})
        mgr = make_manager(project=project, global_=FakeSettings())

        mgr.unset("k", scope="project")

        assert project.unset_calls == ["k"]
        assert project.saved == 1

    def test_project_scope_without_a_project_raises(self):
        mgr = make_manager(project=None, global_=FakeSettings())
        with pytest.raises(RuntimeError):
            mgr.unset("k", scope="project")

    def test_global_scope_unsets_and_saves_global(self):
        glob = FakeSettings({"k": "v"})
        mgr = make_manager(project=None, global_=glob)

        mgr.unset("k", scope="global")

        assert glob.unset_calls == ["k"]
        assert glob.saved == 1

    def test_unlike_set_any_non_project_scope_string_routes_to_global(self):
        """Documents a real asymmetry with set(): unset() never validates scope - anything
        other than the literal string 'project' falls through to the global target."""
        glob = FakeSettings({"k": "v"})
        mgr = make_manager(project=None, global_=glob)

        mgr.unset("k", scope="totally-not-a-real-scope")

        assert glob.unset_calls == ["k"]


class TestAllResolved:
    def test_standalone_returns_global_only(self):
        mgr = make_manager(project=None, global_=FakeSettings({"a": 1}))
        assert mgr.all_resolved() == {"a": 1}

    def test_project_overrides_global_on_conflicting_keys(self):
        mgr = make_manager(
            project=FakeSettings({"a": "project-val", "b": "project-only"}),
            global_=FakeSettings({"a": "global-val", "c": "global-only"}),
        )
        assert mgr.all_resolved() == {"a": "project-val", "b": "project-only", "c": "global-only"}


class TestDunderAccess:
    def test_getitem_returns_resolved_value(self):
        mgr = make_manager(project=None, global_=FakeSettings({"k": "v"}))
        assert mgr["k"] == "v"

    def test_getitem_raises_key_error_when_missing(self):
        mgr = make_manager(project=None, global_=FakeSettings({}))
        with pytest.raises(KeyError):
            mgr["missing"]

    def test_setitem_defaults_to_project_scope_when_available(self):
        project, glob = FakeSettings(), FakeSettings()
        mgr = make_manager(project=project, global_=glob)

        mgr["k"] = "v"

        assert project.get("k") == "v"
        assert glob.get("k") is None

    def test_setitem_defaults_to_global_scope_when_standalone(self):
        glob = FakeSettings()
        mgr = make_manager(project=None, global_=glob)

        mgr["k"] = "v"

        assert glob.get("k") == "v"

    def test_contains_true_and_false(self):
        mgr = make_manager(project=None, global_=FakeSettings({"k": "v"}))
        assert "k" in mgr
        assert "missing" not in mgr

    def test_repr_reports_project_mode_and_keys(self):
        mgr = make_manager(project=FakeSettings({"a": 1}), global_=FakeSettings({}))
        r = repr(mgr)
        assert "Project" in r
        assert "a" in r

    def test_repr_reports_standalone_mode(self):
        mgr = make_manager(project=None, global_=FakeSettings({}))
        assert "Standalone" in repr(mgr)

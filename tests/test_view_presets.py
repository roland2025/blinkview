# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Tests for the view layout preset store (ui/utils/view_presets.py) and the FileManager path it
lives at (plans/view-layout-presets.md)."""

import json

import pytest

from blinkview.ui.utils.view_presets import ViewPresetStore, screens_match
from tests.test_file_manager import make_manager

SCREENS_3 = [
    {"name": "\\\\.\\DISPLAY1", "geometry": [0, 0, 2560, 1440]},
    {"name": "\\\\.\\DISPLAY2", "geometry": [2560, 0, 1920, 1080]},
    {"name": "\\\\.\\DISPLAY3", "geometry": [-1920, 0, 1920, 1080]},
]


class TestViewPresetStore:
    def test_missing_file_is_an_empty_store(self, tmp_path):
        store = ViewPresetStore(tmp_path / "p.view_presets.json")
        assert store.names() == []
        assert store.get("x") is None

    def test_save_writes_immediately_and_round_trips(self, tmp_path):
        path = tmp_path / "p.view_presets.json"
        state = {"open_tabs": [{"class": "LogViewerWidget", "name": "Live Logs", "params": {}}]}

        ViewPresetStore(path).save("Desk", state, SCREENS_3)

        on_disk = json.loads(path.read_text())
        assert on_disk["version"] == 1
        assert on_disk["presets"]["Desk"]["state"] == state
        reloaded = ViewPresetStore(path)
        assert reloaded.names() == ["Desk"]
        assert reloaded.get("Desk")["screens"] == SCREENS_3
        assert "saved_at" in reloaded.get("Desk")

    def test_save_overwrites_existing(self, tmp_path):
        store = ViewPresetStore(tmp_path / "p.json")
        store.save("Desk", {"a": 1}, [])
        store.save("Desk", {"a": 2}, [])
        assert store.names() == ["Desk"]
        assert store.get("Desk")["state"] == {"a": 2}

    def test_names_sorted_case_insensitively(self, tmp_path):
        store = ViewPresetStore(tmp_path / "p.json")
        for name in ["beta", "Alpha", "gamma"]:
            store.save(name, {}, [])
        assert store.names() == ["Alpha", "beta", "gamma"]

    def test_rename(self, tmp_path):
        path = tmp_path / "p.json"
        store = ViewPresetStore(path)
        store.save("Old", {"a": 1}, [])
        store.rename("Old", "New")
        assert ViewPresetStore(path).names() == ["New"]
        assert ViewPresetStore(path).get("New")["state"] == {"a": 1}

    def test_rename_onto_existing_name_raises(self, tmp_path):
        store = ViewPresetStore(tmp_path / "p.json")
        store.save("A", {}, [])
        store.save("B", {}, [])
        with pytest.raises(ValueError):
            store.rename("A", "B")
        assert store.names() == ["A", "B"]

    def test_rename_missing_raises(self, tmp_path):
        with pytest.raises(KeyError):
            ViewPresetStore(tmp_path / "p.json").rename("nope", "x")

    def test_delete(self, tmp_path):
        path = tmp_path / "p.json"
        store = ViewPresetStore(path)
        store.save("A", {}, [])
        store.save("B", {}, [])
        store.delete("A")
        store.delete("does-not-exist")
        assert ViewPresetStore(path).names() == ["B"]

    def test_corrupt_file_is_moved_aside_not_overwritten(self, tmp_path):
        path = tmp_path / "p.view_presets.json"
        path.write_text("{not json")

        store = ViewPresetStore(path)

        assert store.names() == []
        backup = tmp_path / "p.view_presets.json.bak"
        assert backup.read_text() == "{not json"
        store.save("A", {}, [])
        assert backup.read_text() == "{not json"


class TestScreensMatch:
    def test_same_geometry_different_order_and_names_matches(self):
        renamed = [{"name": f"X{i}", "geometry": s["geometry"]} for i, s in enumerate(reversed(SCREENS_3))]
        assert screens_match(SCREENS_3, renamed)

    def test_missing_screen_does_not_match(self):
        assert not screens_match(SCREENS_3, SCREENS_3[:2])

    def test_preset_without_fingerprint_matches(self):
        assert screens_match(None, SCREENS_3)
        assert screens_match([], SCREENS_3)


class TestProfilePath:
    def test_is_the_live_profile_file(self, tmp_path):
        fm = make_manager(tmp_path)
        assert fm.get_profile_path("view_presets") == tmp_path / "config" / "myconfig.view_presets.json"

    def test_is_not_redirected_while_replaying(self, tmp_path):
        """get_config_path() goes to the replay scratch folder; presets must still reach the profile."""
        fm = make_manager(tmp_path)
        original_session = tmp_path / "original_session"
        original_session.mkdir()
        fm.replay_source_dir = original_session

        assert fm.get_config_path("view_presets").parent == original_session / "replay"
        assert fm.get_profile_path("view_presets") == tmp_path / "config" / "myconfig.view_presets.json"


class TestScreenChangeSupport:
    def test_offer_on_screen_change_defaults_on_and_persists(self, tmp_path):
        path = tmp_path / "p.json"
        store = ViewPresetStore(path)
        assert store.offer_on_screen_change is True

        store.offer_on_screen_change = False

        assert ViewPresetStore(path).offer_on_screen_change is False

    def test_old_file_without_the_setting_defaults_on(self, tmp_path):
        path = tmp_path / "p.json"
        path.write_text(json.dumps({"version": 1, "presets": {"A": {"screens": [], "state": {}}}}))
        store = ViewPresetStore(path)
        assert store.offer_on_screen_change is True
        assert store.names() == ["A"]

    def test_best_match_picks_the_most_recently_saved_matching_preset(self, tmp_path):
        store = ViewPresetStore(tmp_path / "p.json")
        store._presets = {
            "old desk": {"saved_at": "2026-01-01T00:00:00", "screens": SCREENS_3, "state": {}},
            "new desk": {"saved_at": "2026-09-01T00:00:00", "screens": SCREENS_3, "state": {}},
            "laptop": {"saved_at": "2026-12-01T00:00:00", "screens": SCREENS_3[:1], "state": {}},
        }
        assert store.best_match(SCREENS_3) == "new desk"
        assert store.best_match(SCREENS_3[:1]) == "laptop"
        assert store.best_match(SCREENS_3[:2]) is None

    def test_best_match_ignores_presets_without_a_fingerprint(self, tmp_path):
        store = ViewPresetStore(tmp_path / "p.json")
        store.save("legacy", {}, [])
        assert store.best_match(SCREENS_3) is None

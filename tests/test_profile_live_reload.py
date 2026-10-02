# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""BlinkMainWindow noticing that the profile files were saved by someone else - another
BlinkView instance on the same profile (one per board, see --params) or a text editor - and
offering to reload them (ConfigManager.poll_external_change / reload_from_disk)."""

import json

import pytest

from blinkview.ui import main_window as main_window_module
from blinkview.ui.main_window import PROFILE_WATCH_INTERVAL_MS, BlinkMainWindow
from tests.fakes.real_registry import make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "profile_live_reload_test")
    yield reg
    reg.stop()


@pytest.fixture
def main_window(qapp, qtbot, registry):
    w = BlinkMainWindow(registry)
    qtbot.addWidget(w)
    return w


@pytest.fixture
def toasts(monkeypatch):
    shown = []
    monkeypatch.setattr(main_window_module.ToastManager, "show", lambda message, *a, **kw: shown.append((message, kw)))
    return shown


def edit_on_disk(path, mutate):
    data = json.loads(path.read_text()) if path.exists() else {}
    mutate(data)
    path.write_text(json.dumps(data, indent=2))


def test_watch_timer_runs_for_live_sessions(main_window):
    assert main_window._profile_watch_timer.isActive()
    assert main_window._profile_watch_timer.interval() == PROFILE_WATCH_INTERVAL_MS


def test_quiet_when_nothing_changed(main_window, toasts):
    main_window._poll_profile_changes()
    assert toasts == []


def test_external_change_offers_reload_once(main_window, registry, toasts):
    edit_on_disk(registry.config.filepath, lambda d: d.update(note="from another instance"))

    main_window._poll_profile_changes()
    main_window._poll_profile_changes()

    assert len(toasts) == 1
    message, kwargs = toasts[0]
    assert "changed on disk" in message
    assert kwargs["action_text"] == "Reload"
    assert kwargs["action_callback"] == main_window.reload_profile_from_disk


def test_reload_applies_change_and_rearms_the_offer(main_window, registry, toasts):
    edit_on_disk(registry.config.filepath, lambda d: d.update(note="v1"))
    main_window._poll_profile_changes()

    main_window.reload_profile_from_disk().result(timeout=10)
    assert registry.config.get_by_path("/note") == "v1"
    assert registry.config.poll_external_change() is False

    edit_on_disk(registry.config.filepath, lambda d: d.update(note="v2"))
    main_window._poll_profile_changes()
    assert len(toasts) == 2


def test_gui_config_watches_are_watched_too(main_window, toasts):
    gui_config = main_window.gui_context.gui_config
    edit_on_disk(gui_config.filepath, lambda d: d.setdefault("watches", {}).update(w1={"id": "w1", "entries": []}))

    main_window._poll_profile_changes()
    assert len(toasts) == 1

    main_window.reload_profile_from_disk().result(timeout=10)
    assert gui_config.get_by_path("/watches/w1/id") == "w1"


def test_menu_marks_pending_change(main_window, registry):
    def reload_text():
        main_window.populate_main_menu()
        return [a.text() for a in main_window.app_menu.actions() if a.text().startswith("Reload Profile")]

    assert reload_text() == ["Reload Profile from Disk"]
    edit_on_disk(registry.config.filepath, lambda d: d.update(note="x"))
    assert reload_text() == ["Reload Profile from Disk (changed)"]


def test_own_edit_does_not_offer_reload(main_window, registry, toasts):
    registry.config.apply_patch("/", [{"op": "add", "path": "/note", "value": "mine"}])
    main_window._poll_profile_changes()
    assert toasts == []


def test_two_real_registries_pipeline_added_by_one_is_built_by_the_other(tmp_path):
    """Two instances of one profile: A adds a pipeline (as the config editor would), B
    reloads and its PipelineManager actually builds it - no restart."""
    a = make_real_registry(tmp_path, "board_a")
    b = make_real_registry(tmp_path, "board_b")
    try:
        assert a.config.filepath == b.config.filepath
        pipeline = {"enabled": False, "type": "serial_default", "name": "dev2"}
        a.config.apply_patch("/pipelines", [{"op": "add", "path": "/pipe_new", "value": pipeline}])
        assert "pipe_new" in a.pipelines.pipelines

        assert b.config.poll_external_change() is True
        assert "pipe_new" not in b.pipelines.pipelines
        assert b.config.reload_from_disk() is True
        assert "pipe_new" in b.pipelines.pipelines
        assert b.config.get_by_path("/pipelines/pipe_new/name") == "dev2"
    finally:
        a.stop()
        b.stop()

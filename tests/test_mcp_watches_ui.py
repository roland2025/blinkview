# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""What the mcp_server pipeline asks of the window, through the real BlinkMainWindow and the
real TelemetryWatch: a watch the agent created opens as a tab, an open watch follows a patch,
and a change's notice is a toast whose Undo button reverts it."""

import json
import threading

import pytest
import shiboken6

from blinkview import agent_mcp
from blinkview.core import agent_api
from blinkview.ui.main_window import BlinkMainWindow
from blinkview.ui.widgets.TelemetryWatch import RowEntry, SectionEntry, TelemetryWatch
from blinkview.ui.widgets.toast import ToastManager
from tests.fakes.real_registry import make_real_registry

TIMEOUT_MS = 20_000


@pytest.fixture
def run_dir(tmp_path, monkeypatch):
    """Run files go under tmp_path, not the developer's real ~/.blinkview/run."""
    path = tmp_path / "run"
    monkeypatch.setattr(agent_api, "get_run_dir", lambda: path)
    return path


@pytest.fixture
def registry(tmp_path, run_dir):
    (tmp_path / "test_config.json").write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {},
                "pipelines": {"pipe_mcp": {"enabled": True, "type": "mcp_server", "name": "mcp"}},
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )
    reg = make_real_registry(tmp_path, "mcp_watches_ui_test", with_value_tracker=True)
    yield reg
    reg.stop()


@pytest.fixture
def main_window(qapp, qtbot, registry, tmp_path):
    # Not handed to qtbot.addWidget: pytest-qt would close the window before this fixture's
    # teardown, and closing a window whose registry runs stops it on a QThread of the window's
    # own - which is then destroyed with the window while it still runs. Closed below instead.
    w = BlinkMainWindow(registry)
    # The watch lists must be this test's own file, never the developer's real profile.
    assert tmp_path in registry.gui_config.filepath.parents
    # The window configures the registry itself; the pipelines (the server) start after that.
    registry.start()
    yield w
    for toast in list(ToastManager._toasts):
        toast.hide_toast()
    registry.stop()
    w._shutdown_ready_to_close = True  # nothing left to stop: close at once
    w.close()
    shiboken6.delete(w)


def _call(qtbot, run_dir, tool, **args):
    """A tool call as `blink mcp` makes it, from another thread: the window's side of it runs
    on this (the UI) thread, which therefore has to keep spinning meanwhile."""
    outcome = {}

    def work():
        try:
            outcome["result"] = agent_mcp.call_tool(tool, args, run_dir=run_dir)
        except Exception as e:  # handed to the test thread below
            outcome["error"] = e

    qtbot.waitUntil(lambda: bool(agent_api.list_instances(run_dir)), timeout=TIMEOUT_MS)
    thread = threading.Thread(target=work)
    thread.start()
    qtbot.waitUntil(lambda: not thread.is_alive(), timeout=TIMEOUT_MS)
    if "error" in outcome:
        raise outcome["error"]
    return outcome["result"]


def _watch_tabs(main_window):
    return main_window.findChildren(TelemetryWatch)


def test_a_created_watch_opens_as_a_tab_and_follows_a_patch(qtbot, main_window, registry, run_dir):
    registry.id_registry.get_device("board").get_module("battery.vbat")

    created = _call(
        qtbot,
        run_dir,
        "create_watch",
        name="Battery",
        entries=[{"type": "section", "label": "Pack"}, {"label": "Voltage", "modules": ["board.battery.vbat"]}],
    )

    assert created["opened"] is True and created["warnings"] == []
    qtbot.waitUntil(lambda: any(w.entries for w in _watch_tabs(main_window)), timeout=TIMEOUT_MS)
    watch = _watch_tabs(main_window)[0]
    assert watch.watch_id == created["id"]
    assert watch.name == "Battery"
    assert [type(e) for e in watch.entries] == [SectionEntry, RowEntry]
    assert [m.name_with_device() for m in watch.entries[1].modules] == ["board.battery.vbat"]

    # Opening it again brings the tab forward instead of adding a second one.
    assert _call(qtbot, run_dir, "open_view", id=created["id"])["opened"] is True
    qtbot.wait(200)
    assert len(_watch_tabs(main_window)) == 1

    _call(
        qtbot,
        run_dir,
        "apply_config_patch",
        target="gui",
        path=f"/watches/{created['id']}",
        patch=[{"op": "replace", "path": "/entries/1/label", "value": "Pack voltage"}],
    )
    qtbot.waitUntil(lambda: watch.entries[1].label == "Pack voltage", timeout=TIMEOUT_MS)


def test_a_change_is_shown_as_a_toast_whose_undo_button_reverts_it(qtbot, main_window, registry, run_dir):
    created = _call(qtbot, run_dir, "create_watch", name="Scratch", open=False)
    assert created["opened"] is False
    assert created["id"] in registry.gui_config.get_by_path("/watches")

    def notices():  # other toasts (startup, ...) may be up as well
        text = f"Agent changed /watches/{created['id']}"
        return [t for t in ToastManager._toasts if t.msg_label.text() == text]

    qtbot.waitUntil(lambda: bool(notices()), timeout=TIMEOUT_MS)
    toast = notices()[0]
    assert toast.action_btn.text() == "Undo"

    toast.action_btn.click()

    qtbot.waitUntil(lambda: created["id"] not in registry.gui_config.get_by_path("/watches"), timeout=TIMEOUT_MS)
    assert json.loads(registry.gui_config.filepath.read_text())["watches"] == {}

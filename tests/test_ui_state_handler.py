# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Tests for UIStateHandler (src/blinkview/ui/utils/ui_state_handler.py) - the startup dock/tab/
floating-window/geometry restore logic run from BlinkMainWindow.load_ui_state(). Two real bugs in
this file were found while building a subprocess smoke test for `blink gui` (see
plans/test-workaround-bug-audit.md):

1. load_ui_state() used to store `ui_restored_cb` on self.ui_restored_cb AFTER its "file doesn't
   exist" early return - true for every brand new profile - so the completion callback
   (BlinkMainWindow._start_registry, which calls registry.start()) was silently dropped on every
   fresh install. Confirmed live via `python -m blinkview gui` under QT_QPA_PLATFORM=offscreen:
   the process sat idle in app.exec() forever, having never printed "[Registry] Starting central
   storage...". py-spy dump showed the main thread genuinely idle inside app.exec() (not
   deadlocked), which is what pointed at a dropped callback rather than a hang.

2. In the floating-windows restore loop, `windows_to_restore` was initialized to the *total*
   saved-window count but only ever decremented inside the per-window completion closure
   (scheduled only when create_widget() returns a real widget). A single saved window whose
   class no longer exists (create_widget() returns None -> `continue`) meant the counter could
   never reach zero, so on_ui_restoration_complete() (and thus registry.start()) would never fire
   - the same silent-hang bug as #1, one step further down the same startup chain, triggered by
   any stale/removed floating-widget class in a user's saved layout instead of a fresh profile.

Both are fixed; the tests below cover the previously-broken paths plus the surrounding
restore_window_geometry()/load_ui_state() branches that had no coverage at all."""

import json

from qtpy.QtCore import QByteArray

from blinkview.ui.utils.ui_state_handler import UIStateHandler


class FakeWindow:
    """A minimal stand-in for BlinkMainWindow covering exactly what load_ui_state() touches -
    no real Qt widgets needed since none of these tests exercise real geometry restoration
    (that's restore_window_geometry_safe's job, covered separately in test_window_manager.py)."""

    def __init__(self, create_widget_result=None):
        self.restored_state = None
        self.sources_dock = _FakeDock()
        self.pipelines_dock = _FakeDock()
        self.central_tabs = _FakeTabs()
        self.create_widget_calls = []
        self._create_widget_result = create_widget_result if create_widget_result is not None else _FakeFloatingWindow

    def restoreState(self, qbytearray):
        self.restored_state = bytes(qbytearray)

    def create_widget(self, **kwargs):
        self.create_widget_calls.append(kwargs)
        result = self._create_widget_result
        return result(**kwargs) if callable(result) else result


class _FakeDock:
    def __init__(self):
        self.visible = None

    def setVisible(self, value):
        self.visible = value


class _FakeTabs:
    def __init__(self):
        self.block_calls = []
        self.current_index = None

    def blockSignals(self, value):
        self.block_calls.append(value)

    def setCurrentIndex(self, index):
        self.current_index = index


class _FakeFloatingWindow:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.opacity = None
        self.shown = False
        self.raised = False
        self.activated = False

    def setWindowOpacity(self, value):
        self.opacity = value

    def show(self):
        self.shown = True

    def raise_(self):
        self.raised = True

    def activateWindow(self):
        self.activated = True


class FakeGeoWindow:
    """Stand-in covering exactly what restore_window_geometry()/_geometry_settled() touch."""

    def __init__(self, frame_pos, size, visible=True):
        self.frame_pos = frame_pos
        self.size = size
        self.visible = visible

    def frameGeometry(self):
        return _FakeFrame(*self.frame_pos)

    def width(self):
        return self.size[0]

    def height(self):
        return self.size[1]

    def isVisible(self):
        return self.visible


class _FakeFrame:
    def __init__(self, x, y):
        self._x, self._y = x, y

    def x(self):
        return self._x

    def y(self):
        return self._y


class TestLoadUiStateFreshProfile:
    def test_calls_callback_on_a_fresh_profile_with_no_saved_state(self, tmp_path):
        handler = UIStateHandler(FakeWindow())
        missing_state_file = tmp_path / "gui_state.json"
        assert not missing_state_file.exists()

        calls = []
        handler.load_ui_state(missing_state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert calls == [True]

    def test_calls_callback_when_a_saved_state_file_has_no_floating_windows(self, tmp_path):
        """The "file exists, but nothing left to restore" path already set ui_restored_cb before
        calling on_ui_restoration_complete(), so this one was never broken - included as the
        single-file counterpart to the fresh-profile case above."""
        handler = UIStateHandler(FakeWindow())
        state_file = tmp_path / "gui_state.json"
        state_file.write_text("{}")

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert calls == [True]

    def test_corrupt_json_falls_back_to_completion_callback(self, tmp_path, capsys):
        handler = UIStateHandler(FakeWindow())
        state_file = tmp_path / "gui_state.json"
        state_file.write_text("{not valid json")

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert calls == [True]
        assert "Could not restore UI state" in capsys.readouterr().out


class TestLoadUiStateDocksAndTabs:
    def test_restores_window_state_dock_visibility_and_open_tabs(self, tmp_path):
        window = FakeWindow()
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        encoded_state = bytes(QByteArray(b"fakestate").toBase64()).decode("ascii")
        state_file.write_text(
            json.dumps(
                {
                    "window_state": encoded_state,
                    "sources_visible": False,
                    "pipelines_visible": True,
                    "open_tabs": [{"class": "LogViewerWidget", "name": "Log", "params": {"tab_name": "Log"}}],
                    "current_tab_index": 0,
                    "floating_windows": [],
                }
            )
        )

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert window.restored_state == b"fakestate"
        assert window.sources_dock.visible is False
        assert window.pipelines_dock.visible is True
        assert window.central_tabs.block_calls == [True, False]
        assert window.central_tabs.current_index == 0
        assert window.create_widget_calls == [
            {"cls_name": "LogViewerWidget", "name": "Log", "as_window": False, "params": {"tab_name": "Log"}}
        ]
        # floating_windows is empty, so completion fires synchronously in the same call.
        assert calls == [True]


class TestLoadUiStateFloatingWindows:
    def test_restores_all_floating_windows_and_completes_once_after_the_last_one(self, tmp_path, monkeypatch):
        created = []

        def create_widget(**kwargs):
            win = _FakeFloatingWindow(**kwargs)
            created.append(win)
            return win

        window = FakeWindow(create_widget_result=create_widget)
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(
            json.dumps(
                {
                    "floating_windows": [
                        {"class": "LogViewerWidget", "name": "A", "params": {}, "window_geometry": {}},
                        {"class": "PlotterWidget", "name": "B", "params": {}, "window_geometry": {}},
                    ]
                }
            )
        )
        # Fires the retry chain synchronously so the test doesn't need a real event loop/delay.
        monkeypatch.setattr("blinkview.ui.utils.ui_state_handler.QTimer.singleShot", lambda ms, cb: cb())

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert len(created) == 2
        assert all(w.shown and w.raised and w.activated for w in created)
        assert calls == [True]

    def test_a_stale_unknown_widget_class_still_lets_startup_complete(self, tmp_path, monkeypatch):
        """Regression test for bug #2 in the module docstring: if create_widget() returns None
        for ANY saved floating window (its class no longer exists), the completion callback must
        still eventually fire for the rest - it must not silently hang forever."""
        calls_seen = {"n": 0}

        def create_widget(**kwargs):
            calls_seen["n"] += 1
            if kwargs.get("cls_name") == "RemovedWidget":
                return None  # simulates a widget class that no longer exists
            return _FakeFloatingWindow(**kwargs)

        window = FakeWindow(create_widget_result=create_widget)
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(
            json.dumps(
                {
                    "floating_windows": [
                        {"class": "RemovedWidget", "name": "Gone", "params": {}},
                        {"class": "LogViewerWidget", "name": "Still Here", "params": {}},
                    ]
                }
            )
        )
        monkeypatch.setattr("blinkview.ui.utils.ui_state_handler.QTimer.singleShot", lambda ms, cb: cb())

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert calls_seen["n"] == 2
        assert calls == [True]

    def test_a_floating_window_with_saved_geometry_gets_it_restored(self, tmp_path, monkeypatch):
        window = FakeWindow()
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(
            json.dumps(
                {
                    "floating_windows": [
                        {
                            "class": "LogViewerWidget",
                            "name": "A",
                            "params": {},
                            "window_geometry": {"frame_pos": [1, 2], "client_size": [3, 4]},
                        }
                    ]
                }
            )
        )
        monkeypatch.setattr("blinkview.ui.utils.ui_state_handler.QTimer.singleShot", lambda ms, cb: cb())
        restore_calls = []
        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.restore_window_geometry_safe",
            lambda win, geo: restore_calls.append((win, geo)),
        )

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert len(restore_calls) == 1
        assert restore_calls[0][1] == {"frame_pos": [1, 2], "client_size": [3, 4]}
        assert calls == [True]

    def test_all_floating_windows_unknown_still_completes(self, tmp_path, monkeypatch):
        window = FakeWindow(create_widget_result=lambda **kwargs: None)
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(
            json.dumps({"floating_windows": [{"class": "Gone1", "name": "A"}, {"class": "Gone2", "name": "B"}]})
        )
        monkeypatch.setattr("blinkview.ui.utils.ui_state_handler.QTimer.singleShot", lambda ms, cb: cb())

        calls = []
        handler.load_ui_state(state_file, ui_state_restored_cb=lambda: calls.append(True))

        assert calls == [True]


class TestGetData:
    """Covers UIStateHandler.get_data() - the save-side counterpart of load_ui_state(), invoked
    when persisting UI state on shutdown/tab-close. get_window_geometry_data() itself is real Qt
    geometry logic already covered by test_window_manager.py, so it's stubbed here."""

    def test_captures_tabs_docks_and_floating_window_state(self, monkeypatch):
        class FakeSaveState:
            def data(self):
                return b"savedstate"

        class WidgetWithGetState:
            def get_state(self):
                return {"a": 1}

        class WidgetWithoutGetState:
            tab_params = {"b": 2}

        class FakeCentralTabs:
            def __init__(self, widgets):
                self._widgets = widgets

            def count(self):
                return len(self._widgets)

            def widget(self, i):
                return self._widgets[i][0]

            def tabText(self, i):
                return self._widgets[i][1]

            def currentIndex(self):
                return 1

        class FakeWindowManager:
            def get_windows_state(self):
                return [{"class": "Floating", "name": "F"}]

        class FakeWindowForGetData:
            def __init__(self):
                self.central_tabs = FakeCentralTabs(
                    [(WidgetWithGetState(), "Tab A"), (WidgetWithoutGetState(), "Tab B")]
                )
                self.sources_dock = _FakeVisibleDock(True)
                self.pipelines_dock = _FakeVisibleDock(False)
                self.window_manager = FakeWindowManager()

            def saveState(self):
                return FakeSaveState()

        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.get_window_geometry_data", lambda window: {"frame_pos": [1, 2]}
        )

        handler = UIStateHandler(FakeWindowForGetData())
        data = handler.get_data()

        assert data["open_tabs"] == [
            {"class": "WidgetWithGetState", "name": "Tab A", "params": {"a": 1}},
            {"class": "WidgetWithoutGetState", "name": "Tab B", "params": {"b": 2}},
        ]
        assert data["sources_visible"] is True
        assert data["pipelines_visible"] is False
        assert data["floating_windows"] == [{"class": "Floating", "name": "F"}]
        assert data["current_tab_index"] == 1
        assert data["window_geometry"] == {"frame_pos": [1, 2]}
        assert data["window_state"]  # base64-encoded, non-empty


class _FakeVisibleDock:
    def __init__(self, visible):
        self._visible = visible

    def isVisible(self):
        return self._visible


class TestRestoreWindowGeometry:
    def test_no_saved_geometry_file_completes_immediately(self, tmp_path):
        window = FakeGeoWindow(frame_pos=(0, 0), size=(1, 1))
        handler = UIStateHandler(window)
        missing_file = tmp_path / "gui_state.json"

        calls = []
        handler.restore_window_geometry(missing_file, on_complete=lambda: calls.append(True))

        assert calls == [True]

    def test_corrupt_json_completes_and_prints_error(self, tmp_path, capsys):
        window = FakeGeoWindow(frame_pos=(0, 0), size=(1, 1))
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text("{not valid json")

        calls = []
        handler.restore_window_geometry(state_file, on_complete=lambda: calls.append(True))

        assert calls == [True]
        assert "Could not restore window geometry" in capsys.readouterr().out

    def test_geometry_missing_frame_pos_or_client_size_counts_as_already_settled(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.restore_window_geometry_safe", lambda window, geo: None
        )
        window = FakeGeoWindow(frame_pos=(0, 0), size=(1, 1), visible=True)
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        # Truthy window_geometry dict, but without frame_pos/client_size to compare against -
        # _geometry_settled() treats that as "nothing to check, call it settled".
        state_file.write_text(json.dumps({"window_geometry": {"geometry": "somebase64"}}))

        singleshot_calls = []
        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.QTimer.singleShot",
            lambda ms, cb: singleshot_calls.append((ms, cb)),
        )

        calls = []
        handler.restore_window_geometry(state_file, on_complete=lambda: calls.append(True))

        assert calls == [True]
        assert singleshot_calls == []

    def test_no_window_geometry_key_completes_immediately(self, tmp_path):
        window = FakeGeoWindow(frame_pos=(0, 0), size=(1, 1))
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(json.dumps({}))

        calls = []
        handler.restore_window_geometry(state_file, on_complete=lambda: calls.append(True))

        assert calls == [True]

    def test_already_settled_window_completes_without_scheduling_a_retry(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.restore_window_geometry_safe", lambda window, geo: None
        )
        window = FakeGeoWindow(frame_pos=(10, 20), size=(300, 200), visible=True)
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(json.dumps({"window_geometry": {"frame_pos": [10, 20], "client_size": [300, 200]}}))

        singleshot_calls = []
        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.QTimer.singleShot",
            lambda ms, cb: singleshot_calls.append((ms, cb)),
        )

        calls = []
        handler.restore_window_geometry(state_file, on_complete=lambda: calls.append(True))

        assert calls == [True]
        assert singleshot_calls == []

    def test_a_window_that_never_settles_still_completes_after_exhausting_retries(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "blinkview.ui.utils.ui_state_handler.restore_window_geometry_safe", lambda window, geo: None
        )
        window = FakeGeoWindow(frame_pos=(999, 999), size=(1, 1), visible=True)
        handler = UIStateHandler(window)
        state_file = tmp_path / "gui_state.json"
        state_file.write_text(json.dumps({"window_geometry": {"frame_pos": [10, 20], "client_size": [300, 200]}}))
        # Fire the retry chain synchronously (real recursion, bounded by attempts_left) instead
        # of waiting through 20 real 50ms timers.
        monkeypatch.setattr("blinkview.ui.utils.ui_state_handler.QTimer.singleShot", lambda ms, cb: cb())

        calls = []
        handler.restore_window_geometry(state_file, on_complete=lambda: calls.append(True))

        assert calls == [True]

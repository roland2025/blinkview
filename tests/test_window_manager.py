# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Tests for src/blinkview/ui/utils/window_manager.py - the deadzone-filtered geometry capture/
restore helpers and the WindowManager class that tracks floating (detached) tool windows for
save/restore across sessions."""

import shiboken6
from qtpy.QtWidgets import QWidget

from blinkview.ui.utils.window_manager import WindowManager, get_window_geometry_data, restore_window_geometry_safe


class FakeContent(QWidget):
    """Stand-in for a floating window's inner tool widget (e.g. LogViewerWidget).
    Must be a real QObject/QWidget - WindowManager.register() connects to its `destroyed`
    signal, which a plain Python object doesn't have."""

    def __init__(self, tab_name="Tool", state=None):
        super().__init__()
        self.tab_name = tab_name
        self._state = state if state is not None else {"filter": "abc"}

    def get_state(self):
        return self._state


class TestGetWindowGeometryData:
    def test_first_capture_has_no_deadzone_and_caches_the_result(self, qapp, qtbot):
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(50, 60, 300, 200)
        assert not hasattr(window, "_last_saved_geo")

        data = get_window_geometry_data(window)

        frame = window.frameGeometry()
        assert data["frame_pos"] == [frame.x(), frame.y()]
        assert data["client_size"] == [window.width(), window.height()]
        assert window._last_saved_geo is data

    def test_small_movement_within_deadzone_returns_the_cached_geometry_unchanged(self, qapp, qtbot):
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(50, 60, 300, 200)
        first = get_window_geometry_data(window)

        # Nudge by less than the 15px deadzone threshold on every axis.
        window.setGeometry(55, 63, 305, 197)
        second = get_window_geometry_data(window)

        assert second is first
        assert window._last_saved_geo is first

    def test_large_movement_outside_deadzone_returns_and_caches_new_geometry(self, qapp, qtbot):
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(50, 60, 300, 200)
        first = get_window_geometry_data(window)

        window.setGeometry(500, 500, 640, 480)
        second = get_window_geometry_data(window)

        assert second is not first
        frame = window.frameGeometry()
        assert second["frame_pos"] == [frame.x(), frame.y()]
        assert second["client_size"] == [640, 480]
        assert window._last_saved_geo is second


class TestRestoreWindowGeometrySafe:
    def test_empty_geo_dict_is_a_no_op(self, qapp, qtbot):
        window = QWidget()
        qtbot.addWidget(window)

        restore_window_geometry_safe(window, {})

        assert not hasattr(window, "_last_saved_geo")

    def test_geometry_blob_is_restored_via_restoreGeometry(self, qapp, qtbot):
        source = QWidget()
        qtbot.addWidget(source)
        source.setGeometry(75, 85, 250, 175)
        saved = get_window_geometry_data(source)

        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(0, 0, 100, 100)

        restore_window_geometry_safe(window, saved)

        assert window.width() == 250
        assert window.height() == 175

    def test_frame_pos_and_client_size_move_and_resize_the_window(self, qapp, qtbot):
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(0, 0, 100, 100)

        restore_window_geometry_safe(window, {"frame_pos": [200, 150], "client_size": [400, 300]})

        assert window._last_saved_geo == {"frame_pos": [200, 150], "client_size": [400, 300]}
        assert window.width() == 400
        assert window.height() == 300
        # Under offscreen there's no window-manager decoration, so title_bar_height is 0 and the
        # frame should land exactly on frame_pos.
        frame = window.frameGeometry()
        assert [frame.x(), frame.y()] == [200, 150]

    def test_off_screen_window_with_reattach_hook_schedules_a_reattach(self, qapp, qtbot, monkeypatch):
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(0, 0, 100, 100)

        reattach_calls = []
        window.reattach_to_main = lambda: reattach_calls.append(True)

        monkeypatch.setattr(
            "blinkview.ui.utils.window_manager.QGuiApplication.screenAt", staticmethod(lambda point: None)
        )
        scheduled = []
        monkeypatch.setattr(
            "blinkview.ui.utils.window_manager.QTimer.singleShot", lambda ms, cb: scheduled.append(cb) or cb()
        )

        restore_window_geometry_safe(window, {"frame_pos": [10, 10], "client_size": [50, 50]})

        assert reattach_calls == [True]
        assert len(scheduled) == 1

    def test_off_screen_window_without_reattach_hook_moves_to_primary_screen_center(self, qapp, qtbot, monkeypatch):
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(0, 0, 100, 100)
        assert not hasattr(window, "reattach_to_main")

        monkeypatch.setattr(
            "blinkview.ui.utils.window_manager.QGuiApplication.screenAt", staticmethod(lambda point: None)
        )

        restore_window_geometry_safe(window, {"frame_pos": [10, 10], "client_size": [50, 50]})

        from qtpy.QtGui import QGuiApplication

        primary = QGuiApplication.primaryScreen()
        expected_center = primary.availableGeometry().center() - window.rect().center()
        assert (window.x(), window.y()) == (expected_center.x(), expected_center.y())


class TestWindowManagerRegistration:
    def test_register_and_raise_window_by_tab_name(self, qapp, qtbot):
        wm = WindowManager()
        window = QWidget()
        qtbot.addWidget(window)
        content = FakeContent(tab_name="Logs")
        qtbot.addWidget(content)

        wm.register(window, content)

        assert wm.raise_window("Logs") is True
        assert wm.raise_window("Nonexistent") is False

    def test_deregister_removes_a_tracked_window(self, qapp, qtbot):
        wm = WindowManager()
        window = QWidget()
        qtbot.addWidget(window)
        content = FakeContent()
        qtbot.addWidget(content)
        wm.register(window, content)

        wm.deregister(window)

        assert wm.raise_window(content.tab_name) is False

    def test_deregister_of_an_untracked_window_is_a_no_op(self, qapp, qtbot):
        wm = WindowManager()
        window = QWidget()
        qtbot.addWidget(window)

        wm.deregister(window)  # never registered - must not raise

    def test_window_is_auto_deregistered_when_destroyed(self, qapp, qtbot):
        # Uses shiboken6.delete() for an immediate, synchronous C++ destruction (firing
        # `destroyed` right away) instead of deleteLater() + pumping the event loop for a fixed
        # wall-clock time - the latter is vulnerable to unrelated stray QTimers left behind by
        # other test modules (sharing the same process-wide QApplication) firing during the wait
        # and failing THIS test with an unrelated RuntimeError.
        wm = WindowManager()
        window = QWidget()
        content = FakeContent(tab_name="AutoGone")
        qtbot.addWidget(content)
        wm.register(window, content)
        assert wm.raise_window("AutoGone") is True

        shiboken6.delete(window)

        assert wm.raise_window("AutoGone") is False


class TestGetWindowsState:
    def test_returns_serializable_state_for_each_registered_window(self, qapp, qtbot):
        wm = WindowManager()
        window = QWidget()
        qtbot.addWidget(window)
        window.setGeometry(20, 30, 200, 150)
        window.reattach_on_close = True
        content = FakeContent(tab_name="Watch", state={"expr": "x+1"})
        qtbot.addWidget(content)
        wm.register(window, content)

        states = wm.get_windows_state()

        assert len(states) == 1
        state = states[0]
        assert state["class"] == "FakeContent"
        assert state["name"] == "Watch"
        assert state["params"] == {"expr": "x+1"}
        assert state["reattach_on_close"] is True
        assert "frame_pos" in state["window_geometry"]

    def test_falls_back_to_tab_params_when_content_has_no_get_state(self, qapp, qtbot):
        wm = WindowManager()
        window = QWidget()
        qtbot.addWidget(window)

        class NoGetState(QWidget):
            tab_name = "Plain"
            tab_params = {"a": 1}

        content = NoGetState()
        qtbot.addWidget(content)
        wm.register(window, content)

        states = wm.get_windows_state()

        assert states[0]["params"] == {"a": 1}

    def test_skips_a_window_whose_native_object_was_already_deleted(self, qapp, qtbot):
        wm = WindowManager()

        class DeletedWindow:
            def frameGeometry(self):
                raise RuntimeError("wrapped C++ object of type QWidget has been deleted")

        wm._windows[DeletedWindow()] = FakeContent(tab_name="Ghost")

        states = wm.get_windows_state()

        assert states == []


class TestCloseAll:
    def test_closes_and_clears_all_tracked_windows(self, qapp, qtbot):
        wm = WindowManager()
        w1, w2 = QWidget(), QWidget()
        qtbot.addWidget(w1)
        qtbot.addWidget(w2)
        c1, c2 = FakeContent(tab_name="One"), FakeContent(tab_name="Two")
        qtbot.addWidget(c1)
        qtbot.addWidget(c2)
        wm.register(w1, c1)
        wm.register(w2, c2)

        wm.close_all()

        assert wm._windows == {}

    def test_tolerates_a_window_whose_close_raises(self, qapp, qtbot):
        wm = WindowManager()

        class BrokenWindow:
            def close(self):
                raise RuntimeError("already deleted")

            def deleteLater(self):
                pass

        wm._windows[BrokenWindow()] = FakeContent()

        wm.close_all()  # must not raise

        assert wm._windows == {}

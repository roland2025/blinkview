# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""UIStateHandler.apply_layout() and the View menu on a real BlinkMainWindow
(plans/view-layout-presets.md). apply_layout only *moves* views - it must never recreate one that
is already open, so every "moved" test checks the very same widget object ends up in its new place."""

import copy
from types import SimpleNamespace

import pytest
import shiboken6
from qtpy.QtWidgets import QWidget

import blinkview.ui.main_window as main_window_module
from blinkview.ui.main_window import BlinkMainWindow
from blinkview.ui.windows.detached_tab_window import DetachedTabWindow
from tests.fakes.real_registry import make_real_registry


class FakeView(QWidget):
    def __init__(self, gui_context, params):
        super().__init__()
        self.params = params
        self.tab_name = params.get("tab_name")
        self.signal_destroy = None

    def get_state(self):
        return dict(self.params)


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "view_presets_test")
    yield reg
    reg.stop()


@pytest.fixture
def main_window(qapp, qtbot, registry):
    w = BlinkMainWindow(registry)
    qtbot.addWidget(w)
    w.widget_factories["Fake"] = FakeView
    yield w
    for floating in list(w.window_manager._windows):
        if shiboken6.isValid(floating):
            shiboken6.delete(floating)
    w.window_manager._windows.clear()


def entry(name, **params):
    return {"class": "Fake", "name": name, "params": {"tab_name": name, **params}}


def floating_entry(name, **params):
    return {**entry(name, **params), "window_geometry": {}, "reattach_on_close": True}


def apply(main_window, qtbot, state):
    done = []
    main_window.gui_context.gui_state.apply_layout(state, on_complete=lambda: done.append(True))
    qtbot.waitUntil(lambda: bool(done), timeout=3000)


def tab_names(main_window):
    tabs = main_window.central_tabs
    return [tabs.tabText(i) for i in range(tabs.count())]


class TestApplyLayout:
    def test_tab_is_floated_without_recreating_it(self, main_window, qtbot):
        widget = main_window.create_widget("Fake", "A", params={"tab_name": "A", "content": "live"})

        apply(main_window, qtbot, {"floating_windows": [floating_entry("A", content="preset")]})

        assert tab_names(main_window) == []
        floating, content = main_window.window_manager.find("A")
        assert content is widget
        assert content.params["content"] == "live"
        assert isinstance(floating, DetachedTabWindow)
        assert floating.reattach_on_close is True

    def test_floating_window_is_docked_back_as_a_tab(self, main_window, qtbot):
        floating = main_window.create_widget("Fake", "B", as_window=True, show=False)
        widget = floating.widget

        apply(main_window, qtbot, {"open_tabs": [entry("B")]})

        assert tab_names(main_window) == ["B"]
        assert main_window.central_tabs.widget(0) is widget
        assert main_window.window_manager.find("B") is None

    def test_missing_views_are_created_from_preset_params(self, main_window, qtbot):
        state = {"open_tabs": [entry("C", content="preset")], "floating_windows": [floating_entry("F")]}
        original = copy.deepcopy(state)

        apply(main_window, qtbot, state)

        assert tab_names(main_window) == ["C"]
        assert main_window.central_tabs.widget(0).params["content"] == "preset"
        assert main_window.window_manager.find("F") is not None
        assert state == original  # the stored preset is never mutated by create_widget

    def test_views_not_in_the_preset_are_left_alone(self, main_window, qtbot):
        main_window.create_widget("Fake", "D")
        floating = main_window.create_widget("Fake", "E", as_window=True, show=False)

        apply(main_window, qtbot, {"open_tabs": [entry("X")]})

        assert tab_names(main_window) == ["X", "D"]  # preset tabs first, the rest keep their order after
        assert main_window.window_manager.find("E")[0] is floating

    def test_tab_order_and_current_tab_follow_the_preset(self, main_window, qtbot):
        for name in ["X", "Y", "Z"]:
            main_window.create_widget("Fake", name)

        apply(main_window, qtbot, {"open_tabs": [entry("Z"), entry("X")], "current_tab_index": 1})

        assert tab_names(main_window) == ["Z", "X", "Y"]
        assert main_window.central_tabs.currentIndex() == 1

    def test_already_floating_window_stays_the_same_window(self, main_window, qtbot):
        floating = main_window.create_widget("Fake", "G", as_window=True, show=False)

        apply(main_window, qtbot, {"floating_windows": [floating_entry("G")]})

        assert main_window.window_manager.find("G")[0] is floating
        assert floating.isVisible()
        assert floating.windowOpacity() == 1.0

    def test_dock_visibility_is_applied(self, main_window, qtbot):
        main_window.show()
        apply(main_window, qtbot, {"sources_visible": True, "pipelines_visible": False, "playback_visible": True})

        assert main_window.sources_dock.isVisible()
        assert not main_window.pipelines_dock.isVisible()
        assert main_window.action_view_playback.isChecked()


class TestViewMenu:
    def _labels(self, main_window):
        main_window.populate_view_menu()
        return [(a.text(), a.isEnabled()) for a in main_window.view_menu.actions() if not a.isSeparator()]

    def test_toolbar_order(self, main_window):
        """Menu, View, Rotate, Playback, Devices | Live Logs, System Logs, Telemetry, Watch | rate."""
        labels = []
        for action in main_window.toolbar.actions():
            if action.isSeparator():
                labels.append("|")
                continue
            widget = main_window.toolbar.widgetForAction(action)
            labels.append(widget.text() if hasattr(widget, "text") else action.text())
        labels[-1] = "rate"  # the msg/s label's text changes at runtime

        assert labels == [
            "Menu", "View", "Rotate", "Playback", "Devices", "|",
            "Live Logs", "System Logs", "Telemetry", "Watch", "|", "rate",
        ]  # fmt: skip

    def test_playback_toggle_is_not_duplicated_in_the_view_menu(self, main_window):
        main_window.populate_view_menu()
        assert main_window.action_view_playback not in main_window.view_menu.actions()

    def test_saving_is_disabled_until_the_startup_restore_finishes(self, main_window):
        labels = dict(self._labels(main_window))
        assert labels["Save Current Layout As..."] is False

        main_window._layout_restored = True
        assert dict(self._labels(main_window))["Save Current Layout As..."] is True

    def test_saved_preset_is_listed_and_persisted(self, main_window, registry):
        main_window._layout_restored = True
        main_window.create_widget("Fake", "A")

        main_window._store_view_preset("Desk")

        assert any(text.startswith("Desk") and enabled for text, enabled in self._labels(main_window))
        path = registry.file_manager.get_profile_path("view_presets")
        assert path.exists()
        saved_tabs = main_window.view_presets.get("Desk")["state"]["open_tabs"]
        assert [t["name"] for t in saved_tabs] == ["A"]

    def test_apply_preset_round_trip(self, main_window, qtbot, monkeypatch):
        """Save with A floating, dock it back by hand, apply the preset -> A floats again."""
        messages = []
        monkeypatch.setattr(
            main_window_module.ToastManager, "show", staticmethod(lambda msg, *a, **kw: messages.append(msg))
        )
        main_window._layout_restored = True
        main_window.create_widget("Fake", "A")
        main_window.detach_tab(0)
        main_window._store_view_preset("Floating A")

        floating, widget = main_window.window_manager.find("A")
        main_window.window_manager.deregister(floating)
        floating.reattach_to_main()
        assert tab_names(main_window) == ["A"]

        main_window.apply_view_preset("Floating A")
        # Wait for the whole apply (incl. the delayed window placement) so no timer leaks into the next test.
        qtbot.waitUntil(lambda: "Layout 'Floating A' applied" in messages, timeout=3000)

        assert tab_names(main_window) == []
        assert main_window.window_manager.find("A")[1] is widget


SCREENS_1 = [{"name": "A", "geometry": [0, 0, 1920, 1080]}]
SCREENS_2 = SCREENS_1 + [{"name": "B", "geometry": [1920, 0, 1920, 1080]}]


class TestScreenChangePrompt:
    """_on_screens_settled offers the preset saved for the new screen setup in a 20s toast."""

    @pytest.fixture
    def screens(self, monkeypatch):
        current = {"value": SCREENS_1}
        monkeypatch.setattr(main_window_module, "current_screen_fingerprint", lambda: current["value"])
        return current

    @pytest.fixture
    def toasts(self, monkeypatch):
        shown = []

        class FakeToast(QWidget):
            dismissed = False

            def dismiss(self):
                self.dismissed = True

        def fake_show(message, toast_type=None, duration=None, action_text=None, action_callback=None, **kw):
            toast = FakeToast()
            shown.append(
                SimpleNamespace(
                    message=message,
                    duration=duration,
                    action_text=action_text,
                    action_callback=action_callback,
                    widget=toast,
                )
            )
            return toast

        monkeypatch.setattr(main_window_module.ToastManager, "show", staticmethod(fake_show))
        yield shown
        for t in shown:
            shiboken6.delete(t.widget)

    @pytest.fixture
    def window(self, main_window, screens):
        main_window._last_screens = SCREENS_1
        main_window._layout_restored = True
        main_window.view_presets.save("Desk", {"open_tabs": [entry("A")]}, SCREENS_2)
        return main_window

    def test_offers_matching_preset_for_20_seconds(self, window, screens, toasts):
        screens["value"] = SCREENS_2
        window._on_screens_settled()

        assert len(toasts) == 1
        assert "Desk" in toasts[0].message
        assert toasts[0].duration == 20.0
        assert toasts[0].action_text == "Apply"

    def test_apply_button_applies_the_preset(self, window, screens, toasts, qtbot):
        screens["value"] = SCREENS_2
        window._on_screens_settled()

        toasts[0].action_callback()

        qtbot.waitUntil(lambda: any(t.message == "Layout 'Desk' applied" for t in toasts), timeout=3000)
        assert tab_names(window) == ["A"]

    def test_no_offer_when_the_screens_did_not_really_change(self, window, screens, toasts):
        window._on_screens_settled()
        assert toasts == []

    def test_no_offer_without_a_matching_preset(self, window, screens, toasts):
        screens["value"] = [{"name": "C", "geometry": [0, 0, 800, 600]}]
        window._on_screens_settled()
        assert toasts == []

    def test_no_offer_when_disabled(self, window, screens, toasts):
        window.view_presets.offer_on_screen_change = False
        screens["value"] = SCREENS_2
        window._on_screens_settled()
        assert toasts == []

    def test_no_offer_before_startup_restore_finished(self, window, screens, toasts):
        window._layout_restored = False
        screens["value"] = SCREENS_2
        window._on_screens_settled()
        assert toasts == []

    def test_monitors_off_then_on_again_offers_once_they_are_back(self, window, screens, toasts):
        window._last_screens = SCREENS_2
        screens["value"] = SCREENS_1  # monitor turned off - no preset for a single screen
        window._on_screens_settled()
        assert toasts == []

        screens["value"] = SCREENS_2  # back on
        window._on_screens_settled()
        assert len(toasts) == 1

    def test_a_stale_offer_is_dismissed_when_screens_change_again(self, window, screens, toasts):
        screens["value"] = SCREENS_2
        window._on_screens_settled()
        screens["value"] = SCREENS_1
        window._on_screens_settled()

        assert toasts[0].widget.dismissed

    def test_screen_events_are_debounced(self, window, screens, toasts, qtbot, monkeypatch):
        window._screen_change_timer.setInterval(10)
        screens["value"] = SCREENS_2
        for _ in range(5):
            window._on_screens_changed()
        qtbot.wait(100)

        assert len(toasts) == 1

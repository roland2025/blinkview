# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from base64 import b64decode, b64encode
from copy import deepcopy

from qtpy.QtCore import QByteArray, QTimer
from shiboken6 import isValid

from blinkview.ui.utils.window_manager import get_window_geometry_data, restore_window_geometry_safe


def _is_alive(obj) -> bool:
    """False once a Qt object's C++ side is deleted. Non-Qt stand-ins (tests) count as alive."""
    try:
        return isValid(obj)
    except TypeError:
        return True


class UIStateHandler:
    def __init__(self, main_window):
        self.window = main_window
        self.ui_restored_cb = None

    def get_data(self):
        """Captures geometry and dock states to JSON."""

        # Map open tabs to identifiers
        open_tabs = []
        for i in range(self.window.central_tabs.count()):
            widget = self.window.central_tabs.widget(i)
            tab_text = self.window.central_tabs.tabText(i)

            # Use get_state() if it exists; fallback to tab_params; then empty dict
            if hasattr(widget, "get_state"):
                params = widget.get_state()
            else:
                params = getattr(widget, "tab_params", {})

            tab_settings = {"class": widget.__class__.__name__, "name": tab_text, "params": params}
            open_tabs.append(tab_settings)
        state_data = {
            "window_geometry": get_window_geometry_data(self.window),
            "window_state": b64encode(self.window.saveState().data()).decode("utf-8"),
            "sources_visible": self.window.sources_dock.isVisible(),
            "pipelines_visible": self.window.pipelines_dock.isVisible(),
            # Plain layout widget, not a dock/toolbar, so saveState() doesn't cover it
            "playback_visible": self.window.action_view_playback.isChecked(),
            "open_tabs": open_tabs,
            "floating_windows": self.window.window_manager.get_windows_state(),
            "current_tab_index": self.window.central_tabs.currentIndex(),
        }

        return state_data

    def restore_window_geometry(self, file_path, on_complete=None):
        """Moves/resizes the main window to its last saved position. Kept separate from
        load_ui_state() so it can run as the very first thing on startup, before docks,
        tabs, and floating windows are restored.

        Right after window creation, Windows' DWM can silently ignore or clip an early
        setGeometry() call, so this retries for up to ~1 second until the window has
        actually settled at the target geometry and is visible. on_complete (if given) is
        called once that settling finishes (or immediately if there's nothing to restore),
        so callers can chain the next startup stage off of it."""
        if not file_path.exists():
            if on_complete:
                on_complete()
            return

        try:
            data = json.loads(file_path.read_text())
            geo_dict_window = data.get("window_geometry", {})
        except Exception:
            print("Could not restore window geometry")

            import traceback

            print(traceback.format_exc())
            if on_complete:
                on_complete()
            return

        if not geo_dict_window:
            if on_complete:
                on_complete()
            return

        self._restore_geometry_until_settled(geo_dict_window, attempts_left=20, on_complete=on_complete)

    def _restore_geometry_until_settled(self, geo_dict_window, attempts_left, on_complete=None, interval_ms=50):
        restore_window_geometry_safe(self.window, geo_dict_window)

        if self.window.isVisible() and self._geometry_settled(geo_dict_window):
            if on_complete:
                on_complete()
            return

        if attempts_left <= 0:
            if on_complete:
                on_complete()
            return

        QTimer.singleShot(
            interval_ms,
            lambda: self._restore_geometry_until_settled(geo_dict_window, attempts_left - 1, on_complete, interval_ms),
        )

    def _geometry_settled(self, geo_dict_window, threshold=15):
        """Checks the window actually landed at the saved frame position/size, within the
        same deadzone used elsewhere to tolerate OS-level pixel creep."""
        frame_pos = geo_dict_window.get("frame_pos")
        client_size = geo_dict_window.get("client_size")
        if not frame_pos or not client_size:
            return True

        frame = self.window.frameGeometry()
        dx = abs(frame.x() - frame_pos[0])
        dy = abs(frame.y() - frame_pos[1])
        dw = abs(self.window.width() - client_size[0])
        dh = abs(self.window.height() - client_size[1])
        return dx < threshold and dy < threshold and dw < threshold and dh < threshold

    def load_ui_state(self, file_path, ui_state_restored_cb=None):
        """Restores dock/tab/floating-window states from JSON. Assumes restore_window_geometry()
        has already positioned the main window."""
        self.ui_restored_cb = ui_state_restored_cb

        if not file_path.exists():
            self.on_ui_restoration_complete()
            return

        try:
            data = json.loads(file_path.read_text())

            self._restore_docks(data)

            # --- Restore Central Tabs ---
            if "open_tabs" in data:
                self.window.central_tabs.blockSignals(True)

                for tab_info in data["open_tabs"]:
                    params = tab_info.get("params", {})
                    tab_name = params.get("tab_name") or tab_info.get("name")
                    self.window.create_widget(
                        cls_name=tab_info.get("class"), name=tab_name, as_window=False, params=params
                    )

                self.window.central_tabs.blockSignals(False)

                if "current_tab_index" in data:
                    self.window.central_tabs.setCurrentIndex(data["current_tab_index"])

            # --- Restore Floating Windows ---
            placements = []
            for win_info in data.get("floating_windows", []):
                params = win_info.get("params", {})
                tab_name = params.get("tab_name") or win_info.get("name", "Floating Tool")

                new_win = self.window.create_widget(
                    cls_name=win_info.get("class"),
                    name=tab_name,
                    as_window=True,
                    show=False,
                    params=params,
                    reattach_on_close=win_info.get("reattach_on_close", False),
                )

                # Unknown widget classes (removed/renamed since the layout was saved) are just
                # skipped - they must not hold up on_ui_restoration_complete().
                if new_win:
                    placements.append((new_win, win_info.get("window_geometry", {})))

            self._place_floating_windows(placements, self.on_ui_restoration_complete)

        except Exception:
            print("Could not restore UI state")

            import traceback

            print(traceback.format_exc())

            self.on_ui_restoration_complete()

    def _restore_docks(self, data: dict):
        """Dock/toolbar layout plus the visibility flags saveState() doesn't reliably cover."""
        if "window_state" in data:
            self.window.restoreState(QByteArray(b64decode(data["window_state"])))

        # Explicitly sync dock visibility (if saveState didn't catch it)
        if "sources_visible" in data:
            self.window.sources_dock.setVisible(data["sources_visible"])
        if "pipelines_visible" in data:
            self.window.pipelines_dock.setVisible(data["pipelines_visible"])
        if "playback_visible" in data:
            # Goes through the action so its checked state and the widget stay in sync.
            # Replay mode always shows it, regardless of what the profile saved.
            registry = getattr(getattr(self.window, "gui_context", None), "registry", None)
            replay_mode = getattr(registry, "replay_mode", False)
            self.window.action_view_playback.setChecked(bool(data["playback_visible"] or replay_mode))

    @staticmethod
    def _place_floating_windows(placements, on_complete):
        """Shows each (window, geometry dict) invisibly ("ghost mode"), then moves it into place
        after giving the OS 100ms, so it doesn't visibly jump. on_complete fires once every window
        is placed (immediately if there are none)."""
        remaining = len(placements)
        if remaining == 0:
            on_complete()
            return

        for win, geo_dict in placements:
            win.setWindowOpacity(0.0)
            win.show()

            def place_this_window(win=win, geo_dict=geo_dict):
                nonlocal remaining
                # The window may have been closed (WA_DeleteOnClose) during the 100ms wait.
                if _is_alive(win):
                    if geo_dict:
                        restore_window_geometry_safe(win, geo_dict)

                    win.raise_()
                    win.activateWindow()
                    win.setWindowOpacity(1.0)
                remaining -= 1
                if remaining <= 0:
                    on_complete()

            QTimer.singleShot(100, place_this_window)

    # --- Layout presets (plans/view-layout-presets.md) ---

    def apply_layout(self, state: dict, on_complete=None):
        """Applies a saved layout (same structure as get_data()) to the live UI without closing
        anything: views are matched by name and only moved - floated, docked back into tabs,
        reordered, repositioned - so their contents stay as they are now. Preset views that aren't
        open are created from the preset's params; open views the preset doesn't mention are left
        where they are."""
        done = on_complete or (lambda: None)

        def after_main_window():
            try:
                self._restore_docks(state)
                placements = self._arrange_views(state)
            except Exception:
                import traceback

                print("Could not apply layout preset")
                print(traceback.format_exc())
                placements = []
            self._place_floating_windows(placements, done)

        geo_dict_window = state.get("window_geometry")
        if geo_dict_window:
            # Same settle-retry as startup: DWM can ignore a move across screens, especially for
            # a maximized window.
            self._restore_geometry_until_settled(geo_dict_window, attempts_left=20, on_complete=after_main_window)
        else:
            after_main_window()

    @staticmethod
    def _entry_name(entry: dict):
        return (entry.get("params") or {}).get("tab_name") or entry.get("name")

    def _tab_index(self, name) -> int:
        tabs = self.window.central_tabs
        for i in range(tabs.count()):
            if tabs.tabText(i) == name:
                return i
        return -1

    def _arrange_views(self, state: dict) -> list:
        """Docks/floats/creates views to match `state`. Returns the floating windows still to be
        positioned, as (window, geometry dict) pairs for _place_floating_windows()."""
        win = self.window
        tabs = win.central_tabs
        window_manager = win.window_manager

        # --- Tabs ---
        wanted_tabs = []
        tabs.blockSignals(True)
        try:
            for entry in state.get("open_tabs", []):
                name = self._entry_name(entry)
                if not name:
                    continue
                wanted_tabs.append(name)
                if self._tab_index(name) != -1:
                    continue

                found = window_manager.find(name)
                if found is not None:
                    floating = found[0]
                    # Deregister now rather than on `destroyed` (deleteLater), so the floating pass
                    # below can't find this soon-to-be-dead shell under the same name.
                    window_manager.deregister(floating)
                    floating.reattach_to_main()
                else:
                    win.create_widget(
                        cls_name=entry.get("class"),
                        name=name,
                        as_window=False,
                        params=deepcopy(entry.get("params", {})),
                    )

            # Preset tabs first, in preset order; tabs the preset doesn't know keep their relative
            # order after them.
            target = 0
            for name in wanted_tabs:
                index = self._tab_index(name)
                if index == -1:
                    continue
                if index != target:
                    tabs.tabBar().moveTab(index, target)
                target += 1
        finally:
            tabs.blockSignals(False)

        current = state.get("current_tab_index")
        open_tabs = state.get("open_tabs", [])
        if isinstance(current, int) and 0 <= current < len(open_tabs):
            index = self._tab_index(self._entry_name(open_tabs[current]))
            if index != -1:
                tabs.setCurrentIndex(index)

        # --- Floating windows ---
        placements = []
        for entry in state.get("floating_windows", []):
            name = self._entry_name(entry)
            if not name:
                continue

            found = window_manager.find(name)
            if found is not None:
                floating = found[0]
            else:
                index = self._tab_index(name)
                if index != -1:
                    floating = win.detach_tab(index)
                else:
                    floating = win.create_widget(
                        cls_name=entry.get("class"),
                        name=name,
                        as_window=True,
                        show=False,
                        params=deepcopy(entry.get("params", {})),
                        reattach_on_close=entry.get("reattach_on_close", False),
                    )

            if not floating:
                continue
            if hasattr(floating, "reattach_on_close"):
                floating.reattach_on_close = entry.get("reattach_on_close", False)
            placements.append((floating, entry.get("window_geometry", {})))

        return placements

    def on_ui_restoration_complete(self):
        """This is your callback method."""
        if self.ui_restored_cb:
            self.ui_restored_cb()

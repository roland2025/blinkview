# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from base64 import b64decode, b64encode
from pathlib import Path

from qtpy.QtCore import QByteArray, QPoint, QTimer
from qtpy.QtGui import QGuiApplication

from blinkview.ui.utils.window_manager import get_window_geometry_data, restore_window_geometry_safe
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.utils.atomic_json_dump import atomic_json_dump


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
        if not file_path.exists():
            self.on_ui_restoration_complete()
            return

        self.ui_restored_cb = ui_state_restored_cb

        try:
            data = json.loads(file_path.read_text())

            if "window_state" in data:
                self.window.restoreState(QByteArray(b64decode(data["window_state"])))
            #
            # frame_pos = data.get("frame_pos")
            # # Reapply exact frame position
            # if frame_pos:
            #     self.window.move(QPoint(frame_pos[0], frame_pos[1]))

            # Explicitly sync dock visibility (if saveState didn't catch it)
            if "sources_visible" in data:
                self.window.sources_dock.setVisible(data["sources_visible"])
            if "pipelines_visible" in data:
                self.window.pipelines_dock.setVisible(data["pipelines_visible"])

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
            floating_data = data.get("floating_windows", [])
            windows_to_restore = len(floating_data)
            if windows_to_restore == 0:
                self.on_ui_restoration_complete()
                return
            for win_info in floating_data:
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

                if not new_win:
                    continue  # Skip unknown widgets

                # Ghost Mode
                new_win.setWindowOpacity(0.0)
                new_win.show()

                # Create the closure.
                def restore_this_window(win=new_win, info=win_info):
                    nonlocal windows_to_restore
                    geo_dict = info.get("window_geometry", {})
                    if geo_dict:
                        restore_window_geometry_safe(win, geo_dict)

                    win.raise_()
                    win.activateWindow()
                    win.setWindowOpacity(1.0)
                    windows_to_restore -= 1
                    if windows_to_restore <= 0:
                        self.on_ui_restoration_complete()

                # Give the OS 100ms
                QTimer.singleShot(100, restore_this_window)

        except Exception:
            print("Could not restore UI state")

            import traceback

            print(traceback.format_exc())

            self.on_ui_restoration_complete()

    def on_ui_restoration_complete(self):
        """This is your callback method."""
        if self.ui_restored_cb:
            self.ui_restored_cb()

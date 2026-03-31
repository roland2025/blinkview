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

    def load_ui_state(self, file_path):
        """Restores geometry and dock states from JSON."""
        if not file_path.exists():
            return

        try:
            data = json.loads(file_path.read_text())

            # Restore binary geometry/state
            geo_dict = data.get("window_geometry", {})
            restore_window_geometry_safe(self.window, geo_dict)

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
                if "floating_windows" in data:
                    for win_info in data["floating_windows"]:
                        params = win_info.get("params", {})
                        tab_name = params.get("tab_name") or win_info.get("name", "Floating Tool")
                        new_win = self.window.create_widget(
                            cls_name=win_info.get("class"),
                            name=tab_name,
                            as_window=True,
                            show=False,
                            params=params,
                            reattach_on_close=win_info.get("reattach_on_close", True),
                        )

                        if not new_win:
                            continue  # Skip unknown widgets

                        # Ghost Mode
                        new_win.setWindowOpacity(0.0)
                        new_win.show()

                        # Create the closure.
                        def restore_this_window(win=new_win, info=win_info):
                            geo_dict = info.get("window_geometry", {})
                            if geo_dict:
                                restore_window_geometry_safe(win, geo_dict)

                            win.raise_()
                            win.activateWindow()
                            win.setWindowOpacity(1.0)

                        # Give the OS 100ms
                        QTimer.singleShot(100, restore_this_window)

        except Exception as e:
            print(f"⚠Could not restore UI state: {e}")

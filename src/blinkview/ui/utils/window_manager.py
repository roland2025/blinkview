# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from base64 import b64decode, b64encode

from PySide6.QtCore import QByteArray, QPoint, QTimer
from PySide6.QtGui import QGuiApplication
from PySide6.QtWidgets import QMainWindow


def get_window_geometry_data(window, threshold=15) -> dict:
    """Extracts geometry, applying a deadzone to prevent OS-level pixel creep."""
    frame = window.frameGeometry()
    new_x, new_y = frame.x(), frame.y()
    new_w, new_h = window.width(), window.height()

    new_dict = {
        "geometry": b64encode(window.saveGeometry().data()).decode("utf-8"),
        "frame_pos": [new_x, new_y],
        "client_size": [new_w, new_h],
    }

    # -- THE DEADZONE FILTER --
    old_geo_dict = getattr(window, "_last_saved_geo", None)
    if old_geo_dict:
        old_pos = old_geo_dict.get("frame_pos")
        old_size = old_geo_dict.get("client_size")

        if old_pos and len(old_pos) == 2 and old_size and len(old_size) == 2:
            dx = abs(new_x - old_pos[0])
            dy = abs(new_y - old_pos[1])
            dw = abs(new_w - old_size[0])
            dh = abs(new_h - old_size[1])

            # If the change is purely the creeping bug (under 15px)
            if dx < threshold and dy < threshold and dw < threshold and dh < threshold:
                # The user didn't intentionally move it. Keep the exact old data!
                return old_geo_dict

    # If it moved a lot (user dragged it) or there's no old data, return the new data
    # and update the cache for next time.
    window._last_saved_geo = new_dict
    return new_dict


def restore_window_geometry_safe(window, geo_dict: dict):
    """Atomic geometry restore to prevent Windows DWM from re-calculating margins."""
    if not geo_dict:
        return

    from base64 import b64decode

    from PySide6.QtCore import QByteArray, QRect
    from PySide6.QtGui import QGuiApplication

    window._last_saved_geo = geo_dict

    # 1. Restore Native Geometry (This handles Maximize/Minimize/Fullscreen states)
    geometry_b64 = geo_dict.get("geometry")
    if geometry_b64:
        window.restoreGeometry(QByteArray(b64decode(geometry_b64)))

    # 2. ATOMIC OVERRIDE:
    # Instead of move() then resize(), we use setGeometry() which defines the
    # exact inner rectangle of the window.
    frame_pos = geo_dict.get("frame_pos")
    client_size = geo_dict.get("client_size")

    if frame_pos and client_size:
        # We calculate the Title Bar height once.
        # (frameGeometry.top - geometry.top) gives us the OS border offset.
        title_bar_height = window.frameGeometry().top() - window.geometry().top()

        # Set the EXACT rectangle for the internal part of the window
        window.setGeometry(frame_pos[0], frame_pos[1] - title_bar_height, client_size[0], client_size[1])

    # 3. Off-screen check (unchanged)
    frame = window.frameGeometry()
    if QGuiApplication.screenAt(frame.center()) is None:
        reattach_func = getattr(window, "reattach_to_main", None)

        if reattach_func and callable(reattach_func):
            print(f"🪟 Off-screen window detected. Re-attaching...")
            # Use a timer to ensure we don't conflict with the current geometry event
            QTimer.singleShot(0, reattach_func)
            return
        primary = QGuiApplication.primaryScreen()
        if primary:
            window.move(primary.availableGeometry().center() - window.rect().center())


class WindowManager:
    """Manages secondary windows, tracking their content for state restoration."""

    def __init__(self):
        # We use a dict to map the Window Wrapper -> Content Widget
        self._windows = {}

    def raise_window(self, name):
        """Brings a window with the given content class name to the front."""
        for window, content in self._windows.items():
            print(f"[WindowManager] Checking window {hex(id(window))} with content {content}")
            if content.tab_name == name:
                window.raise_()
                window.activateWindow()
                return True

        return False

    def register(self, window, content_widget):
        """
        Adds a window to the manager.
        :param window: The QMainWindow/QDialog wrapper.
        :param content_widget: The actual tool (e.g., LogViewerWidget) inside.
        """
        self._windows[window] = content_widget

        print(
            f"[WindowManager] Registered {content_widget.__class__.__name__} "
            f"in {hex(id(window))}. Total: {len(self._windows)}"
        )

        # Clean up when the window is closed/destroyed
        window.destroyed.connect(lambda: self.deregister(window))
        content_widget.destroyed.connect(window.close)

    def deregister(self, window):
        """Removes a window from tracking."""
        if window in self._windows:
            self._windows.pop(window)
            print(f"[WindowManager] Deregistered. Total remaining: {len(self._windows)}")

    def get_windows_state(self) -> list:
        """
        Returns a serializable list of dictionaries representing
        all open floating windows and their internal states.
        """
        states = []
        for window, content in self._windows.items():
            # Basic validation to ensure the C++ object still exists
            try:
                # 1. Use get_params() if it exists; fallback to tab_params; then empty dict
                if hasattr(content, "get_state"):
                    params = content.get_state()
                else:
                    params = getattr(content, "tab_params", {})

                states.append(
                    {
                        "class": content.__class__.__name__,
                        "name": getattr(content, "tab_name", "Window"),
                        # --- UPDATED: Use the unified helper with the Deadzone fix ---
                        "window_geometry": get_window_geometry_data(window),
                        # Grab the parameters we defined earlier for restoration
                        "params": params,
                    }
                )
            except RuntimeError:
                continue
        return states

    def close_all(self):
        """Gracefully closes all registered windows."""
        # Work on a copy of keys to avoid 'dict changed size during iteration'
        for window in list(self._windows.keys()):
            try:
                window.close()
                window.deleteLater()
            except (RuntimeError, AttributeError):
                pass
        self._windows.clear()

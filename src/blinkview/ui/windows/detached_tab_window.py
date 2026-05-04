# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import Qt, QTimer
from qtpy.QtWidgets import QMainWindow, QMenu
from shiboken6 import isValid

from blinkview.ui.gui_context import GUIContext
from blinkview.ui.native_dark_mode import set_native_dark_mode


class DetachedTabWindow(QMainWindow):
    """A floating window that holds a detached tab and re-attaches it when closed."""

    def __init__(self, gui_context, widget, title, reattach=False):
        super().__init__(None)
        self.gui_context: GUIContext = gui_context
        self._force_destroy = False
        self.reattach_on_close = reattach

        self.setWindowTitle(f"{title} - BlinkView")
        self.resize(800, 600)
        # Force it to be an independent OS window, even though it has a parent
        self.setWindowFlags(
            Qt.Window
            | Qt.CustomizeWindowHint
            | Qt.WindowTitleHint
            | Qt.WindowSystemMenuHint
            | Qt.WindowMinMaxButtonsHint
            | Qt.WindowCloseButtonHint
        )
        self.setAttribute(Qt.WA_DeleteOnClose)  # Free memory when closed

        set_native_dark_mode(self)

        self.widget = widget
        self.title = title

        # self.setCentralWidget(widget)
        # self.widget.show()

        QTimer.singleShot(0, self._attach_widget_deferred)

    def _attach_widget_deferred(self):
        """Perform the expensive layout and parenting operations."""
        if not self.widget:
            return

        # Disable updates on the window while we cram the widget inside
        self.setUpdatesEnabled(False)
        try:
            self.setCentralWidget(self.widget)
            self.widget.show()
        finally:
            self.setUpdatesEnabled(True)
            self.update()  # Force one clean final draw

    def reattach_to_main(self):
        """Programmatically pops the widget back into the main tabs and closes the shell."""
        if self.widget and self.gui_context.reattach_tab is not None:
            self.widget.setParent(None)
            self.gui_context.reattach_tab(self.widget, self.title)
            self.widget = None  # Clear reference so it doesn't double-fire
        self.close()

    def closeEvent(self, event):
        """Handle window closing, ensuring we don't touch deleted C++ objects."""

        # If we are force-destroying, just clear references and exit
        if self._force_destroy or not self.reattach_on_close:
            if self.widget and isValid(self.widget):
                self.widget.close()

            self.setCentralWidget(None)
            self.widget = None
            event.accept()
            return

        # Re-attach logic (Standard close)
        # Check if the python reference exists AND the C++ object is still alive
        if self.widget is not None and isValid(self.widget):
            try:
                if not self.gui_context.is_shutting_down:
                    # Attempt to move the widget back to main window
                    self.widget.setParent(None)
                    if self.gui_context.reattach_tab is not None:
                        self.gui_context.reattach_tab(self.widget, self.title)
            except RuntimeError:
                # This catches the case where isValid was True but the
                # object died in the millisecond before setParent was called
                print(f"[DetachedTabWindow] Widget for '{self.title}' already deleted. Skipping reattach.")

        # Always clear the reference to allow Python GC to work
        self.widget = None
        event.accept()

    def force_destroy(self):
        """Closes the window and prevents it from re-attaching to main window."""
        self._force_destroy = True

        # Proactively clear the central widget to stop the window from
        # owning/touching the dying widget's memory.
        try:
            if self.widget and isValid(self.widget):
                self.widget.close()
                self.setCentralWidget(None)
        except RuntimeError:
            pass

        self.close()

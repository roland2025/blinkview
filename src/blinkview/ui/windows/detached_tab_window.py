# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtWidgets import QMainWindow, QMenu
from PySide6.QtCore import Qt

from blinkview.ui.gui_context import GUIContext
from blinkview.ui.native_dark_mode import set_native_dark_mode


class DetachedTabWindow(QMainWindow):
    """A floating window that holds a detached tab and re-attaches it when closed."""

    def __init__(self, gui_context, widget, title):
        super().__init__(None)
        self.gui_context: GUIContext = gui_context

        set_native_dark_mode(self)
        # Force it to be an independent OS window, even though it has a parent
        self.setWindowFlags(Qt.Window | Qt.CustomizeWindowHint |
                            Qt.WindowTitleHint | Qt.WindowSystemMenuHint |
                            Qt.WindowMinMaxButtonsHint | Qt.WindowCloseButtonHint)
        self.setAttribute(Qt.WA_DeleteOnClose)  # Free memory when closed

        self.widget = widget
        self.title = title

        self.setWindowTitle(f"{title} - BlinkView")
        self.setCentralWidget(widget)
        self.widget.show()

        self.resize(800, 600)

    def reattach_to_main(self):
        """Programmatically pops the widget back into the main tabs and closes the shell."""
        if self.widget and self.gui_context.reattach_tab is not None:
            self.widget.setParent(None)
            self.gui_context.reattach_tab(self.widget, self.title)
            self.widget = None  # Clear reference so it doesn't double-fire
        self.close()

    def closeEvent(self, event):
        """When the user closes this window, pop the widget back into the main tabs."""
        # Safety check: Don't reattach if the main app is shutting down!

        if self.widget and not self.gui_context.is_shutting_down:
            self.widget.setParent(None)
            if self.gui_context.reattach_tab is not None:
                self.gui_context.reattach_tab(self.widget, self.title)
                self.widget = None

        event.accept()

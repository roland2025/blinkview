# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from PySide6.QtWidgets import QMessageBox


class MessageBox:
    """
    Drop-in replacement for QMessageBox that lazy-loads everything.
    Keeps the 'MessageBox.Btn.Yes' syntax without the startup cost.
    """

    # The Proxy: This intercepts access to MessageBox.Btn
    class _BtnProxy:
        def __getattr__(self, name):
            from PySide6.QtWidgets import QMessageBox

            return getattr(QMessageBox.StandardButton, name)

    Btn: "QMessageBox.StandardButton" = _BtnProxy()

    @staticmethod
    def _show(parent, title, text, icon_type, buttons, default_btn):
        # Local import: Only happens when a dialog is actually shown
        from PySide6.QtWidgets import QMessageBox

        from blinkview.ui.native_dark_mode import set_native_dark_mode

        msg = QMessageBox(parent)
        msg.setWindowTitle(title)
        msg.setText(text)

        # Resolve the Icon from the string/enum
        icon = getattr(QMessageBox.Icon, icon_type)
        msg.setIcon(icon)

        msg.setStandardButtons(buttons)
        if default_btn:
            msg.setDefaultButton(default_btn)

        set_native_dark_mode(msg)
        return msg.exec()

    @staticmethod
    def question(parent, title, text, buttons=None, default_btn=None):
        from PySide6.QtWidgets import QMessageBox

        # Handle Defaults inside the method to avoid definition-time imports
        if buttons is None:
            buttons = QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        if default_btn is None:
            default_btn = QMessageBox.StandardButton.No

        return MessageBox._show(parent, title, text, "Question", buttons, default_btn)

    @staticmethod
    def warning(parent, title, text, buttons=None):
        from PySide6.QtWidgets import QMessageBox

        if buttons is None:
            buttons = QMessageBox.StandardButton.Ok
        return MessageBox._show(parent, title, text, "Warning", buttons, buttons)

    @staticmethod
    def critical(parent, title, text, buttons=None):
        from PySide6.QtWidgets import QMessageBox

        if buttons is None:
            buttons = QMessageBox.StandardButton.Ok
        return MessageBox._show(parent, title, text, "Critical", buttons, buttons)

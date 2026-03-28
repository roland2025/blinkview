# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

from PySide6.QtCore import QObject, Signal, Slot

if TYPE_CHECKING:
    from blinkview.ui.widgets.toast import ToastType


class ToastDispatcher(QObject):
    _instance = None
    # Signal: message, type, duration, action_text, action_cb, click_cb, parent
    _request_signal = Signal(str, dict, int, object, object, object, object)

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        if hasattr(self, "_initialized"):
            return
        super().__init__()
        self._initialized = True
        self._request_signal.connect(self._handle_request)

    @Slot(str, dict, int, object, object, object, object)
    def _handle_request(self, msg, t_type, dur, a_text, a_cb, c_cb, parent):
        from blinkview.ui.widgets.toast import ToastManager

        ToastManager.show(
            message=msg,
            toast_type=t_type,
            duration=dur,
            action_text=a_text,
            action_callback=a_cb,
            click_callback=c_cb,
            parent=parent,  # Now passed correctly to the manager
        )

    def notify(
        self,
        message,
        toast_type: "ToastType" = None,
        duration=5000,
        action_text=None,
        action_callback=None,
        click_callback=None,
        parent=None,  # Added parent support here
    ):
        from blinkview.ui.widgets.toast import ToastType

        self._request_signal.emit(
            message, toast_type or ToastType.INFO, duration, action_text, action_callback, click_callback, parent
        )


toast_dispatcher = ToastDispatcher()

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

from qtpy.QtCore import QObject, Signal, Slot

if TYPE_CHECKING:
    from blinkview.ui.widgets.toast import ToastType


class Singleton(type(QObject)):  # Combine QObject's meta with our singleton meta
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class ToastDispatcher(QObject, metaclass=Singleton):
    # Signal: message, type, duration, action_text, action_cb, click_cb, parent
    _request_signal = Signal(str, dict, float, object, object, object, object)

    def __init__(self, *args, **kwargs):
        # With the Metaclass, __init__ only runs ONCE.
        # No more need for __new__ or hasattr checks.
        super().__init__()
        self._request_signal.connect(self._handle_request)

    @Slot(str, dict, float, object, object, object, object)
    def _handle_request(self, msg, t_type, dur, a_text, a_cb, c_cb, parent):
        from blinkview.ui.widgets.toast import ToastManager

        ToastManager.show(
            message=msg,
            toast_type=t_type,
            duration=dur,
            action_text=a_text,
            action_callback=a_cb,
            click_callback=c_cb,
            parent=parent,
        )

    def notify(self, message, toast_type=None, duration=5.0, **kwargs):
        from blinkview.ui.widgets.toast import ToastType

        # Use .emit so it's thread-safe (in case notify is called from a worker)
        self._request_signal.emit(
            message,
            toast_type or ToastType.INFO,
            duration,
            kwargs.get("action_text"),
            kwargs.get("action_callback"),
            kwargs.get("click_callback"),
            kwargs.get("parent"),
        )


# Create the global instance
toast_dispatcher = ToastDispatcher()

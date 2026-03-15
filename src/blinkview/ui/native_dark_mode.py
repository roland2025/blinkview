# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def set_native_dark_mode(window):
    import sys
    if sys.platform != "win32":
        return  # Dark mode attribute is only supported on Windows 10 and later

    # This specifically tells Windows to use the Dark mode attribute for the title bar
    # 20 = DWMWA_USE_IMMERSIVE_DARK_MODE
    hwnd = window.winId()
    import ctypes
    value = ctypes.c_int(1)
    ctypes.windll.dwmapi.DwmSetWindowAttribute(
        hwnd, 20, ctypes.byref(value), ctypes.sizeof(value)
    )

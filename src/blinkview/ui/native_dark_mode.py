# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def set_native_dark_mode(window):
    import ctypes
    import sys
    from ctypes import wintypes

    if sys.platform != "win32":
        return

    # Get the HWND from the Qt window
    # Cast to int to ensure it's a clean Python integer
    hwnd = int(window.winId())

    # Define the function signature properly
    # This tells ctypes exactly how to handle 64-bit pointers
    dwmapi = ctypes.windll.dwmapi
    dwmapi.DwmSetWindowAttribute.argtypes = [
        wintypes.HWND,  # hwnd
        wintypes.DWORD,  # dwAttribute
        wintypes.LPCVOID,  # pvAttribute
        wintypes.DWORD,  # cbAttribute
    ]
    dwmapi.DwmSetWindowAttribute.restype = ctypes.HRESULT

    # Call the function
    # DWMWA_USE_IMMERSIVE_DARK_MODE = 20
    # Note: On some older Win10 builds, the attribute was 19
    attr = 20
    value = ctypes.c_int(1)

    try:
        dwmapi.DwmSetWindowAttribute(
            hwnd, attr, ctypes.byref(value), ctypes.sizeof(value)
        )
    except Exception as e:
        print(f"Failed to set dark mode: {e}")

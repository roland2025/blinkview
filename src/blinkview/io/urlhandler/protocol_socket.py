# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""socket:// with an in_waiting that counts bytes.

pyserial's own handler (serial/urlhandler/protocol_socket.py, pyserial 3.5) returns
len(select(...)) from in_waiting: 0 or 1, never the number of bytes queued
(https://github.com/pyserial/pyserial/issues/759). UARTReader reads `in_waiting` bytes after
its lead byte, so over socket:// it moved 2 bytes per loop pass and topped out at ~285 KB/s with
the reader thread at 100% CPU, while the same loopback carries ~867 MB/s with read(4096).

This subclass only replaces in_waiting, with FIONREAD (ioctlsocket on Windows, ioctl elsewhere):
the bytes the OS has queued for the socket, without reading or copying them. If that call
fails, it falls back to pyserial's select() answer, so it is never worse than upstream.
"""

import sys

from serial.serialutil import PortNotOpenError
from serial.urlhandler import protocol_socket as _upstream

if sys.platform == "win32":
    import ctypes
    from ctypes import wintypes

    _ws2_32 = ctypes.WinDLL("ws2_32")
    _ioctlsocket = _ws2_32.ioctlsocket
    _ioctlsocket.argtypes = [ctypes.c_size_t, ctypes.c_long, ctypes.POINTER(wintypes.ULONG)]  # SOCKET is UINT_PTR
    _ioctlsocket.restype = ctypes.c_int
    _FIONREAD = 0x4004667F

    def _queued_bytes(sock):
        n = wintypes.ULONG()
        if _ioctlsocket(sock.fileno(), _FIONREAD, ctypes.byref(n)) != 0:
            return None
        return n.value

else:
    import array
    import fcntl
    import termios

    def _queued_bytes(sock):
        buf = array.array("i", [0])
        fcntl.ioctl(sock.fileno(), termios.FIONREAD, buf, True)
        return buf[0]


class Serial(_upstream.Serial):
    """pyserial's socket:// Serial with a byte-counting in_waiting."""

    @property
    def in_waiting(self):
        if not self.is_open:
            raise PortNotOpenError()
        try:
            n = _queued_bytes(self._socket)
        except (OSError, AttributeError, ValueError):
            n = None
        if n is None:
            return super().in_waiting
        return n

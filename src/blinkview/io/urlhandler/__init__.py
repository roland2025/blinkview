# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""BlinkView's own pyserial URL handlers, found through pyserial's protocol_handler_packages.

serial_for_url("<scheme>://...") imports `.protocol_<scheme>` from each package in
serial.protocol_handler_packages in order and uses the first one that imports. install() puts
this package first, so the schemes defined here (only socket:// for now) replace pyserial's,
and every other scheme (rfc2217://, loop://, ...) still falls through to pyserial's own.
"""

from threading import Lock

PACKAGE = __name__

# Readers open their ports on their own threads; serializes the one edit of pyserial's global list.
_install_lock = Lock()
_installed = False


def install() -> None:
    """Puts this package first in serial.protocol_handler_packages, once per process. Idempotent
    and thread-safe; call it before serial_for_url()."""
    global _installed
    if _installed:
        return

    import serial

    with _install_lock:
        if _installed:
            return
        packages = serial.protocol_handler_packages
        if packages[:1] != [PACKAGE]:
            if PACKAGE in packages:
                packages.remove(PACKAGE)
            packages.insert(0, PACKAGE)
        _installed = True

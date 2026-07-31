# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


def esp32_wifi(id_registry):
    """Returns (device, module) for the canonical esp32/wifi pair used throughout
    log-fetch/log-table-viewer tests."""
    device = id_registry.get_device("esp32")
    return device, device.get_module("wifi")

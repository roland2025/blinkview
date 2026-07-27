# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

# Every submodule here registers a Factory type (@XFactory.register(...)) or a factory category
# (@register_factory_category(...)) as an import-time side effect. The "as x" re-export alias on
# each is deliberate, not decorative: it's the standard pyflakes/ruff/pyright signal for "this
# import is an intentional public re-export, not an accidental unused one" - see
# core/factory_category_registry.py's module docstring for the registration mechanism itself.
from . import adb_reader as adb_reader
from . import adb_time_syncer as adb_time_syncer
from . import benchmark as benchmark
from . import binary_file_reader as binary_file_reader
from . import can_bus as can_bus
from . import file_tail_reader as file_tail_reader
from . import logging as logging
from . import rtt as rtt
from . import serial_time_syncer as serial_time_syncer
from . import tcp_client as tcp_client
from . import tcp_server as tcp_server
from . import uart as uart
from . import udp_reader as udp_reader

__all__ = [
    "adb_reader",
    "adb_time_syncer",
    "benchmark",
    "binary_file_reader",
    "can_bus",
    "file_tail_reader",
    "logging",
    "rtt",
    "serial_time_syncer",
    "tcp_client",
    "tcp_server",
    "uart",
    "udp_reader",
]

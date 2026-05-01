# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Optional

from blinkview.core.device_identity import DeviceIdentity, ModuleIdentity
from blinkview.utils.log_level import LevelIdentity, LogLevel


class LogFilter:
    def __init__(
        self, id_registry, allowed_device=None, filtered_module=None, log_level=None, filtered_module_children=False
    ):
        self.registry = id_registry

        self.filter_index = None

        self.allowed_device: Optional[DeviceIdentity] = id_registry.resolve_device(allowed_device)
        self.filtered_module: Optional[ModuleIdentity] = id_registry.resolve_module(filtered_module)
        self.filtered_module_children = filtered_module_children
        self.log_level: Optional[LevelIdentity] = LogLevel.from_string(log_level, LogLevel.ALL)

    def set_level(self, log_level):
        self.log_level = LogLevel.from_string(log_level)

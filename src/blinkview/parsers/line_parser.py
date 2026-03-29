# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from ..core.configurable import configuration_property
from ..core.device_identity import DeviceIdentity
from ..core.id_registry import IDRegistry
from ..core.log_row import LogRow
from ..core.system_context import SystemContext
from ..utils.level_map import LogLevel
from ..utils.settings_updater import update_object_from_config
from .assembler import AssemblerFactory, BaseAssembler


@AssemblerFactory.register("default")
@configuration_property(
    "time_index",
    title="Time Index",
    type="integer",
    minimum=0,
    ui_order=1,
    description="Index of the timestamp in the log line (0-based). If not set, the timestamp from the reader will be used.",
)
@configuration_property(
    "level_index",
    title="Level Index",
    type="integer",
    minimum=0,
    ui_order=2,
    description="Index of the log level in the log line (0-based). If not set, INFO level will be used.",
)
@configuration_property(
    "module_index",
    title="Module Index",
    type="integer",
    minimum=0,
    ui_order=3,
    description="Index of the module name in the log line (0-based). If not set, module will be set to 'unknown'.",
)
@configuration_property(
    "message_index",
    title="Message Index",
    type="integer",
    minimum=0,
    ui_order=4,
    default=0,
    description="Index of the log message in the log line (0-based). This is required.",
)
@configuration_property(
    "level_map",
    type="object",
    ui_order=5,
    title="Level Mapping Strategy",
    _factory="log_level_map",  # Points to your new factory category
    _factory_default="default",  # Default to your standard char-map
    required=True,
)
class LineParser(BaseAssembler):
    def __init__(self):
        super().__init__()
        self.local_level_map = None
        self.time_idx = None
        self.level_idx = None
        self.module_idx = None
        self.message_idx = 0

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        self.time_idx = config.get("time_index")
        self.level_idx = config.get("level_index")
        self.module_idx = config.get("module_index")
        self.message_idx = config.get("message_index", 0)

        level_map_config = config.get("level_map")
        if level_map_config:
            self.local_level_map = self.shared.factories.build(
                "log_level_map", level_map_config, self.shared
            )
        else:
            self.local_level_map = None

        self._bake()

        return changed

    def _bake(self):

        # Cache frequently used objects locally
        get_level_obj = (
            self.local_level_map.get_level
            if self.local_level_map
            else self.shared.id_registry.level_map.get_level
        )
        LogRowCtor = LogRow

        m_idx = self.module_idx
        l_idx = self.level_idx
        t_idx = self.time_idx
        msg_idx = self.message_idx

        info_level = LogLevel.INFO

        def fast_parse(timestamp: int, dev_id: DeviceIdentity, line: str):
            parts = line.split(maxsplit=msg_idx)

            # timestamp
            time_val = parts[t_idx] if t_idx is not None else timestamp

            # level
            level_val = get_level_obj(parts[l_idx]) if l_idx is not None else info_level
            if level_val is None:
                raise ValueError(f"Unknown log level: {parts[l_idx]}")

            # module
            if m_idx is not None:
                mod_id = dev_id.get_module(parts[m_idx].rstrip(":"))
            else:
                mod_id = dev_id.get_module("_unknown")

            return LogRowCtor(time_val, level_val, mod_id, parts[msg_idx])

        self.process = fast_parse

    def process(self, timestamp, dev_id, line):
        raise RuntimeError("ID Registry must be set before parsing.")

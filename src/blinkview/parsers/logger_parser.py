# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.parsers.assembler import AssemblerFactory, BaseAssembler

from ..core.base_configurable import (
    configuration_factory,
    configuration_property,
    override_property,
)
from ..core.base_daemon import BaseDaemon
from ..core.device_identity import DeviceIdentity
from ..core.log_row import LogRow
from ..utils.level_map import LogLevel
from .parser import ParserFactory, ParserThread


@AssemblerFactory.register("log_record")
class LogRecordToLogRow(BaseAssembler):
    __doc__ = "Converts msgpack-encoded log lines into LogRow objects. Expects the msgpack format to be: (created, levelno, name, msg)."

    def __init__(self):
        super().__init__()

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        self._bake()
        return changed

    def _bake(self):
        # Cache frequently used objects locally
        level_from_int = LogLevel.from_value
        LogRowCtor = LogRow

        from logging import LogRecord

        def fast_parse(created: int, dev_id: DeviceIdentity, line: LogRecord):
            return LogRowCtor(
                created,
                level_from_int(line.levelno),
                dev_id.get_module(line.name),
                line.getMessage(),
            )

        self.process = fast_parse

    def process(self, timestamp, dev_id, line):
        raise RuntimeError("ID Registry must be set before parsing.")


@ParserFactory.register("logging_parser")
@override_property("split", hidden=True)
@override_property("printable", hidden=True)
@override_property("decode", hidden=True)
@override_property("transform", hidden=True)
@override_property("assembler", hidden=True, default={"type": "log_record"})
@configuration_property(
    "sources_", title="Source", type="string", required=True, _reference="/sources"
)
class LoggerParser(ParserThread):
    __doc__ = (
        "A parser that converts LogRecord objects into LogRow objects."
        "Expects input from sources that emit LogRecord objects (e.g., log readers)."
    )

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import msgpack

from blinkview.parsers.assembler import AssemblerFactory, BaseAssembler
from .parser import ParserFactory, ParserThread
from ..core.base_configurable import override_property, configuration_property
from ..core.device_identity import DeviceIdentity
from ..core.log_row import LogRow
from ..utils.level_map import LogLevel


@AssemblerFactory.register("msgpack")
class MsgPackToLogRow(BaseAssembler):
    __doc__ = "Converts msgpack-encoded log lines into LogRow objects. Expects the msgpack format to be: (created, levelno, name, msg)."

    def __init__(self):
        super().__init__()

    def apply_config(self, config: dict):
        super().apply_config(config)
        self._bake()

    def _bake(self):

        # Cache frequently used objects locally
        level_from_int = LogLevel.from_value
        LogRowCtor = LogRow
        unpackb = msgpack.unpackb

        def fast_parse(_: int, dev_id: DeviceIdentity, line: bytes):
            created, levelno, name, msg = unpackb(line, use_list=False)

            return LogRowCtor(created, level_from_int(levelno), dev_id.get_module(name), msg)

        self.process = fast_parse

    def process(self, timestamp, dev_id, line):
        raise RuntimeError("ID Registry must be set before parsing.")


@ParserFactory.register("msgpack_parser")
@override_property("split", hidden=True, default={"char": 0})
@override_property("printable", hidden=True)
@override_property("decode", hidden=True, default={"type": "cobs_decode"})
@override_property("transform", hidden=True)
@override_property("assembler", hidden=True, default={"type": "msgpack"})
@configuration_property("sources_", title="Source", type="string", required=True, _reference="/sources")
class MsgPackParser(ParserThread):
    __doc__ = "A parser that converts msgpack-encoded log lines into LogRow objects."

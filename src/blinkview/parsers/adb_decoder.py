# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Optional

from blinkview.core import dtypes
from blinkview.core.configurable import override_property
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.modules import DynamicWidthConfig
from blinkview.core.types.parsing import (
    CodecID,
    EmptyUnifiedParserConfig,
    EmptyUnifiedParserState,
    ParserID,
    UnifiedParserConfig,
)
from blinkview.ops.codec_adb_long import decode_adb_long_frame
from blinkview.ops.constants import CHAR_LBRACKET, CHAR_LF, CHAR_ZERO
from blinkview.parsers.frame_decoders import FrameDecoder, FrameDecoderFactory
from blinkview.parsers.frame_parsers import (
    FrameSectionParser,
    FrameSectionParserFactory,
    ModuleNameParserBase,
    TimestampParser,
)
from blinkview.utils.log_level import LogLevel


@FrameDecoderFactory.register("decode_adb_long_frame")
@override_property("frame_delimiter", default=CHAR_LF)
@override_property("frame_length_maximum", default=32 * 1024)
class AdbDecoder(FrameDecoder):
    """Frame processor for SLIP-encoded frames"""

    def __init__(self):
        super().__init__()
        self.codec_id = CodecID.ADB_LONG
        self.decode = decode_adb_long_frame


@FrameSectionParserFactory.register("module_name_adb_long_frame")
class AdbModuleName(ModuleNameParserBase):
    """This parser extracts module names from variable-width fields by scanning for common delimiters (spaces, tabs, brackets) and normalizing them. It is designed for log formats where module names may be of varying lengths and may include hierarchical components separated by dots or enclosed in brackets."""

    max_length: int
    max_depth: int
    enable_brackets: bool
    enable_dot_separator: bool

    def __init__(self):
        super().__init__()

    def bundle(self):
        # 1. Build the IMMUTABLE config snapshot
        # Note: 'tracker' is removed from here.
        config = UnifiedParserConfig(
            string_table=self.local.device_id.modules_table.bundle(),
            module_config=DynamicWidthConfig(
                max_length=128,
                max_depth=3,
            ),
        )

        # 2. Return the universal 3-tuple: (Function, Mutable State, Immutable Config)
        return ParserID.MOD_ADB_LONG, self.tracker_state, config


@FrameSectionParserFactory.register("timestamp_adb_long_frame")
class AdbLongTimestamp(TimestampParser):
    def __init__(self):
        super().__init__()

        self._bundle = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        self._bundle = ParserID.TS_ADB_LONG, self.state, EmptyUnifiedParserConfig

        return changed

    def bundle(self):
        return self._bundle


@FrameSectionParserFactory.register("process_pid_tid_adb_long_frame")
class AdbPidTid(FrameSectionParser):
    def __init__(self):
        super().__init__()

        self._bundle = ParserID.PID_TID_ADB_LONG, EmptyUnifiedParserState, EmptyUnifiedParserConfig

    def bundle(self):
        return self._bundle


@FrameSectionParserFactory.register("log_level_adb_long_frame")
class LevelMap(FrameSectionParser):
    def __init__(self):
        super().__init__()

        # ADB standard single-letter codes mapped to your LogLevel values
        # Note: ADB 'V' is Verbose (Trace), 'S' is Silent (Off)
        levels = {
            "V": LogLevel.TRACE.value,
            "D": LogLevel.DEBUG.value,
            "I": LogLevel.INFO.value,
            "W": LogLevel.WARN.value,
            "E": LogLevel.ERROR.value,
            "F": LogLevel.FATAL.value,
            "S": LogLevel.OFF.value,
        }

        self._table: Optional[IndexedStringTable] = IndexedStringTable(
            initial_capacity=len(levels), buffer_size_bytes=len(levels) * 8, values_dtype=dtypes.VALUES_TYPE
        )

        # Fix: enumerate(levels.items()) is required for dict iteration
        for i, (text, level_val) in enumerate(levels.items()):
            # Register string at index i
            self._table.register_name(i, text, level_val)

        self._bundle = (
            ParserID.LEVEL_MAP_ADB_LONG,
            EmptyUnifiedParserState,
            UnifiedParserConfig(string_table=self._table.bundle()),
        )

    def bundle(self):
        """Returns the StringTableParams for backend processing."""
        return self._bundle

    def release(self):
        """Explicitly release pool resources."""
        if self._table:
            self._table.release()
            self._table = None
            self._bundle = None

    def __del__(self):
        self.release()

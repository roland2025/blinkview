# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Optional

from blinkview.core import dtypes
from blinkview.core.configurable import override_property
from blinkview.core.frame_warmup_registry import frame_decoder_warmup, frame_section_warmup
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.parsing import CodecID
from blinkview.ops.codec_adb_long import (
    nb_decode_adb_long_frame,
    nb_decode_frames_adb_long,
    nb_parse_adb_level_stage,
    nb_parse_adb_pid_tid_stage,
    nb_parse_adb_tag_stage,
    nb_parse_adb_timestamp_monotonic_stage,
)
from blinkview.ops.constants import CHAR_LF
from blinkview.parsers.frame_decoders import FrameDecoder, FrameDecoderFactory
from blinkview.parsers.frame_parsers import (
    FrameSectionParser,
    FrameSectionParserFactory,
    ModuleNameParserBase,
    TimestampParser,
)
from blinkview.utils.log_level import LogLevel


@FrameDecoderFactory.register("decode_adb_long_frame")
@frame_decoder_warmup("decode_adb_long_frame", warmup_weight=4.0)
@override_property("frame_delimiter", default=CHAR_LF)
@override_property("frame_length_maximum", default=32 * 1024)
@override_property("frame_resync_on_start", default=False)  # logcat output always starts on a line boundary
class AdbDecoder(FrameDecoder):
    """Splits `adb logcat -v long` output into messages: a '[ ... ]' header line and the text lines after it."""

    def __init__(self):
        super().__init__()
        self.codec_id = CodecID.ADB_LONG
        self.decode = nb_decode_adb_long_frame
        self._kernel = nb_decode_frames_adb_long


@FrameSectionParserFactory.register("module_name_adb_long_frame")
@frame_section_warmup("module_name_adb_long_frame")
class AdbModuleName(ModuleNameParserBase):
    """Reads the tag of an `adb logcat -v long` header as the module name."""

    max_length: int
    max_depth: int
    enable_brackets: bool
    enable_dot_separator: bool

    def __init__(self):
        super().__init__()

    def kernel(self, octx, fctx, n):
        nb_parse_adb_tag_stage(octx, fctx, n, self.tracker_state, self.table_bundle())


@FrameSectionParserFactory.register("timestamp_adb_long_frame")
@frame_section_warmup("timestamp_adb_long_frame")
class AdbLongTimestamp(TimestampParser):
    """Reads the monotonic timestamp that opens an `adb logcat -v long` header, e.g. '[ 1726228.928'."""

    def kernel(self, octx, fctx, n):
        nb_parse_adb_timestamp_monotonic_stage(octx, fctx, n, self.sync_state)


@FrameSectionParserFactory.register("process_pid_tid_adb_long_frame")
@frame_section_warmup("process_pid_tid_adb_long_frame")
class AdbPidTid(FrameSectionParser):
    """Reads the process and thread id of an `adb logcat -v long` header, e.g. '2680: 2701'."""

    def kernel(self, octx, fctx, n):
        nb_parse_adb_pid_tid_stage(octx, fctx, n)


@FrameSectionParserFactory.register("log_level_adb_long_frame")
@frame_section_warmup("log_level_adb_long_frame")
class LevelMap(FrameSectionParser):
    """Reads the level letter of an `adb logcat -v long` header: V, D, I, W, E, F or S."""

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

        self._table_bundle = self._table.bundle()

    def kernel(self, octx, fctx, n):
        nb_parse_adb_level_stage(octx, fctx, n, self._table_bundle)

    def release(self):
        """Explicitly release pool resources."""
        if self._table:
            self._table.release()
            self._table = None

    def __del__(self):
        self.release()

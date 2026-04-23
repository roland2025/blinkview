# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple, Tuple

import numpy as np

from blinkview.core.id_registry.types import EmptyStringTableParams, StringTableParams
from blinkview.core.types.empty import ZERO_UTC_OFFSET
from blinkview.core.types.modules import (
    DynamicWidthConfig,
    EmptyDynamicWidthConfig,
    EmptyModuleTrackerState,
    ModuleTrackerState,
)


class ParserID:
    # --- Category Bases ---
    CAT_TEMPORAL = 100
    CAT_IDENTITY = 200
    CAT_CLASSIFICATION = 300
    CAT_STRUCTURAL = 400
    CAT_SANITIZATION = 500
    CAT_PLUGIN_V1 = 1000

    # --- 100: Temporal ---
    TS_UNIX_SEC = CAT_TEMPORAL + 0
    TS_UNIX_MS = CAT_TEMPORAL + 1
    TS_ISO8601 = CAT_TEMPORAL + 2
    TS_CUSTOM_STRFTIME = CAT_TEMPORAL + 3

    TS_ADB_LONG = CAT_TEMPORAL + 4

    # --- 200: Identity ---
    MOD_FIXED_WIDTH = CAT_IDENTITY + 0
    MOD_DYNAMIC_SM = CAT_IDENTITY + 1
    MOD_BRACKETED = CAT_IDENTITY + 2
    MOD_ADB_LONG = CAT_IDENTITY + 3

    DEVICE_ID_STATIC = CAT_IDENTITY + 10

    PID_TID_ADB_LONG = CAT_IDENTITY + 20

    # --- 300: Classification ---
    LEVEL_NAME_MAP = CAT_CLASSIFICATION + 0

    LEVEL_MAP_ADB_LONG = CAT_CLASSIFICATION + 2

    # --- 400: Structural ---
    SKIP_WORDS = CAT_STRUCTURAL + 0


class CodecID:
    NONE = 0
    NEWLINE = 10
    COBS = 20
    SLIP = 30
    ADB_LONG = 40

    # 99 is reserved for custom/plugin decoders
    PLUGIN = 99


STATE_COMPLETE = 0
STATE_INCOMPLETE = 1
STATE_ERROR = 2


class ParserConfig(NamedTuple):
    level_default: int = 0
    level_error: int = 0
    module_unknown: int = 0
    module_log: int = 0
    device_id: int = 0
    report_error: bool = False
    filter_squash_spaces: bool = False


EmptyParserConfig = ParserConfig()


class InputParams(NamedTuple):
    ts_in: int  # Timestamp for this batch
    split_char: int  # Delimiter (e.g., ord('\n'))
    def_lvl: int  # Default Level
    def_mod: int  # Default Module ID
    def_dev: int  # Default Device ID


class LogOutput(NamedTuple):
    ts: np.ndarray  # int64
    off: np.ndarray  # uint32
    lengths: np.ndarray  # uint32
    buf: np.ndarray  # uint8
    # Optional columns (Pass empty arrays if not used)
    lvl: np.ndarray  # uint8
    mod: np.ndarray  # uint16
    dev: np.ndarray  # uint16
    seq: np.ndarray  # uint64
    # Status flags
    has_lvl: bool
    has_mod: bool
    has_dev: bool
    has_seq: bool


class TimeParserState(NamedTuple):
    utc_offset: np.ndarray = ZERO_UTC_OFFSET  # int64[:]  # utc seconds


EmptyTimeParserState = TimeParserState()


class UnifiedParserState(NamedTuple):
    modules: ModuleTrackerState = EmptyModuleTrackerState
    timestamp: TimeParserState = EmptyTimeParserState


EmptyUnifiedParserState = UnifiedParserState()


class UnifiedParserConfig(NamedTuple):
    parser_id: int = 0

    # --- Main Config Defaults ---
    parser_config: ParserConfig = EmptyParserConfig

    # --- StringTable / Level Mapping Defaults ---
    # We use our 'Immortal' empty arrays as defaults
    string_table: StringTableParams = EmptyStringTableParams

    # --- Module Name Defaults ---
    module_config: DynamicWidthConfig = EmptyDynamicWidthConfig


EmptyUnifiedParserConfig = UnifiedParserConfig()


class ParserPipelineBundle(NamedTuple):
    """
    The complete, bundled state required for the Parser logic in the Numba kernel.
    """

    config: ParserConfig
    pipeline: Tuple[Tuple[int, UnifiedParserState, UnifiedParserConfig], ...]
    # pipeline: Tuple[Tuple[Callable, Any, Any], ...]

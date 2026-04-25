# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from numba import types
from numba.extending import overload

from blinkview.core.id_registry.types import StringTableParams
from blinkview.core.numba_config import app_njit, literal_unroll
from blinkview.core.types.modules import DynamicWidthConfig, FixedWidthConfig
from blinkview.core.types.parsing import ParserID
from blinkview.ops.codec_adb_long import (
    parse_adb_level,
    parse_adb_pid_tid,
    parse_adb_tag,
    parse_adb_timestamp_iso,
    parse_adb_timestamp_monotonic,
)
from blinkview.ops.generic import SkipWordsConfig, skip_words_parser
from blinkview.ops.levels import parse_log_level
from blinkview.ops.modules import parse_fixed_width_name, parse_module_tags_statemachine

# --- Extract Categories for Numba ---
_CAT_IDENTITY = ParserID.CAT_IDENTITY
_CAT_CLASSIFICATION = ParserID.CAT_CLASSIFICATION
_CAT_STRUCTURAL = ParserID.CAT_STRUCTURAL
_CAT_SANITIZATION = ParserID.CAT_SANITIZATION
_CAT_PLUGIN = ParserID.CAT_PLUGIN_V1

# --- Extract Specific IDs for Numba ---
_ID_MOD_FIXED = ParserID.MOD_FIXED_WIDTH
_ID_MOD_DYNAMIC = ParserID.MOD_DYNAMIC_SM
_ID_LEVEL_MAP = ParserID.LEVEL_NAME_MAP
_ID_SKIP_WORDS = ParserID.SKIP_WORDS

_ID_MOD_ADB_LONG = ParserID.MOD_ADB_LONG

_ID_TS_ADB_LONG = ParserID.TS_ADB_LONG

_ID_PID_TID_ADB_LONG = ParserID.PID_TID_ADB_LONG
_ID_LEVEL_MAP_ADB_LONG = ParserID.LEVEL_MAP_ADB_LONG


@app_njit()
def execute_parser_pipeline(buffer, start_cursor, end_cursor, out_b, out_idx, parser_bundles):
    # parser_bundles is now a standard homogeneous List or Tuple
    if len(parser_bundles) == 0:
        return start_cursor

    cursor = start_cursor

    # for bundle in literal_unroll(parser_bundles): # 20% faster
    for bundle in parser_bundles:  # 20% slower
        p_id = bundle[0]
        state = bundle[1]  # ALWAYS UnifiedParserState
        config = bundle[2]  # ALWAYS UnifiedParserConfig
        if p_id == _ID_LEVEL_MAP:
            cursor = parse_log_level(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_MOD_FIXED:
            cursor = parse_fixed_width_name(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_MOD_DYNAMIC:
            cursor = parse_module_tags_statemachine(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_SKIP_WORDS:
            cursor = skip_words_parser(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_TS_ADB_LONG:
            cursor = parse_adb_timestamp_monotonic(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_PID_TID_ADB_LONG:
            cursor = parse_adb_pid_tid(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_LEVEL_MAP_ADB_LONG:
            cursor = parse_adb_level(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        elif p_id == _ID_MOD_ADB_LONG:
            cursor = parse_adb_tag(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        else:
            return -1

        if cursor == -1:
            return -1

    return cursor

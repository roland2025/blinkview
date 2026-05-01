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
    parse_adb_timestamp_monotonic,
)
from blinkview.ops.generic import skip_words_parser
from blinkview.ops.levels import parse_log_level
from blinkview.ops.modules import parse_fixed_width_name, parse_module_tags_statemachine
from blinkview.ops.timestamps import nb_parse_int_timestamp
from blinkview.ops.zephyr_timestamp import nb_parse_zephyr_uptime_formatted

# --- Extract Categories for Numba ---
_CAT_IDENTITY = ParserID.CAT_IDENTITY
_CAT_CLASSIFICATION = ParserID.CAT_CLASSIFICATION
_CAT_STRUCTURAL = ParserID.CAT_STRUCTURAL
_CAT_SANITIZATION = ParserID.CAT_SANITIZATION
_CAT_PLUGIN = ParserID.CAT_PLUGIN_V1

# --- Extract Specific IDs for Numba ---
MOD_FIXED_WIDTH = ParserID.MOD_FIXED_WIDTH
MOD_DYNAMIC_SM = ParserID.MOD_DYNAMIC_SM
LEVEL_NAME_MAP = ParserID.LEVEL_NAME_MAP
SKIP_WORDS = ParserID.SKIP_WORDS

MOD_ADB_LONG = ParserID.MOD_ADB_LONG

TS_ADB_LONG = ParserID.TS_ADB_LONG
TS_ZEPHYR_UPTIME_FORMATTED = ParserID.TS_ZEPHYR_UPTIME_FORMATTED
TS_INTEGER = ParserID.TS_INTEGER

PID_TID_ADB_LONG = ParserID.PID_TID_ADB_LONG
LEVEL_MAP_ADB_LONG = ParserID.LEVEL_MAP_ADB_LONG


@app_njit(inline="always")  # Force inline just to be absolutely certain, though Numba usually does this automatically
def _process_bundle(buffer, cursor, end_cursor, out_b, out_idx, bundle):
    p_id = bundle[0]
    state = bundle[1]  # ALWAYS UnifiedParserState
    config = bundle[2]  # ALWAYS UnifiedParserConfig

    if p_id == LEVEL_NAME_MAP:
        return parse_log_level(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == MOD_FIXED_WIDTH:
        return parse_fixed_width_name(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == MOD_DYNAMIC_SM:
        return parse_module_tags_statemachine(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == SKIP_WORDS:
        return skip_words_parser(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_INTEGER:
        return nb_parse_int_timestamp(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_ADB_LONG:
        return parse_adb_timestamp_monotonic(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_ZEPHYR_UPTIME_FORMATTED:
        return nb_parse_zephyr_uptime_formatted(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == PID_TID_ADB_LONG:
        return parse_adb_pid_tid(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == LEVEL_MAP_ADB_LONG:
        return parse_adb_level(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == MOD_ADB_LONG:
        return parse_adb_tag(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    # Fallback if an unknown p_id is passed
    return -1


@app_njit()
def execute_parser_pipeline_(buffer, start_cursor, end_cursor, out_b, out_idx, parser_bundles):
    # parser_bundles is now a standard homogeneous List or Tuple
    if len(parser_bundles) == 0:
        return start_cursor

    cursor = start_cursor

    for bundle in literal_unroll(parser_bundles):  # 20% faster
        # for bundle in parser_bundles:  # 20% slower
        cursor = _process_bundle(buffer, cursor, end_cursor, out_b, out_idx, bundle)

        if cursor == -1:
            return -1

    return cursor


@app_njit()
def execute_parser_pipeline(buffer, start_cursor, end_cursor, out_b, out_idx, parser_bundles):
    # parser_bundles is now a standard homogeneous List or Tuple
    length = len(parser_bundles)
    if length == 0:
        return start_cursor

    cursor = start_cursor

    if length > 0:
        cursor = _process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[0])
        if cursor == -1:
            return -1
    if length > 1:
        cursor = _process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[1])
        if cursor == -1:
            return -1
    if length > 2:
        cursor = _process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[2])
        if cursor == -1:
            return -1
    if length > 3:
        cursor = _process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[3])
        if cursor == -1:
            return -1

    return cursor

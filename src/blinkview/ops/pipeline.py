# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


from blinkview.core.numba_config import app_njit, literal_unroll
from blinkview.core.types.parsing import ParserID
from blinkview.ops.codec_adb_long import (
    nb_parse_adb_level,
    nb_parse_adb_pid_tid,
    nb_parse_adb_tag,
    nb_parse_adb_timestamp_monotonic,
)
from blinkview.ops.desktop_timestamp import nb_parse_iso8601_desktop, nb_parse_syslog_timestamp
from blinkview.ops.generic import nb_skip_words_parser
from blinkview.ops.levels import nb_parse_log_level
from blinkview.ops.modules import nb_parse_fixed_width_name, nb_parse_module_tags_statemachine
from blinkview.ops.timestamp_idf import nb_parse_int_timestamp_idf_v1
from blinkview.ops.timestamps import nb_parse_int_timestamp
from blinkview.ops.zephyr_timestamp import nb_parse_zephyr_realtime, nb_parse_zephyr_uptime_formatted

# --- Extract Specific IDs for Numba ---
MOD_FIXED_WIDTH = ParserID.MOD_FIXED_WIDTH
MOD_DYNAMIC_SM = ParserID.MOD_DYNAMIC_SM
LEVEL_NAME_MAP = ParserID.LEVEL_NAME_MAP
SKIP_WORDS = ParserID.SKIP_WORDS

MOD_ADB_LONG = ParserID.MOD_ADB_LONG

TS_ADB_LONG = ParserID.TS_ADB_LONG
TS_ZEPHYR_UPTIME_FORMATTED = ParserID.TS_ZEPHYR_UPTIME_FORMATTED
TS_ZEPHYR_REALTIME = ParserID.TS_ZEPHYR_REALTIME
TS_INTEGER = ParserID.TS_INTEGER
TS_IDF_V1 = ParserID.TS_IDF_V1
TS_ISO8601 = ParserID.TS_ISO8601
TS_SYSLOG = ParserID.TS_SYSLOG

PID_TID_ADB_LONG = ParserID.PID_TID_ADB_LONG
LEVEL_MAP_ADB_LONG = ParserID.LEVEL_MAP_ADB_LONG


@app_njit()
def nb_process_bundle(buffer, cursor, end_cursor, out_b, out_idx, bundle):
    p_id = bundle[0]
    state = bundle[1]  # ALWAYS UnifiedParserState
    config = bundle[2]  # ALWAYS UnifiedParserConfig

    if p_id == LEVEL_NAME_MAP:
        return nb_parse_log_level(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == MOD_FIXED_WIDTH:
        return nb_parse_fixed_width_name(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == MOD_DYNAMIC_SM:
        return nb_parse_module_tags_statemachine(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == SKIP_WORDS:
        return nb_skip_words_parser(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_INTEGER:
        return nb_parse_int_timestamp(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_IDF_V1:
        return nb_parse_int_timestamp_idf_v1(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_ADB_LONG:
        return nb_parse_adb_timestamp_monotonic(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_ZEPHYR_UPTIME_FORMATTED:
        return nb_parse_zephyr_uptime_formatted(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_ZEPHYR_REALTIME:
        return nb_parse_zephyr_realtime(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_ISO8601:
        return nb_parse_iso8601_desktop(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == TS_SYSLOG:
        return nb_parse_syslog_timestamp(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == PID_TID_ADB_LONG:
        return nb_parse_adb_pid_tid(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == LEVEL_MAP_ADB_LONG:
        return nb_parse_adb_level(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    elif p_id == MOD_ADB_LONG:
        return nb_parse_adb_tag(buffer, cursor, end_cursor, out_b, out_idx, state, config)

    # Fallback if an unknown p_id is passed
    return -1


@app_njit(inline="always")
def nb_execute_parser_pipeline(buffer, start_cursor, end_cursor, out_b, out_idx, parser_bundles):
    # parser_bundles is now a standard homogeneous List or Tuple
    if len(parser_bundles) == 0:
        return start_cursor

    cursor = start_cursor

    # for bundle in literal_unroll(parser_bundles):  # very slow compile times, maybe faster
    for bundle in parser_bundles:
        cursor = nb_process_bundle(buffer, cursor, end_cursor, out_b, out_idx, bundle)

        if cursor == -1:
            return -1

    return cursor


@app_njit()
def nb_execute_parser_pipeline_(buffer, start_cursor, end_cursor, out_b, out_idx, parser_bundles):
    # parser_bundles is now a standard homogeneous List or Tuple
    length = len(parser_bundles)
    if length == 0:
        return start_cursor

    cursor = start_cursor

    if length > 0:
        cursor = nb_process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[0])
        if cursor == -1:
            return -1
    if length > 1:
        cursor = nb_process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[1])
        if cursor == -1:
            return -1
    if length > 2:
        cursor = nb_process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[2])
        if cursor == -1:
            return -1
    if length > 3:
        cursor = nb_process_bundle(buffer, cursor, end_cursor, out_b, out_idx, parser_bundles[3])
        if cursor == -1:
            return -1

    return cursor

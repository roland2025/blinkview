# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.constants import CHAR_LPAREN, CHAR_RPAREN
from blinkview.ops.stage_loop import FS_OK, FS_STEP_FAILED
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.timestamps import nb_parse_int_timestamp_optimized
from blinkview.ops.views import nb_log_bundle_views, nb_sync_views, nb_view


@app_njit(inline="always")
def nb_parse_int_timestamp_idf_v1_optimized(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    sync,
    precision,
    timestamp_unix,
):
    cursor = start_cursor

    # Check and consume the opening parenthesis '('
    if cursor >= end_cursor or buffer[cursor] != CHAR_LPAREN:
        return -1
    cursor += 1  # Move past '('

    # Delegate to the core integer parser
    cursor = nb_parse_int_timestamp_optimized(
        buffer, cursor, end_cursor, out_b, out_idx, sync, precision, timestamp_unix
    )

    # If the inner parser failed, propagate the error
    if cursor == -1:
        return -1

    if cursor >= end_cursor or buffer[cursor] != CHAR_RPAREN:
        return -1
    cursor += 1  # Move past ')'

    return nb_skip_whitespace(buffer, cursor, end_cursor)


@app_njit(inline="always")
def nb_parse_int_timestamp_idf_v1(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
    return nb_parse_int_timestamp_idf_v1_optimized(
        buffer,
        start_cursor,
        end_cursor,
        out_b,
        out_idx,
        state.timestamp.sync,
        config.timestamp_precision,
        config.timestamp_unix,
    )


@app_njit()
def nb_parse_int_timestamp_idf_v1_stage(out_b0, f_state, n, sync0, precision, timestamp_unix):
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    sync = nb_sync_views(sync0)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = nb_parse_int_timestamp_idf_v1_optimized(
                buffer, cur[k], end[k], out_b, first + k, sync, precision, timestamp_unix
            )
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r

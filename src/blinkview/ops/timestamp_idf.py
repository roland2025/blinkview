# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.constants import CHAR_LPAREN, CHAR_RPAREN, CHAR_SPACE
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.timestamps import nb_parse_int_timestamp


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
    cursor = start_cursor

    # Check and consume the opening parenthesis '('
    if cursor >= end_cursor or buffer[cursor] != CHAR_LPAREN:
        return -1
    cursor += 1  # Move past '('

    # 3. Delegate to the core integer parser
    cursor = nb_parse_int_timestamp(
        buffer,
        cursor,
        end_cursor,
        out_b,
        out_idx,
        state,
        config,
    )

    # If the inner parser failed, propagate the error
    if cursor == -1:
        return -1

    if cursor >= end_cursor or buffer[cursor] != CHAR_RPAREN:
        return -1
    cursor += 1  # Move past ')'

    return nb_skip_whitespace(buffer, cursor, end_cursor)

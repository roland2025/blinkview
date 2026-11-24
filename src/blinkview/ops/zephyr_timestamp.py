# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.ops.constants import (
    CHAR_COLON,
    CHAR_COMMA,
    CHAR_DASH,
    CHAR_DOT,
    CHAR_LBRACKET,
    CHAR_NINE,
    CHAR_RBRACKET,
    CHAR_SPACE,
    CHAR_ZERO,
)
from blinkview.ops.stage_loop import nb_stage_loop_a1
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.timestamps import nb_project_synced_ns
from blinkview.ops.views import nb_sync_views


@app_njit(inline="always")
def nb_parse_zephyr_uptime_formatted(buffer, start_cursor, end_cursor, out_b, out_idx, sync):
    if start_cursor + 10 > end_cursor or buffer[start_cursor] != CHAR_LBRACKET:
        return -1

    s = start_cursor
    if (
        s + 18 <= end_cursor
        and buffer[s + 3] == CHAR_COLON
        and buffer[s + 6] == CHAR_COLON
        and buffer[s + 9] == CHAR_DOT
        and buffer[s + 13] == CHAR_COMMA
        and buffer[s + 17] == CHAR_RBRACKET
    ):
        # Fast path, the layout Zephyr prints: '[hh:mm:ss.mmm,uuu]' with a digit at every other place. The
        # general loop below gives the same value for it, so only the layout and the digits are checked.
        d1, d2 = np.int64(buffer[s + 1]) - CHAR_ZERO, np.int64(buffer[s + 2]) - CHAR_ZERO
        d4, d5 = np.int64(buffer[s + 4]) - CHAR_ZERO, np.int64(buffer[s + 5]) - CHAR_ZERO
        d7, d8 = np.int64(buffer[s + 7]) - CHAR_ZERO, np.int64(buffer[s + 8]) - CHAR_ZERO
        d10, d11, d12 = (
            np.int64(buffer[s + 10]) - CHAR_ZERO,
            np.int64(buffer[s + 11]) - CHAR_ZERO,
            np.int64(buffer[s + 12]) - CHAR_ZERO,
        )
        d14, d15, d16 = (
            np.int64(buffer[s + 14]) - CHAR_ZERO,
            np.int64(buffer[s + 15]) - CHAR_ZERO,
            np.int64(buffer[s + 16]) - CHAR_ZERO,
        )
        # A digit d is 0..9, so d and 9 - d are both >= 0: OR them all, and any byte that is not a digit
        # makes the result negative.
        signs = d1 | d2 | d4 | d5 | d7 | d8 | d10 | d11 | d12 | d14 | d15 | d16
        signs |= (9 - d1) | (9 - d2) | (9 - d4) | (9 - d5) | (9 - d7) | (9 - d8)
        signs |= (9 - d10) | (9 - d11) | (9 - d12) | (9 - d14) | (9 - d15) | (9 - d16)
        if signs >= 0:
            raw_ns = (
                (d1 * 10 + d2) * 3_600_000_000_000
                + (d4 * 10 + d5) * 60_000_000_000
                + (d7 * 10 + d8) * 1_000_000_000
                + (d10 * 100 + d11 * 10 + d12) * 1_000_000
                + (d14 * 100 + d15 * 10 + d16) * 1_000
            )
            rx_ns = out_b.rx_timestamps[out_idx]
            out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, sync)
            return nb_skip_whitespace(buffer, s + 18, end_cursor)

    ts_end = -1
    for i in range(start_cursor + 1, end_cursor):
        if buffer[i] == CHAR_RBRACKET:
            ts_end = i
            break
    if ts_end == -1:
        return -1

    raw_ns = 0
    current_val = 0
    parse_state = 0  # 0:H, 1:M, 2:S, 3:ms, 4:us

    for i in range(start_cursor + 1, ts_end):
        c = buffer[i]

        if CHAR_ZERO <= c <= CHAR_NINE:
            current_val = current_val * 10 + (c - CHAR_ZERO)
        elif c == CHAR_COLON:
            if parse_state == 0:
                raw_ns += current_val * 3_600_000_000_000
                parse_state = 1
            elif parse_state == 1:
                raw_ns += current_val * 60_000_000_000
                parse_state = 2
            current_val = 0
        elif c == CHAR_DOT:
            raw_ns += current_val * 1_000_000_000
            parse_state = 3
            current_val = 0
        elif c == CHAR_COMMA:
            raw_ns += current_val * 1_000_000
            parse_state = 4
            current_val = 0
        elif c == CHAR_SPACE:
            continue  # Robustness against [ 2:50...]

    # Final piece: Microseconds
    if parse_state == 4:
        raw_ns += current_val * 1_000
    else:
        # If your format is sometimes [S.ms,us] without H:M, handle fallback here
        return -1

    # This is the 'now' timestamp the Reader captured when it read the chunk.
    rx_ns = out_b.rx_timestamps[out_idx]
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, sync)

    # Move cursor past the closing bracket ']' and skip trailing whitespace
    return nb_skip_whitespace(buffer, ts_end + 1, end_cursor)


@app_njit()
def nb_parse_zephyr_uptime_formatted_stage(out_b0, f_state, n, sync0):
    nb_stage_loop_a1(nb_parse_zephyr_uptime_formatted, out_b0, f_state, n, nb_sync_views(sync0))


@app_njit(inline="always")
def nb_parse_zephyr_realtime(buffer, start_cursor, end_cursor, out_b, out_idx, sync):
    # Minimum valid length for whole timestamp block is 29 characters:
    # e.g., '[1970-01-01 00:00:00.000,000]'
    if start_cursor + 29 > end_cursor or buffer[start_cursor] != CHAR_LBRACKET:
        return -1

    # Verify basic formatting landmarks (Dashes and Space)
    if (
        buffer[start_cursor + 5] != CHAR_DASH
        or buffer[start_cursor + 8] != CHAR_DASH
        or buffer[start_cursor + 11] != CHAR_SPACE
    ):
        return -1

    # 1. Direct slice conversion for the Date parts
    year = (
        (buffer[start_cursor + 1] - CHAR_ZERO) * 1000
        + (buffer[start_cursor + 2] - CHAR_ZERO) * 100
        + (buffer[start_cursor + 3] - CHAR_ZERO) * 10
        + (buffer[start_cursor + 4] - CHAR_ZERO)
    )
    month = (buffer[start_cursor + 6] - CHAR_ZERO) * 10 + (buffer[start_cursor + 7] - CHAR_ZERO)
    day = (buffer[start_cursor + 9] - CHAR_ZERO) * 10 + (buffer[start_cursor + 10] - CHAR_ZERO)

    # 2. Branchless Epoch Calculation (Howard Hinnant algorithm)
    if month < 3:
        year -= 1
        month += 12

    era = (year if year >= 0 else year - 399) // 400
    yofea = year - era * 400
    doy = (153 * (month - 3) + 2) // 5 + day - 1
    doe = yofea * 365 + yofea // 4 - yofea // 100 + doy
    epoch_days = era * 146097 + doe - 719468

    raw_ns = epoch_days * 86_400_000_000_000

    # 3. Single-Pass State Machine for Time + Bracket Search
    current_val = 0
    parse_state = 0  # 0:H, 1:M, 2:S, 3:ms, 4:us
    ts_end = -1

    for i in range(start_cursor + 12, end_cursor):
        c = buffer[i]

        if CHAR_ZERO <= c <= CHAR_NINE:
            current_val = current_val * 10 + (c - CHAR_ZERO)
        elif c == CHAR_COLON:
            if parse_state == 0:
                raw_ns += current_val * 3_600_000_000_000
                parse_state = 1
            elif parse_state == 1:
                raw_ns += current_val * 60_000_000_000
                parse_state = 2
            current_val = 0
        elif c == CHAR_DOT:
            raw_ns += current_val * 1_000_000_000
            parse_state = 3
            current_val = 0
        elif c == CHAR_COMMA:
            raw_ns += current_val * 1_000_000
            parse_state = 4
            current_val = 0
        elif c == CHAR_RBRACKET:
            ts_end = i
            break  # Exit the loop immediately when the bracket is found

    if ts_end == -1:
        return -1  # Reached end_cursor without finding ']'

    # Final piece: Microseconds accumulator
    if parse_state == 4:
        raw_ns += current_val * 1_000
    else:
        return -1

    # 4. Timeline Synchronization
    rx_ns = out_b.rx_timestamps[out_idx]
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, sync)

    # 5. Advance Cursor
    return nb_skip_whitespace(buffer, ts_end + 1, end_cursor)


@app_njit()
def nb_parse_zephyr_realtime_stage(out_b0, f_state, n, sync0):
    nb_stage_loop_a1(nb_parse_zephyr_realtime, out_b0, f_state, n, nb_sync_views(sync0))

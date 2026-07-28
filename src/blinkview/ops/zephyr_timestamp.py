# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

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
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.timestamps import nb_project_synced_ns


@app_njit(inline="always")
def nb_parse_zephyr_uptime_formatted(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
    if start_cursor + 10 > end_cursor or buffer[start_cursor] != CHAR_LBRACKET:
        return -1

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
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, state.timestamp.sync)

    # Move cursor past the closing bracket ']' and skip trailing whitespace
    return nb_skip_whitespace(buffer, ts_end + 1, end_cursor)


@app_njit(inline="always")
def nb_parse_zephyr_realtime(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
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
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, state.timestamp.sync)

    # 5. Advance Cursor
    return nb_skip_whitespace(buffer, ts_end + 1, end_cursor)

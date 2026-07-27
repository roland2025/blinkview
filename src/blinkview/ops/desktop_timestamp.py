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
    CHAR_NINE,
    CHAR_SPACE,
    CHAR_ZERO,
)
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.timestamps import nb_parse_iso8601_to_ns, nb_project_synced_ns


@app_njit(inline="always")
def nb_parse_iso8601_desktop(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
    """
    Parses 'YYYY-MM-DD HH:MM:SS[.,]fff' at the cursor - no bracket wrapper,
    accepting either a dot (ISO8601) or comma (Python logging/log4j) fraction
    separator. Fixed 23-byte width, e.g. "2026-01-15 10:23:01,456".
    """
    if start_cursor + 23 > end_cursor:
        return -1

    if (
        buffer[start_cursor + 4] != CHAR_DASH
        or buffer[start_cursor + 7] != CHAR_DASH
        or buffer[start_cursor + 10] != CHAR_SPACE
        or buffer[start_cursor + 13] != CHAR_COLON
        or buffer[start_cursor + 16] != CHAR_COLON
    ):
        return -1

    sep = buffer[start_cursor + 19]
    if sep != CHAR_DOT and sep != CHAR_COMMA:
        return -1

    raw_ns = nb_parse_iso8601_to_ns(buffer, start_cursor, 0)

    rx_ns = out_b.rx_timestamps[out_idx]
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, state.timestamp.sync)

    return nb_skip_whitespace(buffer, start_cursor + 23, end_cursor)


@app_njit(inline="always")
def nb_parse_syslog_timestamp(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
    """
    Parses classic RFC3164 syslog timestamps: 'Mon DD HH:MM:SS' (day is
    space- or zero-padded), e.g. "Jan  2 15:04:05". Fixed 15-byte width.
    Has no year field, so the year is taken from `config.syslog_year`.
    """
    if start_cursor + 15 > end_cursor:
        return -1

    m0 = buffer[start_cursor]
    m1 = buffer[start_cursor + 1]
    m2 = buffer[start_cursor + 2]

    if m0 == 74 and m1 == 97 and m2 == 110:  # Jan
        month = 1
    elif m0 == 70 and m1 == 101 and m2 == 98:  # Feb
        month = 2
    elif m0 == 77 and m1 == 97 and m2 == 114:  # Mar
        month = 3
    elif m0 == 65 and m1 == 112 and m2 == 114:  # Apr
        month = 4
    elif m0 == 77 and m1 == 97 and m2 == 121:  # May
        month = 5
    elif m0 == 74 and m1 == 117 and m2 == 110:  # Jun
        month = 6
    elif m0 == 74 and m1 == 117 and m2 == 108:  # Jul
        month = 7
    elif m0 == 65 and m1 == 117 and m2 == 103:  # Aug
        month = 8
    elif m0 == 83 and m1 == 101 and m2 == 112:  # Sep
        month = 9
    elif m0 == 79 and m1 == 99 and m2 == 116:  # Oct
        month = 10
    elif m0 == 78 and m1 == 111 and m2 == 118:  # Nov
        month = 11
    elif m0 == 68 and m1 == 101 and m2 == 99:  # Dec
        month = 12
    else:
        return -1

    if buffer[start_cursor + 3] != CHAR_SPACE or buffer[start_cursor + 6] != CHAR_SPACE:
        return -1

    day_tens_c = buffer[start_cursor + 4]
    if day_tens_c == CHAR_SPACE:
        day_tens = 0
    elif CHAR_ZERO <= day_tens_c <= CHAR_NINE:
        day_tens = day_tens_c - CHAR_ZERO
    else:
        return -1

    day_ones_c = buffer[start_cursor + 5]
    if not (CHAR_ZERO <= day_ones_c <= CHAR_NINE):
        return -1
    day = day_tens * 10 + (day_ones_c - CHAR_ZERO)

    if buffer[start_cursor + 9] != CHAR_COLON or buffer[start_cursor + 12] != CHAR_COLON:
        return -1

    hh = (buffer[start_cursor + 7] - CHAR_ZERO) * 10 + (buffer[start_cursor + 8] - CHAR_ZERO)
    mm = (buffer[start_cursor + 10] - CHAR_ZERO) * 10 + (buffer[start_cursor + 11] - CHAR_ZERO)
    ss = (buffer[start_cursor + 13] - CHAR_ZERO) * 10 + (buffer[start_cursor + 14] - CHAR_ZERO)

    # Howard Hinnant civil_from_days epoch calculation
    year = config.syslog_year
    y = year
    mo = month
    if mo < 3:
        y -= 1
        mo += 12

    era = (y if y >= 0 else y - 399) // 400
    yofea = y - era * 400
    doy = (153 * (mo - 3) + 2) // 5 + day - 1
    doe = yofea * 365 + yofea // 4 - yofea // 100 + doy
    epoch_days = era * 146097 + doe - 719468

    raw_ns = epoch_days * 86_400_000_000_000 + (hh * 3600 + mm * 60 + ss) * 1_000_000_000

    rx_ns = out_b.rx_timestamps[out_idx]
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, state.timestamp.sync)

    return nb_skip_whitespace(buffer, start_cursor + 15, end_cursor)

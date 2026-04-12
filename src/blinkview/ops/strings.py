# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.constants import (
    CHAR_CR,
    CHAR_ESC,
    CHAR_LBRACKET,
    CHAR_LF,
    CHAR_LOWER_A,
    CHAR_LOWER_Z,
    CHAR_NINE,
    CHAR_RBRACKET,
    CHAR_SPACE,
    CHAR_TAB,
    CHAR_UPPER_A,
    CHAR_UPPER_Z,
    CHAR_ZERO,
)


@app_njit()
def is_whitespace(char):
    # Covers Space, Tab, LF, CR
    return char == CHAR_SPACE or char == CHAR_TAB or char == CHAR_LF or char == CHAR_CR


@app_njit()
def is_digit(char):
    return CHAR_ZERO <= char <= CHAR_NINE


@app_njit()
def is_alpha(char):
    return (CHAR_UPPER_A <= char <= CHAR_UPPER_Z) or (CHAR_LOWER_A <= char <= CHAR_LOWER_Z)


@app_njit()
def to_lower(char):
    """Converts uppercase ASCII to lowercase using the bitwise trick."""
    if CHAR_UPPER_A <= char <= CHAR_UPPER_Z:
        return char | 32  # Set the 6th bit
    return char


@app_njit()
def to_upper(char):
    """Converts lowercase ASCII to uppercase using the bitwise trick."""
    if CHAR_LOWER_A <= char <= CHAR_LOWER_Z:
        return char & ~32  # Clear the 6th bit
    return char


@app_njit()
def filter_printable_inplace(out_buf, start_cursor, end_cursor):
    """
    Sweeps through the decoded payload and drops non-printable characters.
    Because it writes to the same buffer it reads from, it operates
    with zero memory allocation overhead.
    """
    write_cursor = start_cursor
    for i in range(start_cursor, end_cursor):
        val = out_buf[i]
        # Standard printable ASCII range
        if CHAR_SPACE <= val <= 126:
            out_buf[write_cursor] = val
            write_cursor += 1

    return write_cursor


@app_njit()
def filter_ansi_inplace(out_buf, start_cursor, end_cursor):
    """
    Sweeps through the decoded payload and drops ANSI escape sequences
    (specifically standard CSI sequences like `ESC [ ... m`).
    Operates in-place with zero memory allocation.
    """
    write_cursor = start_cursor
    state = 0  # 0: normal text, 1: seen ESC, 2: inside CSI sequence

    for i in range(start_cursor, end_cursor):
        val = out_buf[i]

        if state == 0:
            if val == CHAR_ESC:
                state = 1
            else:
                out_buf[write_cursor] = val
                write_cursor += 1

        elif state == 1:
            if val == CHAR_LBRACKET:
                state = 2
            else:
                # Not a CSI sequence. Drop the orphan ESC, keep this byte, and reset.
                state = 0
                out_buf[write_cursor] = val
                write_cursor += 1

        elif state == 2:
            # CSI sequences end with a byte in the range 0x40-0x7E (64-126)
            # Intermediate/parameter bytes are 0x20-0x3F, which we just skip.
            if 64 <= val <= 126:
                state = 0  # Sequence finished

    return write_cursor


@app_njit()
def squash_spaces_inplace(buffer, start_cursor, end_cursor):
    """
    1. Skips leading spaces.
    2. Squashes internal consecutive spaces into a single space.
    3. Strips trailing spaces.
    Returns (new_start, new_end).
    """
    # 1. Skip leading spaces
    read_idx = start_cursor
    while read_idx < end_cursor and buffer[read_idx] == CHAR_SPACE:
        read_idx += 1

    # If the whole thing was spaces, return start, start (zero length)
    if read_idx == end_cursor:
        return start_cursor, start_cursor

    write_idx = start_cursor
    last_was_space = False

    # 2. Process the rest
    for i in range(read_idx, end_cursor):
        val = buffer[i]
        if val == CHAR_SPACE:
            if not last_was_space:
                buffer[write_idx] = val
                write_idx += 1
                last_was_space = True
        else:
            buffer[write_idx] = val
            write_idx += 1
            last_was_space = False

    # 3. Strip trailing space
    # If the last character written was a space, back up one.
    if write_idx > start_cursor and buffer[write_idx - 1] == CHAR_SPACE:
        write_idx -= 1

    return start_cursor, write_idx


@app_njit()
def trim_spaces(buffer, start_cursor, end_cursor):
    """
    Slices off leading and trailing spaces without copying.
    Returns (new_start, new_end).
    """
    # Trim trailing first
    while end_cursor > start_cursor and buffer[end_cursor - 1] == CHAR_SPACE:
        end_cursor -= 1

    # Trim leading
    while start_cursor < end_cursor and buffer[start_cursor] == CHAR_SPACE:
        start_cursor += 1

    return start_cursor, end_cursor


@app_njit()
def skip_n_words(buffer, cursor, end_cursor, n):
    """Core logic to advance cursor past N words and prepare for the next field."""
    words_skipped = 0
    while cursor < end_cursor and words_skipped < n:
        # 1. Skip leading whitespace to find the start of a word
        while cursor < end_cursor and is_whitespace(buffer[cursor]):
            cursor += 1

        if cursor >= end_cursor:
            break

        # 2. Skip the word itself
        while cursor < end_cursor and not is_whitespace(buffer[cursor]):
            cursor += 1

        words_skipped += 1

    # 3. HOIST: Skip trailing whitespace after the final word
    # This ensures the cursor sits on the start of the NEXT field.
    while cursor < end_cursor and is_whitespace(buffer[cursor]):
        cursor += 1

    return cursor

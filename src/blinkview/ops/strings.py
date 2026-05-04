# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

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
    if char > 32:
        return False  # Early exit for non-whitespace
    # Mask has bits 9, 10, 13, and 32 set: 0x100002600
    mask = np.uint64(4294977024)
    return (mask >> np.uint64(char)) & 1


@app_njit()
def is_digit(char):
    return np.uint8(char - 48) < 10


@app_njit()
def is_alpha(char):
    # Check A-Z and a-z using the same subtraction trick
    is_upper = np.uint8(char - 65) < 26
    is_lower = np.uint8(char - 97) < 26
    return is_upper | is_lower


@app_njit()
def to_lower(char):
    """Converts uppercase ASCII to lowercase using the bitwise trick."""
    # 1 if char is UPPER, else 0
    is_upper = np.uint8(char - 65) < 26
    # If is_upper is 1, mask is 32. If 0, mask is 0.
    return char | (is_upper << 5)


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
        # 1. Branchless Range Check [32, 126]
        # Returns 1 if printable, 0 otherwise.
        # (val - 32) will be a very large number if val < 32
        # due to unsigned integer underflow.
        keep = np.uint8(val - 32) <= 94

        # 2. Store Unconditionally
        # We always write the byte to the current write_cursor.
        # Since write_cursor is always <= i, we never overwrite
        # data we haven't read yet.
        out_buf[write_cursor] = val

        # 3. Increment Conditionally
        # If keep is 1, the pointer moves; if 0, it stays put
        # and the next printable byte will overwrite the junk.
        write_cursor += keep

    return write_cursor


@app_njit()
def filter_ansi_inplace(out_buf, start_cursor, end_cursor):
    """
    Optimized Fast-Path ANSI filter.
    Allows LLVM to auto-vectorize the normal text path while
    handling CSI sequences efficiently when detected.
    """
    write_cursor = start_cursor
    read_cursor = start_cursor

    while read_cursor < end_cursor:
        val = out_buf[read_cursor]

        # --- FAST PATH ---
        # The CPU will stay in this predictable branch 99% of the time.
        if val != 27:  # CHAR_ESC
            out_buf[write_cursor] = val
            write_cursor += 1
            read_cursor += 1
            continue

        # --- SLOW PATH (ANSI Detected) ---
        # Look ahead for the '[' character
        if read_cursor + 1 < end_cursor and out_buf[read_cursor + 1] == 91:  # CHAR_LBRACKET
            # It's a CSI sequence. Skip ESC and [.
            read_cursor += 2

            # Sub-loop to consume the CSI sequence rapidly
            while read_cursor < end_cursor:
                end_val = out_buf[read_cursor]
                read_cursor += 1
                if 64 <= end_val <= 126:
                    break  # Sequence finished
        else:
            # It was an orphan ESC or a non-CSI escape.
            # Drop the ESC, but don't eat the next character.
            read_cursor += 1

    return write_cursor


@app_njit()
def squash_spaces_inplace(buffer, start_cursor, end_cursor):
    """
    Branchless space squashing and stripping.
    Uses the 'unconditional write, conditional increment' pattern.
    """
    write_idx = start_cursor
    # Initialize prev_val as a space (32).
    # This automatically 'squashes' any leading spaces at the start.
    prev_val = np.uint8(32)

    for i in range(start_cursor, end_cursor):
        val = buffer[i]

        is_space = val == 32
        prev_is_space = prev_val == 32

        # LOGIC:
        # We only increment the write_idx if:
        # 1. The current character is NOT a space.
        # 2. OR the previous character was NOT a space.
        #
        # If both are spaces (consecutive or leading), keep = 0.
        keep = (is_space ^ 1) | (prev_is_space ^ 1)

        # Always write. If keep is 0, the next valid char will overwrite this.
        buffer[write_idx] = val
        write_idx += keep

        # Update state for the next iteration
        prev_val = val

    # Final step: Strip trailing space.
    # If the last character written was a space, we back up.
    # This is a single potential branch at the very end of the record.
    if write_idx > start_cursor:
        is_trailing_space = buffer[write_idx - 1] == 32
        write_idx -= is_trailing_space

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

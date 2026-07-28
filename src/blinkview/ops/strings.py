# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.ops.constants import (
    CHAR_LOWER_A,
    CHAR_LOWER_Z,
)


@app_njit()
def nb_is_whitespace(char):
    # Covers Space, Tab, LF, CR
    if char > 32:
        return False  # Early exit for non-whitespace
    # Mask has bits 9, 10, 13, and 32 set: 0x100002600
    mask = np.uint64(4294977024)
    return (mask >> np.uint64(char)) & 1


@app_njit()
def nb_is_digit(char):
    return np.uint8(char - 48) < 10


@app_njit()
def nb_is_alpha(char):
    # Check A-Z and a-z using the same subtraction trick
    is_upper = np.uint8(char - 65) < 26
    is_lower = np.uint8(char - 97) < 26
    return is_upper | is_lower


@app_njit()
def nb_to_lower(char):
    """Converts uppercase ASCII to lowercase using the bitwise trick."""
    # 1 if char is UPPER, else 0
    is_upper = np.uint8(char - 65) < 26
    # If is_upper is 1, mask is 32. If 0, mask is 0.
    return char | (is_upper << 5)


@app_njit()
def nb_to_upper(char):
    """Converts lowercase ASCII to uppercase using the bitwise trick."""
    if CHAR_LOWER_A <= char <= CHAR_LOWER_Z:
        return char & ~32  # Clear the 6th bit
    return char


@app_njit()
def nb_filter_printable_inplace(out_buf, start_cursor, end_cursor):
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
def nb_filter_ansi_inplace(out_buf, start_cursor, end_cursor):
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


@app_njit(inline="always")
def nb_skip_whitespace(buffer, cursor, end_cursor):
    """
    Advances the cursor past any space (32) or tab (9) characters.
    """
    while cursor < end_cursor and nb_is_whitespace(buffer[cursor]):
        cursor += 1
    return cursor


@app_njit(inline="always")
def nb_skip_whitespace_reverse(buffer, start_cursor, end_cursor):
    """
    Moves the end_cursor backward past any trailing space (32) or tab (9) characters.
    Stops if it hits the start_cursor boundary.
    """
    while end_cursor > start_cursor and nb_is_whitespace(buffer[end_cursor - 1]):
        end_cursor -= 1
    return end_cursor


@app_njit(inline="always")
def nb_skip_non_whitespace(buffer, cursor, end_cursor):
    """
    Advances the cursor past any non-whitespace characters.
    Stops when it hits an inline space (32), tab (9), or the end of the buffer.
    """
    while cursor < end_cursor and not nb_is_whitespace(buffer[cursor]):
        cursor += 1
    return cursor


@app_njit()
def nb_squash_spaces_inplace(buffer, start_cursor, end_cursor):
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
    write_idx = nb_skip_whitespace_reverse(buffer, start_cursor, write_idx)

    return start_cursor, write_idx


@app_njit()
def nb_trim_spaces(buffer, start_cursor, end_cursor):
    """
    Slices off leading and trailing spaces without copying.
    Returns (new_start, new_end).
    """
    # Trim trailing first
    end_cursor = nb_skip_whitespace_reverse(buffer, start_cursor, end_cursor)

    # Trim leading
    start_cursor = nb_skip_whitespace(buffer, start_cursor, end_cursor)

    return start_cursor, end_cursor


@app_njit()
def nb_skip_n_words(buffer, cursor, end_cursor, n):
    """Core logic to advance cursor past N words and prepare for the next field."""
    words_skipped = 0
    while cursor < end_cursor and words_skipped < n:
        # 1. Skip leading whitespace to find the start of a word
        cursor = nb_skip_whitespace(buffer, cursor, end_cursor)

        if cursor >= end_cursor:
            break

        # 2. Skip the word itself
        cursor = nb_skip_non_whitespace(buffer, cursor, end_cursor)

        words_skipped += 1

    return nb_skip_whitespace(buffer, cursor, end_cursor)


@app_njit()
def nb_find_and_parse_int(buffer, offset, length, key):
    """
    Pure algorithmic parser. Decoupled from LogBundle for easy unit testing.
    Returns: (found: bool, value: int)
    """
    key_len = len(key)
    payload_end = offset + length

    i = offset
    while i <= payload_end - key_len:
        if buffer[i] == key[0]:
            match = True
            for j in range(1, key_len):
                if buffer[i + j] != key[j]:
                    match = False
                    break

            if match:
                is_start_valid = i == offset or buffer[i - 1] == 32

                if is_start_valid and (i + key_len < payload_end) and (buffer[i + key_len] == 61):
                    idx = i + key_len + 1
                    res = 0
                    is_negative = False
                    has_digits = False

                    if idx < payload_end and buffer[idx] == 45:  # '-'
                        is_negative = True
                        idx += 1

                    while idx < payload_end and buffer[idx] != 32:
                        val = buffer[idx]
                        if 48 <= val <= 57:
                            res = res * 10 + (val - 48)
                            has_digits = True
                        else:
                            return False, 0
                        idx += 1

                    if not has_digits:
                        return False, 0

                    if is_negative:
                        res = -res

                    return True, res
        i += 1

    return False, 0


@app_njit()
def nb_bundle_find_and_parse_int(bundle, index, key):
    """
    Thin wrapper to keep the Python hot loop clean.
    Delegates immediately to the pure parsing algorithm.
    """
    return nb_find_and_parse_int(bundle.buffer, bundle.offsets[index], bundle.lengths[index], key)


np.seterr(over="ignore")


def fnv1a_64_python(buffer, start, length) -> int:
    data = memoryview(buffer)[start : start + length]
    hval = 0xCBF29CE484222325
    prime = 0x100000001B3
    mask = 0xFFFFFFFFFFFFFFFF
    for b in data:
        hval = ((hval ^ b) * prime) & mask
    return hval


@app_njit(fallback=fnv1a_64_python)
def nb_fnv1a_64_fast(buffer, start, length) -> int:
    """Numba-compiled FNV-1a. Refactored to prevent array slice allocations."""
    hash_val = np.uint64(14695981039346656037)
    fnv_prime = np.uint64(1099511628211)

    for i in range(length):
        hash_val ^= np.uint64(buffer[start + i])
        hash_val *= fnv_prime

    return hash_val

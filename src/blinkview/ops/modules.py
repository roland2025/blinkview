# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.modules import MODULE_ID_FULL
from blinkview.ops.constants import (
    CHAR_COLON,
    CHAR_DOT,
    CHAR_LBRACKET,
    CHAR_NULL,
    CHAR_RBRACKET,
    CHAR_SPACE,
    CHAR_TAB,
    CHAR_UNDERSCORE,
)
from blinkview.ops.discovery import resolve_module_id
from blinkview.ops.strings import (
    is_alpha,
    is_digit,
    is_whitespace,
    to_lower,
)


@app_njit()
def normalize_name_inplace(buffer, start_idx, length):
    """
    Converts A-Z to lowercase.
    Allows a-z, 0-9, and '.' (dot).
    All other characters are treated as underscores.
    Squashes duplicate dots and duplicate underscores.
    Strips leading and trailing separators.
    """
    write_idx = start_idx
    last_written = CHAR_NULL

    for i in range(length):
        val = buffer[start_idx + i]

        val = to_lower(val)

        is_alphanum = is_alpha(val) or is_digit(val)
        is_dot = val == CHAR_DOT

        if is_alphanum:
            buffer[write_idx] = val
            last_written = val
            write_idx += 1
        elif is_dot:
            # SQUASH DOTS: Write only if not at start and not a duplicate
            if last_written != CHAR_NULL and last_written != CHAR_DOT:
                buffer[write_idx] = CHAR_DOT
                last_written = CHAR_DOT
                write_idx += 1
        else:
            # SQUASH UNDERSCORES: All other chars become '_'
            # Write only if not at start and not a duplicate
            if last_written != CHAR_NULL and last_written != CHAR_UNDERSCORE:
                buffer[write_idx] = CHAR_UNDERSCORE
                last_written = CHAR_UNDERSCORE
                write_idx += 1

    # STRIP TRAILING: Remove any separator at the very end
    while write_idx > start_idx:
        last_char = buffer[write_idx - 1]
        if last_char == CHAR_DOT or last_char == CHAR_UNDERSCORE:
            write_idx -= 1
        else:
            break

    return write_idx - start_idx


@app_njit()
def parse_fixed_width_name(
    buffer,
    start_cursor,
    end_cursor,  # Inputs
    out_b,
    out_idx,  # Outputs
    tracker,  # Mutable State
    config,  # Read-only Config
):
    width = config.width

    actual_width = width
    if start_cursor + width > end_cursor:
        actual_width = end_cursor - start_cursor

    if actual_width <= 0:
        return start_cursor

    # --- 1. Optimized Forward Scan ---
    logical_len = 0
    prev_space = False

    while logical_len < actual_width:
        curr_byte = buffer[start_cursor + logical_len]
        if curr_byte == CHAR_TAB:
            break
        if curr_byte == CHAR_SPACE:
            if prev_space:
                logical_len -= 1
                break
            prev_space = True
        else:
            prev_space = False
        logical_len += 1

    while logical_len > 0 and buffer[start_cursor + logical_len - 1] == CHAR_SPACE:
        logical_len -= 1

    if logical_len != 0:
        # --- 2. Direct State Access ---
        # Notice we no longer have to dig through config.tracker
        current_byte_write = tracker.bytes_cursor[0]

        if current_byte_write + logical_len > len(tracker.name_bytes):
            return -1

        # --- 3. Slice Assignment ---
        tracker.name_bytes[current_byte_write : current_byte_write + logical_len] = buffer[
            start_cursor : start_cursor + logical_len
        ]

        squashed_len = normalize_name_inplace(tracker.name_bytes, current_byte_write, logical_len)

        if squashed_len > 0:
            # Config provides the map, Tracker provides the state
            mod_id = resolve_module_id(tracker.name_bytes, current_byte_write, squashed_len, config.byte_map, tracker)
            if mod_id == MODULE_ID_FULL:
                return -1
            out_b.modules[out_idx] = mod_id
    else:
        return -1

    return start_cursor + actual_width


@app_njit()
def parse_module_tags_statemachine(
    buffer,
    cursor,
    end_cursor,  # Inputs
    out_b,
    out_idx,  # Outputs
    tracker,  # Mutable State
    config,  # Read-only Config
):
    write_start = tracker.bytes_cursor[0]
    write_ptr = write_start

    tag_count = 0
    curr = cursor
    in_bracket_mode = False

    while curr < end_cursor:
        # 1. Skip whitespace using utility
        while curr < end_cursor and is_whitespace(buffer[curr]):
            curr += 1

        if curr >= end_cursor:
            break

        first_char = buffer[curr]
        if in_bracket_mode and first_char != CHAR_LBRACKET:
            break

        if config.enable_dot_separator:
            if curr < end_cursor and buffer[curr] == CHAR_DOT:
                while curr < end_cursor and buffer[curr] == CHAR_DOT:
                    curr += 1
                continue

        found_current_tag = False
        tag_len = 0
        tag_data_start = 0
        move_cursor_to = 0

        # --- BRANCH A: Bracketed Tag [...] ---
        if config.enable_brackets and first_char == CHAR_LBRACKET:
            tag_data_start = curr + 1
            scan_ptr = tag_data_start
            while scan_ptr < end_cursor and buffer[scan_ptr] != CHAR_RBRACKET:
                scan_ptr += 1

            if scan_ptr < end_cursor and buffer[scan_ptr] == CHAR_RBRACKET:
                tag_len = scan_ptr - tag_data_start
                move_cursor_to = scan_ptr + 1
                if move_cursor_to < end_cursor and buffer[move_cursor_to] == CHAR_COLON:
                    move_cursor_to += 1

                found_current_tag = True
                in_bracket_mode = True
            else:
                return -1

        # --- BRANCH B: Word ending in colon ---
        else:
            tag_data_start = curr
            scan_ptr = curr

            while scan_ptr < end_cursor:
                char = buffer[scan_ptr]
                # Break on whitespace or colon
                if is_whitespace(char) or char == CHAR_COLON:
                    break
                scan_ptr += 1

            if scan_ptr < end_cursor and buffer[scan_ptr] == CHAR_COLON:
                tag_len = scan_ptr - tag_data_start
                move_cursor_to = scan_ptr + 1
                found_current_tag = True
            else:
                if tag_count == 0:
                    return -1
                break

        # --- TAG VALIDATION & WRITE ---
        if found_current_tag:
            if tag_len == 0 or tag_count >= config.max_depth:
                return -1

            sep_len = 1 if tag_count > 0 else 0
            total_projected_len = (write_ptr - write_start) + sep_len + tag_len
            if total_projected_len > config.max_length:
                return -1

            if tag_count > 0:
                tracker.name_bytes[write_ptr] = CHAR_DOT
                write_ptr += 1

            for i in range(tag_len):
                tracker.name_bytes[write_ptr + i] = buffer[tag_data_start + i]

            write_ptr += tag_len
            tag_count += 1
            curr = move_cursor_to

    if tag_count == 0:
        return -1

    logical_len = write_ptr - write_start
    final_len = normalize_name_inplace(tracker.name_bytes, write_start, logical_len)

    if final_len <= 0:
        return -1

    mod_id = resolve_module_id(tracker.name_bytes, write_start, final_len, config.byte_map, tracker)
    if mod_id == MODULE_ID_FULL:
        return -1

    out_b.modules[out_idx] = mod_id
    return curr

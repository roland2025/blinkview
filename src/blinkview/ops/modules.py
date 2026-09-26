# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

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
from blinkview.ops.discovery import nb_resolve_module_id, nb_resolve_module_id_hashed
from blinkview.ops.strings import (
    nb_is_alpha,
    nb_is_digit,
    nb_is_whitespace,
    nb_skip_whitespace,
    nb_to_lower,
)


@app_njit(inline="always")
def nb_normalize_name_inplace(buffer, start_idx, length):
    """
    Converts A-Z to lowercase.
    Allows a-z, 0-9, and '.' (dot).
    All other characters are treated as underscores.
    Squashes duplicate dots and duplicate underscores.
    Strips leading and trailing separators.
    """
    start_idx = np.int64(start_idx)
    length = np.int64(length)
    write_idx = start_idx
    last_written = CHAR_NULL

    for i in range(length):
        val = buffer[start_idx + i]

        val = nb_to_lower(val)

        is_alphanum = nb_is_alpha(val) or nb_is_digit(val)
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


@app_njit(inline="always")
def nb_parse_fixed_width_name(
    buffer,
    start_cursor,
    end_cursor,  # Inputs
    out_b,
    out_idx,  # Outputs
    state,  # Mutable State
    config,  # Read-only Config
):
    tracker = state.modules
    width = config.module_config.max_length

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

        squashed_len = int(nb_normalize_name_inplace(tracker.name_bytes, current_byte_write, logical_len))

        if squashed_len > 0:
            tracker.name_bytes[current_byte_write + squashed_len] = 0
            # Config provides the map, Tracker provides the state
            mod_id = nb_resolve_module_id(
                tracker.name_bytes, current_byte_write, squashed_len, config.string_table, tracker
            )
            if mod_id == MODULE_ID_FULL:
                return -1
            out_b.modules[out_idx] = mod_id
    else:
        return -1

    return start_cursor + actual_width


# Character tables for the single-pass tag parser below.
# NORM[c] is what nb_normalize_name_inplace turns c into (lowercase alnum, '.', or '_');
# VALID[c] marks the characters allowed in an unbracketed "word:" tag: [0-9A-Za-z_./-].
NORM = np.full(256, 95, dtype=np.uint8)
VALID = np.zeros(256, dtype=np.uint8)
for _c in range(256):
    if 48 <= _c <= 57 or 97 <= _c <= 122:
        NORM[_c] = _c
        VALID[_c] = 1
    elif 65 <= _c <= 90:
        NORM[_c] = _c + 32
        VALID[_c] = 1
    elif _c == 46:
        NORM[_c] = 46
        VALID[_c] = 1
    elif _c in (95, 45, 47):
        VALID[_c] = 1

FNV_PRIME = np.uint64(1099511628211)
FNV_BASIS = np.uint64(14695981039346656037)


@app_njit(inline="always")
def _put(nc, nb, w, hw, last, h):
    """Streaming normalize of one char (already mapped through NORM) with incremental FNV-1a.
    w = physical write index, hw = index up to which h has folded (excludes pending separators)."""
    if nc != 46 and nc != 95:
        while hw < w:
            h = (h ^ np.uint64(nb[hw])) * FNV_PRIME
            hw += 1
        nb[w] = nc
        w += 1
        h = (h ^ np.uint64(nc)) * FNV_PRIME
        hw = w
        last = nc
    elif last != 0 and last != nc:
        nb[w] = nc
        w += 1
        last = nc
    return w, hw, last, h


@app_njit(inline="always")
def nb_parse_module_tags_statemachine(buffer, cursor, end_cursor, out_b, out_idx, state, unified_config):
    tracker = state.modules
    write_start = np.int64(tracker.bytes_cursor[0])
    nb = tracker.name_bytes
    config = unified_config.module_config

    tag_count = 0
    in_bracket_mode = False

    prefix_len = config.prefix_bytes.size
    if prefix_len > 0 and (config.prefix_match or config.prefix_remove):
        temp_curr = cursor
        while temp_curr < end_cursor and nb_is_whitespace(buffer[temp_curr]):
            temp_curr += 1
        if temp_curr + prefix_len > end_cursor:
            if config.prefix_match:
                return -1
        else:
            has_prefix = True
            for i in range(prefix_len):
                if buffer[temp_curr + i] != config.prefix_bytes[i]:
                    has_prefix = False
                    break
            if config.prefix_match and not has_prefix:
                return -1
            if config.prefix_remove and has_prefix:
                cursor = temp_curr + prefix_len
                if cursor < end_cursor and buffer[cursor] == CHAR_DOT:
                    cursor += 1

    curr = cursor
    max_length = config.max_length
    max_depth = config.max_depth

    raw_len = 0  # projected pre-normalisation length, for the max_length rule
    w = write_start
    hw = write_start
    last = 0
    h = FNV_BASIS

    while curr < end_cursor:
        while curr < end_cursor and nb_is_whitespace(buffer[curr]):
            curr += 1
        if curr >= end_cursor:
            break

        saw_dot = False
        if config.enable_dot_separator:
            if buffer[curr] == CHAR_DOT:
                while curr < end_cursor and buffer[curr] == CHAR_DOT:
                    curr += 1
                saw_dot = True
                while curr < end_cursor and nb_is_whitespace(buffer[curr]):
                    curr += 1
                if curr >= end_cursor:
                    break

        first_char = buffer[curr]

        if tag_count > 0:
            if first_char != CHAR_LBRACKET:
                if in_bracket_mode:
                    break
                elif not saw_dot:
                    break

        sep_len = 1 if tag_count > 0 else 0
        # trial state
        tw = w
        thw = hw
        tlast = last
        th = h
        if tag_count > 0:
            tw, thw, tlast, th = _put(46, nb, tw, thw, tlast, th)

        if config.enable_brackets and first_char == CHAR_LBRACKET:
            tag_data_start = curr + 1
            scan_ptr = tag_data_start
            while scan_ptr < end_cursor and buffer[scan_ptr] != CHAR_RBRACKET:
                scan_ptr += 1
            if scan_ptr >= end_cursor:
                return -1
            tag_len = scan_ptr - tag_data_start
            move_cursor_to = scan_ptr + 1
            if move_cursor_to < end_cursor and buffer[move_cursor_to] == CHAR_COLON:
                move_cursor_to += 1
            in_bracket_mode = True
            if tag_len == 0 or tag_count >= max_depth or raw_len + sep_len + tag_len > max_length:
                return -1
            for i in range(tag_len):
                tw, thw, tlast, th = _put(NORM[buffer[tag_data_start + i]], nb, tw, thw, tlast, th)
        else:
            tag_data_start = curr
            scan_ptr = curr
            overflow = False
            while scan_ptr < end_cursor:
                c = buffer[scan_ptr]
                if VALID[c] == 0:
                    break
                if raw_len + sep_len + (scan_ptr - curr) < max_length:
                    tw, thw, tlast, th = _put(NORM[c], nb, tw, thw, tlast, th)
                else:
                    overflow = True
                scan_ptr += 1

            ok = False
            if tag_data_start < scan_ptr < end_cursor and buffer[scan_ptr] == CHAR_COLON:
                if scan_ptr + 1 >= end_cursor or nb_is_whitespace(buffer[scan_ptr + 1]):
                    ok = True
            if not ok:
                if tag_count == 0:
                    return -1
                break
            tag_len = scan_ptr - tag_data_start
            move_cursor_to = scan_ptr + 1
            if tag_count >= max_depth or overflow or raw_len + sep_len + tag_len > max_length:
                return -1

        raw_len += sep_len + tag_len
        w = tw
        hw = thw
        last = tlast
        h = th
        tag_count += 1
        curr = move_cursor_to

    if tag_count == 0:
        return -1

    final_len = hw - write_start
    if final_len <= 0:
        return -1

    mod_id = nb_resolve_module_id_hashed(nb, write_start, final_len, h, unified_config.string_table, tracker)
    if mod_id == MODULE_ID_FULL:
        return -1
    out_b.modules[out_idx] = mod_id
    return nb_skip_whitespace(buffer, curr, end_cursor)


@app_njit(inline="always")
def nb_parse_rsyslog_tag(
    buffer,
    cursor,
    end_cursor,  # Inputs
    out_b,
    out_idx,  # Outputs
    state,  # Mutable State
    unified_config,  # Read-only Config
):
    """Parses the classic syslog/rsyslog TAG field: 'tag[pid]: ' or 'tag: ' - a bare
    identifier, an optional numeric PID in brackets, and a terminating colon. The PID
    itself is not captured, only used to validate/skip past the bracketed section."""
    tracker = state.modules
    config = unified_config.module_config

    tag_start = cursor
    curr = cursor
    while curr < end_cursor:
        char = buffer[curr]
        if char == CHAR_LBRACKET or char == CHAR_COLON:
            break
        if nb_is_whitespace(char):
            return -1
        curr += 1

    tag_len = curr - tag_start
    if tag_len == 0 or curr >= end_cursor:
        return -1

    if buffer[curr] == CHAR_LBRACKET:
        pid_start = curr + 1
        scan_ptr = pid_start
        while scan_ptr < end_cursor and nb_is_digit(buffer[scan_ptr]):
            scan_ptr += 1

        if scan_ptr == pid_start or scan_ptr >= end_cursor or buffer[scan_ptr] != CHAR_RBRACKET:
            return -1

        curr = scan_ptr + 1
        if curr >= end_cursor or buffer[curr] != CHAR_COLON:
            return -1

    # buffer[curr] is now the terminating colon
    curr += 1

    max_length = config.max_length
    if max_length > 0 and tag_len > max_length:
        tag_len = max_length

    write_start = tracker.bytes_cursor[0]
    if write_start + tag_len > len(tracker.name_bytes):
        return -1

    tracker.name_bytes[write_start : write_start + tag_len] = buffer[tag_start : tag_start + tag_len]

    squashed_len = int(nb_normalize_name_inplace(tracker.name_bytes, write_start, tag_len))
    if squashed_len <= 0:
        return -1

    mod_id = nb_resolve_module_id(tracker.name_bytes, write_start, squashed_len, unified_config.string_table, tracker)
    if mod_id == MODULE_ID_FULL:
        return -1

    out_b.modules[out_idx] = mod_id

    return nb_skip_whitespace(buffer, curr, end_cursor)

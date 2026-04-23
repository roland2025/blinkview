# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.parsing import STATE_COMPLETE, STATE_INCOMPLETE, UnifiedParserState
from blinkview.ops.constants import (
    CHAR_COLON,
    CHAR_CR,
    CHAR_ESC,
    CHAR_LBRACKET,
    CHAR_LF,
    CHAR_NBSP,
    CHAR_NINE,
    CHAR_RBRACKET,
    CHAR_RS,
    CHAR_SPACE,
    CHAR_TAB,
    CHAR_TILDE,
    CHAR_UPPER_A,
    CHAR_ZERO,
)
from blinkview.ops.discovery import resolve_module_id
from blinkview.ops.modules import normalize_name_inplace
from blinkview.ops.timestamps import parse_iso8601_to_ns


@app_njit(inline="always")
def is_adb_long_header(buffer, cursor, limit):
    """
    Validates if the sequence starting at `cursor` is a valid ADB Long Header.
    Matches the rigid punctuation skeleton of: [ YYYY-MM-DD HH:MM:SS.
    """
    # We need at least 22 bytes to verify up to the period
    if limit - cursor < 22:
        return False

    if buffer[cursor] != 91:
        return False  # '['  (Index 0)
    if buffer[cursor + 1] != 32:
        return False  # ' '  (Index 1)
    if buffer[cursor + 6] != 45:
        return False  # '-'  (Index 6)
    if buffer[cursor + 9] != 45:
        return False  # '-'  (Index 9)
    if buffer[cursor + 12] != 32:
        return False  # ' '  (Index 12)
    if buffer[cursor + 15] != 58:
        return False  # ':'  (Index 15)
    if buffer[cursor + 18] != 58:
        return False  # ':'  (Index 18)
    if buffer[cursor + 21] != 46:
        return False  # '.'  (Index 21)

    return True


@app_njit(inline="always")
def decode_adb_long_frame(f_buf, start, end, out_buf, out_cursor, f_cfg, f_state):

    # print(f"Raw Frame Slice: {f_buf[start:end].tobytes()}")
    # --- 1. ORACLE BOUNDARY CHECK ---
    second_header_idx = -1

    search_start = start
    if is_adb_long_header(f_buf, start, end):
        search_start = start + 22

    for i in range(search_start, end):
        if f_buf[i] == CHAR_LF:
            if is_adb_long_header(f_buf, i + 1, end):
                second_header_idx = i + 1  # Point exactly to the '[' of the next header
                break

    if second_header_idx == -1:
        # Return 0 bytes consumed for STATE_INCOMPLETE
        return STATE_INCOMPLETE, out_cursor, 0

    # Calculate bytes consumed for this frame
    bytes_consumed = second_header_idx - start

    # --- 2. DEFINE BOUNDS ---
    true_end = second_header_idx - 1
    while true_end > start and (f_buf[true_end - 1] == CHAR_LF or f_buf[true_end - 1] == CHAR_CR):
        true_end -= 1

    # Skip leading whitespace/newlines leaking from previous irregular frames
    true_start = start
    while true_start < true_end and (
        f_buf[true_start] == CHAR_LF or f_buf[true_start] == CHAR_CR or f_buf[true_start] == CHAR_SPACE
    ):
        true_start += 1

    if true_start >= true_end:
        return STATE_COMPLETE, out_cursor, bytes_consumed

    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    # --- 3. Find the closing bracket ' ]' ---
    header_end = -1
    for i in range(true_start, true_end - 1):
        if f_buf[i] == CHAR_SPACE and f_buf[i + 1] == CHAR_RBRACKET:
            header_end = i + 1
            break

    # --- 4. Copy Header ---
    if header_end != -1:
        if f_buf[true_start] != CHAR_LBRACKET:
            if cursor < buf_cap:
                out_buf[cursor] = CHAR_LBRACKET
                cursor += 1

        for i in range(true_start, header_end + 1):
            if cursor < buf_cap:
                out_buf[cursor] = f_buf[i]
                cursor += 1
        msg_ptr = header_end + 1
    else:
        msg_ptr = true_start

    if cursor < buf_cap:
        out_buf[cursor] = CHAR_SPACE
        cursor += 1

    # --- 5. Skip whitespace/newlines ---
    while msg_ptr < true_end and (
        f_buf[msg_ptr] == CHAR_SPACE or f_buf[msg_ptr] == CHAR_CR or f_buf[msg_ptr] == CHAR_LF
    ):
        msg_ptr += 1

    # --- 6. Copy Body ---
    f_ansi = f_cfg.filter_ansi
    f_print = f_cfg.filter_printable
    ansi_state = 0

    for i in range(msg_ptr, true_end):
        if cursor >= buf_cap:
            break

        val = f_buf[i]

        if f_ansi:
            if ansi_state == 0:
                if val == CHAR_ESC:
                    ansi_state = 1
                    continue
            elif ansi_state == 1:
                if val == CHAR_LBRACKET:
                    ansi_state = 2
                    continue
                else:
                    ansi_state = 0
            elif ansi_state == 2:
                if CHAR_UPPER_A <= val <= CHAR_TILDE:
                    ansi_state = 0
                continue

        if f_print:
            if not (CHAR_SPACE <= val <= CHAR_TILDE or val == CHAR_LF or val == CHAR_TAB):
                continue

        if val == CHAR_CR:
            continue

        if val == CHAR_LF:
            val = CHAR_RS

        out_buf[cursor] = val
        cursor += 1

    return STATE_COMPLETE, cursor, bytes_consumed


@app_njit(inline="always")
def parse_adb_pid_tid(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
    cursor = start_cursor

    # --- 1. Parse PID ---
    pid = 0
    while cursor < end_cursor:
        val = buffer[cursor]
        if CHAR_ZERO <= val <= CHAR_NINE:
            pid = pid * 10 + (val - CHAR_ZERO)
            cursor += 1
        else:
            break

    # Skip the colon ':'
    if cursor < end_cursor and buffer[cursor] == CHAR_COLON:
        cursor += 1

    # --- 2. Skip potential whitespace after colon ---
    # Example: "2680: 2701"
    while cursor < end_cursor:
        val = buffer[cursor]
        if val == CHAR_SPACE or val == CHAR_TAB or val == CHAR_NBSP:
            cursor += 1
        else:
            break

    # --- 3. Parse TID ---
    tid = 0
    while cursor < end_cursor:
        val = buffer[cursor]
        if CHAR_ZERO <= val <= CHAR_NINE:
            tid = tid * 10 + (val - CHAR_ZERO)
            cursor += 1
        else:
            break

    # --- 4. Assignment (Commented out as requested) ---
    # out_b.pids[out_idx] = pid
    # out_b.tids[out_idx] = tid

    # --- 5. Final Scan to Log Level ---
    # Move the cursor past any trailing whitespace so it sits
    # exactly at the start of the Log Level (e.g., 'I/...')
    while cursor < end_cursor:
        val = buffer[cursor]
        if val == CHAR_SPACE or val == CHAR_TAB or val == CHAR_NBSP:
            cursor += 1
        else:
            break

    return cursor


@app_njit(inline="always")
def parse_adb_level(buffer, start_cursor, end_cursor, out_b, out_idx, state, unified_config):
    if start_cursor + 1 >= end_cursor:
        return -1

    level_char = buffer[start_cursor]

    # --- Local Variable Cache ---
    cfg = unified_config.string_table
    count = cfg.count
    offsets = cfg.offsets
    values = cfg.values
    ref_buf = cfg.buffer

    # Tight loop using local register references
    for i in range(count):
        if ref_buf[offsets[i]] == level_char:
            out_b.levels[out_idx] = values[i]
            return start_cursor + 2

    return -1


@app_njit(inline="always")
def parse_adb_tag(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,
):
    tracker = state.modules
    t_bytes = tracker.name_bytes
    write_pos = tracker.bytes_cursor[0]
    s_table = config.string_table

    # --- 1. Find the Absolute End of the Tag Area ---
    # In ADB Long, the header ALWAYS ends with ' ]' (Space + Bracket)
    header_delimiter_idx = -1
    for i in range(start_cursor, end_cursor - 1):
        if buffer[i] == CHAR_SPACE and buffer[i + 1] == CHAR_RBRACKET:
            header_delimiter_idx = i
            break

    if header_delimiter_idx == -1:
        return -1  # Should not happen in a valid frame

    # --- 2. Search for Metadata Delimiters within that area ---
    actual_end = header_delimiter_idx
    meta_type = 0  # 0: None, 1: Colon, 2: LBracket

    for i in range(start_cursor, header_delimiter_idx):
        val = buffer[i]
        if val == CHAR_COLON:
            actual_end = i
            meta_type = 1
            break
        elif val == CHAR_LBRACKET and i > start_cursor:
            # Only count '[' as metadata if it's not the first char (handles [BT])
            actual_end = i
            meta_type = 2
            break

    # Calculate tag length (trimmed of trailing spaces if no meta found)
    tag_limit = actual_end
    if meta_type == 0:
        while tag_limit > start_cursor and buffer[tag_limit - 1] == CHAR_SPACE:
            tag_limit -= 1

    tag_len = tag_limit - start_cursor

    # --- 3. Extract, Normalize, and Resolve ---
    if tag_len > 0:
        if write_pos + tag_len > len(t_bytes):
            return -1
        t_bytes[write_pos : write_pos + tag_len] = buffer[start_cursor:tag_limit]
        squashed_len = int(normalize_name_inplace(t_bytes, write_pos, tag_len))

        if squashed_len > 0:
            t_bytes[write_pos + squashed_len] = 0
            mod_id = resolve_module_id(t_bytes, write_pos, squashed_len, s_table, tracker)
            if mod_id == -1:
                return -1
            out_b.modules[out_idx] = mod_id
        else:
            out_b.modules[out_idx] = 0
    else:
        out_b.modules[out_idx] = 0

    # --- 4. Final Cursor Routing ---
    if meta_type == 1:
        # COLON CASE: Check for Empty Delimiter 'bt: ]'
        peek_idx = actual_end + 1
        while peek_idx < header_delimiter_idx and buffer[peek_idx] == CHAR_SPACE:
            peek_idx += 1

        if peek_idx == header_delimiter_idx:
            # It's 'bt: ]' -> Skip the colon and the ' ]'
            return header_delimiter_idx + 2

        if buffer[peek_idx] == CHAR_LBRACKET:
            # It's 'vri: [Settings]' -> Jump to bracket
            return peek_idx

        # Standard metadata: Inject '[' over the colon
        buffer[actual_end] = CHAR_LBRACKET
        return actual_end

    elif meta_type == 2:
        # LBRACKET SHIFT: VRI[NotificationShade]
        prefix_len = header_delimiter_idx - actual_end
        shift_dist = 2
        for i in range(prefix_len - 1, -1, -1):
            buffer[actual_end + shift_dist + i] = buffer[actual_end + i]
        return actual_end + shift_dist

    # STANDARD CASE: Just jump past the ' ]'
    cursor = header_delimiter_idx + 2
    while cursor < end_cursor and (buffer[cursor] == CHAR_SPACE or buffer[cursor] == CHAR_LF):
        cursor += 1
    return cursor


@app_njit(inline="always")
def parse_adb_timestamp(
    buffer,
    start_cursor,
    end_cursor,
    out_b: LogBundle,
    out_idx,
    state: UnifiedParserState,
    config,
):
    # Logcat Long format starts with "[ " (2 bytes) before the Year
    # Total header check length "[ 2026-04-21 19:53:52.754" is 25 bytes
    if start_cursor + 25 > end_cursor or buffer[start_cursor] != 91:
        return -1

    # Call the generic ISO parser
    # We pass 'start_cursor + 2' to skip the '[ '
    out_b.timestamps[out_idx] = parse_iso8601_to_ns(buffer, start_cursor + 2, state.timestamp.utc_offset[0])

    # Move cursor past timestamp (index 25) and skip whitespace to find PID
    cursor = start_cursor + 25
    while cursor < end_cursor:
        b = buffer[cursor]
        if b == 32 or b == 9 or b == 160:
            cursor += 1
        else:
            break

    return cursor

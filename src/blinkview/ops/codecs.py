# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit


@app_njit()
def process_byte_filters(val, ansi_state, filter_ansi, filter_printable):
    """
    Evaluates a single byte against ANSI and printable filters.
    Returns (should_write: bool, new_ansi_state: int)
    """
    if filter_ansi:
        if ansi_state == 0:
            if val == 27:  # 0x1B (ESC)
                return False, 1
        elif ansi_state == 1:
            if val == 91:  # 0x5B ('[')
                return False, 2
            else:
                # Orphan ESC dropped, proceed to evaluate current byte
                ansi_state = 0
        elif ansi_state == 2:
            if 64 <= val <= 126:
                return False, 0  # Sequence finished
            return False, 2  # Inside sequence

    if filter_printable:
        if not (32 <= val <= 126):
            return False, ansi_state

    return True, ansi_state


@app_njit(inline="always")
def decode_newline_frame(f_buf, start, end, out_buf, out_cursor, f_cfg):
    # 1. O(1) Trailing Carriage Return Strip
    # Fixes the issue where \r trips the filter scanner on every single frame

    buf_cap = out_buf.shape[0]
    cursor = out_cursor

    filter_ansi = f_cfg.filter_ansi
    filter_printable = f_cfg.filter_printable

    # Determine safe loop bounds once
    available = buf_cap - cursor
    frame_len = end - start
    process_len = frame_len if frame_len < available else available

    if process_len <= 0:
        return cursor

    # 2. OPTIMISTIC FAST PATH SCAN
    # Read-only loop to check if we can safely bypass the heavy filters
    needs_filtering = False

    if filter_printable:
        # If printable is ON, it automatically catches ESC (27) and \r (13)
        for i in range(start, start + process_len):
            if not (32 <= f_buf[i] <= 126):
                needs_filtering = True
                break
    elif filter_ansi:
        # Only scan specifically for ESC if the printable filter is OFF
        for i in range(start, start + process_len):
            if f_buf[i] == 27:
                needs_filtering = True
                break

    # 3. VECTORIZED BLOCK COPY
    # If the payload is clean, let LLVM do a high-speed memory copy
    if not needs_filtering:
        out_buf[cursor : cursor + process_len] = f_buf[start : start + process_len]
        return cursor + process_len

    # 4. SLOW PATH: Flattened Fused Filtering
    # Only triggered if unexpected garbage or ANSI codes are detected
    ansi_state = 0
    for i in range(start, start + process_len):
        val = f_buf[i]

        # ANSI Check
        if filter_ansi:
            if ansi_state == 0:
                if val == 27:
                    ansi_state = 1
                    continue
            elif ansi_state == 1:
                if val == 91:
                    ansi_state = 2
                    continue
                else:
                    ansi_state = 0
            elif ansi_state == 2:
                if 64 <= val <= 126:
                    ansi_state = 0
                continue

        # Printable Check
        if filter_printable:
            if not (32 <= val <= 126):
                continue

        out_buf[cursor] = val
        cursor += 1

    return cursor


@app_njit()
def decode_newline_frame_no_filters(f_buf, start, end, out_buf, out_cursor, f_cfg):
    """
    ULTRA-FAST PATH: Optimized for zero filtering.
    Performs O(1) trailing strip and O(N) vectorized copy.
    """
    # 1. O(1) Trailing Carriage Return Strip
    # We check end > start to prevent index -1 on empty lines (\n)
    # if f_buf[end - 1] == 13:
    #     end -= 1

    frame_len = end - start
    # if frame_len <= 0:
    #     return out_cursor

    # 2. BOUNDS CHECKING
    # Ensure we don't overrun the output batch buffer
    buf_cap = out_buf.shape[0]
    available = buf_cap - out_cursor
    write_len = frame_len if frame_len < available else available

    if write_len > 0:
        # 3. VECTORIZED BLOCK COPY
        # Numba translates this slice assignment into a C-level memcpy/memmove.
        # This is the fastest way to move bytes in a single pass.
        out_buf[out_cursor : out_cursor + write_len] = f_buf[start : start + write_len]
        return out_cursor + write_len

    return out_cursor


@app_njit()
def decode_cobs_frame(f_buf, start, end, out_buf, out_cursor, f_cfg):
    """Type 1: Decodes COBS pointers with fused inline filtering."""
    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    if end - start < 2:
        return cursor

    filter_ansi = f_cfg.filter_ansi
    filter_printable = f_cfg.filter_printable
    ansi_state = 0
    read_idx = start

    while read_idx < end:
        code = f_buf[read_idx]
        if code == 0:
            break
        read_idx += 1

        for i in range(1, code):
            if read_idx >= end:
                break

            val = f_buf[read_idx]
            should_write, ansi_state = process_byte_filters(val, ansi_state, filter_ansi, filter_printable)

            if should_write and cursor < buf_cap:
                out_buf[cursor] = val
                cursor += 1
            read_idx += 1

        if code < 0xFF and read_idx < end:
            val = 0x00
            should_write, ansi_state = process_byte_filters(val, ansi_state, filter_ansi, filter_printable)

            if should_write and cursor < buf_cap:
                out_buf[cursor] = val
                cursor += 1

    return cursor


@app_njit()
def decode_slip_frame(f_buf, start, end, out_buf, out_cursor, f_cfg):
    """Type 2: Unescapes SLIP byte sequences with fused inline filtering."""
    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    filter_ansi = f_cfg.filter_ansi
    filter_printable = f_cfg.filter_printable
    ansi_state = 0

    i = start
    while i < end:
        val = f_buf[i]
        if val == 0xDB:
            i += 1
            if i < end:
                esc = f_buf[i]
                if esc == 0xDC:
                    val = 0xC0
                elif esc == 0xDD:
                    val = 0xDB
                else:
                    val = esc

        should_write, ansi_state = process_byte_filters(val, ansi_state, filter_ansi, filter_printable)
        if should_write and cursor < buf_cap:
            out_buf[cursor] = val
            cursor += 1
        i += 1

    return cursor


@app_njit()
def parser_noop(buffer, start_cursor, end_cursor, out_b, out_idx, config):
    """
    Used as a placeholder in the NamedTuple if a specific parser isn't needed.
    """
    return start_cursor


@app_njit()
def shift_frame_buffer(f_buf, read_ptr, write_ptr):
    residue_len = write_ptr - read_ptr
    if residue_len > 0 and read_ptr > 0:
        for n in range(residue_len):
            f_buf[n] = f_buf[read_ptr + n]
        return residue_len
    elif read_ptr > 0:
        return 0
    return write_ptr

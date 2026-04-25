# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

# --- Configuration Constants ---
# The threshold where Numba's loop-lifting becomes less efficient than
# calling the C-library's optimized memcpy/memmove (in bytes).
from blinkview.core.numba_config import app_njit

COPY_THRESHOLD = 256


@app_njit(inline="always")
def nb_copy_buf(src, src_off, dst, dst_off, length):
    """Hybrid copy: Loop for small, Slicing for large."""
    if length < COPY_THRESHOLD:
        for i in range(length):
            dst[dst_off + i] = src[src_off + i]
    else:
        dst[dst_off : dst_off + length] = src[src_off : src_off + length]


@app_njit(inline="always")
def nb_fill_buf(dst, dst_off, length, value):
    """Hybrid fill: Loop for small, Slicing for large."""
    if length < COPY_THRESHOLD:
        for i in range(length):
            dst[dst_off + i] = value
    else:
        dst[dst_off : dst_off + length] = value


@app_njit(inline="always")
def nb_sync_push(f_buf, f_ts_buf, write_pos, src_bytes, src_ts, length):
    """Atomic push to both byte and timestamp buffers."""
    nb_copy_buf(src_bytes, 0, f_buf, write_pos, length)
    nb_fill_buf(f_ts_buf, write_pos, length, src_ts)


@app_njit(inline="always")
def nb_sync_shift_leftovers(f_buf, f_ts_buf, target_buf, target_off, ts_in, is_zero_copy, length):
    """Shifts data to start of buffers while preserving/broadcasting timestamps."""
    if is_zero_copy:
        # FAST PATH (No Overlap): target_buf is in_b.buffer, f_buf is f_state.buffer
        # It is perfectly safe to let LLVM use memcpy here.
        nb_copy_buf(target_buf, target_off, f_buf, 0, length)
        nb_fill_buf(f_ts_buf, 0, length, ts_in)
    else:
        # BUFFER PATH (Overlap!): target_buf IS f_buf.
        # DO NOT use slice assignment/memcpy. We must use a safe forward loop
        # to prevent memory corruption during the self-shift.
        for i in range(length):
            f_buf[i] = f_buf[target_off + i]
            f_ts_buf[i] = f_ts_buf[target_off + i]


@app_njit(inline="always")
def nb_move_buf(buf, src_off, dst_off, length):
    """
    Safely moves data within the same buffer (memmove equivalent).
    Handles overlapping regions by checking direction.
    """
    if length <= 0 or src_off == dst_off:
        return

    if dst_off < src_off:
        # Left Shift: Safe to use a forward loop or slice (Numba handles this)
        if length < COPY_THRESHOLD:
            for i in range(length):
                buf[dst_off + i] = buf[src_off + i]
        else:
            buf[dst_off : dst_off + length] = buf[src_off : src_off + length]
    else:
        # Right Shift: MUST use a backward loop to avoid overwriting source
        # (Though compaction is almost always a left shift)
        for i in range(length - 1, -1, -1):
            buf[dst_off + i] = buf[src_off + i]


@app_njit(inline="always")
def nb_report_error(out_b, out_idx, out_cursor, src_buf, src_off, length, level, module):
    """Copies mangled source data to output and marks it as an error entry."""
    # Ensure we don't overflow the physical output buffer string space
    safe_len = min(length, out_b.buffer.shape[0] - out_cursor)
    if safe_len > 0:
        nb_copy_buf(src_buf, src_off, out_b.buffer, out_cursor, safe_len)

    out_b.offsets[out_idx] = out_cursor
    out_b.lengths[out_idx] = safe_len
    out_b.levels[out_idx] = level
    out_b.modules[out_idx] = module
    return out_cursor + safe_len

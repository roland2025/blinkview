# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import NUMBA_DISABLE, app_njit

# Below this size nb_fill_buf loops instead of slice-assigning
COPY_THRESHOLD = 256

# Constants for the word-at-a-time (SWAR) byte search
_ONES = np.uint64(0x0101010101010101)
_HIGH = np.uint64(0x8080808080808080)

if NUMBA_DISABLE:

    def nb_memmove(dst, dst_off, src, src_off, length):
        """memmove between (possibly identical) arrays; numpy slice assignment handles overlap."""
        dst[dst_off : dst_off + length] = src[src_off : src_off + length]

    def nb_find_byte(buf, start, end, value):
        """Index of the first `value` byte in buf[start:end], or `end` if there is none."""
        i = start
        while i < end and buf[i] != value:
            i += 1
        return i

else:
    from llvmlite import ir
    from numba import types
    from numba.extending import intrinsic

    @intrinsic
    def _nb_memmove_addr(typingctx, dst, src, length):
        sig = types.void(types.int64, types.int64, types.int64)

        def codegen(context, builder, sig, args):
            i8p = ir.PointerType(ir.IntType(8))
            fn = builder.module.declare_intrinsic("llvm.memmove", [i8p, i8p, ir.IntType(64)])
            builder.call(
                fn,
                [
                    builder.inttoptr(args[0], i8p),
                    builder.inttoptr(args[1], i8p),
                    args[2],
                    ir.Constant(ir.IntType(1), 0),
                ],
            )
            return context.get_dummy_value()

        return sig, codegen

    @intrinsic
    def _nb_load_u64(typingctx, addr):
        sig = types.uint64(types.int64)

        def codegen(context, builder, sig, args):
            return builder.load(builder.inttoptr(args[0], ir.PointerType(ir.IntType(64))), align=1)

        return sig, codegen

    @app_njit(inline="always")
    def nb_memmove(dst, dst_off, src, src_off, length):
        """memmove between (possibly identical) arrays via llvm.memmove.

        Much cheaper than numba slice assignment (~35 ns fixed cost per call) or a byte loop for the small
        payloads that dominate log parsing. Arrays must be C-contiguous."""
        size = dst.itemsize
        _nb_memmove_addr(
            np.int64(dst.ctypes.data) + dst_off * size,
            np.int64(src.ctypes.data) + src_off * size,
            length * size,
        )

    @app_njit(inline="always")
    def nb_find_byte(buf, start, end, value):
        """Index of the first `value` byte in buf[start:end], or `end` if there is none.

        Scans 8 bytes per step (zero-byte test on word ^ broadcast value), then finishes bytewise. Never reads
        outside [start, end). Only detects *some* matching byte within a word, so endianness is irrelevant."""
        i = start
        pattern = np.uint64(value) * _ONES
        base = np.int64(buf.ctypes.data)
        while i + 8 <= end:
            w = _nb_load_u64(base + i) ^ pattern
            if ((w - _ONES) & ~w & _HIGH) != np.uint64(0):
                break
            i += 8
        while i < end and buf[i] != value:
            i += 1
        return i


@app_njit(inline="always")
def nb_copy_buf(src, src_off, dst, dst_off, length):
    """Copy `length` items from src to dst (memmove semantics)."""
    if length > 0:
        nb_memmove(dst, dst_off, src, src_off, length)


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
    """Safely moves data within the same buffer (memmove; overlapping regions are fine)."""
    if length <= 0 or src_off == dst_off:
        return
    nb_memmove(buf, dst_off, buf, src_off, length)


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

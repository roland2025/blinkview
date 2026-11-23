# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import NUMBA_DISABLE, app_njit

# Constants for the word-at-a-time (SWAR) byte search
_ONES = np.uint64(0x0101010101010101)
_HIGH = np.uint64(0x8080808080808080)

if NUMBA_DISABLE:

    def nb_memmove(dst, dst_off, src, src_off, length):
        """memmove between (possibly identical) arrays; numpy slice assignment handles overlap.

        `src` may also be a plain buffer (the push kernel's `bytes` message), which numpy will
        not assign into a uint8 slice directly - the jitted path takes its address instead, so
        wrap it to match."""
        if not isinstance(src, np.ndarray):
            src = np.frombuffer(src, dtype=dst.dtype)
        dst[dst_off : dst_off + length] = src[src_off : src_off + length]

    def nb_find_byte(buf, start, end, value):
        """Index of the first `value` byte in buf[start:end], or `end` if there is none."""
        i = start
        while i < end and buf[i] != value:
            i += 1
        return i

    def nb_find_unprintable(buf, start, end):
        """Index of the first byte in buf[start:end] outside printable ASCII (0x20..0x7E), or `end`."""
        i = start
        while i < end and 32 <= buf[i] <= 126:
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
    def _nb_buf_addr(typingctx, buf):
        """Data pointer of any Numba buffer, as an int64.

        `arr.ctypes.data` only exists for `types.Array`, but `PooledLogBatch.insert()` hands the
        push kernel a plain Python `bytes` object, which Numba types as `types.Bytes` - same
        array struct underneath, no `.ctypes` attribute. Reading `.data` off the struct directly
        covers both, so nb_memmove works for a bytes/bytearray source as well as an ndarray
        one."""
        if not isinstance(buf, types.Buffer):
            return None
        sig = types.int64(buf)

        def codegen(context, builder, sig, args):
            ary = context.make_helper(builder, sig.args[0], value=args[0])
            return builder.ptrtoint(ary.data, ir.IntType(64))

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
            _nb_buf_addr(dst) + dst_off * size,
            _nb_buf_addr(src) + src_off * size,
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
    def nb_find_unprintable(buf, start, end):
        """Index of the first byte in buf[start:end] outside printable ASCII (0x20..0x7E), or `end`.

        Scans 8 bytes per step, then finishes bytewise. Per byte lane of the word w, (w - 0x20) sets the high
        bit for a byte below 0x20, (w + 0x01) for 0x7F, and w itself for 0x80 and up. A borrow or carry into
        the next lane only happens next to such a byte, so a clean word never stops the scan."""
        i = start
        low = np.uint64(0x20) * _ONES
        base = np.int64(buf.ctypes.data)
        while i + 8 <= end:
            w = _nb_load_u64(base + i)
            if ((w - low) | (w + _ONES) | w) & _HIGH != np.uint64(0):
                break
            i += 8
        while i < end and 32 <= buf[i] <= 126:
            i += 1
        return i


@app_njit(inline="always")
def nb_copy_buf(src, src_off, dst, dst_off, length):
    """Copy `length` items from src to dst (memmove semantics)."""
    if length > 0:
        nb_memmove(dst, dst_off, src, src_off, length)


@app_njit(inline="always")
def nb_sync_push(f_buf, write_pos, src_bytes, length):
    """Appends `length` bytes of src_bytes to the pending frame in f_buf at write_pos."""
    nb_copy_buf(src_bytes, 0, f_buf, write_pos, length)


@app_njit(inline="always")
def nb_sync_shift_leftovers(f_buf, target_buf, target_off, length):
    """Moves the unconsumed tail of a frame, target_buf[target_off:target_off + length], to the start of f_buf.
    target_buf is either the input buffer or f_buf itself; nb_copy_buf is a memmove, so the overlap of the
    latter is fine."""
    nb_copy_buf(target_buf, target_off, f_buf, 0, length)


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

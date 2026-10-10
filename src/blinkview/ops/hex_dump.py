# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Raw bytes of a source as hexdump rows (parsers/raw_hex.py):

    00000000: 48 65 6c 6c 6f 20 77 6f 72 6c 64 0a              |Hello world.|

Every chunk a source read starts a new row and keeps its receive time, so the rows show how
the bytes arrived, not only what they were."""

import numpy as np

from blinkview.core.dtypes import BYTE
from blinkview.core.numba_config import app_njit
from blinkview.core.types.hex_dump import HexDumpState
from blinkview.core.types.log_batch import LogBundle

HEX_OFFSET_DIGITS = 8
HEX_DIGITS = np.frombuffer(b"0123456789abcdef", dtype=BYTE)


def hex_dump_line_length(bytes_per_row: int) -> int:
    """Longest row for this width: offset, ': ', 'xx ' per byte, and the text between bars."""
    return HEX_OFFSET_DIGITS + 2 + bytes_per_row * 3 + 1 + bytes_per_row + 1


@app_njit()
def nb_hex_dump(
    in_b: LogBundle,
    state: HexDumpState,
    out_b: LogBundle,
    bytes_per_row,
    split_on,
    max_bytes,
    level,
    module_id,
    device_id,
):
    """Dumps in_b's rows (raw chunks) into out_b from where `state` left off. Returns True when
    out_b filled up first: flush it and call again. A row ends after `bytes_per_row` bytes, at
    the end of its chunk, or after a `split_on` byte (-1: none). With `max_bytes` > 0 the dump
    stops there, sets state.capped and drops the rest."""
    in_size = in_b.size[0]
    in_buf = in_b.buffer
    out_buf = out_b.buffer
    out_buf_len = len(out_buf)
    out_capacity = out_b.capacity
    text_col = HEX_OFFSET_DIGITS + 2 + bytes_per_row * 3
    max_line_len = text_col + 1 + bytes_per_row + 1

    i = state.in_idx[0]
    pos = state.in_pos[0]
    offset = state.stream_offset[0]

    while i < in_size:
        chunk_start = np.int64(in_b.offsets[i])
        chunk_len = np.int64(in_b.lengths[i])

        while pos < chunk_len:
            if max_bytes > 0 and offset >= max_bytes:
                state.capped[0] = 1
                state.in_idx[0] = in_size
                state.in_pos[0] = 0
                state.stream_offset[0] = offset
                return False

            out_idx = out_b.size[0]
            cursor = np.int64(out_b.msg_cursor[0])
            if out_idx >= out_capacity or cursor + max_line_len > out_buf_len:
                state.in_idx[0] = i
                state.in_pos[0] = pos
                state.stream_offset[0] = offset
                return True

            row_len = min(bytes_per_row, chunk_len - pos)
            if max_bytes > 0:
                row_len = min(row_len, max_bytes - offset)
            first = chunk_start + pos
            if split_on >= 0:
                for k in range(row_len):
                    if in_buf[first + k] == split_on:
                        row_len = k + 1
                        break

            # Offset column
            for digit in range(HEX_OFFSET_DIGITS):
                shift = 4 * (HEX_OFFSET_DIGITS - 1 - digit)
                out_buf[cursor + digit] = HEX_DIGITS[(offset >> shift) & 0xF]
            out_buf[cursor + HEX_OFFSET_DIGITS] = 58  # ':'
            out_buf[cursor + HEX_OFFSET_DIGITS + 1] = 32

            # Hex columns, padded so the text column lines up on a short row
            hex_col = cursor + HEX_OFFSET_DIGITS + 2
            for k in range(bytes_per_row):
                if k < row_len:
                    value = np.int64(in_buf[first + k])
                    out_buf[hex_col + k * 3] = HEX_DIGITS[value >> 4]
                    out_buf[hex_col + k * 3 + 1] = HEX_DIGITS[value & 0xF]
                else:
                    out_buf[hex_col + k * 3] = 32
                    out_buf[hex_col + k * 3 + 1] = 32
                out_buf[hex_col + k * 3 + 2] = 32

            # Text column
            text = cursor + text_col
            out_buf[text] = 124  # '|'
            for k in range(row_len):
                value = in_buf[first + k]
                out_buf[text + 1 + k] = value if 32 <= value < 127 else 46  # '.'
            out_buf[text + 1 + row_len] = 124
            line_len = text_col + 1 + row_len + 1

            out_b.timestamps[out_idx] = in_b.timestamps[i]
            out_b.rx_timestamps[out_idx] = in_b.rx_timestamps[i]
            out_b.offsets[out_idx] = cursor
            out_b.lengths[out_idx] = line_len
            if out_b.has_levels:
                out_b.levels[out_idx] = level
            if out_b.has_modules:
                out_b.modules[out_idx] = module_id
            if out_b.has_devices:
                out_b.devices[out_idx] = device_id
            # Recycled memory is not zeroed: a column this row has nothing for is cleared.
            if out_b.has_sequences:
                out_b.sequences[out_idx] = 0
            if out_b.has_pids:
                out_b.pids[out_idx] = 0
            if out_b.has_tids:
                out_b.tids[out_idx] = 0
            if out_b.has_ext_u32_1:
                out_b.ext_u32_1[out_idx] = 0
            if out_b.has_ext_u32_2:
                out_b.ext_u32_2[out_idx] = 0
            if out_b.has_ext_u64_1:
                out_b.ext_u64_1[out_idx] = 0

            out_b.msg_cursor[0] = cursor + line_len
            out_b.size[0] = out_idx + 1
            pos += row_len
            offset += row_len

        i += 1
        pos = 0

    state.in_idx[0] = in_size
    state.in_pos[0] = 0
    state.stream_offset[0] = offset
    return False

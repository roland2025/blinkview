# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit, literal_unroll
from blinkview.core.types.frames import FrameConfig, FrameStateParams
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import ParserPipelineBundle
from blinkview.ops.strings import squash_spaces_inplace, trim_spaces


@app_njit()
def execute_parser_pipeline(buffer, start_cursor, end_cursor, out_b, out_idx, parser_bundles):
    """
    Refactored to support the (func, state, config) universal signature.
    Using literal_unroll ensures Numba unrolls this loop at compile-time
    for each specific tuple type, preventing type-inference failure.
    """
    if len(parser_bundles) == 0:
        return start_cursor

    cursor = start_cursor

    for bundle in literal_unroll(parser_bundles):
        # 1. Destructure the 3-tuple (Function, Mutable State, Immutable Config)
        func = bundle[0]
        state = bundle[1]
        config = bundle[2]

        # 2. Call the parser with the universal 7-argument signature
        # This is now consistent across all parsing stages (stateful or stateless)
        cursor = func(buffer, cursor, end_cursor, out_b, out_idx, state, config)

        # 3. Fast-fail the entire pipeline if any stage returns -1
        if cursor == -1:
            return -1

    return cursor


@app_njit()
def process_batch_kernel(
    f_cfg: FrameConfig,
    f_state: FrameStateParams,
    in_b: LogBundle,
    parser: ParserPipelineBundle,
    o_cfg: OutputConfig,
    out_b: LogBundle,
):
    curr_write = f_state.offset[0]
    in_idx = f_state.in_idx[0]
    read_offset = f_state.in_offset[0]
    in_frame = f_state.in_frame[0]
    f_buf = f_state.buffer

    in_size = in_b.size

    p_cfg = parser.config

    filter_squash_spaces = p_cfg.filter_squash_spaces
    frame_length_min = f_cfg.length_min
    frame_length_max = f_cfg.length_max
    frame_length_fixed = f_cfg.length_fixed
    frame_length = f_cfg.length
    frame_decode_func = f_cfg.decode_func
    frame_delimiter = f_cfg.delimiter
    filter_trim_r = f_cfg.filter_trim_r

    compact_buffer = o_cfg.compact_buffer

    trim_r = 0

    start_out_idx = out_b.size
    curr_out_idx = start_out_idx
    curr_out_cursor = out_b.msg_cursor

    out_cap = out_b.timestamps.shape[0]
    out_buf_cap = out_b.buffer.shape[0]

    device_id = p_cfg.device_id
    default_level = p_cfg.level_default

    out_full = False
    report_frame_error = f_cfg.report_error
    report_parser_error = p_cfg.report_error

    report_errors = report_frame_error or report_parser_error

    parser_bundles = parser.pipeline

    while in_idx < in_size:
        in_len = in_b.lengths[in_idx]
        in_off = in_b.offsets[in_idx]
        ts_in = in_b.timestamps[in_idx]

        # 1. FIND ALL DELIMITERS IN THE CHUNK AT ONCE
        chunk_slice = in_b.buffer[in_off : in_off + in_len]
        delim_indices = np.where(chunk_slice == frame_delimiter)[0]
        num_delims = len(delim_indices)
        delim_ptr = 0

        while read_offset < in_len:
            while delim_ptr < num_delims and delim_indices[delim_ptr] < read_offset:
                delim_ptr += 1

            if delim_ptr < num_delims:
                found_delim = True
                scan_idx = delim_indices[delim_ptr]
            else:
                found_delim = False
                scan_idx = in_len

            if filter_trim_r:
                is_r = found_delim and (scan_idx > read_offset) and (in_b.buffer[in_off + scan_idx - 1] == 13)
                trim_r = 1 if is_r else 0

            chunk_len = (scan_idx - read_offset) + (1 if found_delim else 0)

            # Look-ahead: Only check output capacity if we intend to process this frame
            if found_delim and in_frame:
                if (curr_out_idx >= out_cap) or (curr_out_cursor + curr_write + chunk_len > out_buf_cap):
                    out_full = True
                    break

            process_frame = False
            target_buf = f_buf
            target_start = 0
            target_end = 0
            error_code = 0  # 0: OK, 1: Frame Length Error, 2: Parser Error

            # 2. ROUTING: ZERO-COPY, STITCH, OR DISCARD
            if found_delim and in_frame and curr_write == 0 and chunk_len <= frame_length_max:
                # --- ZERO-COPY PATH ---
                target_buf = in_b.buffer
                target_start = in_off + read_offset
                target_end = target_start + chunk_len - 1 - trim_r

                read_offset += chunk_len
                process_frame = True
            else:
                # --- STITCH OR DISCARD PATH ---
                if in_frame:
                    if curr_write + chunk_len <= frame_length_max:
                        f_buf[curr_write : curr_write + chunk_len] = in_b.buffer[
                            in_off + read_offset : in_off + read_offset + chunk_len
                        ]
                        curr_write += chunk_len
                    else:
                        # Frame exceeded max length. Mute receiver until next delimiter.
                        in_frame = False

                read_offset += chunk_len

                if found_delim:
                    if in_frame:
                        target_buf = f_buf
                        target_start = 0
                        target_end = curr_write - 1 - trim_r
                        process_frame = True
                    else:
                        # We were discarding, but we just hit a delimiter.
                        # Time to resync and start listening again.
                        in_frame = True
                        curr_write = 0

            # 3. PROCESS VALID FRAME
            if process_frame:
                out_b.timestamps[curr_out_idx] = ts_in
                out_b.levels[curr_out_idx] = default_level

                raw_cursor = frame_decode_func(
                    target_buf, target_start, target_end, out_b.buffer, curr_out_cursor, f_cfg
                )
                final_cursor = raw_cursor

                total_frame_length = final_cursor - curr_out_cursor
                if total_frame_length > 0:
                    is_valid_frame = False

                    if frame_length_fixed != 0:
                        if total_frame_length == frame_length:
                            is_valid_frame = True
                    else:
                        if total_frame_length >= frame_length_min:
                            is_valid_frame = True

                    if not is_valid_frame:
                        if report_frame_error:
                            error_code = 1  # Frame size mismatch
                    else:
                        # Parser Pipeline
                        msg_start = execute_parser_pipeline(
                            out_b.buffer, curr_out_cursor, final_cursor, out_b, curr_out_idx, parser_bundles
                        )

                        if msg_start == -1:
                            if report_parser_error:
                                error_code = 2  # Parsing failed
                        else:
                            if filter_squash_spaces:
                                msg_start, final_cursor = squash_spaces_inplace(out_b.buffer, msg_start, final_cursor)
                            else:
                                msg_start, final_cursor = trim_spaces(out_b.buffer, msg_start, final_cursor)

                            payload_length = final_cursor - msg_start

                            if payload_length > 0:
                                if compact_buffer:
                                    if msg_start > curr_out_cursor:
                                        for idx in range(payload_length):
                                            out_b.buffer[curr_out_cursor + idx] = out_b.buffer[msg_start + idx]
                                    out_b.offsets[curr_out_idx] = curr_out_cursor
                                    out_b.lengths[curr_out_idx] = payload_length
                                    curr_out_cursor += payload_length
                                else:
                                    out_b.offsets[curr_out_idx] = msg_start
                                    out_b.lengths[curr_out_idx] = payload_length
                                    curr_out_cursor = final_cursor

                                curr_out_idx += 1

                    if report_errors and error_code > 0:
                        out_b.offsets[curr_out_idx] = curr_out_cursor
                        out_b.lengths[curr_out_idx] = total_frame_length
                        out_b.levels[curr_out_idx] = p_cfg.level_error
                        out_b.modules[curr_out_idx] = p_cfg.module_unknown

                        curr_out_cursor += total_frame_length
                        curr_out_idx += 1

                curr_write = 0

        if out_full:
            break

        in_idx += 1
        read_offset = 0

    # --- VECTORIZED BLOCK FILL ---
    # This happens once per batch instead of once per frame.
    if curr_out_idx > start_out_idx:
        out_b.devices[start_out_idx:curr_out_idx] = device_id

    # SAVE STATE BACK TO THE TUPLE
    f_state.offset[0] = curr_write
    f_state.in_idx[0] = in_idx
    f_state.in_offset[0] = read_offset
    f_state.in_frame[0] = in_frame

    return curr_out_idx, curr_out_cursor, out_full

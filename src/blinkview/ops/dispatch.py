# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.frames import FrameConfig, FrameStateParams
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import STATE_COMPLETE, STATE_ERROR, STATE_INCOMPLETE, ParserPipelineBundle
from blinkview.ops.buffers import nb_copy_buf, nb_move_buf, nb_report_error, nb_sync_push, nb_sync_shift_leftovers
from blinkview.ops.frame_dispatch import dispatch_frame_decoder
from blinkview.ops.pipeline import execute_parser_pipeline
from blinkview.ops.strings import squash_spaces_inplace, trim_spaces


@app_njit()
def process_batch_kernel(
    f_cfg,
    f_state: FrameStateParams,
    in_b,
    parser,
    o_cfg,
    out_b,
):
    # --- 1. Load State from length-1 arrays ---
    curr_write = f_state.offset[0]
    in_idx = f_state.in_idx[0]
    read_offset = f_state.in_offset[0]
    in_frame = f_state.in_frame[0]
    f_buf = f_state.buffer
    f_ts_buf = f_state.ts_buffer

    in_size = in_b.size[0]
    p_cfg = parser.config

    # Frame Configs
    frame_delimiter = f_cfg.delimiter
    frame_length_min = f_cfg.length_min
    frame_length_max = f_cfg.length_max
    frame_length_fixed = f_cfg.length_fixed
    frame_length = f_cfg.length
    report_frame_error = f_cfg.report_error
    is_pre_framed = f_cfg.decode_id == 0

    # Parser/Output Configs
    filter_squash_spaces = p_cfg.filter_squash_spaces
    report_parser_error = p_cfg.report_error
    device_id = p_cfg.device_id
    default_level = p_cfg.level_default
    default_module = p_cfg.module_log
    compact_buffer = o_cfg.compact_buffer

    start_out_idx = out_b.size[0]
    curr_out_idx = start_out_idx
    curr_out_cursor = out_b.msg_cursor[0]

    out_cap = out_b.timestamps.shape[0]
    out_buf_cap = out_b.buffer.shape[0]
    report_errors = report_frame_error or report_parser_error
    out_full = False

    # --- 2. Main Chunking Loop ---
    while in_idx < in_size:
        in_len = in_b.lengths[in_idx]
        in_off = in_b.offsets[in_idx]
        ts_in = in_b.timestamps[in_idx]

        scan_idx = read_offset

        # Unified Scan & Process Loop
        while scan_idx < in_len:
            found_frame = False
            chunk_len = 0

            # Step A: Determine Frame Boundaries
            if is_pre_framed:
                chunk_len = in_len
                scan_idx = in_len - 1  # Force inner loop to terminate after this pass
                found_frame = True
            else:
                byte = in_b.buffer[in_off + scan_idx]
                if byte == frame_delimiter:
                    chunk_len = (scan_idx - read_offset) + 1
                    found_frame = True

            # Step B: Process the Found Frame
            if found_frame:
                if in_frame:
                    if (curr_out_idx >= out_cap) or (curr_out_cursor + curr_write + chunk_len > out_buf_cap):
                        out_full = True
                        break

                target_buf = f_buf
                target_start = 0
                target_end = 0
                process_frame = False
                is_zero_copy = False
                error_code = 0

                # Determine if Zero-Copy or Buffer Push
                if in_frame and curr_write == 0 and chunk_len <= frame_length_max:
                    target_buf = in_b.buffer
                    target_start = in_off + read_offset
                    target_end = target_start + chunk_len
                    process_frame = True
                    is_zero_copy = True
                else:
                    if in_frame:
                        if curr_write + chunk_len <= frame_length_max:
                            src_view = in_b.buffer[in_off + read_offset : in_off + read_offset + chunk_len]
                            nb_sync_push(f_buf, f_ts_buf, curr_write, src_view, ts_in, chunk_len)

                            curr_write += chunk_len
                            target_buf = f_buf
                            target_start = 0
                            target_end = curr_write
                            process_frame = True
                        else:
                            in_frame = False
                    else:
                        in_frame = True
                        curr_write = 0

                if process_frame:
                    if is_pre_framed:
                        # Bypass decoder: direct memory copy
                        nb_copy_buf(target_buf, target_start, out_b.buffer, curr_out_cursor, chunk_len)

                        final_cursor = curr_out_cursor + chunk_len
                        bytes_consumed = chunk_len
                        decoder_state = STATE_COMPLETE
                    else:
                        # Standard decoder dispatch
                        decoder_state, final_cursor, bytes_consumed = dispatch_frame_decoder(
                            target_buf, target_start, target_end, out_b.buffer, curr_out_cursor, f_cfg, f_state
                        )

                    if decoder_state == STATE_INCOMPLETE:
                        if is_zero_copy:
                            src_view = in_b.buffer[in_off + read_offset : in_off + scan_idx + 1]
                            nb_sync_push(f_buf, f_ts_buf, 0, src_view, ts_in, chunk_len)
                            curr_write = chunk_len

                        read_offset = scan_idx + 1
                        scan_idx += 1
                        continue

                    elif decoder_state == STATE_ERROR:
                        mangled_len = target_end - target_start
                        if report_errors and curr_out_idx < out_cap and mangled_len > 0:
                            curr_out_cursor = nb_report_error(
                                out_b,
                                curr_out_idx,
                                curr_out_cursor,
                                target_buf,
                                target_start,
                                mangled_len,
                                p_cfg.level_error,
                                p_cfg.module_unknown,
                            )
                            curr_out_idx += 1
                        bytes_consumed = target_end - target_start

                    else:
                        # STATE_COMPLETE: Unified Pipeline Execution
                        frame_ts = ts_in

                        out_b.rx_timestamps[curr_out_idx] = frame_ts
                        out_b.timestamps[curr_out_idx] = frame_ts
                        out_b.levels[curr_out_idx] = default_level
                        out_b.modules[curr_out_idx] = default_module

                        total_frame_length = final_cursor - curr_out_cursor

                        if total_frame_length <= 0:
                            error_code = 0
                        else:
                            if frame_length_fixed != 0:
                                is_valid_frame = total_frame_length == frame_length
                            else:
                                is_valid_frame = total_frame_length >= frame_length_min

                            if not is_valid_frame:
                                if report_frame_error:
                                    error_code = 1
                            else:
                                msg_start = execute_parser_pipeline(
                                    out_b.buffer, curr_out_cursor, final_cursor, out_b, curr_out_idx, parser.pipeline
                                )

                                if msg_start == -1:
                                    if report_parser_error:
                                        error_code = 2
                                else:
                                    if filter_squash_spaces:
                                        msg_start, final_cursor = squash_spaces_inplace(
                                            out_b.buffer, msg_start, final_cursor
                                        )
                                    else:
                                        msg_start, final_cursor = trim_spaces(out_b.buffer, msg_start, final_cursor)

                                    payload_length = final_cursor - msg_start

                                    if payload_length > 0:
                                        if compact_buffer:
                                            if msg_start > curr_out_cursor:
                                                nb_move_buf(out_b.buffer, msg_start, curr_out_cursor, payload_length)
                                            out_b.offsets[curr_out_idx] = curr_out_cursor
                                            out_b.lengths[curr_out_idx] = payload_length
                                            curr_out_cursor += payload_length
                                        else:
                                            out_b.offsets[curr_out_idx] = msg_start
                                            out_b.lengths[curr_out_idx] = payload_length
                                            curr_out_cursor = final_cursor

                                        curr_out_idx += 1

                        if report_errors and error_code > 0 and total_frame_length > 0:
                            out_b.offsets[curr_out_idx] = curr_out_cursor
                            out_b.lengths[curr_out_idx] = total_frame_length
                            out_b.levels[curr_out_idx] = p_cfg.level_error
                            out_b.modules[curr_out_idx] = p_cfg.module_unknown
                            curr_out_cursor += total_frame_length
                            curr_out_idx += 1

                    # Shift unconsumed bytes
                    unconsumed = (target_end - target_start) - bytes_consumed
                    if unconsumed > 0:
                        nb_sync_shift_leftovers(
                            f_buf, f_ts_buf, target_buf, target_start + bytes_consumed, ts_in, is_zero_copy, unconsumed
                        )
                        curr_write = unconsumed
                        in_frame = True
                    else:
                        curr_write = 0

                read_offset = scan_idx + 1

            scan_idx += 1

        if out_full:
            break

        # --- 3. Handle Batch Tail-End Carryover ---
        remaining = in_len - read_offset
        if remaining > 0 and in_frame:
            if curr_write + remaining <= frame_length_max:
                src_view = in_b.buffer[in_off + read_offset : in_off + in_len]
                nb_sync_push(f_buf, f_ts_buf, curr_write, src_view, ts_in, remaining)
                curr_write += remaining
            else:
                in_frame = False

        in_idx += 1
        read_offset = 0

    # --- 4. Final Updates & State Persistence ---
    if curr_out_idx > start_out_idx:
        out_b.devices[start_out_idx:curr_out_idx] = device_id
        out_b.size[0] = curr_out_idx
        out_b.msg_cursor[0] = curr_out_cursor

    f_state.offset[0] = curr_write
    f_state.in_idx[0] = in_idx
    f_state.in_offset[0] = read_offset
    f_state.in_frame[0] = in_frame

    return out_full

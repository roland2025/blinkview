# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.frames import FrameConfig, FrameStateParams
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import ParserPipelineBundle
from blinkview.ops.frame_dispatch import dispatch_frame_decoder
from blinkview.ops.pipeline import execute_parser_pipeline
from blinkview.ops.strings import squash_spaces_inplace, trim_spaces


@app_njit()
def process_batch_kernel(
    f_cfg: FrameConfig,
    f_state: FrameStateParams,
    in_b: LogBundle,
    parser: ParserPipelineBundle,
    o_cfg: OutputConfig,
    out_b: LogBundle,
):
    # --- Local Variable Cache ---
    curr_write = f_state.offset[0]
    in_idx = f_state.in_idx[0]
    read_offset = f_state.in_offset[0]
    in_frame = f_state.in_frame[0]
    f_buf = f_state.buffer

    in_size = in_b.size[0]
    p_cfg = parser.config

    # Frame Configs
    frame_delimiter = f_cfg.delimiter
    frame_length_min = f_cfg.length_min
    frame_length_max = f_cfg.length_max
    frame_length_fixed = f_cfg.length_fixed
    frame_length = f_cfg.length
    filter_trim_r = f_cfg.filter_trim_r
    report_frame_error = f_cfg.report_error

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
    parser_bundles = parser.pipeline
    out_full = False

    # --- Main Processing Loop ---
    while in_idx < in_size:
        in_len = in_b.lengths[in_idx]
        in_off = in_b.offsets[in_idx]
        ts_in = in_b.timestamps[in_idx]

        # We scan from where we left off in this chunk
        # Manual scan replaces np.where to avoid allocations and enable SIMD
        for scan_idx in range(read_offset, in_len):
            byte = in_b.buffer[in_off + scan_idx]

            # Check for Delimiter
            if byte == frame_delimiter:
                # Calculate trim for \r if enabled
                trim_r = 0
                if filter_trim_r:
                    if (scan_idx > read_offset) and (in_b.buffer[in_off + scan_idx - 1] == 13):
                        trim_r = 1

                chunk_len = (scan_idx - read_offset) + 1

                # Capacity Check
                if in_frame:
                    if (curr_out_idx >= out_cap) or (curr_out_cursor + curr_write + chunk_len > out_buf_cap):
                        out_full = True
                        break

                target_buf = f_buf
                target_start = 0
                target_end = 0
                process_frame = False
                error_code = 0

                # Routing Logic
                if in_frame and curr_write == 0 and chunk_len <= frame_length_max:
                    # ZERO-COPY PATH
                    target_buf = in_b.buffer
                    target_start = in_off + read_offset
                    target_end = target_start + chunk_len - 1 - trim_r
                    process_frame = True
                else:
                    # STITCH PATH
                    if in_frame:
                        if curr_write + chunk_len <= frame_length_max:
                            # Use slice copy for speed
                            f_buf[curr_write : curr_write + chunk_len] = in_b.buffer[
                                in_off + read_offset : in_off + read_offset + chunk_len
                            ]
                            curr_write += chunk_len

                            target_buf = f_buf
                            target_start = 0
                            target_end = curr_write - 1 - trim_r
                            process_frame = True
                        else:
                            in_frame = False  # Frame too long, mute until next delim
                    else:
                        in_frame = True  # Was muted, now resyncing
                        curr_write = 0

                # Process the frame if routing was successful
                if process_frame:
                    out_b.timestamps[curr_out_idx] = ts_in
                    out_b.levels[curr_out_idx] = default_level
                    out_b.modules[curr_out_idx] = default_module

                    # 1. Frame Decoding (Hard-inlined)
                    final_cursor = dispatch_frame_decoder(
                        target_buf, target_start, target_end, out_b.buffer, curr_out_cursor, f_cfg
                    )

                    total_frame_length = final_cursor - curr_out_cursor

                    # 2. Validation
                    is_valid_frame = False
                    if total_frame_length > 0:
                        if frame_length_fixed != 0:
                            is_valid_frame = total_frame_length == frame_length
                        else:
                            is_valid_frame = total_frame_length >= frame_length_min

                    if not is_valid_frame:
                        if report_frame_error:
                            error_code = 1
                    else:
                        # 3. Parser Pipeline
                        msg_start = execute_parser_pipeline(
                            out_b.buffer, curr_out_cursor, final_cursor, out_b, curr_out_idx, parser_bundles
                        )

                        if msg_start == -1:
                            if report_parser_error:
                                error_code = 2
                        else:
                            # 4. Sanitization
                            if filter_squash_spaces:
                                msg_start, final_cursor = squash_spaces_inplace(out_b.buffer, msg_start, final_cursor)
                            else:
                                msg_start, final_cursor = trim_spaces(out_b.buffer, msg_start, final_cursor)

                            payload_length = final_cursor - msg_start

                            if payload_length > 0:
                                if compact_buffer:
                                    # Copy-back for compaction
                                    if msg_start > curr_out_cursor:
                                        for k in range(payload_length):
                                            out_b.buffer[curr_out_cursor + k] = out_b.buffer[msg_start + k]
                                    out_b.offsets[curr_out_idx] = curr_out_cursor
                                    out_b.lengths[curr_out_idx] = payload_length
                                    curr_out_cursor += payload_length
                                else:
                                    out_b.offsets[curr_out_idx] = msg_start
                                    out_b.lengths[curr_out_idx] = payload_length
                                    curr_out_cursor = final_cursor

                                curr_out_idx += 1

                    # Handle Errors
                    if report_errors and error_code > 0:
                        out_b.offsets[curr_out_idx] = curr_out_cursor
                        out_b.lengths[curr_out_idx] = total_frame_length
                        out_b.levels[curr_out_idx] = p_cfg.level_error
                        out_b.modules[curr_out_idx] = p_cfg.module_unknown
                        curr_out_cursor += total_frame_length
                        curr_out_idx += 1

                # Reset for next search in this chunk
                curr_write = 0
                read_offset = scan_idx + 1

        if out_full:
            break

        # Handle data remaining after the last delimiter in the chunk
        remaining = in_len - read_offset
        if remaining > 0 and in_frame:
            if curr_write + remaining <= frame_length_max:
                f_buf[curr_write : curr_write + remaining] = in_b.buffer[in_off + read_offset : in_off + in_len]
                curr_write += remaining
            else:
                in_frame = False

        in_idx += 1
        read_offset = 0

    # Final Block Fill
    if curr_out_idx > start_out_idx:
        out_b.devices[start_out_idx:curr_out_idx] = device_id

        # USE INDEX [0] TO UPDATE BY REFERENCE
        out_b.size[0] = curr_out_idx
        out_b.msg_cursor[0] = curr_out_cursor

        # SAVE STATE BACK TO THE TUPLE (already using [0] here, which is correct)
    f_state.offset[0] = curr_write
    f_state.in_idx[0] = in_idx
    f_state.in_offset[0] = read_offset
    f_state.in_frame[0] = in_frame

    return out_full

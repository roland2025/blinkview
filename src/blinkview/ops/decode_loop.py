# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The framing/decoding loop behind every frame decoder kernel.

A decoder module defines its kernel as a thin jitted wrapper around its own per-frame function:

    @app_njit()
    def nb_decode_frames_newline(f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0):
        return nb_decode_loop(nb_decode_newline_frame, f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0)

This module has no decoder imports so the decoder modules can import it.
"""

from blinkview.core.numba_config import app_njit
from blinkview.core.types.frames import FrameStateParams
from blinkview.core.types.parsing import STATE_COMPLETE, STATE_ERROR, STATE_INCOMPLETE
from blinkview.ops.buffers import nb_copy_buf, nb_find_byte, nb_report_error, nb_sync_push, nb_sync_shift_leftovers
from blinkview.ops.stage_loop import FS_DECODER_ERR, FS_DROP, FS_FRAME_ERR, FS_OK
from blinkview.ops.views import nb_log_bundle_views, nb_view


@app_njit(inline="always")
def nb_decode_loop(decode_frame, f_cfg, f_state: FrameStateParams, in_b0, p_cfg, o_cfg, out_b0):
    """Framing + decoding loop shared by every frame decoder kernel (Phase A of the stage-major pipeline).

    Splits the input batch into frames, decodes each with `decode_frame(buf, start, end, out_buf, out_cursor,
    f_cfg) -> (state, final_cursor, bytes_consumed)`, reserves one output row per decoded frame and fills the
    per-frame scratch arrays in `f_state`. Returns (out_full, nframes). `decode_frame` is the only decoder
    specific part; it is inlined. Pre-framed input (decode_id 0) is copied through without calling it."""
    # Meminfo-free views of the arrays used per frame (see ops/views.py); they never leave this call.
    in_b = nb_log_bundle_views(in_b0)
    out_b = nb_log_bundle_views(out_b0)
    curr_write = f_state.offset[0]
    in_idx = f_state.in_idx[0]
    read_offset = f_state.in_offset[0]
    in_frame = f_state.in_frame[0]
    f_buf = nb_view(f_state.buffer)
    f_ts_buf = nb_view(f_state.ts_buffer)

    in_size = in_b.size[0]

    frame_delimiter = f_cfg.delimiter
    frame_length_min = f_cfg.length_min
    frame_length_max = f_cfg.length_max
    frame_length_fixed = f_cfg.length_fixed
    frame_length = f_cfg.length
    report_frame_error = f_cfg.report_error
    is_pre_framed = f_cfg.decode_id == 0

    report_parser_error = p_cfg.report_error
    default_level = p_cfg.level_default
    default_module = p_cfg.module_log

    start_out_idx = out_b.size[0]
    curr_out_idx = start_out_idx  # rows reserved so far (one per decoded frame)
    start_out_cursor = out_b.msg_cursor[0]
    curr_out_cursor = start_out_cursor  # uncompacted decode position

    out_cap = out_b.timestamps.shape[0]
    out_buf_cap = out_b.buffer.shape[0]
    report_errors = report_frame_error or report_parser_error
    out_full = False

    # per-frame bookkeeping (preallocated in FrameState), indexed by frame ordinal k = row slot - start_out_idx.
    # Its length caps the rows this call may reserve, so it doubles as an output-capacity limit.
    fstart = f_state.fstart
    fcur = f_state.fcur
    fend = f_state.fend
    ftotal = f_state.ftotal
    fstatus = f_state.fstatus
    out_cap = min(out_cap, start_out_idx + fstatus.shape[0])
    nframes = 0

    # ---------------- Phase A: decode frames, reserve a row slot for each ----------------
    while in_idx < in_size:
        in_len = in_b.lengths[in_idx]
        in_off = in_b.offsets[in_idx]
        ts_in = in_b.timestamps[in_idx]

        scan_idx = read_offset

        while scan_idx < in_len:
            found_frame = False
            chunk_len = 0

            if is_pre_framed:
                chunk_len = in_len
                scan_idx = in_len - 1
                found_frame = True
            else:
                # jump straight to the next delimiter (or the end of this input row)
                scan_idx = nb_find_byte(in_b.buffer, in_off + scan_idx, in_off + in_len, frame_delimiter) - in_off
                if scan_idx < in_len:
                    chunk_len = (scan_idx - read_offset) + 1
                    found_frame = True

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
                        nb_copy_buf(target_buf, target_start, out_b.buffer, curr_out_cursor, chunk_len)
                        final_cursor = curr_out_cursor + chunk_len
                        bytes_consumed = chunk_len
                        decoder_state = STATE_COMPLETE
                    else:
                        decoder_state, final_cursor, bytes_consumed = decode_frame(
                            target_buf, target_start, target_end, out_b.buffer, curr_out_cursor, f_cfg
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
                            # the raw bytes are not in the output buffer yet; nb_report_error copies them in
                            # place at curr_out_cursor and takes a row slot, so it doubles as a frame here
                            k = curr_out_idx - start_out_idx
                            new_cursor = nb_report_error(
                                out_b,
                                curr_out_idx,
                                curr_out_cursor,
                                target_buf,
                                target_start,
                                mangled_len,
                                p_cfg.level_error,
                                p_cfg.module_unknown,
                            )
                            fstart[k] = curr_out_cursor
                            ftotal[k] = new_cursor - curr_out_cursor  # nb_report_error may truncate
                            fstatus[k] = FS_DECODER_ERR  # already fully written
                            curr_out_cursor = new_cursor
                            curr_out_idx += 1
                            nframes += 1
                        bytes_consumed = target_end - target_start

                    else:
                        total_frame_length = final_cursor - curr_out_cursor
                        if total_frame_length > 0:
                            k = curr_out_idx - start_out_idx
                            out_b.rx_timestamps[curr_out_idx] = ts_in
                            out_b.timestamps[curr_out_idx] = ts_in
                            out_b.levels[curr_out_idx] = default_level
                            out_b.modules[curr_out_idx] = default_module
                            fstart[k] = curr_out_cursor
                            fcur[k] = curr_out_cursor
                            fend[k] = final_cursor
                            ftotal[k] = total_frame_length
                            valid = True
                            if frame_length_fixed != 0:
                                valid = total_frame_length == frame_length
                            else:
                                valid = total_frame_length >= frame_length_min
                            if valid:
                                fstatus[k] = FS_OK
                            elif report_frame_error:
                                fstatus[k] = FS_FRAME_ERR
                            else:
                                fstatus[k] = FS_DROP
                            curr_out_idx += 1
                            nframes += 1
                            curr_out_cursor = final_cursor

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

    f_state.offset[0] = curr_write
    f_state.in_idx[0] = in_idx
    f_state.in_offset[0] = read_offset
    f_state.in_frame[0] = in_frame

    return out_full, nframes

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.buffers import nb_move_buf
from blinkview.ops.stage_loop import (
    FS_DECODER_ERR,
    FS_DROP,
    FS_FRAME_ERR,
    FS_OK,
    FS_PARSER_ERR,
    FS_STEP_FAILED,
)
from blinkview.ops.strings import nb_skip_whitespace, nb_skip_whitespace_reverse, nb_squash_spaces_inplace
from blinkview.ops.views import nb_log_bundle_views


@app_njit(inline="always")
def nb_copy_row(out_b, dst, src):
    """Moves every per-row column the pipeline can write from row `src` to row `dst` (offsets/lengths and
    devices are set by the caller)."""
    out_b.rx_timestamps[dst] = out_b.rx_timestamps[src]
    out_b.timestamps[dst] = out_b.timestamps[src]
    if out_b.has_levels:
        out_b.levels[dst] = out_b.levels[src]
    if out_b.has_modules:
        out_b.modules[dst] = out_b.modules[src]
    if out_b.has_sequences:
        out_b.sequences[dst] = out_b.sequences[src]
    if out_b.has_pids:
        out_b.pids[dst] = out_b.pids[src]
    if out_b.has_tids:
        out_b.tids[dst] = out_b.tids[src]
    if out_b.has_ext_u32_1:
        out_b.ext_u32_1[dst] = out_b.ext_u32_1[src]
    if out_b.has_ext_u32_2:
        out_b.ext_u32_2[dst] = out_b.ext_u32_2[src]
    if out_b.has_ext_u64_1:
        out_b.ext_u64_1[dst] = out_b.ext_u64_1[src]


@app_njit()
def nb_finish_frames(fstart, fcur, fend, ftotal, fstatus, p_cfg, compact_buffer, out_b0, nframes):
    """Phase C of the stage-major pipeline: trims, compacts and emits the rows of the `nframes` frames decoded
    by a frame decoder kernel (ops/decode_loop.py) in frame order, after the parser section kernels have run.

    Takes only what it reads: the per-frame scratch arrays of FrameState, the parser config (scalars only: it
    supplies squash_spaces, report_error, device_id, level_error and module_unknown), the output compaction flag
    and the output bundle."""
    out_b = nb_log_bundle_views(out_b0)
    filter_squash_spaces = p_cfg.filter_squash_spaces
    report_parser_error = p_cfg.report_error
    device_id = p_cfg.device_id
    start_out_idx = out_b.size[0]
    start_out_cursor = out_b.msg_cursor[0]

    # ---------------- Phase C: trim, compact, emit rows in order ----------------
    w_cursor = start_out_cursor
    w_idx = start_out_idx
    for k in range(nframes):
        slot = start_out_idx + k
        st = fstatus[k]
        if st == FS_DECODER_ERR:
            # decoder-error row written by nb_report_error at the frame's own position
            if compact_buffer and w_cursor != fstart[k]:
                nb_move_buf(out_b.buffer, fstart[k], w_cursor, ftotal[k])
            if w_idx != slot:
                nb_copy_row(out_b, w_idx, slot)
            out_b.offsets[w_idx] = w_cursor if compact_buffer else fstart[k]
            out_b.lengths[w_idx] = ftotal[k]
            w_cursor = w_cursor + ftotal[k] if compact_buffer else fstart[k] + ftotal[k]
            w_idx += 1
            continue

        if st == FS_STEP_FAILED:
            st = FS_PARSER_ERR if report_parser_error else FS_DROP

        if st == FS_OK:
            msg_start = fcur[k]
            final_cursor = fend[k]
            if filter_squash_spaces:
                msg_start, final_cursor = nb_squash_spaces_inplace(out_b.buffer, msg_start, final_cursor)
            else:
                final_cursor = nb_skip_whitespace_reverse(out_b.buffer, msg_start, final_cursor)
                msg_start = nb_skip_whitespace(out_b.buffer, msg_start, final_cursor)
            payload_length = final_cursor - msg_start
            if payload_length > 0:
                if compact_buffer:
                    if msg_start > w_cursor:
                        nb_move_buf(out_b.buffer, msg_start, w_cursor, payload_length)
                    off = w_cursor
                    w_cursor += payload_length
                else:
                    off = msg_start
                    w_cursor = final_cursor
                if w_idx != slot:
                    nb_copy_row(out_b, w_idx, slot)
                out_b.offsets[w_idx] = off
                out_b.lengths[w_idx] = payload_length
                w_idx += 1
        elif st == FS_FRAME_ERR or st == FS_PARSER_ERR:
            if w_idx != slot:
                nb_copy_row(out_b, w_idx, slot)
            if compact_buffer and w_cursor != fstart[k]:
                nb_move_buf(out_b.buffer, fstart[k], w_cursor, ftotal[k])
            off = w_cursor if compact_buffer else fstart[k]
            out_b.offsets[w_idx] = off
            out_b.lengths[w_idx] = ftotal[k]
            out_b.levels[w_idx] = p_cfg.level_error
            out_b.modules[w_idx] = p_cfg.module_unknown
            w_cursor = w_cursor + ftotal[k] if compact_buffer else fstart[k] + ftotal[k]
            w_idx += 1
        else:
            # dropped frame: nothing emitted; without compaction the region stays as padding
            if not compact_buffer:
                w_cursor = fend[k]

    if w_idx > start_out_idx:
        out_b.devices[start_out_idx:w_idx] = device_id
        out_b.size[0] = w_idx
        out_b.msg_cursor[0] = w_cursor

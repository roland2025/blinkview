# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The per-step loop shared by every parser stage kernel.

A parser module defines its stage as a thin jitted wrapper around its own per-message step function:

    @app_njit()
    def nb_skip_words_parser_stage(out_b0, f_state, n, count):
        nb_stage_loop_a1(nb_skip_words_parser, out_b0, f_state, n, count)

This module has no parser imports so the parser modules can import it.
"""

from blinkview.core.numba_config import app_njit
from blinkview.ops.views import nb_log_bundle_views, nb_view

# Per-frame status values shared with ops/decode_loop.py and ops/dispatch.py
FS_OK = 0
FS_FRAME_ERR = 1
FS_PARSER_ERR = 2
FS_STEP_FAILED = 3  # a pipeline step returned -1 (turned into FS_PARSER_ERR / FS_DROP in the emit phase)
FS_DROP = 4
FS_DECODER_ERR = 5  # row already written by nb_report_error


@app_njit(inline="always")
def nb_stage_loop_a0(step, out_b0, f_state, n):
    """The per-step frame loop for steps that take 0 extra arguments: `step(buffer, cursor, end, out_b, out_idx)`.
    The extras must already be meminfo-free views / scalars (build them in the stage function that calls this
    one; this function is inlined, so they are not converted twice). Numba's inliner does not support *args,
    hence one function per arity."""
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = step(buffer, cur[k], end[k], out_b, first + k)
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r


@app_njit(inline="always")
def nb_stage_loop_a1(step, out_b0, f_state, n, x0):
    """The per-step frame loop for steps that take 1 extra argument: `step(buffer, cursor, end, out_b, out_idx, x0)`.
    The extras must already be meminfo-free views / scalars (build them in the stage function that calls this
    one; this function is inlined, so they are not converted twice). Numba's inliner does not support *args,
    hence one function per arity."""
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = step(buffer, cur[k], end[k], out_b, first + k, x0)
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r


@app_njit(inline="always")
def nb_stage_loop_a2(step, out_b0, f_state, n, x0, x1):
    """The per-step frame loop for steps that take 2 extra arguments: `step(buffer, cursor, end, out_b, out_idx, x0, x1)`.
    The extras must already be meminfo-free views / scalars (build them in the stage function that calls this
    one; this function is inlined, so they are not converted twice). Numba's inliner does not support *args,
    hence one function per arity."""
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = step(buffer, cur[k], end[k], out_b, first + k, x0, x1)
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r


@app_njit(inline="always")
def nb_stage_loop_a3(step, out_b0, f_state, n, x0, x1, x2):
    """The per-step frame loop for steps that take 3 extra arguments: `step(buffer, cursor, end, out_b, out_idx, x0, x1, x2)`.
    The extras must already be meminfo-free views / scalars (build them in the stage function that calls this
    one; this function is inlined, so they are not converted twice). Numba's inliner does not support *args,
    hence one function per arity."""
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = step(buffer, cur[k], end[k], out_b, first + k, x0, x1, x2)
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r

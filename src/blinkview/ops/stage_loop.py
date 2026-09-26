# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The per-step loop shared by every parser stage kernel.

A parser module defines its stage as a thin jitted wrapper around its own per-message step function:

    @app_njit()
    def nb_parse_log_level_stage(out_b0, f_state, n, state0, config0):
        nb_stage_loop(nb_parse_log_level, out_b0, f_state, n, state0, config0)

This module has no parser imports so the parser modules can import it.
"""

from blinkview.core.numba_config import app_njit
from blinkview.ops.views import nb_config_views, nb_log_bundle_views, nb_state_views, nb_view

# Per-frame status values shared with ops/dispatch.py
FS_OK = 0
FS_FRAME_ERR = 1
FS_PARSER_ERR = 2
FS_STEP_FAILED = 3  # a pipeline step returned -1 (turned into FS_PARSER_ERR / FS_DROP in the emit phase)
FS_DROP = 4
FS_DECODER_ERR = 5  # row already written by nb_report_error


@app_njit(inline="always")
def nb_stage_loop(step, out_b0, f_state, n, state0, config0):
    """Runs the per-message `step` over frames 0..n-1 (frame k owns output row `first + k`, `first` being the
    output bundle's current size). Frames whose status is not FS_OK are skipped; a step returning -1 marks its
    frame FS_STEP_FAILED, otherwise the frame's cursor advances to the returned cursor.

    Like nb_stage, the array-carrying arguments are converted to meminfo-free views here, inside the function
    that runs the loop."""
    out_b = nb_log_bundle_views(out_b0)
    buffer = out_b.buffer
    state = nb_state_views(state0)
    config = nb_config_views(config0)
    cur = nb_view(f_state.fcur)
    end = nb_view(f_state.fend)
    status = nb_view(f_state.fstatus)
    first = out_b.size[0]
    for k in range(n):
        if status[k] == FS_OK:
            r = step(buffer, cur[k], end[k], out_b, first + k, state, config)
            if r == -1:
                status[k] = FS_STEP_FAILED
            else:
                cur[k] = r

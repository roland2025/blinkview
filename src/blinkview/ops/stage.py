# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Stage-major parser pipeline: one function, one loop over every frame of a chunk, per pipeline step.

The per-message step functions are the same ones ``ops.pipeline.nb_process_bundle`` dispatches to; they are
inlined into the loop here. Keep the branch list in sync with ``nb_process_bundle``.
"""

from blinkview.core.numba_config import app_njit
from blinkview.ops import pipeline as P
from blinkview.ops.stage_loop import FS_OK, FS_STEP_FAILED
from blinkview.ops.views import nb_config_views, nb_log_bundle_views, nb_state_views, nb_view


@app_njit()
def nb_stage(p_id, buffer0, cur, end, status, first, n, out_b0, state0, config0):
    """Runs pipeline step `p_id` over frames 0..n-1 (frame k owns output row `first + k`).

    The arguments are plain arrays; they are converted to meminfo-free views *here*. Passing views in from the
    caller instead was measured 2x slower overall (246 -> 501 ns/msg with three steps).

    Frames whose status is not FS_OK are skipped; a step returning -1 marks its frame FS_STEP_FAILED.
    `cur[k]` advances to the step's returned cursor."""
    out_b = nb_log_bundle_views(out_b0)
    buffer = nb_view(buffer0)
    state = nb_state_views(state0)
    config = nb_config_views(config0)
    if p_id == P.LEVEL_NAME_MAP:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_log_level(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.MOD_FIXED_WIDTH:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_fixed_width_name(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.MOD_DYNAMIC_SM:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_module_tags_statemachine(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.MOD_RSYSLOG_TAG:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_rsyslog_tag(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.SKIP_WORDS:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_skip_words_parser(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_INTEGER:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_int_timestamp(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_IDF_V1:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_int_timestamp_idf_v1(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_ADB_LONG:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_adb_timestamp_monotonic(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_ZEPHYR_UPTIME_FORMATTED:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_zephyr_uptime_formatted(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_ZEPHYR_REALTIME:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_zephyr_realtime(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_ISO8601:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_iso8601_desktop(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_RFC3339:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_rfc3339(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.TS_RFC3164:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_syslog_timestamp(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.PID_TID_ADB_LONG:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_adb_pid_tid(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.LEVEL_MAP_ADB_LONG:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_adb_level(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    elif p_id == P.MOD_ADB_LONG:
        for k in range(n):
            if status[k] == FS_OK:
                r = P.nb_parse_adb_tag(buffer, cur[k], end[k], out_b, first + k, state, config)
                if r == -1:
                    status[k] = FS_STEP_FAILED
                else:
                    cur[k] = r
    else:
        # unknown step id: same outcome as nb_process_bundle returning -1
        for k in range(n):
            if status[k] == FS_OK:
                status[k] = FS_STEP_FAILED

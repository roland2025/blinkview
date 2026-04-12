# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.types import RegistryParams
from blinkview.core.numba_config import app_njit
from blinkview.core.types.formatting import FormattingConfig
from blinkview.core.types.segments import LogSegmentParams
from blinkview.ops.constants import CHAR_COLON, CHAR_DOT, CHAR_LF, CHAR_QUESTION, CHAR_SPACE, CHAR_ZERO


@app_njit()
def format_log_batch(
    indices: np.ndarray,
    segment: LogSegmentParams,
    tables: RegistryParams,
    cfg: FormattingConfig,
    tz_offset_sec: int,  # Added timezone offset
):
    # Unpack tables
    l_buf, l_off, l_len, l_hash, l_values, l_count = tables.levels
    m_buf, m_off, m_len, m_hash, m_values, m_count = tables.modules
    d_buf, d_off, d_len, d_hash, d_values, d_count = tables.devices

    show_ts, show_dev = cfg.show_ts, cfg.show_dev
    show_lvl, show_mod = cfg.show_lvl, cfg.show_mod

    # Constants
    tz_offset_ns = tz_offset_sec * 1_000_000_000

    # Pre-defined tuples for placeholders (Numba optimizes these perfectly)
    # "unknown" -> (u, n, k, n, o, w, n)
    UNKNOWN_TEXT = (117, 110, 107, 110, 111, 119, 110)

    # --- PHASE 1: SIZE CALCULATION ---
    total_size = 0
    for idx in indices:
        row_size = 0
        if show_ts:
            # CHAR_SPACE that separates the timestamp from the next active field.
            row_size += 13
        if show_dev:
            d_id = segment.devices[idx]
            row_size += (d_len[d_id] if d_id < d_count else 3) + 1  # Name or "???" + space
        if show_lvl:
            l_id = segment.levels[idx]
            row_size += (l_len[l_id] if l_id < l_count else 3) + 1
        if show_mod:
            m_id = segment.modules[idx]
            row_size += (m_len[m_id] if m_id < m_count else 7) + 2  # Name or "unknown" + ": "

        row_size += segment.lengths[idx] + 1  # Msg + \n
        total_size += row_size

    out = np.empty(total_size, dtype=dtypes.BYTE)
    curr = 0

    # --- PHASE 2: WRITING ---
    for idx in indices:
        first_field = True

        # 1. Timestamp with Timezone
        if show_ts:
            ts_ns = segment.timestamps[idx] + tz_offset_ns
            ms = (ts_ns // 1_000_000) % 1000
            sec = (ts_ns // 1_000_000_000) % 60
            mn = (ts_ns // 60_000_000_000) % 60
            hr = (ts_ns // 3_600_000_000_000) % 24

            out[curr + 0], out[curr + 1] = CHAR_ZERO + (hr // 10), CHAR_ZERO + (hr % 10)
            out[curr + 2] = CHAR_COLON
            out[curr + 3], out[curr + 4] = CHAR_ZERO + (mn // 10), CHAR_ZERO + (mn % 10)
            out[curr + 5] = CHAR_COLON
            out[curr + 6], out[curr + 7] = CHAR_ZERO + (sec // 10), CHAR_ZERO + (sec % 10)
            out[curr + 8] = CHAR_DOT
            out[curr + 9] = CHAR_ZERO + (ms // 100)
            out[curr + 10] = CHAR_ZERO + ((ms // 10) % 10)
            out[curr + 11] = CHAR_ZERO + (ms % 10)
            curr += 12
            first_field = False

        # 2. Device
        if show_dev:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            d_id = segment.devices[idx]
            if d_id < d_count:
                ln, off = d_len[d_id], d_off[d_id]
                out[curr : curr + ln] = d_buf[off : off + ln]
                curr += ln
            else:
                for i in range(3):
                    out[curr + i] = CHAR_QUESTION  # "???"
                curr += 3
            first_field = False

        # 3. Level
        if show_lvl:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            l_id = segment.levels[idx]
            if l_id < l_count:
                ln, off = l_len[l_id], l_off[l_id]
                out[curr : curr + ln] = l_buf[off : off + ln]
                curr += ln
            else:
                for i in range(3):
                    out[curr + i] = CHAR_QUESTION
                curr += 3
            first_field = False

        # 4. Module
        if show_mod:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            m_id = segment.modules[idx]
            if m_id < m_count:
                ln, off = m_len[m_id], m_off[m_id]
                out[curr : curr + ln] = m_buf[off : off + ln]
                curr += ln
            else:
                for i in range(len(UNKNOWN_TEXT)):
                    out[curr + i] = UNKNOWN_TEXT[i]
                curr += 7

            out[curr] = CHAR_COLON
            curr += 1
            first_field = False

        # 5. Message
        if not first_field:
            out[curr] = CHAR_SPACE
            curr += 1
        mo, ml = segment.offsets[idx], segment.lengths[idx]
        out[curr : curr + ml] = segment.buffer[mo : mo + ml]
        curr += ml

        out[curr] = CHAR_LF
        curr += 1

    return out[:curr]

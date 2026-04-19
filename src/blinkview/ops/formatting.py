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
def estimate_log_batch_size(
    indices: np.ndarray,
    segment: LogSegmentParams,
    tables: RegistryParams,
    cfg: FormattingConfig,
) -> int:
    # Unpack registry lengths
    _, _, l_len, _, _, l_count = tables.levels
    _, _, m_len, _, _, m_count = tables.modules
    _, _, d_len, _, _, d_count = tables.devices

    # Unpack segment metadata
    s_lens = segment.lengths
    s_devs = segment.devices
    s_lvls = segment.levels
    s_mods = segment.modules

    show_ts, show_dev = cfg.show_ts, cfg.show_dev
    show_lvl, show_mod = cfg.show_lvl, cfg.show_mod

    total_size = 0
    for idx in indices:
        row_size = 0
        is_first = True

        if show_ts:
            row_size += 12  # HH:MM:SS.mmm
            is_first = False

        if show_dev:
            if not is_first:
                row_size += 1
            d_id = s_devs[idx]
            row_size += d_len[d_id] if d_id < d_count else 3
            is_first = False

        if show_lvl:
            if not is_first:
                row_size += 1
            l_id = s_lvls[idx]
            row_size += l_len[l_id] if l_id < l_count else 3
            is_first = False

        if show_mod:
            if not is_first:
                row_size += 1
            m_id = s_mods[idx]
            row_size += (m_len[m_id] if m_id < m_count else 7) + 1  # Name + ":"
            is_first = False

        # Message: space (if needed) + content + newline
        if not is_first:
            row_size += 1
        row_size += s_lens[idx] + 1

        total_size += row_size

    return total_size


@app_njit()
def find_id_index(val_arr: np.ndarray, count: int, target_id: int) -> int:
    """Returns the internal index for a given identity ID, or -1 if not found."""
    for i in range(count):
        if val_arr[i] == target_id:
            return i
    return -1


@app_njit()
def format_log_batch(
    out: np.ndarray,
    indices: np.ndarray,
    segment: LogSegmentParams,
    tables: RegistryParams,
    cfg: FormattingConfig,
    tz_offset_sec: int,
):
    # 1. Unpack Tables
    l_buf, l_off, l_len, _, l_values, l_count = tables.levels
    m_buf, m_off, m_len, _, _, m_count = tables.modules
    d_buf, d_off, d_len, _, _, d_count = tables.devices

    # 2. Unpack Segment (Crucial for Numba stability)
    s_ts = segment.timestamps
    s_lvls = segment.levels
    s_mods = segment.modules
    s_devs = segment.devices
    s_offs = segment.offsets
    s_lens = segment.lengths
    s_buf = segment.buffer

    show_ts, show_dev = cfg.show_ts, cfg.show_dev
    show_lvl, show_mod = cfg.show_lvl, cfg.show_mod
    tz_offset_ns = tz_offset_sec * 1_000_000_000
    UNKNOWN_TEXT = (117, 110, 107, 110, 111, 119, 110)  # "unknown"

    curr = 0
    for idx in indices:
        first_field = True

        # 1. Timestamp
        if show_ts:
            ts_ns = s_ts[idx] + tz_offset_ns
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
            out[curr + 9], out[curr + 10], out[curr + 11] = (
                CHAR_ZERO + (ms // 100),
                CHAR_ZERO + ((ms // 10) % 10),
                CHAR_ZERO + (ms % 10),
            )
            curr += 12
            first_field = False

        # 2. Device
        if show_dev:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            d_id = s_devs[idx]
            if d_id < d_count:
                ln, off = d_len[d_id], d_off[d_id]
                out[curr : curr + ln] = d_buf[off : off + ln]
                curr += ln
            else:
                for i in range(3):
                    out[curr + i] = CHAR_QUESTION
                curr += 3
            first_field = False

        # 3. Level
        if show_lvl:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1

            raw_id = s_lvls[idx]
            # Use the helper to find where this ID lives in our table
            tbl_idx = find_id_index(l_values, l_count, raw_id)

            if tbl_idx != -1:
                ln, off = l_len[tbl_idx], l_off[tbl_idx]
                out[curr : curr + ln] = l_buf[off : off + ln]
                curr += ln
            else:
                # Fallback for unknown LogLevel ID
                for i in range(3):
                    out[curr + i] = CHAR_QUESTION
                curr += 3
            first_field = False

        # 4. Module
        if show_mod:
            if not first_field:
                out[curr] = CHAR_SPACE
                curr += 1
            m_id = s_mods[idx]
            if m_id < m_count:
                ln, off = m_len[m_id], m_off[m_id]
                out[curr : curr + ln] = m_buf[off : off + ln]
                curr += ln
            else:
                for i in range(7):
                    out[curr + i] = UNKNOWN_TEXT[i]
                curr += 7
            out[curr] = CHAR_COLON
            curr += 1
            first_field = False

        # 5. Message
        if not first_field:
            out[curr] = CHAR_SPACE
            curr += 1

        mo, ml = s_offs[idx], s_lens[idx]
        # Explicit bounds check and casting to int64 for the slice
        # This prevents the "assign slice from input" ValueError
        out[curr : curr + ml] = s_buf[mo : mo + ml]
        curr += ml

        out[curr] = CHAR_LF
        curr += 1

    return curr

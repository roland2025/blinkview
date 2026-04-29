# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import SyncState


@app_njit(inline="always")
def parse_iso8601_to_ns(buffer, start, offset_sec):
    """
    Parses 'YYYY-MM-DD HH:MM:SS.mmm' starting at 'start'.
    Returns UTC nanoseconds as int64.
    """
    # 1. Extraction (Fixed offsets relative to YYYY)
    y = (
        (buffer[start + 0] - 48) * 1000
        + (buffer[start + 1] - 48) * 100
        + (buffer[start + 2] - 48) * 10
        + (buffer[start + 3] - 48)
    )
    m = (buffer[start + 5] - 48) * 10 + (buffer[start + 6] - 48)
    d = (buffer[start + 8] - 48) * 10 + (buffer[start + 9] - 48)

    hh = (buffer[start + 11] - 48) * 10 + (buffer[start + 12] - 48)
    mm = (buffer[start + 14] - 48) * 10 + (buffer[start + 15] - 48)
    ss = (buffer[start + 17] - 48) * 10 + (buffer[start + 18] - 48)
    ms = (buffer[start + 20] - 48) * 100 + (buffer[start + 21] - 48) * 10 + (buffer[start + 22] - 48)

    # 2. Julian Day Number Algorithm
    # Formula: $$JDN = d + \lfloor\frac{153m + 2}{5}\rfloor + 365y + \lfloor\frac{y}{4}\rfloor - \lfloor\frac{y}{100}\rfloor + \lfloor\frac{y}{400}\rfloor - 32045$$
    temp_a = (14 - m) // 12
    temp_y = y + 4800 - temp_a
    temp_m = m + 12 * temp_a - 3

    jdn = d + (153 * temp_m + 2) // 5 + 365 * temp_y + temp_y // 4 - temp_y // 100 + temp_y // 400 - 32045
    days_since_1970 = jdn - 2440588

    # 3. Epoch Math
    res_ns = (days_since_1970 * 86400 + hh * 3600 + mm * 60 + ss) * 1_000_000_000
    res_ns += ms * 1_000_000

    return res_ns - (offset_sec * 1_000_000_000)


@app_njit(inline="always")
def nb_apply_drift_projection(raw_ns, anchor_raw, anchor_rx, drift_m, drift_d):
    """
    Core math to project an MCU timestamp to PC time using an anchor and a drift ratio.
    """
    delta = np.int64(raw_ns) - np.int64(anchor_raw)
    drift = np.float64(drift_m) / np.float64(drift_d)

    return dtypes.TS_TYPE(np.int64(anchor_rx) + np.int64(np.float64(delta) * drift))


@app_njit(inline="always")
def nb_auto_sync_fallback(raw_ns, rx_ns, sync: SyncState):
    is_init = sync.auto_init[0]
    last_raw = sync.auto_last_raw[0]
    last_out = sync.auto_anchor_rx[0]
    current_offset = np.int64(rx_ns) - np.int64(raw_ns)

    # 1. Initialization / Reboot
    if not is_init or raw_ns < last_raw:
        sync.auto_init[0] = 1
        sync.auto_last_raw[0] = raw_ns
        sync.auto_anchor_rx[0] = rx_ns
        sync.auto_window_min_offset[0] = current_offset
        sync.auto_warmup_cnt[0] = 512  # Reset warmup on reboot
        return dtypes.TS_TYPE(rx_ns)

    # 2. Track Minimum Offset (Baseline)
    min_offset = sync.auto_window_min_offset[0]
    if current_offset < min_offset:
        min_offset = current_offset
    else:
        min_offset += 1000  # 1us leaky bucket
    sync.auto_window_min_offset[0] = min_offset

    # 3. Hardware Spacing
    delta_raw = np.int64(raw_ns) - np.int64(last_raw)
    predicted_rx = last_out + delta_raw

    # 4. Phase-Locked Loop (PLL) Correction
    ideal_rx = np.int64(raw_ns) + min_offset
    error = ideal_rx - predicted_rx

    # --- DUAL-STAGE CORRECTION ---
    if sync.auto_warmup_cnt[0] > 0:
        divisor = 16  # Fast Lock: ~70 logs to 99% convergence
        sync.auto_warmup_cnt[0] -= 1
    else:
        divisor = 256  # Stable Track: Resists jitter, handles drift

    correction = error // divisor

    # Slew Rate Limiter (CRITICAL for fast-sync)
    # Even with aggressive correction, we protect the micro-spacing
    # of logs within a burst by capping the shift to 50% of the gap.
    max_adj = delta_raw // 2
    if correction < -max_adj:
        correction = -max_adj
    elif correction > max_adj:
        correction = max_adj

    corrected_rx = predicted_rx + correction

    # 5. Monotonicity Guard
    if corrected_rx <= last_out:
        corrected_rx = last_out + 1000

    # Persist state
    sync.auto_last_raw[0] = raw_ns
    sync.auto_anchor_rx[0] = corrected_rx

    return dtypes.TS_TYPE(corrected_rx)


@app_njit(inline="always")
def nb_project_synced_ns(raw_ns, rx_ns, sync: SyncState):
    # Delegate to Auto-Sync if hardware sync isn't ready
    if not sync.enabled[0]:
        return nb_auto_sync_fallback(raw_ns, rx_ns, sync)

    # Delegate to Formal Sync
    i = sync.active_idx[0]
    return nb_apply_drift_projection(raw_ns, sync.ref_time[i], sync.offset[i], sync.drift_m[i], sync.drift_d[i])

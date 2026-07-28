# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import SyncState
from blinkview.ops.constants import CHAR_NINE, CHAR_ZERO
from blinkview.ops.strings import nb_skip_whitespace


@app_njit(inline="always")
def nb_parse_iso8601_to_ns(buffer, start, offset_sec):
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
def nb_parse_int_timestamp(
    buffer,
    start_cursor,
    end_cursor,
    out_b,
    out_idx,
    state,
    config,  # Precision is pulled from here
):
    cursor = start_cursor

    if cursor >= end_cursor:
        return -1

    # 2. Parse the integer value
    raw_val = 0
    found_digits = False

    while cursor < end_cursor:
        c = buffer[cursor]
        if CHAR_ZERO <= c <= CHAR_NINE:
            raw_val = raw_val * 10 + int(c - CHAR_ZERO)
            found_digits = True
            cursor += 1
        else:
            break

    if not found_digits:
        return -1

    # 3. Determine multiplier from config
    # 0: Seconds, 1: Millis, 2: Micros, 3: Nanos
    precision = config.timestamp_precision
    multiplier = 1

    if precision == 0:  # Seconds
        multiplier = 1_000_000_000
    elif precision == 1:  # Millis
        multiplier = 1_000_000
    elif precision == 2:  # Micros
        multiplier = 1_000
    elif precision == 3:  # Nanos
        multiplier = 1
    else:
        # Fallback or error if precision is undefined
        return -1

    raw_ns = raw_val * multiplier

    timestamp_unix = config.timestamp_unix
    if timestamp_unix:
        ts = raw_ns
    else:
        rx_ns = out_b.rx_timestamps[out_idx]
        ts = nb_project_synced_ns(raw_ns, rx_ns, state.timestamp.sync)
    # 4. Project and Store

    out_b.timestamps[out_idx] = ts

    return nb_skip_whitespace(buffer, cursor, end_cursor)


@app_njit(inline="always")
def nb_apply_drift_projection(raw_ns, anchor_raw, anchor_rx, drift_m, drift_d):
    """
    Core math to project an MCU timestamp to PC time using an anchor and a drift ratio.
    """
    delta = np.int64(raw_ns) - np.int64(anchor_raw)
    drift = np.float64(drift_m) / np.float64(drift_d)

    return dtypes.TS_TYPE(np.int64(anchor_rx) + np.int64(np.float64(delta) * drift))


@app_njit(inline="always")
def nb_auto_sync_fallback_2(raw_ns, rx_ns, sync: SyncState):
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
        sync.auto_warmup_cnt[0] = 512
        return dtypes.TS_TYPE(rx_ns)

    # --- MOVED UP: We need delta_raw for time-based calculations ---
    delta_raw = np.int64(raw_ns) - np.int64(last_raw)
    safe_delta_raw = delta_raw if delta_raw > 0 else 1  # Prevent division by zero

    # 2. Track Minimum Offset (Baseline)
    min_offset = sync.auto_window_min_offset[0]
    offset_diff = current_offset - min_offset

    if current_offset < min_offset or offset_diff > 5_000_000:
        min_offset = current_offset
        sync.auto_warmup_cnt[0] = 64
    else:
        drift_allowance = safe_delta_raw // 10_000

        # FIX: Eliminate integer truncation dead-zones by adding standard rounding
        # offset instead of standard floor truncation
        adj = (offset_diff + 64) // 128 if offset_diff >= 0 else (offset_diff - 64) // 128
        min_offset += drift_allowance + adj

    sync.auto_window_min_offset[0] = min_offset

    # 3. Hardware Spacing
    predicted_rx = last_out + delta_raw

    # 4. Phase-Locked Loop (PLL) Correction
    ideal_rx = np.int64(raw_ns) + min_offset
    error = ideal_rx - predicted_rx

    if sync.auto_warmup_cnt[0] > 0:
        target_tc_ns = 100_000_000
        sync.auto_warmup_cnt[0] -= 1
    else:
        target_tc_ns = 500_000_000

        # FIX: Guard against close-burst packet arrivals clamping the divisor out
    # Ensure safe_delta_raw doesn't break the PLL tracking weight if packets are side-by-side
    clamped_delta = max(safe_delta_raw, 5_000_000)  # Floor at 5ms window equivalent weight
    divisor = target_tc_ns // clamped_delta
    if divisor < 1:
        divisor = 1

        # FIX: Apply rounded integer division to avoid stuck zero-corrections
    if error >= 0:
        correction = (error + (divisor // 2)) // divisor
    else:
        correction = (error - (divisor // 2)) // divisor

    # Slew Rate Limiter
    max_adj = delta_raw // 2
    if correction > max_adj:
        correction = max_adj
    elif correction < -delta_raw:
        correction = -delta_raw

    corrected_rx = predicted_rx + correction

    # 5. Monotonicity Guard
    if corrected_rx <= last_out:
        corrected_rx = last_out + 1000

    # Persist state
    sync.auto_last_raw[0] = raw_ns
    sync.auto_anchor_rx[0] = corrected_rx

    return dtypes.TS_TYPE(corrected_rx)


@app_njit(inline="always")
def nb_auto_sync_fallback(raw_ns, rx_ns, sync: SyncState):
    is_init = sync.auto_init[0]
    last_raw = sync.auto_last_raw[0]
    last_out = sync.auto_anchor_rx[0]

    # We use auto_window_min_offset to hold our fixed baseline anchor
    anchor_offset = sync.auto_window_min_offset[0]

    # 1. Initialization / Wraparound Reboot
    # Trigger hard reset ONLY if raw_ns jumps backward by more than 1 second (1,000,000,000 ns).
    # This protects the anchor from resetting due to minor out-of-order network packets.
    if not is_init or (raw_ns < last_raw and (last_raw - raw_ns) > 1_000_000_000):
        sync.auto_init[0] = 1
        sync.auto_last_raw[0] = raw_ns
        sync.auto_anchor_rx[0] = rx_ns
        sync.auto_window_min_offset[0] = np.int64(rx_ns) - np.int64(raw_ns)
        return dtypes.TS_TYPE(rx_ns)

    # 2. Strict Linear Projection (Zero Algorithm Jitter)
    # Output spacing is exactly 1:1 with hardware raw_ns spacing.
    projected_rx = np.int64(raw_ns) + anchor_offset

    # 3. Causality Guard (The "Future" Check)
    # If projection exceeds receive time, the host clock is slower than the hardware clock.
    # Clip to reality and drag the anchor backward.
    if projected_rx > np.int64(rx_ns):
        anchor_offset = np.int64(rx_ns) - np.int64(raw_ns)
        projected_rx = np.int64(rx_ns)
        sync.auto_window_min_offset[0] = anchor_offset

    # 4. Monotonicity Guard
    # Ensure time always moves forward (handles out-of-order raw_ns or causality clipping)
    if projected_rx <= last_out:
        projected_rx = last_out + 1000

    # 5. Persist state
    sync.auto_last_raw[0] = raw_ns
    sync.auto_anchor_rx[0] = projected_rx

    return dtypes.TS_TYPE(projected_rx)


@app_njit(inline="always")
def nb_project_synced_ns(raw_ns, rx_ns, sync: SyncState):
    # Delegate to Auto-Sync if hardware sync isn't ready
    if not sync.enabled[0]:
        return nb_auto_sync_fallback(raw_ns, rx_ns, sync)

    # Delegate to Formal Sync
    i = sync.active_idx[0]
    return nb_apply_drift_projection(raw_ns, sync.ref_time[i], sync.offset[i], sync.drift_m[i], sync.drift_d[i])

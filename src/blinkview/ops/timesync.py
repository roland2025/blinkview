# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from math import sqrt
from typing import NamedTuple

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import SyncState

INT64_MAX = 9_223_372_036_854_775_807
ONE_SEC_NS = 1_000_000_000

IDX_BEST_RTT = 0
IDX_FIRST_PH = 1
IDX_FIRST_PC = 2
IDX_SAMPLE_COUNT = 3
IDX_SKIPS = 4
IDX_PPB_PTR = 5
IDX_RTT_PTR = 6
IDX_TOTAL_COUNT = 7
IDX_LAST_MEAN = 8
IDX_LAST_STD = 9

IDX_ARRAY_LENGTH = 10


class EngineState(NamedTuple):
    scalars: np.ndarray  # int64[8]
    ppb_hist: np.ndarray  # int64[15]
    rtt_hist: np.ndarray  # uint64[100]


@app_njit()
def nb_sync_kernel(pc_tx, phone_mono, phone_boot, pc_rx, engine: EngineState, sync: SyncState):
    sc = engine.scalars
    ppb_hist = engine.ppb_hist
    rtt_hist = engine.rtt_hist

    best_rtt = sc[IDX_BEST_RTT]
    s_count = sc[IDX_SAMPLE_COUNT]
    skips = sc[IDX_SKIPS]
    rtt_ptr = sc[IDX_RTT_PTR]
    total_count = sc[IDX_TOTAL_COUNT]

    rtt = np.uint64(pc_rx - pc_tx)

    if rtt > 2_000_000_000:
        return False, 0.0, 0.0, 0.0

    # 1. Jitter Filter
    if rtt < best_rtt:
        best_rtt = int(rtt)
        skips = 0
        is_acceptable = True
    else:
        last_mean_ns = sc[IDX_LAST_MEAN]
        last_std_ns = sc[IDX_LAST_STD]

        if total_count < 20:
            allowance = 15_000_000
            dynamic_ceiling = best_rtt + allowance
        else:
            allowance = max(5_000_000, int(last_std_ns * 3))
            base_anchor = max(last_mean_ns, best_rtt)
            dynamic_ceiling = base_anchor + allowance

        is_acceptable = int(rtt) < dynamic_ceiling

        if not is_acceptable:
            skips += 1
            if skips >= 3:
                best_rtt = int(rtt)
                skips = 0

    sc[IDX_BEST_RTT] = best_rtt
    sc[IDX_SKIPS] = skips

    # 2. Update Ring Buffer
    if is_acceptable:
        rtt_hist[rtt_ptr] = rtt
        sc[IDX_RTT_PTR] = (rtt_ptr + 1) % len(rtt_hist)
        total_count += 1
        sc[IDX_TOTAL_COUNT] = total_count

    # 3. Statistical Analysis
    window_size = min(total_count, len(rtt_hist))
    mean_ms = 0.0
    stddev_ms = 0.0
    quality = 0.0

    mean_ns = 0.0
    stddev_ns = 0.0

    if window_size > 0:
        sum_rtt = 0.0
        for i in range(window_size):
            sum_rtt += float(rtt_hist[i])
        mean_ns = sum_rtt / window_size

        sum_sq_diff = 0.0
        for i in range(window_size):
            diff = float(rtt_hist[i]) - mean_ns
            sum_sq_diff += diff * diff
        variance_ns = sum_sq_diff / window_size
        stddev_ns = sqrt(variance_ns)

        mean_ms = mean_ns / 1_000_000.0
        stddev_ms = stddev_ns / 1_000_000.0

        quality = 1.0 - (mean_ms * 0.005) - (stddev_ms * 0.03)
        if window_size < 20:
            quality -= float(20 - window_size) * 0.01

        if quality < 0.0:
            quality = 0.0
        elif quality > 1.0:
            quality = 1.0

    if not is_acceptable:
        return False, quality, mean_ms, stddev_ms

    # 4. Offset Calculation
    s_count += 1
    new_offset = pc_tx + (int(rtt) // 2)

    # 5. Fixed-Point Drift Calculation (ABSOLUTE DRIFT FIX)
    ppb_scale = 1_000_000_000
    avg_ppb = 0

    # Repurpose these indices to hold BOOT and the ANCHORED PC TIME
    first_boot = sc[IDX_FIRST_PH]
    first_pc_offset = sc[IDX_FIRST_PC]

    if s_count > 5:
        if first_boot == 0:
            first_boot = phone_boot
            first_pc_offset = new_offset  # Store anchored offset, NOT pc_tx
        else:
            # Calculate delta using Absolute Boot and Anchored PC Time
            dt_pc = new_offset - first_pc_offset
            dt_boot = phone_boot - first_boot

            if dt_boot > 1_000_000:
                current_ppb = int((float(dt_pc - dt_boot) / float(dt_boot)) * ppb_scale)

                if abs(current_ppb) > 10_000_000:
                    first_boot = phone_boot
                    first_pc_offset = new_offset
                else:
                    ptr = sc[IDX_PPB_PTR]
                    ppb_hist[ptr] = current_ppb
                    sc[IDX_PPB_PTR] = (ptr + 1) % len(ppb_hist)

                    filled = min(s_count - 5, len(ppb_hist))
                    acc = 0
                    for i in range(filled):
                        acc += ppb_hist[i]
                    avg_ppb = acc // filled

    # 6. Atomic Swap
    act_arr = sync.active_idx
    write_idx = 1 - act_arr[0]

    sync.offset[write_idx] = new_offset
    sync.ref_time[write_idx] = phone_mono  # MONO is mapped to the new_offset for Phase
    sync.drift_m[write_idx] = ppb_scale + avg_ppb
    sync.drift_d[write_idx] = ppb_scale

    sc[IDX_SAMPLE_COUNT] = s_count
    sc[IDX_FIRST_PH] = first_boot  # Save the Boot anchor
    sc[IDX_FIRST_PC] = first_pc_offset  # Save the PC Offset anchor
    sc[IDX_LAST_MEAN] = int(mean_ns)
    sc[IDX_LAST_STD] = int(stddev_ns)

    act_arr[0] = write_idx
    sync.enabled[0] = 1

    return True, quality, mean_ms, stddev_ms

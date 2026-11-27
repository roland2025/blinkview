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

MAX_RTT_SKIPS = 20

# --- Phase (offset) loop-filter tuning -------------------------------------
# These constants are what kill the staircase. See nb_sync_kernel, step 4b.
PHASE_GAIN_SHIFT = 3  # steady-state damping: take 1/8 of the
# residual per sample instead of all of it
PHASE_WARMUP_SAMPLES = 8  # snap fully for the first N samples so
# initial lock-on isn't artificially slow

# The step/slew boundary is NOT a fixed constant - different transports
# (USB CDC, BLE, RTT/UART) have wildly different RTT jitter floors, and a
# threshold calibrated for one will be wrong for another. Instead it's
# derived live each call from stddev_ns (already computed in step 3): a
# residual has to be both bigger than the floor AND many sigma outside the
# link's own currently-measured jitter before it's treated as a real step
# rather than noise to be damped.
PHASE_STEP_THRESHOLD_FLOOR_NS = 100_000_000  # 100ms absolute floor
PHASE_STEP_THRESHOLD_SIGMA_MULT = 8  # ...or 8x measured RTT stddev, whichever is bigger

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
IDX_ASYM_RATIO = 10  # <--- NEW: Asymmetry ratio scaled by 1_000_000 (e.g., 500_000 = 50%)
IDX_PPB_COUNT = 11  # drift samples stored in ppb_hist (capped at its length)
IDX_FIRST_RTT = 12  # RTT of the pong the drift anchor (IDX_FIRST_PH/IDX_FIRST_PC) was taken from
IDX_LAST_CLEAN_PC = 13  # pc_tx of the last pong that was clean against the drift anchor

# A drift sample is measured from the first clean sample on: over a short baseline a clean sample's own
# jitter dominates it (0.2 ms over 10 s reads as 20 ppm; seen on the emulator, whose real drift was 0.2 ppm).
# Below this baseline the drift in force is kept. 60 s: 0.2 ms of jitter is then at most ~3 ppm.
DRIFT_MIN_BASELINE_NS = 60_000_000_000

# A pong is "clean" (measures drift) when its RTT is within this of the anchor pong's RTT. Two pongs whose RTTs
# differ by more read their offsets on different delay splits: a 1.3 ms shorter ESP32 return path moves the raw
# offset by ~0.65 ms, 10 ppm over a 60 s baseline.
DRIFT_CLEAN_WINDOW_NS = 200_000
# With no clean pong for this long the link has changed (e.g. the RTT rose under load), so the next pong near the
# recent minimum RTT becomes the new anchor. Judging "clean" against an all-time best RTT instead froze the drift
# for good after one short pong or a load change (wiki:reports/blinksync-hw-time-sync).
DRIFT_REANCHOR_NS = 60_000_000_000

IDX_ARRAY_LENGTH = 14


class EngineState(NamedTuple):
    scalars: np.ndarray  # int64[IDX_ARRAY_LENGTH]
    ppb_hist: np.ndarray  # int64[15]
    rtt_hist: np.ndarray  # uint64[100]


@app_njit()
def nb_sync_kernel(pc_tx, phone_mono, phone_boot, pc_rx, engine: EngineState, sync: SyncState, anchor_is_boot: bool):
    if pc_rx <= pc_tx:
        return False, 0.0, 0.0, 0.0

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
            # 1. Calculate a dynamic floor based on the link's actual speed (e.g., 50% of best_rtt)
            # We also include a tiny 0.5ms absolute failsafe so we never lock up on perfect links.
            dynamic_floor = max(1_500_000, int(best_rtt * 0.5))

            # 2. Use the greater of the $3\sigma$ variance OR the dynamic floor
            allowance = max(dynamic_floor, int(last_std_ns * 3))
            base_anchor = max(last_mean_ns, best_rtt)
            dynamic_ceiling = base_anchor + allowance

        is_acceptable = int(rtt) < dynamic_ceiling

        if not is_acceptable:
            skips += 1
            if skips >= MAX_RTT_SKIPS:
                best_rtt = int(rtt)
                skips = 0
        else:
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
    recent_min_rtt = int(rtt)  # lowest RTT of the accepted pongs in rtt_hist

    if window_size > 0:
        sum_rtt = 0.0
        for i in range(window_size):
            sum_rtt += float(rtt_hist[i])
            if int(rtt_hist[i]) < recent_min_rtt:
                recent_min_rtt = int(rtt_hist[i])
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

    asym_ratio = sc[IDX_ASYM_RATIO]
    if asym_ratio == 0:
        asym_ratio = 500_000  # Default to 50/50 (0.5 * 1_000_000) if not set

    rtt_offset = (int(rtt) * asym_ratio) // 1_000_000
    raw_offset = pc_tx + rtt_offset

    if anchor_is_boot:
        current_anchor_time = phone_boot
    else:
        current_anchor_time = phone_mono

    act_arr = sync.active_idx
    act_idx = act_arr[0]
    write_idx = 1 - act_idx

    # 4b. Phase loop-filter (this is what removes the staircase).
    #
    # The old code wrote raw_offset (pc_tx + rtt/2) straight into the
    # published offset on every accepted sample. Even accepted samples carry
    # +/- half-RTT of jitter, so the published (offset, ref_time) seam jumps
    # by that noise on every single sample - that jump is the staircase.
    #
    # Fix: predict where the *currently published* model already says we are
    # at this instant, then only nudge the estimate a damped fraction of the
    # way toward the new raw measurement. Consecutive segments then meet
    # almost exactly at the seam, and the filter still converges to the true
    # offset within a handful of samples. Big residuals (real steps, not
    # noise) bypass the damping entirely and snap, so the filter doesn't lag
    # behind genuine clock jumps.
    if sync.enabled[0] == 0:
        # First sample ever - nothing published yet to extrapolate from.
        new_offset = raw_offset
    else:
        prev_offset = sync.offset[act_idx]
        prev_ref = sync.ref_time[act_idx]
        prev_drift_m = sync.drift_m[act_idx]
        prev_drift_d = sync.drift_d[act_idx]

        dt_anchor = current_anchor_time - prev_ref

        if dt_anchor < 0:
            # Anchor clock went backwards (a reboot, or a 32-bit `m` that
            # wrapped) - nothing sane to extrapolate: step, and start over as
            # on a cold start, so the next PHASE_WARMUP_SAMPLES snap too.
            new_offset = raw_offset
            s_count = 1
        else:
            # float math here (matching the style used below for ppb) avoids
            # int64 overflow on long sessions; precision loss is sub-ns.
            predicted_offset = prev_offset + int((float(dt_anchor) * float(prev_drift_m)) / float(prev_drift_d))
            phase_error = raw_offset - predicted_offset

            # stddev_ns is this call's freshly computed RTT-jitter estimate
            # from step 3 - use it so the step/slew boundary tracks how
            # noisy THIS link actually is right now, instead of a fixed
            # guess that's wrong for half the transports this runs on.
            dynamic_step_threshold = max(
                PHASE_STEP_THRESHOLD_FLOOR_NS,
                int(stddev_ns * PHASE_STEP_THRESHOLD_SIGMA_MULT),
            )

            if s_count <= PHASE_WARMUP_SAMPLES or abs(phase_error) > dynamic_step_threshold:
                # Cold start, or a real step. Smoothing a genuine step just
                # makes the filter chase it slowly, which looks worse than
                # one clean jump.
                new_offset = raw_offset
            else:
                # Steady state: take a damped fraction of the residual.
                new_offset = predicted_offset + (phase_error >> PHASE_GAIN_SHIFT)

    # 5. Fixed-Point Drift Calculation (ABSOLUTE DRIFT FIX)
    ppb_scale = 1_000_000_000
    # Only a clean sample measures the drift anew; every other one republishes the drift already in force,
    # so that the estimate is not reset to 0 between clean samples.
    avg_ppb = (sync.drift_m[act_idx] * ppb_scale) // sync.drift_d[act_idx] - ppb_scale

    # The drift anchor: the device BOOT time and the RAW offset (not the smoothed one) of one pong, and its RTT
    first_boot = sc[IDX_FIRST_PH]
    first_pc_offset = sc[IDX_FIRST_PC]
    first_rtt = sc[IDX_FIRST_RTT]
    last_clean_pc = sc[IDX_LAST_CLEAN_PC]

    if first_boot != 0 and phone_boot < first_boot:
        # Reboot: `b` (64-bit time since boot) only goes backwards when the device restarts; `m` can also wrap
        # (32-bit cycle counter), which must not cost the drift history. The anchor belongs to the old boot,
        # where dt_boot would stay negative for as long as the old uptime. Drop it and the drift samples
        # measured against it; the drift in force stays published until the new baseline gives a sample.
        first_boot = 0
        first_pc_offset = 0
        first_rtt = 0
        ppb_hist[:] = 0
        sc[IDX_PPB_PTR] = 0
        sc[IDX_PPB_COUNT] = 0

    rtt_i = int(rtt)
    near_recent_min = (rtt_i - recent_min_rtt) <= DRIFT_CLEAN_WINDOW_NS
    take_anchor = False

    if s_count > 5:
        if first_boot == 0:
            take_anchor = near_recent_min
        elif abs(rtt_i - first_rtt) <= DRIFT_CLEAN_WINDOW_NS:
            last_clean_pc = pc_tx
            dt_pc = raw_offset - first_pc_offset
            dt_boot = phone_boot - first_boot

            if dt_boot > 1_000_000:
                current_ppb = int((float(dt_pc - dt_boot) / float(dt_boot)) * ppb_scale)

                if abs(current_ppb) > 10_000_000:
                    # Implausible: re-anchor and keep the drift
                    take_anchor = True
                elif dt_boot >= DRIFT_MIN_BASELINE_NS:
                    ptr = sc[IDX_PPB_PTR]
                    ppb_hist[ptr] = current_ppb
                    sc[IDX_PPB_PTR] = (ptr + 1) % len(ppb_hist)

                    # Average the samples actually stored: counting every accepted sample instead let the
                    # unfilled slots' zeros pull the average toward 0.
                    filled = min(sc[IDX_PPB_COUNT] + 1, len(ppb_hist))
                    sc[IDX_PPB_COUNT] = filled
                    acc = 0
                    for i in range(filled):
                        acc += ppb_hist[i]
                    avg_ppb = acc // filled
        elif pc_tx - last_clean_pc > DRIFT_REANCHOR_NS and near_recent_min:
            # Not clean against the anchor for a minute: the RTT level moved, re-anchor on the new level
            take_anchor = True

    if take_anchor:
        first_boot = phone_boot
        first_pc_offset = raw_offset
        first_rtt = rtt_i
        last_clean_pc = pc_tx

    # 6. Atomic Swap
    sync.offset[write_idx] = new_offset
    sync.ref_time[write_idx] = current_anchor_time  # computed once in step 4

    sync.drift_m[write_idx] = ppb_scale + avg_ppb
    sync.drift_d[write_idx] = ppb_scale

    sc[IDX_SAMPLE_COUNT] = s_count
    sc[IDX_FIRST_PH] = first_boot  # Save the Boot anchor
    sc[IDX_FIRST_PC] = first_pc_offset  # Save the PC Offset anchor
    sc[IDX_FIRST_RTT] = first_rtt
    sc[IDX_LAST_CLEAN_PC] = last_clean_pc
    sc[IDX_LAST_MEAN] = int(mean_ns)
    sc[IDX_LAST_STD] = int(stddev_ns)

    act_arr[0] = write_idx
    sync.enabled[0] = 1
    # The receive-time fallback restarts from the next row that needs it (nb_project_synced_ns): what it
    # tracked before this model belongs to an older stretch of the device clock.
    sync.auto_init[0] = 0

    return True, quality, mean_ms, stddev_ms

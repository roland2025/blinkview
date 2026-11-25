# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import random

import numpy as np
import pytest

from blinkview.core.types.parsing import create_default_sync
from blinkview.ops.timesync import (
    DRIFT_MIN_BASELINE_NS,
    IDX_ARRAY_LENGTH,
    IDX_BEST_RTT,
    IDX_SAMPLE_COUNT,
    IDX_SKIPS,
    IDX_TOTAL_COUNT,
    MAX_RTT_SKIPS,
    EngineState,
    nb_sync_kernel,
)


def _engine():
    # Mirrors TimeSyncEngine.__init__'s real scalar initialization - IDX_BEST_RTT must start at
    # int64 max, not 0, or the "rtt < best_rtt" jitter-filter branch never fires for any real
    # (positive) RTT.
    scalars = np.zeros(IDX_ARRAY_LENGTH, dtype=np.int64)
    scalars[IDX_BEST_RTT] = np.iinfo(np.int64).max
    return EngineState(scalars=scalars, ppb_hist=np.zeros(15, dtype=np.int64), rtt_hist=np.zeros(50, dtype=np.uint64))


def _sync():
    return create_default_sync(now_ns=0, start_enabled=False)


class TestEarlyRejection:
    def test_pc_rx_not_after_pc_tx_is_rejected(self):
        success, quality, mean_ms, stddev_ms = nb_sync_kernel(
            1000, 0, 0, 1000, _engine(), _sync(), anchor_is_boot=False
        )
        assert (success, quality, mean_ms, stddev_ms) == (False, 0.0, 0.0, 0.0)

    def test_excessive_rtt_is_rejected(self):
        pc_tx = 0
        pc_rx = 3_000_000_000  # 3s RTT, over the 2s hard cutoff
        success, quality, mean_ms, stddev_ms = nb_sync_kernel(
            pc_tx, 0, 0, pc_rx, _engine(), _sync(), anchor_is_boot=False
        )
        assert (success, quality, mean_ms, stddev_ms) == (False, 0.0, 0.0, 0.0)

    def test_rejected_sample_does_not_touch_enabled_flag(self):
        sync = _sync()
        nb_sync_kernel(1000, 0, 0, 1000, _engine(), sync, anchor_is_boot=False)
        assert sync.enabled[0] == 0


class TestFirstAcceptedSample:
    def test_returns_success_with_bounded_quality(self):
        engine = _engine()
        sync = _sync()
        pc_tx = 1_000_000_000
        rtt_ns = 5_000_000  # 5ms
        pc_rx = pc_tx + rtt_ns

        success, quality, mean_ms, stddev_ms = nb_sync_kernel(
            pc_tx, 500_000_000, 500_000_000, pc_rx, engine, sync, anchor_is_boot=False
        )

        assert success is True
        assert 0.0 <= quality <= 1.0
        assert mean_ms == 5.0
        assert stddev_ms == 0.0  # single-sample window has zero variance

    def test_enables_sync_and_flips_active_index(self):
        engine = _engine()
        sync = _sync()
        pc_tx = 1_000_000_000
        pc_rx = pc_tx + 5_000_000

        nb_sync_kernel(pc_tx, 500_000_000, 500_000_000, pc_rx, engine, sync, anchor_is_boot=False)

        assert sync.enabled[0] == 1
        assert sync.active_idx[0] == 1  # started at 0, write_idx = 1 - 0 = 1

    def test_offset_uses_default_fifty_fifty_asymmetry(self):
        engine = _engine()
        sync = _sync()
        pc_tx = 1_000_000_000
        rtt_ns = 5_000_000
        pc_rx = pc_tx + rtt_ns

        nb_sync_kernel(pc_tx, 500_000_000, 500_000_000, pc_rx, engine, sync, anchor_is_boot=False)

        # default asym_ratio is 500_000 (50%) -> rtt_offset = rtt // 2
        assert int(sync.offset[1]) == pc_tx + rtt_ns // 2

    def test_ref_time_uses_phone_mono_when_anchor_is_not_boot(self):
        engine = _engine()
        sync = _sync()
        phone_mono = 42
        phone_boot = 999

        nb_sync_kernel(1_000_000_000, phone_mono, phone_boot, 1_005_000_000, engine, sync, anchor_is_boot=False)

        assert int(sync.ref_time[1]) == phone_mono

    def test_ref_time_uses_phone_boot_when_anchor_is_boot(self):
        engine = _engine()
        sync = _sync()
        phone_mono = 42
        phone_boot = 999

        nb_sync_kernel(1_000_000_000, phone_mono, phone_boot, 1_005_000_000, engine, sync, anchor_is_boot=True)

        assert int(sync.ref_time[1]) == phone_boot

    def test_drift_defaults_to_identity_ratio_before_enough_samples(self):
        engine = _engine()
        sync = _sync()

        nb_sync_kernel(1_000_000_000, 0, 0, 1_005_000_000, engine, sync, anchor_is_boot=False)

        assert int(sync.drift_m[1]) == 1_000_000_000
        assert int(sync.drift_d[1]) == 1_000_000_000

    def test_sample_count_and_best_rtt_are_tracked(self):
        engine = _engine()
        sync = _sync()

        nb_sync_kernel(1_000_000_000, 0, 0, 1_005_000_000, engine, sync, anchor_is_boot=False)

        assert engine.scalars[IDX_SAMPLE_COUNT] == 1
        assert engine.scalars[IDX_BEST_RTT] == 5_000_000
        assert engine.scalars[IDX_TOTAL_COUNT] == 1


class TestSteadyStateAndWarmup:
    def test_second_sample_within_warmup_window_snaps_to_raw_offset(self):
        engine = _engine()
        sync = _sync()

        # First sample: anchors the model.
        nb_sync_kernel(1_000_000_000, 0, 0, 1_005_000_000, engine, sync, anchor_is_boot=False)

        # Second sample, 1 real second later with an identical 5ms RTT and no drift - still
        # within PHASE_WARMUP_SAMPLES, so it should snap directly rather than being damped.
        pc_tx_2 = 2_000_000_000
        phone_mono_2 = 1_000_000_000
        pc_rx_2 = pc_tx_2 + 5_000_000

        success, *_ = nb_sync_kernel(pc_tx_2, phone_mono_2, phone_mono_2, pc_rx_2, engine, sync, anchor_is_boot=False)

        assert success is True
        assert engine.scalars[IDX_SAMPLE_COUNT] == 2
        assert sync.active_idx[0] == 0  # flipped back from 1
        expected_raw_offset = pc_tx_2 + 5_000_000 // 2
        assert int(sync.offset[0]) == expected_raw_offset


class TestJitterRejection:
    def test_a_much_slower_sample_after_a_fast_anchor_is_rejected(self):
        engine = _engine()
        sync = _sync()

        # Anchor a fast best_rtt first.
        nb_sync_kernel(1_000_000_000, 0, 0, 1_001_000_000, engine, sync, anchor_is_boot=False)  # 1ms RTT
        count_after_first = engine.scalars[IDX_SAMPLE_COUNT]

        # Then a wildly slower sample - well past the dynamic ceiling (total_count<20 uses
        # best_rtt + 15ms allowance).
        pc_tx = 2_000_000_000
        pc_rx = pc_tx + 100_000_000  # 100ms RTT
        success, quality, mean_ms, stddev_ms = nb_sync_kernel(
            pc_tx, 1_000_000_000, 1_000_000_000, pc_rx, engine, sync, anchor_is_boot=False
        )

        assert success is False
        assert engine.scalars[IDX_SAMPLE_COUNT] == count_after_first  # not advanced
        assert engine.scalars[IDX_SKIPS] == 1

    def test_enough_consecutive_skips_resets_best_rtt(self):
        engine = _engine()
        sync = _sync()

        nb_sync_kernel(1_000_000_000, 0, 0, 1_001_000_000, engine, sync, anchor_is_boot=False)  # 1ms anchor

        pc_tx = 2_000_000_000
        bad_rtt = 100_000_000
        for _ in range(MAX_RTT_SKIPS):
            pc_rx = pc_tx + bad_rtt
            nb_sync_kernel(pc_tx, 1_000_000_000, 1_000_000_000, pc_rx, engine, sync, anchor_is_boot=False)
            pc_tx += 1_000_000_000

        assert engine.scalars[IDX_SKIPS] == 0
        assert engine.scalars[IDX_BEST_RTT] == bad_rtt


def _simulate_drift(device_ppm, n_samples, seed=1, clean_fraction=0.2):
    """Feeds nb_sync_kernel one ping per host second from a device whose clock runs `device_ppm` fast. Round
    trips are 10-10.2 ms ("clean": within 0.2 ms of the best) for `clean_fraction` of the pings, else
    10.5-15 ms; the device reads its clock halfway, as the default asymmetry assumes, give or take 0.1 ms
    (one leg slower than the other). Returns the engine, the sync state, each sample's published drift in
    ppm, and a function mapping device time to true host time."""
    rng = random.Random(seed)
    engine, sync = _engine(), _sync()
    host0 = 1_795_000_000_000_000_000
    dev0 = 5_000_000_000_000
    rate = 1 + device_ppm * 1e-6  # device ns per host ns

    published_ppm = []
    for i in range(n_samples):
        tx = host0 + i * 1_000_000_000
        if rng.random() < clean_fraction:
            rtt = rng.randint(10_000_000, 10_200_000)
        else:
            rtt = rng.randint(10_500_000, 15_000_000)
        read = tx + rtt // 2 + rng.randint(-100_000, 100_000)
        mono = dev0 + int((read - host0) * rate)
        nb_sync_kernel(tx, mono, mono + 1_000_000, tx + rtt, engine, sync, False)
        idx = int(sync.active_idx[0])
        published_ppm.append((int(sync.drift_m[idx]) / int(sync.drift_d[idx]) - 1) * 1e6)

    def true_host(dev_ns):
        return host0 + (dev_ns - dev0) / rate

    return engine, sync, published_ppm, true_host


def _project(sync, dev_ns):
    idx = int(sync.active_idx[0])
    return int(sync.offset[idx]) + (dev_ns - int(sync.ref_time[idx])) * int(sync.drift_m[idx]) / int(sync.drift_d[idx])


class TestDriftEstimate:
    """The engine publishes host-per-device drift: a device running 50 ppm fast maps to about -50 ppm."""

    def test_published_drift_converges_and_survives_unclean_samples(self):
        _, _, published, _ = _simulate_drift(50, 300)

        # every sample of the last 100 - most of them not clean - publishes the estimate, not 0
        assert all(-55 < ppm < -45 for ppm in published[-100:]), published[-100:]

    def test_no_drift_is_published_from_a_short_baseline(self):
        """Over the first minute a clean sample's jitter would dominate the estimate (0.2 ms over 10 s reads
        as 20 ppm), so the identity drift stays in force until DRIFT_MIN_BASELINE_NS of baseline."""
        _, _, published, _ = _simulate_drift(50, 300)
        baseline_s = DRIFT_MIN_BASELINE_NS // 1_000_000_000

        assert all(ppm == 0 for ppm in published[:baseline_s])
        assert any(ppm != 0 for ppm in published[baseline_s:])

    @pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
    def test_jitter_does_not_make_up_drift(self, seed):
        """The emulator case: a device with no drift must not be given tens of ppm from noisy early samples."""
        _, _, published, _ = _simulate_drift(0, 600, seed=seed)

        assert max(abs(ppm) for ppm in published) < 5, max(published, key=abs)

    def test_no_drift_stays_near_zero(self):
        _, _, published, _ = _simulate_drift(0, 300)

        assert all(abs(ppm) < 2 for ppm in published[-100:])

    def test_extrapolation_after_the_pongs_stop(self):
        """With no pong for a minute, the projection runs on the published drift alone: uncorrected, a 50 ppm
        device is off by 3 ms after 60 s."""
        _, sync, _, true_host = _simulate_drift(50, 300)
        idx = int(sync.active_idx[0])
        dev_later = int(sync.ref_time[idx]) + 60_000_000_000

        error_ns = _project(sync, dev_later) - true_host(dev_later)

        assert abs(error_ns) < 300_000, error_ns

    def test_soft_reset_keeps_the_drift(self):
        engine, sync, _, _ = _simulate_drift(50, 300)
        from blinkview.core.time_sync_engine import TimeSyncEngine

        wrapper = TimeSyncEngine.__new__(TimeSyncEngine)
        wrapper.engine, wrapper.sync, wrapper.logger, wrapper.logger_sync = engine, sync, None, None
        wrapper.soft_reset()
        idx = int(sync.active_idx[0])

        assert -55 < (int(sync.drift_m[idx]) / int(sync.drift_d[idx]) - 1) * 1e6 < -45

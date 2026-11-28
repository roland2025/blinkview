# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import random

import numpy as np
import pytest

from blinkview.core.types.parsing import create_default_sync
from blinkview.ops.timestamps import nb_project_synced_ns
from blinkview.ops.timesync import (
    DRIFT_MIN_BASELINE_NS,
    IDX_ARRAY_LENGTH,
    IDX_ASYM_RATIO,
    IDX_BEST_RTT,
    IDX_GATED_RUN,
    IDX_PPB_COUNT,
    IDX_SAMPLE_COUNT,
    IDX_SKIPS,
    IDX_TOTAL_COUNT,
    MAX_RTT_SKIPS,
    OFFSET_GATE_MAX_RUN,
    EngineState,
    nb_rtt_rank,
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


def _run_link(n_samples, link, device_ppm=50, seed=1, reboot_at=None, uptime_s=1500, mono_wrap_ns=None):
    """One ping per host second from a device `device_ppm` fast, `uptime_s` into its boot. `link(i, rng)` gives
    pong i's (rtt, outbound) in ns: the device reads its clock `outbound` after the ping was sent, +-20 us, so
    `rtt - outbound` is the return path. At pong `reboot_at` the device clock restarts at 2 s. `m` is the boot
    time modulo `mono_wrap_ns` when given (a 32-bit cycle counter), else the boot time. Returns the engine, the
    sync state and each pong's published drift in ppm."""
    rng = random.Random(seed)
    engine, sync = _engine(), _sync()
    host0 = 1_795_000_000_000_000_000
    rate = 1 + device_ppm * 1e-6
    boot_host = host0 - uptime_s * 1_000_000_000  # host time at which the device clock read 0

    published_ppm = []
    for i in range(n_samples):
        tx = host0 + i * 1_000_000_000
        if reboot_at is not None and i == reboot_at:
            boot_host = tx - 2_000_000_000
        rtt, outbound = link(i, rng)
        read = tx + outbound + rng.randint(-20_000, 20_000)
        dev = int((read - boot_host) * rate)
        mono = dev % mono_wrap_ns if mono_wrap_ns else dev
        nb_sync_kernel(tx, mono, dev, tx + rtt, engine, sync, False)
        idx = int(sync.active_idx[0])
        published_ppm.append((int(sync.drift_m[idx]) / int(sync.drift_d[idx]) - 1) * 1e6)

    return engine, sync, published_ppm


def _drift_changes(published):
    return sum(1 for a, b in zip(published, published[1:]) if a != b)


class TestDriftDoesNotFreeze:
    """A pong measures drift when its RTT is close to the anchor pong's, and a lasting change of the RTT level
    re-anchors. Judging "clean" against an all-time best RTT froze the drift for good on hardware after one short
    pong (ESP32) or a load rise (nRF, 2.0 -> 2.85 ms) - wiki:reports/blinksync-hw-time-sync."""

    def test_one_short_pong_does_not_freeze_the_drift(self):
        """The ESP32 case: ~2.95 ms pongs, one comes back 1.3 ms faster (a shorter return path)."""

        def link(i, rng):
            rtt = rng.randint(2_900_000, 3_000_000)
            return (rtt - 1_300_000 if i == 100 else rtt), 1_450_000

        _, _, published = _run_link(600, link)

        assert _drift_changes(published[-300:]) > 250
        assert -52 < published[-1] < -48, published[-1]

    def test_a_lasting_rtt_rise_resumes_the_drift(self):
        """The nRF case: the return path grows by 0.85 ms under load and stays there."""

        def link(i, rng):
            rtt = rng.randint(2_000_000, 2_100_000)
            return (rtt + 850_000 if i >= 300 else rtt), 1_000_000

        _, _, published = _run_link(900, link)

        # a minute without a clean pong, then a new anchor and a minute of baseline
        assert _drift_changes(published[300 : 300 + 115]) == 0
        assert _drift_changes(published[-300:]) > 250
        assert -52 < published[-1] < -48, published[-1]

    @pytest.mark.parametrize("seed", [1, 2, 3])
    def test_bimodal_rtt_does_not_make_up_drift(self, seed):
        """One pong in ten 1.3 ms faster: mixing the two RTT levels in one baseline would read their 1.3 ms
        offset difference as drift (~20 ppm over a minute)."""

        def link(i, rng):
            rtt = rng.randint(2_900_000, 3_000_000)
            return (rtt - 1_300_000 if rng.random() < 0.1 else rtt), 1_450_000

        _, _, published = _run_link(900, link, device_ppm=0, seed=seed)

        assert max(abs(ppm) for ppm in published) < 2, max(published, key=abs)


class TestDriftAfterReboot:
    """A reboot restarts the device clock below the drift anchor; the anchor has to go with it, or no drift is
    measured for as long as the old uptime (24 min for a board that was up 24 min)."""

    def _link(self, i, rng):
        return rng.randint(2_000_000, 2_100_000), 1_000_000

    def test_drift_is_measured_again_a_minute_after_a_reboot(self):
        engine, _, published = _run_link(700, self._link, reboot_at=400, uptime_s=1500)

        after = published[400:]
        first_change = next(k for k, (a, b) in enumerate(zip(after, after[1:])) if a != b)
        # 5 pongs of warmup, then DRIFT_MIN_BASELINE_NS of baseline
        assert first_change < 70, first_change
        assert -52 < published[-1] < -48, published[-1]

    def test_the_drift_in_force_is_kept_across_the_reboot(self):
        engine, _, published = _run_link(430, self._link, reboot_at=400)

        assert all(ppm == published[399] for ppm in published[400:430])
        assert int(engine.scalars[IDX_PPB_COUNT]) == 0  # the old boot's samples are dropped

    def test_a_wrapping_mono_clock_is_not_a_reboot(self):
        """`m` from a 32-bit cycle counter (nRF at 32768 Hz: every 36.4 h) wraps while `b` keeps counting; the
        drift history is measured on `b` and must survive the wrap."""
        wrap_ns = (2**32 * 1_000_000_000) // 32768
        uptime_s = wrap_ns // 1_000_000_000 - 400  # wraps ~393 pongs in (the device runs 50 ppm fast)
        engine, _, published = _run_link(700, self._link, uptime_s=uptime_s, mono_wrap_ns=wrap_ns)

        assert int(engine.scalars[IDX_PPB_COUNT]) == 15
        # samples again after the 5-pong warmup; a reset anchor would pause them for a 60 s baseline (to ~460)
        assert _drift_changes(published[400:450]) > 0
        assert -52 < published[-1] < -48, published[-1]

    def test_the_offset_tracks_the_new_boot(self):
        """The pongs after a reboot snap to the new clock as on a cold start."""
        _, sync, _ = _run_link(410, self._link, reboot_at=400, device_ppm=0)
        idx = int(sync.active_idx[0])
        tx = 1_795_000_000_000_000_000 + 409 * 1_000_000_000

        # the last pong was read at tx + 1 ms, 2 s + 9 s + 1 ms into the new boot
        error_ns = int(sync.offset[idx]) + (11_001_000_000 - int(sync.ref_time[idx])) - (tx + 1_000_000)
        assert abs(error_ns) < 100_000, error_ns


class TestRowsAfterReboot:
    """Rows projected the way the parsers do (nb_project_synced_ns) between pongs (nb_sync_kernel) across two
    reboots. The first pongs after a reboot are lost while the board boots, as on the rig (esp_b: 2 pings timed
    out). Before the fix the boot rows were stamped as far in the past as the old uptime."""

    @staticmethod
    def _run(reboots):
        """`reboots`: {pong index: device time the clock restarts at, s}. Returns each row's stamp error in s."""
        engine, sync = _engine(), _sync()
        host0 = 1_795_000_000_000_000_000
        boot_host = host0 - 1500 * 1_000_000_000
        lost = set()
        errors = []
        for i in range(1200):
            tx = host0 + i * 1_000_000_000
            if i in reboots:
                boot_host = tx - reboots[i] * 1_000_000_000
                lost = {i, i + 1}
            # a row logged 0.3 s before the ping, received 1 ms after it was logged
            row_host = tx - 300_000_000
            row_dev = row_host - boot_host
            if row_dev >= 0:
                stamp = int(nb_project_synced_ns(row_dev, row_host + 1_000_000, sync))
                errors.append((stamp - row_host) / 1e9)
            if i not in lost:
                dev = tx + 1_000_000 - boot_host
                nb_sync_kernel(tx, dev, dev, tx + 2_000_000, engine, sync, False)
        return errors

    def test_boot_rows_are_stamped_near_their_receive_time(self):
        errors = self._run({300: 2})

        assert max(abs(e) for e in errors) < 0.01, max(errors, key=abs)

    def test_a_second_reboot_does_not_reuse_the_first_reboots_fallback(self):
        """The second boot's first rows (raw 10 s) sit above where the first reboot's fallback stopped (raw 3 s):
        a fallback that kept its old anchor would stamp them ~10 min early."""
        errors = self._run({300: 2, 900: 10})

        assert max(abs(e) for e in errors) < 0.01, max(errors, key=abs)


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

    def test_drift_is_measured_on_most_pongs(self):
        """Every pong within DRIFT_CLEAN_WINDOW_NS of the anchor's RTT measures drift, not only the lucky ones."""
        engine, _, published = _run_link(600, lambda i, rng: (rng.randint(2_000_000, 2_100_000), 1_000_000))

        assert _drift_changes(published[-300:]) > 250
        assert int(engine.scalars[IDX_PPB_COUNT]) == 15

    def test_soft_reset_keeps_the_drift(self):
        engine, sync, _, _ = _simulate_drift(50, 300)
        from blinkview.core.time_sync_engine import TimeSyncEngine

        wrapper = TimeSyncEngine.__new__(TimeSyncEngine)
        wrapper.engine, wrapper.sync, wrapper.logger, wrapper.logger_sync = engine, sync, None, None
        wrapper.soft_reset()
        idx = int(sync.active_idx[0])

        assert -55 < (int(sync.drift_m[idx]) / int(sync.drift_d[idx]) - 1) * 1e6 < -45


def _gate_link(n_samples, link, ratio_ppm=500_000, device_ppm=20, seed=1):
    """One ping per host second; `link(i, rng)` gives pong i's (rtt, outbound) in ns and the device reads its
    clock `outbound` after the ping was sent. Returns the engine and, after each pong, the model's error in ns
    at that ping's send time (projected device time minus true host time)."""
    rng = random.Random(seed)
    engine, sync = _engine(), _sync()
    engine.scalars[IDX_ASYM_RATIO] = ratio_ppm
    host0 = 1_795_000_000_000_000_000
    rate = 1 + device_ppm * 1e-6
    boot_host = host0 - 1_500_000_000_000

    errors, gated = [], []
    for i in range(n_samples):
        tx = host0 + i * 1_000_000_000
        rtt, outbound = link(i, rng)
        dev = int((tx + outbound - boot_host) * rate)
        nb_sync_kernel(tx, dev, dev, tx + rtt, engine, sync, False)
        errors.append(_project(sync, int((tx - boot_host) * rate)) - tx)
        gated.append(int(engine.scalars[IDX_GATED_RUN]) > 0)
    return engine, errors, gated


class TestOffsetGate:
    """A pong whose RTT is far from the recent median moved the offset by ratio x the extra delay, as if both legs
    had waited. On the BlinkSync rig a Zephyr pong queued behind a log line (+0.85 ms on the return path) for
    stretches of seconds swung the offset by up to +330 us, and an ESP32 pong riding an earlier USB transfer
    came back up to 2.4 ms early (wiki finding py-nrf-offset-swings-330-us-whenever-pings-land-just-behind-an)."""

    def test_pongs_delayed_on_the_return_path_do_not_move_the_offset(self):
        def link(i, rng):
            rtt = rng.randint(2_050_000, 2_150_000)
            return (rtt + 850_000 if 300 <= i < 310 else rtt), 1_050_000

        _, errors, gated = _gate_link(400, link)

        assert all(gated[300:310])
        assert max(abs(e) for e in errors[250:400]) < 40_000, max(errors[250:400], key=abs)

    def test_pongs_back_early_do_not_move_the_offset(self):
        """The ESP32 ramp: a ~3.0 ms link whose device reads its clock 0.4 ms in (ratio 0.133), then four pongs
        2.69, 1.64, 1.01 and 0.55 ms long, the same outbound leg."""
        ramp = {300: 2_690_000, 301: 1_640_000, 302: 1_010_000, 303: 550_000}

        def link(i, rng):
            return ramp.get(i, rng.randint(2_980_000, 3_080_000)), 400_000

        _, errors, gated = _gate_link(400, link, ratio_ppm=133_333)

        assert all(gated[300:304])
        assert max(abs(e) for e in errors[250:400]) < 25_000, max(errors[250:400], key=abs)

    def test_a_jittery_link_is_not_shut_out(self):
        """ADB/BLE-like: both legs jitter by ~1 ms. The band follows the MAD, so most pongs still move the offset,
        and the error averages down."""

        def link(i, rng):
            out = 2_000_000 + abs(int(rng.gauss(0, 1_000_000)))
            return out + 2_000_000 + abs(int(rng.gauss(0, 1_000_000))), out

        _, errors, gated = _gate_link(600, link)

        assert sum(gated[100:]) < 0.1 * len(gated[100:])
        tail = errors[300:]
        assert abs(sum(tail) / len(tail)) < 300_000

    def test_a_lasting_rtt_rise_is_followed(self):
        """Both legs 0.5 ms slower from pong 300 on: once the median has moved, the offset follows the new split."""

        def link(i, rng):
            rtt = rng.randint(2_000_000, 2_100_000) + (1_000_000 if i >= 300 else 0)
            return rtt, rtt // 2

        _, errors, _ = _gate_link(500, link)

        assert max(abs(e) for e in errors[400:500]) < 30_000, max(errors[400:500], key=abs)
        assert max(abs(e) for e in errors[300:400]) < 60_000, max(errors[300:400], key=abs)

    @pytest.mark.parametrize("run, moved", [(0, False), (OFFSET_GATE_MAX_RUN, True)])
    def test_a_long_gated_run_lets_the_next_pong_through(self, run, moved):
        """A steady 2.05 ms link, then one pong 1 ms longer on the return path: gated it leaves the model where it
        was, unless OFFSET_GATE_MAX_RUN pongs in a row were gated already; then it moves the offset by 1/8 of
        the 0.5 ms it reads."""
        engine, sync = _engine(), _sync()
        host0, dev0 = 1_795_000_000_000_000_000, 10**12
        for i in range(100):
            dev = dev0 + i * 10**9 + 1_025_000
            nb_sync_kernel(host0 + i * 10**9, dev, dev, host0 + i * 10**9 + 2_050_000, engine, sync, False)
        probe = dev0 + 100 * 10**9
        before = _project(sync, probe)
        engine.scalars[IDX_GATED_RUN] = run

        dev, tx = probe + 1_025_000, host0 + 100 * 10**9
        accepted, *_ = nb_sync_kernel(tx, dev, dev, tx + 3_050_000, engine, sync, False)

        assert accepted
        shift = _project(sync, probe) - before
        if moved:
            assert 50_000 < shift < 75_000, shift
        else:
            assert abs(shift) < 1_000, shift


def test_rtt_rank_matches_a_sort():
    values = [5, 1, 4, 4, 9, 2, 0]
    hist = np.array(values + [7], dtype=np.uint64)
    for k in range(len(values)):
        assert nb_rtt_rank(hist, len(values), k, -1) == sorted(values)[k]
        assert nb_rtt_rank(hist, len(values), k, 4) == sorted(abs(v - 4) for v in values)[k]


def test_a_delayed_pong_in_the_warm_up_is_not_snapped_to():
    """The warm-up snaps fully to its first pongs. On the rig a rebuilt engine's 8th pong had waited 1 ms on the
    return path and the snap put the offset 0.7 ms late, worked off by the 1/8 loop over ~8 s. Gated from the third
    RTT on, it leaves the model where the typical pongs put it."""

    def link(i, rng):
        rtt = rng.randint(1_950_000, 2_050_000)
        return (rtt + 1_000_000 if i == 6 else rtt), 1_000_000

    _, errors, gated = _gate_link(30, link, device_ppm=0)

    assert gated[6]
    assert max(abs(e) for e in errors[3:30]) < 60_000, max(errors[3:30], key=abs)


def test_a_warm_restart_keeps_the_model_until_a_few_pongs_agree():
    """TimeSyncEngine.soft_reset (a parser restart) clears the RTT history. The first pong after it took 7 ms on the
    rig, 5 ms above the link's usual 2.1 ms; snapped to, it put the offset 3.4 ms late. The published model now stays
    in force until three new RTTs are in, and a typical pong moves it from there."""
    from blinkview.core.time_sync_engine import TimeSyncEngine

    def link(i, rng):
        return rng.randint(2_050_000, 2_150_000), 1_050_000

    engine, errors, _ = _gate_link(120, link, device_ppm=0)
    sync = _sync()
    # carry on from where _gate_link left the model: rebuild that state into a TimeSyncEngine wrapper
    wrapper = TimeSyncEngine.__new__(TimeSyncEngine)
    wrapper.engine, wrapper.sync, wrapper.logger, wrapper.logger_sync = engine, sync, None, None
    host0 = 1_795_000_000_000_000_000
    boot_host = host0 - 1_500_000_000_000
    for i in range(120):  # the same steady link into this sync state
        tx = host0 + i * 1_000_000_000
        dev = tx + 1_050_000 - boot_host
        nb_sync_kernel(tx, dev, dev, tx + 2_100_000, engine, sync, False)
    wrapper.soft_reset()
    assert int(sync.enabled[0]) == 1

    errs = []
    for k, rtt in enumerate([7_000_000, 2_100_000, 2_120_000, 2_080_000, 2_100_000]):
        tx = host0 + (120 + k) * 1_000_000_000
        dev = tx + 1_050_000 - boot_host
        nb_sync_kernel(tx, dev, dev, tx + rtt, engine, sync, False)
        errs.append(_project(sync, tx - boot_host) - tx)
    assert max(abs(e) for e in errs) < 30_000, errs

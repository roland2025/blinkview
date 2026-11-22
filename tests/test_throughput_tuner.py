# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.limits import TUNER_MAX_BUFFER_BYTES, TUNER_MAX_CAPACITY
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

MB = 1024 * 1024


class FixedRateSpeedometer(Speedometer):
    """Reports a fixed rate on every update, so a test can set what the tuner sees."""

    def __init__(self, bytes_per_sec, msgs_per_sec):
        super().__init__()
        self.bytes_per_sec = bytes_per_sec
        self.msgs_per_sec = msgs_per_sec

    def update(self, bytes_in, msgs_in):
        return True


def make_tuner(bytes_per_sec=0.0, msgs_per_sec=0.0, **kwargs):
    return ThroughputAutoTuner(FixedRateSpeedometer(bytes_per_sec, msgs_per_sec), **kwargs)


class TestUpdate:
    def test_sizes_for_rate_times_window_times_safety_under_the_cap(self):
        tuner = make_tuner(bytes_per_sec=10 * MB, msgs_per_sec=100_000)
        tuner.update(0, 0, target_window_sec=0.1)

        assert tuner.estimated_buffer_bytes == int(10 * MB * 0.1 * 1.5)
        assert tuner.estimated_capacity == 15_000

    def test_a_fast_source_is_clamped_to_the_cap(self):
        """The finding's case: a loopback TCP source asked for 64-256 MB batches."""
        tuner = make_tuner(bytes_per_sec=2_000 * MB, msgs_per_sec=30_000_000)
        tuner.update(0, 0, target_window_sec=0.1)

        assert tuner.estimated_buffer_bytes == TUNER_MAX_BUFFER_BYTES
        assert tuner.estimated_capacity == TUNER_MAX_CAPACITY

    def test_never_below_the_defaults(self):
        tuner = make_tuner(default_buffer_bytes=4096, msg_size_bytes=32)
        tuner.update(0, 0, target_window_sec=0.1)

        assert tuner.estimated_buffer_bytes == 4096
        assert tuner.estimated_capacity == 128

    def test_a_default_above_the_cap_is_kept(self):
        tuner = make_tuner(default_buffer_bytes=32 * MB, msg_size_bytes=1, max_buffer_bytes=16 * MB)
        tuner.update(0, 0, target_window_sec=0.1)

        assert tuner.estimated_buffer_bytes == 32 * MB
        assert tuner.estimated_capacity == 32 * MB

    def test_brings_a_burst_estimate_back_under_the_cap(self):
        tuner = make_tuner(bytes_per_sec=1 * MB, msgs_per_sec=1_000)
        tuner.ensure_burst_capacity(100 * MB)
        assert tuner.estimated_buffer_bytes == 100 * MB

        tuner.update(0, 0, target_window_sec=0.1)
        assert tuner.estimated_buffer_bytes == int(1 * MB * 0.1 * 1.5)


class TestEnsureBurstCapacity:
    def test_adds_the_safety_factor_under_the_cap(self):
        tuner = make_tuner()
        tuner.ensure_burst_capacity(1 * MB)

        assert tuner.estimated_buffer_bytes == int(1.5 * MB)

    def test_the_safety_factor_stops_at_the_cap(self):
        tuner = make_tuner()
        tuner.ensure_burst_capacity(12 * MB)

        assert tuner.estimated_buffer_bytes == TUNER_MAX_BUFFER_BYTES

    def test_a_burst_bigger_than_the_cap_still_fits(self):
        tuner = make_tuner()
        tuner.ensure_burst_capacity(128 * MB)

        assert tuner.estimated_buffer_bytes == 128 * MB

    def test_bytes_do_not_grow_the_row_estimate(self):
        """The finding's parser case: 128 MB in at msg_size_bytes=32 used to mean ~6 M rows."""
        tuner = make_tuner(msg_size_bytes=32)
        rows_before = tuner.estimated_capacity
        tuner.ensure_burst_capacity(128 * MB)

        assert tuner.estimated_capacity == rows_before

    def test_rows_grow_to_burst_rows_up_to_the_cap(self):
        tuner = make_tuner()
        tuner.ensure_burst_capacity(1024, burst_rows=5_000)
        assert tuner.estimated_capacity == 5_000

        tuner.ensure_burst_capacity(1024, burst_rows=10_000_000)
        assert tuner.estimated_capacity == TUNER_MAX_CAPACITY

    def test_never_shrinks_an_estimate(self):
        tuner = make_tuner(bytes_per_sec=10 * MB, msgs_per_sec=100_000)
        tuner.update(0, 0, target_window_sec=0.1)
        buffer_before, rows_before = tuner.estimated_buffer_bytes, tuner.estimated_capacity

        tuner.ensure_burst_capacity(1024, burst_rows=1)

        assert (tuner.estimated_buffer_bytes, tuner.estimated_capacity) == (buffer_before, rows_before)

    def test_never_asks_for_more_than_safety_factor_times_the_burst(self):
        """BatchFeeder._replace skips the call when safety_factor x the chunk already fits; that
        skip is only safe while this holds."""
        for burst in (1, 1000, 1 * MB, 11 * MB, 12 * MB, 16 * MB, 100 * MB):
            for splittable in (False, True):
                tuner = make_tuner(default_buffer_bytes=1)
                tuner.ensure_burst_capacity(burst, splittable=splittable)
                assert tuner.estimated_buffer_bytes <= int(burst * tuner.safety_factor)

    def test_a_splittable_burst_bigger_than_the_cap_stays_at_the_cap(self):
        """A parser's case: its output spreads over as many batches as it takes, so a 128 MB
        input batch must not mean a 128 MB output batch."""
        tuner = make_tuner()
        tuner.ensure_burst_capacity(128 * MB, splittable=True)

        assert tuner.estimated_buffer_bytes == TUNER_MAX_BUFFER_BYTES

    def test_a_splittable_burst_under_the_cap_is_sized_as_usual(self):
        tuner = make_tuner()
        tuner.ensure_burst_capacity(1 * MB, splittable=True)

        assert tuner.estimated_buffer_bytes == int(1.5 * MB)

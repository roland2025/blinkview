# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.utils import log_velocity_tracker as module
from blinkview.ui.utils.log_velocity_tracker import LogVelocityTracker


class FakeClock:
    """Controllable stand-in for time.perf_counter - tests advance it explicitly instead of
    depending on real wall-clock timing, which would make the 1s-window/burst-duration
    assertions flaky."""

    def __init__(self, start=0.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


@pytest.fixture
def clock(monkeypatch):
    fake = FakeClock()
    monkeypatch.setattr(module, "perf_counter", fake)
    return fake


class TestConstruction:
    def test_defaults(self, clock):
        tracker = LogVelocityTracker()
        assert tracker.limit_per_sec == 1000
        assert tracker.burst_limit_seconds == 3
        assert tracker.instant_cap == 5000
        assert tracker.msg_counter == 0
        assert tracker.over_limit_start_time is None

    def test_custom_limits(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=10, burst_limit_seconds=1, instant_cap=50)
        assert tracker.limit_per_sec == 10
        assert tracker.burst_limit_seconds == 1
        assert tracker.instant_cap == 50


class TestReset:
    def test_reset_clears_counters_and_burst_timer(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=10, instant_cap=100)
        tracker.msg_counter = 42
        tracker.over_limit_start_time = 5.0

        tracker.reset()

        assert tracker.msg_counter == 0
        assert tracker.over_limit_start_time is None


class TestInstantCap:
    def test_single_batch_exceeding_instant_cap_triggers_immediate_pause(self, clock):
        tracker = LogVelocityTracker(instant_cap=100)

        assert tracker.update_and_check(150) is True

    def test_accumulated_batches_exceeding_instant_cap_within_the_same_window_pause(self, clock):
        tracker = LogVelocityTracker(instant_cap=100)

        assert tracker.update_and_check(60) is False  # under cap and under 1s elapsed
        clock.advance(0.1)
        assert tracker.update_and_check(60) is True  # 120 > 100, still within the same <1s window

    def test_batch_at_exactly_the_cap_does_not_trigger(self, clock):
        tracker = LogVelocityTracker(instant_cap=100)

        assert tracker.update_and_check(100) is False  # strictly-greater-than check


class TestWindowAccumulation:
    def test_counter_keeps_accumulating_before_the_1s_window_elapses(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=1000, instant_cap=100_000)

        tracker.update_and_check(10)
        clock.advance(0.3)
        tracker.update_and_check(10)

        assert tracker.msg_counter == 20  # window hasn't reset yet, both batches accumulated

    def test_window_resets_msg_counter_once_a_second_has_elapsed(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=1000, instant_cap=100_000)

        tracker.update_and_check(10)
        clock.advance(1.0)
        tracker.update_and_check(10)  # this call's own elapsed(>=1.0) check fires and resets

        assert tracker.msg_counter == 0  # reset to 0 after evaluating the window


class TestSustainedVelocity:
    def test_velocity_under_limit_never_pauses(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=100, instant_cap=100_000)

        for _ in range(5):
            clock.advance(1.0)
            assert tracker.update_and_check(50) is False  # 50/sec < 100/sec limit

    def test_velocity_over_limit_starts_the_burst_timer_but_does_not_pause_immediately(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=100, burst_limit_seconds=3, instant_cap=100_000)

        clock.advance(1.0)
        result = tracker.update_and_check(200)  # 200/sec > 100/sec limit

        assert result is False
        assert tracker.over_limit_start_time is not None

    def test_sustained_over_limit_velocity_pauses_once_burst_limit_elapses(self, clock):
        """Each call must keep velocity (200/elapsed) comfortably above limit_per_sec (100) -
        elapsed needs to stay near 1.0s per call, since a longer window would drop the computed
        velocity back at/under the limit and reset the burst timer instead of accumulating it."""
        tracker = LogVelocityTracker(limit_per_sec=100, burst_limit_seconds=3, instant_cap=100_000)

        clock.advance(1.0)
        assert tracker.update_and_check(200) is False  # over limit, burst timer starts (t=1.0)

        clock.advance(1.0)
        assert tracker.update_and_check(200) is False  # sustained 1.0s so far (t=2.0)

        clock.advance(1.0)
        assert tracker.update_and_check(200) is False  # sustained 2.0s so far (t=3.0)

        clock.advance(1.0)
        assert tracker.update_and_check(200) is True  # sustained 3.0s >= burst_limit_seconds (t=4.0)

    def test_dipping_back_under_limit_resets_the_burst_timer(self, clock):
        tracker = LogVelocityTracker(limit_per_sec=100, burst_limit_seconds=3, instant_cap=100_000)

        clock.advance(1.0)
        tracker.update_and_check(200)  # over limit - starts the burst timer
        assert tracker.over_limit_start_time is not None

        clock.advance(1.0)
        tracker.update_and_check(50)  # back under the limit

        assert tracker.over_limit_start_time is None

        # Even a long time later, going over the limit again must start a fresh burst window
        # rather than immediately pausing from the earlier (reset) timer.
        clock.advance(1.0)
        assert tracker.update_and_check(200) is False
        assert tracker.over_limit_start_time is not None

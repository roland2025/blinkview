# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.playback_clock import PlaybackClock, PlaybackMode

BASE = 1_000_000_000_000  # arbitrary epoch-ns anchor, far from 0 to catch sign/clamp bugs


class FakeLogPool:
    """Minimal stand-in for CircularLogPool - get_time_bounds()/find_ts_n_rows_away() are the
    only methods PlaybackClock calls."""

    def __init__(self, bounds=(0, 0), row_step_ns=1_000_000):
        self.bounds = bounds
        self.row_step_ns = row_step_ns  # simulates evenly-spaced rows for step_rows tests

    def get_time_bounds(self):
        return self.bounds

    def find_ts_n_rows_away(self, current_ts_ns, delta_rows):
        return current_ts_ns + delta_rows * self.row_step_ns


def make_clock(bounds):
    return PlaybackClock(FakeLogPool(bounds))


def test_starts_live_tracking_the_pool_edge():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    assert clock.mode is PlaybackMode.LIVE
    assert clock.current_ts_ns == BASE + 10_000_000_000


def test_live_snaps_to_a_growing_edge_on_each_tick():
    pool = FakeLogPool((BASE, BASE + 10_000_000_000))
    clock = PlaybackClock(pool)
    pool.bounds = (BASE, BASE + 20_000_000_000)
    clock.tick(0)
    assert clock.current_ts_ns == BASE + 20_000_000_000


def test_play_advances_current_ts_by_elapsed_wall_time_times_speed():
    clock = make_clock((BASE, BASE + 10_000_000_000))

    clock.enter_replay(BASE + 2_000_000_000)
    clock.play(speed=2.0)

    # First tick since construction: no _last_tick_wall_ns baseline yet, so this call only
    # anchors it - by design, tick() never moves the clock on its very first invocation.
    clock.tick(1_000_000_000)
    changed = clock.tick(1_000_000_000 + 500_000_000)  # +0.5s wall -> +1.0s virtual at 2x

    assert changed
    assert clock.current_ts_ns == BASE + 3_000_000_000
    assert clock.mode is PlaybackMode.REPLAY


def test_reaching_the_live_edge_while_playing_forward_snaps_back_to_live():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    wall = 0
    clock.tick(wall)

    clock.enter_replay(BASE + 2_000_000_000)
    clock.play(speed=2.0)
    wall += 10_000_000_000  # comfortably overshoots the live edge at this speed
    clock.tick(wall)

    assert clock.mode is PlaybackMode.LIVE
    assert clock.current_ts_ns == BASE + 10_000_000_000
    assert clock.is_playing is False


def test_reaching_the_min_bound_while_rewinding_autopauses_without_leaving_replay():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    wall = 0
    clock.tick(wall)

    clock.enter_replay(BASE + 2_000_000_000)
    clock.play(speed=-4.0)
    wall += 1_000_000_000
    clock.tick(wall)

    assert clock.current_ts_ns == BASE
    assert clock.is_playing is False
    assert clock.mode is PlaybackMode.REPLAY  # rewinding to the start pauses, doesn't exit replay


def test_seek_clamps_to_bounds_and_enters_replay():
    clock = make_clock((BASE, BASE + 10_000_000_000))

    clock.seek(BASE + 999_000_000_000)
    assert clock.mode is PlaybackMode.REPLAY
    assert clock.current_ts_ns == BASE + 10_000_000_000

    clock.seek(BASE - 999_000_000_000)
    assert clock.current_ts_ns == BASE


def test_bounds_keep_refreshing_while_paused_but_current_ts_stays_fixed():
    pool = FakeLogPool((BASE, BASE + 10_000_000_000))
    clock = PlaybackClock(pool)
    clock.tick(0)

    clock.enter_replay(BASE + 5_000_000_000)
    pool.bounds = (BASE, BASE + 20_000_000_000)
    clock.tick(1_000_000)

    assert clock.current_ts_ns == BASE + 5_000_000_000
    assert clock.bounds_max_ns == BASE + 20_000_000_000


def test_pause_stops_advancing():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    wall = 0
    clock.tick(wall)

    clock.enter_replay(BASE + 2_000_000_000)
    clock.play(speed=1.0)
    wall += 1_000_000_000
    clock.tick(wall)

    clock.pause()
    ts_at_pause = clock.current_ts_ns
    wall += 1_000_000_000
    clock.tick(wall)

    assert clock.current_ts_ns == ts_at_pause


def test_tick_returns_false_when_nothing_observable_changed():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    clock.tick(0)  # settle into steady-state LIVE with unchanging bounds
    assert clock.tick(1) is False


def test_scrubbing_suspends_auto_advance_while_playing():
    """A drag on the transport scrubber must not fight tick()'s own elapsed-time-based
    advance - the classic case being a user jogging the position while playback is still
    active."""
    clock = make_clock((BASE, BASE + 10_000_000_000))
    wall = 0
    clock.tick(wall)

    clock.enter_replay(BASE + 2_000_000_000)
    clock.play(speed=1.0)

    clock.begin_scrub()
    clock.seek(BASE + 7_000_000_000)  # user drags to a new position mid-drag
    wall += 5_000_000_000  # plenty of wall time passes while they keep dragging
    clock.tick(wall)

    # Without the fix, tick() would advance current_ts_ns by ~5s on top of the seek, blowing
    # straight past bounds_max_ns and auto-snapping to LIVE.
    assert clock.current_ts_ns == BASE + 7_000_000_000
    assert clock.mode is PlaybackMode.REPLAY
    assert clock.is_playing is True  # untouched - resumes exactly where it was on release

    clock.end_scrub()
    wall += 1_000_000_000
    clock.tick(wall)
    assert clock.current_ts_ns == BASE + 8_000_000_000  # advance resumes post-release


def test_scrubbing_suspends_auto_pause_at_rewind_bound():
    """Dragging past the rewind bound mid-scrub must not trigger the auto-pause-at-bound side
    effect until the drag actually ends - the bound itself is re-evaluated by seek()'s own
    clamp, not by tick()'s playing-branch."""
    clock = make_clock((BASE, BASE + 10_000_000_000))
    wall = 0
    clock.tick(wall)

    clock.enter_replay(BASE + 2_000_000_000)
    clock.play(speed=-4.0)

    clock.begin_scrub()
    clock.seek(BASE)  # drag all the way to the rewind bound
    wall += 1_000_000_000
    clock.tick(wall)

    assert clock.current_ts_ns == BASE
    assert clock.is_playing is True  # tick()'s auto-pause-at-bound branch never ran

    clock.end_scrub()


def test_step_rows_delegates_to_log_pool_and_enters_replay():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    assert clock.mode is PlaybackMode.LIVE

    clock.step_rows(-3)  # step backward from the live edge so the clamp doesn't mask the delta

    assert clock.mode is PlaybackMode.REPLAY
    assert clock.current_ts_ns == BASE + 10_000_000_000 - 3_000_000


def test_step_rows_zero_is_a_true_noop_and_does_not_enter_replay():
    clock = make_clock((BASE, BASE + 10_000_000_000))
    clock.step_rows(0)
    assert clock.mode is PlaybackMode.LIVE
    assert clock.current_ts_ns == BASE + 10_000_000_000


class TestEnterReplayWhenReady:
    def test_seeks_immediately_when_data_already_exists(self):
        clock = make_clock((BASE, BASE + 10_000_000_000))

        clock.enter_replay_when_ready(BASE + 3_000_000_000)

        assert clock.mode is PlaybackMode.REPLAY
        assert clock.current_ts_ns == BASE + 3_000_000_000

    def test_defaults_to_bounds_min_when_data_already_exists_and_no_target_given(self):
        clock = make_clock((BASE, BASE + 10_000_000_000))

        clock.enter_replay_when_ready()

        assert clock.mode is PlaybackMode.REPLAY
        assert clock.current_ts_ns == BASE

    def test_defers_the_seek_until_data_appears(self):
        pool = FakeLogPool((0, 0))  # empty pool - nothing loaded yet
        clock = PlaybackClock(pool)

        clock.enter_replay_when_ready(BASE + 3_000_000_000)

        # Mode flips to REPLAY right away (so the UI shows DVR mode immediately) even though the
        # target seek can't land yet - bounds are still empty, clamping would force it to 0.
        assert clock.mode is PlaybackMode.REPLAY
        assert clock.current_ts_ns == 0

        pool.bounds = (BASE, BASE + 10_000_000_000)  # data has now streamed in
        clock.tick(0)

        assert clock.current_ts_ns == BASE + 3_000_000_000

    def test_defers_to_bounds_min_once_data_appears_when_no_target_given(self):
        pool = FakeLogPool((0, 0))
        clock = PlaybackClock(pool)

        clock.enter_replay_when_ready()  # no known target yet - land at the start once loaded
        pool.bounds = (BASE, BASE + 10_000_000_000)
        clock.tick(0)

        assert clock.current_ts_ns == BASE

    def test_a_manual_seek_before_bounds_refresh_cancels_the_pending_one(self):
        """seek()/tick() both only ever refresh clock.bounds_* from within tick() itself, so a
        seek() called directly (bypassing tick()) always clamps against whatever bounds the
        clock currently has cached - here, still the empty [0, 0] from construction. That seek
        must still cancel the deferred target, or the *next* tick() (the first one to actually
        see real bounds) would silently overwrite the user's seek with the stale pending one."""
        pool = FakeLogPool((0, 0))
        clock = PlaybackClock(pool)

        clock.enter_replay_when_ready(BASE + 3_000_000_000)
        clock.seek(BASE + 999)  # clamps to 0 - clock.bounds_max_ns is still 0 at this point

        pool.bounds = (BASE, BASE + 10_000_000_000)
        clock.tick(0)

        assert clock.current_ts_ns == 0  # stays where the direct seek() left it

    def test_going_live_before_data_appears_cancels_the_pending_seek(self):
        pool = FakeLogPool((0, 0))
        clock = PlaybackClock(pool)

        clock.enter_replay_when_ready(BASE + 3_000_000_000)
        clock.go_live()
        pool.bounds = (BASE, BASE + 10_000_000_000)
        clock.tick(0)

        assert clock.mode is PlaybackMode.LIVE
        assert clock.current_ts_ns == BASE + 10_000_000_000


def test_step_rows_clamps_like_seek():
    pool = FakeLogPool((BASE, BASE + 10_000_000_000), row_step_ns=1_000_000_000_000)
    clock = PlaybackClock(pool)
    clock.enter_replay(BASE + 2_000_000_000)

    clock.step_rows(-1)  # would overshoot far past bounds_min_ns at this row_step_ns

    assert clock.current_ts_ns == BASE

    clock.step_rows(1000)  # would overshoot far past bounds_max_ns
    assert clock.current_ts_ns == BASE + 10_000_000_000

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Pure-Python transition-table coverage for PlaybackFollowMachine - no Qt/Registry/widget
involved (see plans/playback-follow-state-machine.md Phase 0). Each test name states the
from-state/event/guard combination it locks in; several are ports of specific bugs documented in
the blinkview-playback-wiring skill and in LogViewerWidget/LogTableViewerWidget's own docstrings,
to prove the machine's structure prevents them rather than relying on a special-cased guard."""

from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.playback_follow import (
    ClockSnapshot,
    FollowActionKind,
    FollowEvent,
    FollowState,
    PlaybackFollowMachine,
)

LIVE = ClockSnapshot(mode=PlaybackMode.LIVE, current_ts_ns=0)


def replay(ts, playing=False):
    return ClockSnapshot(mode=PlaybackMode.REPLAY, current_ts_ns=ts, is_playing=playing)


class TestLiveStateTicks:
    def test_tick_while_clock_live_stays_live(self):
        m = PlaybackFollowMachine()
        action = m.handle(FollowEvent.Tick(), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE
        assert action.from_state is FollowState.LIVE

    def test_tick_while_clock_replay_transitions_to_following(self):
        """This is the structural fix for Trap A (blinkview-playback-wiring skill): entering
        REPLAY must never freeze/pause a widget on its very first follow tick - FREEZE is a
        distinct action from FETCH_FOLLOWING and only ever fires from ScrolledAway/ClogDetected/
        TogglePause(True), never from a plain Tick transition into FOLLOWING."""
        m = PlaybackFollowMachine()
        action = m.handle(FollowEvent.Tick(), replay(500))

        assert m.state is FollowState.FOLLOWING
        assert action.kind is FollowActionKind.FETCH_FOLLOWING
        assert action.anchor_ts_ns == 500
        assert action.from_state is FollowState.LIVE

    def test_tick_while_clock_replay_but_force_live_stays_live(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.ToggleForceLive(True), LIVE)

        action = m.handle(FollowEvent.Tick(), replay(500))

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE

    def test_opening_while_replay_already_active_follows_without_a_pause(self):
        """A widget constructed fresh (default state LIVE) whose very first tick already sees
        REPLAY must go straight to FOLLOWING - never touches FREEZE, so there's no pause/resume
        button state to get stuck in (mirrors LogViewerWidget's
        test_opening_while_replay_already_active_follows_without_manual_pause)."""
        m = PlaybackFollowMachine()
        action = m.handle(FollowEvent.Tick(), replay(1000))

        assert m.state is FollowState.FOLLOWING
        assert action.kind is FollowActionKind.FETCH_FOLLOWING


class TestFollowingStateTicks:
    def test_tick_while_clock_still_replay_refetches_new_anchor(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.Tick(), replay(100))

        action = m.handle(FollowEvent.Tick(), replay(200))

        assert m.state is FollowState.FOLLOWING
        assert action.kind is FollowActionKind.FETCH_FOLLOWING
        assert action.anchor_ts_ns == 200

    def test_tick_while_clock_returns_to_live_exits_to_live(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.Tick(), replay(100))

        action = m.handle(FollowEvent.Tick(), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE
        assert action.from_state is FollowState.FOLLOWING

    def test_toggle_force_live_while_following_jumps_to_live_immediately(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.Tick(), replay(100))

        action = m.handle(FollowEvent.ToggleForceLive(True), replay(100))

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE
        assert action.from_state is FollowState.FOLLOWING

    def test_tick_while_following_and_force_live_becomes_true_exits_to_live(self):
        """Defensive: covers a machine reconstructed with force_live already set (e.g. restored
        widget state) landing in FOLLOWING before its first tick observes force_live."""
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.Tick(), replay(100))
        m.force_live = True  # simulate restored state rather than going through the toggle event

        action = m.handle(FollowEvent.Tick(), replay(200))

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE

    def test_scrolled_away_while_following_freezes_in_place(self):
        """The old _on_scroll_value_changed's _playback_anchored branch: freezing must NOT
        re-anchor off the live buffer (there isn't one while following) - from_state FOLLOWING
        tells the widget to keep the already-fetched ts-anchored window in place."""
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.Tick(), replay(100))

        action = m.handle(FollowEvent.ScrolledAway(), replay(100))

        assert m.state is FollowState.FROZEN
        assert action.kind is FollowActionKind.FREEZE
        assert action.from_state is FollowState.FOLLOWING
        assert action.auto is False


class TestFrozenState:
    def test_scrolled_away_from_live_freezes_via_live_anchor(self):
        m = PlaybackFollowMachine()

        action = m.handle(FollowEvent.ScrolledAway(), LIVE)

        assert m.state is FollowState.FROZEN
        assert action.kind is FollowActionKind.FREEZE
        assert action.from_state is FollowState.LIVE
        assert action.auto is False

    def test_clog_detected_freezes_automatically(self):
        m = PlaybackFollowMachine()

        action = m.handle(FollowEvent.ClogDetected(), LIVE)

        assert m.state is FollowState.FROZEN
        assert action.kind is FollowActionKind.FREEZE
        assert action.auto is True
        assert action.from_state is FollowState.LIVE

    def test_clog_detected_while_not_live_is_a_noop(self):
        """Clog protection only ever fires while pulling the ordinary live tail."""
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.Tick(), replay(100))  # -> FOLLOWING

        action = m.handle(FollowEvent.ClogDetected(), replay(100))

        assert m.state is FollowState.FOLLOWING
        assert action.kind is FollowActionKind.NOOP

    def test_toggle_pause_true_freezes_from_live(self):
        m = PlaybackFollowMachine()

        action = m.handle(FollowEvent.TogglePause(True), LIVE)

        assert m.state is FollowState.FROZEN
        assert action.kind is FollowActionKind.FREEZE
        assert action.from_state is FollowState.LIVE

    def test_toggle_pause_true_while_already_frozen_is_a_noop(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.TogglePause(True), LIVE)

        action = m.handle(FollowEvent.TogglePause(True), LIVE)

        assert m.state is FollowState.FROZEN
        assert action.kind is FollowActionKind.NOOP

    def test_resume_while_clock_live_goes_live(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.TogglePause(True), LIVE)

        action = m.handle(FollowEvent.TogglePause(False), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE

    def test_resume_while_clock_replay_rejoins_following_without_an_immediate_fetch(self):
        """'Just clear the freeze; the next apply_updates() tick's follow branch re-anchors to
        wherever the clock has moved to since' - the resume itself must not jump to the live
        tail."""
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.TogglePause(True), replay(100))

        action = m.handle(FollowEvent.TogglePause(False), replay(999))

        assert m.state is FollowState.FOLLOWING
        assert action.kind is FollowActionKind.NOOP

        # Confirms the "next tick re-anchors" half of the contract.
        tick_action = m.handle(FollowEvent.Tick(), replay(999))
        assert tick_action.kind is FollowActionKind.FETCH_FOLLOWING
        assert tick_action.anchor_ts_ns == 999

    def test_resume_while_force_live_goes_live_even_though_clock_is_replay(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.ToggleForceLive(True), LIVE)
        m.handle(FollowEvent.TogglePause(True), replay(100))

        action = m.handle(FollowEvent.TogglePause(False), replay(100))

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE

    def test_resume_while_not_frozen_is_a_noop(self):
        m = PlaybackFollowMachine()

        action = m.handle(FollowEvent.TogglePause(False), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.NOOP

    def test_scrolled_to_live_edge_goes_live_unconditionally(self):
        """Reaching the pool's true newest row is a fact about the widget's own view, not about
        following the replay clock - must go LIVE even while the clock is still REPLAY."""
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.TogglePause(True), replay(100))

        action = m.handle(FollowEvent.ScrolledToLiveEdge(), replay(100))

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE

    def test_scrolled_to_live_edge_while_not_frozen_is_a_noop(self):
        m = PlaybackFollowMachine()

        action = m.handle(FollowEvent.ScrolledToLiveEdge(), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.NOOP

    def test_tick_never_moves_a_frozen_widget(self):
        m = PlaybackFollowMachine()
        m.handle(FollowEvent.TogglePause(True), replay(100))

        action = m.handle(FollowEvent.Tick(), replay(999))

        assert m.state is FollowState.FROZEN
        assert action.kind is FollowActionKind.NOOP


class TestSupportsFreezeFalse:
    """TelemetryTable: no scroll/pause concept - only ever occupies LIVE/FOLLOWING."""

    def test_scrolled_away_is_a_noop(self):
        m = PlaybackFollowMachine(supports_freeze=False)

        action = m.handle(FollowEvent.ScrolledAway(), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.NOOP

    def test_clog_detected_is_a_noop(self):
        m = PlaybackFollowMachine(supports_freeze=False)

        action = m.handle(FollowEvent.ClogDetected(), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.NOOP

    def test_toggle_pause_true_is_a_noop(self):
        m = PlaybackFollowMachine(supports_freeze=False)

        action = m.handle(FollowEvent.TogglePause(True), LIVE)

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.NOOP

    def test_force_live_toggle_still_works_normally(self):
        m = PlaybackFollowMachine(supports_freeze=False)
        m.handle(FollowEvent.Tick(), replay(100))  # -> FOLLOWING

        action = m.handle(FollowEvent.ToggleForceLive(True), replay(100))

        assert m.state is FollowState.LIVE
        assert action.kind is FollowActionKind.FETCH_LIVE


class TestUnhandledEvent:
    def test_unknown_event_type_raises(self):
        m = PlaybackFollowMachine()
        try:
            m.handle(object(), LIVE)
        except TypeError:
            return
        raise AssertionError("expected TypeError for an unrecognized event type")

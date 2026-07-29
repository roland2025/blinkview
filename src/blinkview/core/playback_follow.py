# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Shared playback-follow state machine for LogViewerWidget/LogTableViewerWidget/TelemetryTable.

See plans/playback-follow-state-machine.md for the design rationale. Plain Python, no Qt - mirrors
core/playback_clock.py's own style so this stays testable headless and consistent with this
codebase's convention of keeping state machines out of the UI layer.

Collapses what used to be four loose booleans per widget (follow_playback/is_paused/
_playback_anchored/force_live) into one FollowState enum plus an explicit transition table.
Key simplification: force_live is not a fourth state to cross with the other three - it's a
per-widget policy flag that redirects one transition (Tick while REPLAY -> LIVE instead of
FOLLOWING).

Fetch mechanics stay entirely in each widget (text area vs table model vs snapshot swap - these
are genuinely different implementations) - this module only decides *when/whether* to fetch, via
the FollowAction returned from handle(), never *how*.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Optional

from blinkview.core.playback_clock import PlaybackMode


class FollowState(Enum):
    LIVE = "live"  # showing the true live tail
    FOLLOWING = "following"  # ts-anchored to clock.current_ts_ns, re-fetches every tick
    FROZEN = "frozen"  # anchored to a fixed window; only a Resume-equivalent event exits


class FollowActionKind(Enum):
    NOOP = "noop"
    FETCH_LIVE = "fetch_live"  # (re)fetch the true live tail
    FETCH_FOLLOWING = "fetch_following"  # (re)fetch a window anchored to anchor_ts_ns
    FREEZE = "freeze"  # transition into FROZEN - from_state tells the widget how to pick the
    # anchor: LIVE -> pick one from the live buffer (mirrors the old _enter_history_mode), while
    # FOLLOWING -> the ts-anchored window already on screen is kept in place, just marked frozen


@dataclass(frozen=True)
class FollowAction:
    kind: FollowActionKind
    anchor_ts_ns: Optional[int] = None
    auto: bool = False  # True for clog-triggered auto-freeze, matching the old auto_paused flag
    from_state: Optional[FollowState] = None  # state the machine was in *before* this event


@dataclass(frozen=True)
class ClockSnapshot:
    """What the machine needs to know about registry.playback_clock for one decision. Widgets
    build this fresh from clock.mode/current_ts_ns/is_playing at each call site - the machine
    never reads the clock itself, matching PlaybackClock's own read-only-consumer convention
    elsewhere in this codebase."""

    mode: PlaybackMode
    current_ts_ns: int
    is_playing: bool = False


class FollowEvent:
    """Namespace for the machine's event types - grouped under one name so call sites read as
    FollowEvent.Tick(...)/FollowEvent.ScrolledAway() rather than a pile of same-shaped top-level
    dataclasses. is_scrubbing is deliberately not modeled here - suppressing a transient scrollbar
    valueChanged mid-drag is a UI-input debounce concern, not a playback-follow state concern, so
    widgets are expected to pre-filter it (check clock.is_scrubbing) before ever calling handle()."""

    @dataclass(frozen=True)
    class Tick:
        """Every apply_updates() heartbeat."""

    @dataclass(frozen=True)
    class ScrolledAway:
        """Manual scroll off the live/followed edge."""

    @dataclass(frozen=True)
    class ScrolledToLiveEdge:
        """Manual scroll back to the true live tail (the pool's actual newest row) - unconditional
        regardless of clock mode, since reaching the absolute newest buffered row is a fact about
        the widget's own view, not about following the replay clock."""

    @dataclass(frozen=True)
    class TogglePause:
        checked: bool

    @dataclass(frozen=True)
    class ToggleForceLive:
        checked: bool

    @dataclass(frozen=True)
    class ClogDetected:
        """Velocity-tracker auto-pause trigger - only meaningful from LIVE (clog protection only
        ever fires while pulling the ordinary live tail)."""


class PlaybackFollowMachine:
    """One instance per widget. Call handle(event, clock) for every playback-relevant occurrence
    (a heartbeat tick, a user scroll, a button click) and act on the returned FollowAction.

    supports_freeze=False (TelemetryTable) means the widget never scrolls/pauses - it only ever
    occupies LIVE or FOLLOWING, and ScrolledAway/ClogDetected/TogglePause(True) are no-ops rather
    than errors, so a widget can freely wire up the full event set without special-casing which
    ones apply to it.
    """

    def __init__(self, supports_freeze: bool = True):
        self.state = FollowState.LIVE
        self.force_live = False
        self.supports_freeze = supports_freeze

    def handle(self, event, clock: ClockSnapshot) -> FollowAction:
        if isinstance(event, FollowEvent.ToggleForceLive):
            return self._handle_toggle_force_live(event)
        if isinstance(event, FollowEvent.Tick):
            return self._handle_tick(clock)
        if isinstance(event, FollowEvent.ClogDetected):
            return self._handle_clog_detected()
        if isinstance(event, FollowEvent.ScrolledAway):
            return self._handle_scrolled_away()
        if isinstance(event, FollowEvent.ScrolledToLiveEdge):
            return self._handle_scrolled_to_live_edge()
        if isinstance(event, FollowEvent.TogglePause):
            return self._handle_toggle_pause(event, clock)
        raise TypeError(f"Unhandled FollowEvent: {event!r}")

    def _handle_toggle_force_live(self, event: "FollowEvent.ToggleForceLive") -> FollowAction:
        self.force_live = event.checked
        # Only FOLLOWING needs an immediate reaction - LIVE is already showing the live tail
        # (force_live just prevents it leaving on the next REPLAY tick), and FROZEN's own Resume
        # path (_handle_toggle_pause) is what actually applies force_live once the user unfreezes.
        if event.checked and self.state is FollowState.FOLLOWING:
            prev = self.state
            self.state = FollowState.LIVE
            return FollowAction(FollowActionKind.FETCH_LIVE, from_state=prev)
        return FollowAction(FollowActionKind.NOOP, from_state=self.state)

    def _handle_tick(self, clock: ClockSnapshot) -> FollowAction:
        prev = self.state

        if self.state is FollowState.LIVE:
            if clock.mode is PlaybackMode.REPLAY and not self.force_live:
                self.state = FollowState.FOLLOWING
                return FollowAction(FollowActionKind.FETCH_FOLLOWING, anchor_ts_ns=clock.current_ts_ns, from_state=prev)
            return FollowAction(FollowActionKind.FETCH_LIVE, from_state=prev)

        if self.state is FollowState.FOLLOWING:
            if clock.mode is PlaybackMode.LIVE or self.force_live:
                self.state = FollowState.LIVE
                return FollowAction(FollowActionKind.FETCH_LIVE, from_state=prev)
            return FollowAction(FollowActionKind.FETCH_FOLLOWING, anchor_ts_ns=clock.current_ts_ns, from_state=prev)

        # FROZEN: ticks never move it on their own - a frozen widget's own tail-arrived polling
        # (mirrors the old _poll_history_tail) stays widget-level, driven off state == FROZEN.
        return FollowAction(FollowActionKind.NOOP, from_state=prev)

    def _handle_clog_detected(self) -> FollowAction:
        if not self.supports_freeze or self.state is not FollowState.LIVE:
            return FollowAction(FollowActionKind.NOOP, from_state=self.state)
        prev = self.state
        self.state = FollowState.FROZEN
        return FollowAction(FollowActionKind.FREEZE, auto=True, from_state=prev)

    def _handle_scrolled_away(self) -> FollowAction:
        if not self.supports_freeze or self.state is FollowState.FROZEN:
            return FollowAction(FollowActionKind.NOOP, from_state=self.state)
        prev = self.state
        self.state = FollowState.FROZEN
        return FollowAction(FollowActionKind.FREEZE, auto=False, from_state=prev)

    def _handle_scrolled_to_live_edge(self) -> FollowAction:
        if self.state is not FollowState.FROZEN:
            return FollowAction(FollowActionKind.NOOP, from_state=self.state)
        prev = self.state
        self.state = FollowState.LIVE
        return FollowAction(FollowActionKind.FETCH_LIVE, from_state=prev)

    def _handle_toggle_pause(self, event: "FollowEvent.TogglePause", clock: ClockSnapshot) -> FollowAction:
        prev = self.state
        if event.checked:
            if not self.supports_freeze or self.state is FollowState.FROZEN:
                return FollowAction(FollowActionKind.NOOP, from_state=prev)
            self.state = FollowState.FROZEN
            return FollowAction(FollowActionKind.FREEZE, auto=False, from_state=prev)

        # Resume (checked=False): only meaningful from FROZEN.
        if self.state is not FollowState.FROZEN:
            return FollowAction(FollowActionKind.NOOP, from_state=prev)

        if clock.mode is PlaybackMode.LIVE or self.force_live:
            self.state = FollowState.LIVE
            return FollowAction(FollowActionKind.FETCH_LIVE, from_state=prev)

        # The clock is still playing back and force_live isn't pinning this widget - rejoin it
        # rather than jumping to the live tail. No immediate fetch: the next Tick's FOLLOWING
        # branch re-anchors to wherever the clock has moved to since.
        self.state = FollowState.FOLLOWING
        return FollowAction(FollowActionKind.NOOP, from_state=prev)

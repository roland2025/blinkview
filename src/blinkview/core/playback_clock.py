# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from enum import Enum
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from blinkview.core.numpy_log import CircularLogPool


class PlaybackMode(Enum):
    LIVE = "live"
    REPLAY = "replay"


class PlaybackClock:
    """Owns the virtual "what instant in the log are we looking at" cursor.

    Plain Python, no Qt: mirrors the rest of core/ so headless/CLI use stays
    possible. The UI layer polls tick() every heartbeat and reacts to its
    return value instead of receiving a cross-thread signal.
    """

    def __init__(self, log_pool: "CircularLogPool"):
        self._log_pool = log_pool

        self.mode = PlaybackMode.LIVE
        self.is_playing = False  # only meaningful in REPLAY
        self.speed = 1.0  # signed: negative = rewind, magnitude = multiplier

        # True for the duration of a manual drag on the transport scrubber (SeekBarWidget's
        # mouse-press-to-release window). A single flag on the clock itself, rather than each
        # consumer inventing its own "is the user dragging something" bookkeeping, since any
        # widget reacting to clock state (log viewer, plotter, ...) needs the same answer.
        self.is_scrubbing = False

        self.current_ts_ns = 0
        self.bounds_min_ns = 0
        self.bounds_max_ns = 0  # tracks the live edge

        self._last_tick_wall_ns: Optional[int] = None

        self._refresh_bounds()
        self.current_ts_ns = self.bounds_max_ns

    def _refresh_bounds(self):
        self.bounds_min_ns, self.bounds_max_ns = self._log_pool.get_time_bounds()

    def go_live(self):
        self.mode = PlaybackMode.LIVE
        self.is_playing = False
        self.current_ts_ns = self.bounds_max_ns

    def enter_replay(self, at_ts_ns: Optional[int] = None):
        self.mode = PlaybackMode.REPLAY
        self.current_ts_ns = self._clamp(at_ts_ns if at_ts_ns is not None else self.current_ts_ns)

    def play(self, speed: Optional[float] = None):
        if self.mode is not PlaybackMode.REPLAY:
            self.enter_replay()
        if speed is not None:
            self.speed = speed
        self.is_playing = True

    def pause(self):
        self.is_playing = False

    def set_speed(self, speed: float):
        self.speed = speed

    def seek(self, ts_ns: int):
        if self.mode is not PlaybackMode.REPLAY:
            self.enter_replay()
        self.current_ts_ns = self._clamp(ts_ns)

    def begin_scrub(self):
        """Marks a manual scrub-bar drag in progress. While set, tick() must not auto-advance
        current_ts_ns from elapsed wall-clock time - that would fight a drag with its own
        seek() calls - nor trigger the is_playing auto-pause-at-rewind-bound/auto-go-live-at-
        forward-bound side effects partway through a drag the user hasn't released yet."""
        self.is_scrubbing = True

    def end_scrub(self):
        """Ends a manual scrub-bar drag - tick()'s auto-advance resumes from here."""
        self.is_scrubbing = False

    def _clamp(self, ts_ns: int) -> int:
        return max(self.bounds_min_ns, min(self.bounds_max_ns, ts_ns))

    def tick(self, wall_now_ns: int) -> bool:
        """Advances virtual time by one heartbeat. Returns True if any
        observable state (mode/current_ts_ns/bounds/is_playing) changed."""
        prev_mode = self.mode
        prev_ts = self.current_ts_ns
        prev_playing = self.is_playing
        prev_min, prev_max = self.bounds_min_ns, self.bounds_max_ns

        self._refresh_bounds()

        if self.mode is PlaybackMode.LIVE:
            self.current_ts_ns = self.bounds_max_ns
        elif self.is_playing and not self.is_scrubbing:
            last = self._last_tick_wall_ns if self._last_tick_wall_ns is not None else wall_now_ns
            elapsed_ns = wall_now_ns - last
            self.current_ts_ns = self._clamp(int(self.current_ts_ns + elapsed_ns * self.speed))

            if self.speed > 0 and self.current_ts_ns >= self.bounds_max_ns:
                self.go_live()
            elif self.speed < 0 and self.current_ts_ns <= self.bounds_min_ns:
                self.is_playing = False

        self._last_tick_wall_ns = wall_now_ns

        return (
            self.mode is not prev_mode
            or self.current_ts_ns != prev_ts
            or self.is_playing is not prev_playing
            or self.bounds_min_ns != prev_min
            or self.bounds_max_ns != prev_max
        )

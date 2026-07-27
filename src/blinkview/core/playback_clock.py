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

        # See enter_replay_when_ready(): a seek requested before the pool has any data yet
        # (bounds_max_ns == 0) can't be clamped into a meaningful range immediately - it's
        # deferred here and resolved on the first tick() where real data has appeared.
        self._pending_seek_ts_ns: Optional[int] = None
        self._has_pending_seek = False

        self._refresh_bounds()
        self.current_ts_ns = self.bounds_max_ns

    def _refresh_bounds(self):
        self.bounds_min_ns, self.bounds_max_ns = self._log_pool.get_time_bounds()

    def _cancel_pending_seek(self):
        """Clears any enter_replay_when_ready() seek still waiting for data to appear - called
        by every other method that sets current_ts_ns itself, so a deferred target can never
        clobber a real seek/go_live/etc. that happened to land first."""
        self._has_pending_seek = False
        self._pending_seek_ts_ns = None

    def go_live(self):
        self.mode = PlaybackMode.LIVE
        self.is_playing = False
        self.current_ts_ns = self.bounds_max_ns
        self._cancel_pending_seek()

    def enter_replay(self, at_ts_ns: Optional[int] = None):
        self.mode = PlaybackMode.REPLAY
        self.current_ts_ns = self._clamp(at_ts_ns if at_ts_ns is not None else self.current_ts_ns)
        self._cancel_pending_seek()

    def enter_replay_when_ready(self, at_ts_ns: Optional[int] = None):
        """Like enter_replay(), but safe to call before the pool has any data yet (e.g. right
        after configuring a replay source, before its first batch has streamed in) - seeking
        immediately in that state would just get clamped down to the still-empty [0, 0] bounds.
        If data already exists, behaves exactly like enter_replay(). Otherwise, switches to
        REPLAY now (so the UI reflects DVR mode right away) but defers the actual seek to the
        first tick() where bounds_max_ns becomes nonzero - landing on at_ts_ns if given, or on
        bounds_min_ns (the start of whatever loads) if at_ts_ns is None."""
        self.mode = PlaybackMode.REPLAY
        if self.bounds_max_ns > 0:
            self.current_ts_ns = self._clamp(at_ts_ns if at_ts_ns is not None else self.bounds_min_ns)
            self._cancel_pending_seek()
        else:
            self._pending_seek_ts_ns = at_ts_ns
            self._has_pending_seek = True

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
        self._cancel_pending_seek()

    def step_rows(self, delta_rows: int):
        """Steps the cursor by an exact number of *rows* rather than a time delta - the jog-wheel
        precise-scrub control's primitive (core/numpy_log.py's CircularLogPool.find_ts_n_rows_away
        does the actual row counting; the caller here has already translated drag velocity into a
        row count). Enters REPLAY if not already there, same as seek()."""
        if delta_rows == 0:
            return
        if self.mode is not PlaybackMode.REPLAY:
            self.enter_replay()
        new_ts = self._log_pool.find_ts_n_rows_away(self.current_ts_ns, delta_rows)
        self.current_ts_ns = self._clamp(new_ts)
        self._cancel_pending_seek()

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

        if self._has_pending_seek and self.bounds_max_ns > 0:
            target = self._pending_seek_ts_ns if self._pending_seek_ts_ns is not None else self.bounds_min_ns
            self.current_ts_ns = self._clamp(target)
            self._has_pending_seek = False
            self._pending_seek_ts_ns = None

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

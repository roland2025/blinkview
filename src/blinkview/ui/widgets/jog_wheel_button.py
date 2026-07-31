# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Press-and-drag jog wheel for precise row-by-row playback scrubbing - see
plans/named-playback-ranges.md. Modeled on DaVinci Resolve/Maya-style relative-motion drag
controls: press and hold, drag left/right, the OS cursor hides and gets warped back to the press
origin after every move (so a drag can never run off-screen and there's no absolute position to
run out of), and release ends the scrub. Drag speed - not distance - determines step size: a slow
drag steps one row at a time (frame-accurate), a fast drag accelerates into a coarse shuttle.
"""

import math

from qtpy.QtCore import QPoint, Qt, Signal
from qtpy.QtGui import QCursor
from qtpy.QtWidgets import QToolButton


class JogWheelButton(QToolButton):
    """Emits stepRequested(delta_rows) while being dragged. Positive delta_rows = forward/newer
    (drag right), negative = backward/older (drag left) - matches a left-to-right timeline."""

    stepRequested = Signal(int)
    scrubStarted = Signal()
    scrubEnded = Signal()

    # Velocity (px/s) below which a drag produces no motion at all - filters out jitter from a
    # press that barely moves before release.
    DEAD_ZONE_PX_S = 8.0
    # Velocity (px/s) that maps to exactly 1 row/event - the "fine, frame-accurate" reference
    # speed a slow, deliberate drag naturally lands in.
    REFERENCE_PX_S = 60.0
    # Exponent controlling how aggressively speed above REFERENCE_PX_S accelerates into a coarse
    # shuttle - 1.0 would be linear; >1 gives a jog-wheel-like superlinear ramp.
    ACCELERATION_EXPONENT = 1.5

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setText("⟲⟳")
        self.setToolTip("Press and drag: precise scrub (row-by-row)")

        self._dragging = False
        self._press_global_pos: "QPoint | None" = None
        self._last_event_pos: "QPoint | None" = None
        self._last_event_time_ms: int = 0
        self._row_accumulator: float = 0.0

    def _velocity_to_row_delta(self, dx_px: float, dt_s: float) -> float:
        """Pure function, split out from the mouse-event plumbing so it's directly unit-testable
        without synthesizing real drag events. Returns a (possibly fractional) row count for one
        motion sample - the caller accumulates fractions across samples so slow drags still make
        progress instead of rounding to zero every event."""
        if dt_s <= 0:
            return 0.0

        velocity = dx_px / dt_s
        magnitude = abs(velocity)

        if magnitude < self.DEAD_ZONE_PX_S:
            return 0.0

        rows = (magnitude / self.REFERENCE_PX_S) ** self.ACCELERATION_EXPONENT
        return math.copysign(rows, velocity)

    def mousePressEvent(self, event):
        if event.button() != Qt.LeftButton:
            super().mousePressEvent(event)
            return

        self._dragging = True
        self._press_global_pos = event.globalPosition().toPoint()
        self._last_event_pos = self._press_global_pos
        self._last_event_time_ms = event.timestamp()
        self._row_accumulator = 0.0

        self.grabMouse()
        self.setCursor(Qt.BlankCursor)
        self.scrubStarted.emit()
        event.accept()

    def mouseMoveEvent(self, event):
        if not self._dragging:
            super().mouseMoveEvent(event)
            return

        # Use the event's own hardware timestamp, not wall-clock time at the moment this handler
        # runs - if the event loop briefly stalls (a repaint, several queued moves flushing back
        # to back), multiple events can get *processed* microseconds apart even though the real
        # mouse motion behind them was spread over much longer, and monotonic() would read that
        # as a huge instantaneous velocity, producing large jumps.
        now_ms = event.timestamp()
        current_pos = event.globalPosition().toPoint()

        dx = current_pos.x() - self._last_event_pos.x()
        dt = (now_ms - self._last_event_time_ms) / 1000.0

        if dx != 0 and dt > 0:
            self._row_accumulator += self._velocity_to_row_delta(dx, dt)
            whole = int(self._row_accumulator)
            if whole != 0:
                self._row_accumulator -= whole
                self.stepRequested.emit(whole)

        self._last_event_time_ms = now_ms

        # Warp the cursor back to the press origin (the classic "infinite drag" technique) so a
        # long scrub session never runs off the edge of the screen. The resulting synthetic
        # mouseMoveEvent this generates has dx=0 against _press_global_pos, which the dead-zone
        # check above already treats as a no-op - no reentrancy guard needed.
        QCursor.setPos(self._press_global_pos)
        self._last_event_pos = self._press_global_pos

        event.accept()

    def mouseReleaseEvent(self, event):
        if event.button() != Qt.LeftButton or not self._dragging:
            super().mouseReleaseEvent(event)
            return

        self._dragging = False
        self._press_global_pos = None
        self._last_event_pos = None
        self._row_accumulator = 0.0

        self.releaseMouse()
        self.unsetCursor()
        self.scrubEnded.emit()
        event.accept()

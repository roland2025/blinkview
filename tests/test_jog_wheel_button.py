# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.ui.widgets.jog_wheel_button import JogWheelButton


class TestVelocityToRowDelta:
    """Pure-function tests for the speed->row-count curve, independent of any real mouse/cursor
    plumbing - this is the actual "should depend on mouse movement speed" behavior; the
    mousePress/Move/Release handlers below are just wiring around it.

    Every test here still needs the `qapp` fixture even though it never touches it directly -
    JogWheelButton is a real QToolButton subclass, and constructing any QWidget without a live
    QApplication instance already existing in the process aborts the interpreter outright (no
    Python exception, no traceback - the process just dies), rather than raising something
    catchable. `qapp` (pytest-qt) guarantees exactly one QApplication exists for the whole test
    session before any widget gets constructed."""

    def test_below_dead_zone_produces_no_motion(self, qapp):
        btn = JogWheelButton()
        # 1px moved over 1s = 1 px/s, well under DEAD_ZONE_PX_S.
        assert btn._velocity_to_row_delta(dx_px=1, dt_s=1.0) == 0.0

    def test_zero_dt_is_safe_and_produces_no_motion(self, qapp):
        btn = JogWheelButton()
        assert btn._velocity_to_row_delta(dx_px=100, dt_s=0.0) == 0.0

    def test_reference_speed_maps_to_roughly_one_row(self, qapp):
        btn = JogWheelButton()
        # dx/dt == REFERENCE_PX_S by construction -> (REFERENCE/REFERENCE)**exp == 1.0 rows.
        dt = 0.1
        dx = btn.REFERENCE_PX_S * dt
        assert btn._velocity_to_row_delta(dx_px=dx, dt_s=dt) == 1.0

    def test_direction_is_preserved(self, qapp):
        btn = JogWheelButton()
        dt = 0.1
        dx = btn.REFERENCE_PX_S * dt
        assert btn._velocity_to_row_delta(dx_px=dx, dt_s=dt) > 0
        assert btn._velocity_to_row_delta(dx_px=-dx, dt_s=dt) < 0

    def test_faster_drag_produces_more_rows_than_slower_drag(self, qapp):
        """The core "should depend on mouse movement speed" requirement: a faster drag over the
        same time window must produce a larger step than a slower one, not the same step scaled
        only by distance."""
        btn = JogWheelButton()
        dt = 0.1
        slow = btn._velocity_to_row_delta(dx_px=btn.REFERENCE_PX_S * dt, dt_s=dt)
        fast = btn._velocity_to_row_delta(dx_px=btn.REFERENCE_PX_S * dt * 10, dt_s=dt)
        assert fast > slow * 5  # superlinear acceleration, not merely proportional

    def test_same_distance_slower_drag_produces_fewer_rows(self, qapp):
        """Same pixel distance, more time elapsed (slower drag) -> smaller step - distance alone
        must not determine the result, speed must."""
        btn = JogWheelButton()
        dx = btn.REFERENCE_PX_S * 0.1
        fast = btn._velocity_to_row_delta(dx_px=dx, dt_s=0.1)
        slow = btn._velocity_to_row_delta(dx_px=dx, dt_s=1.0)
        assert fast > slow


def _mouse_event(event_type, local_pos, global_pos, button, buttons, timestamp_ms=0):
    from qtpy.QtCore import QPointF, Qt
    from qtpy.QtGui import QMouseEvent

    event = QMouseEvent(event_type, QPointF(*local_pos), QPointF(*global_pos), button, buttons, Qt.NoModifier)
    event.setTimestamp(timestamp_ms)
    return event


class TestPressDragRelease:
    def test_press_starts_dragging_grabs_mouse_and_hides_cursor(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        started = []
        btn.scrubStarted.connect(lambda: started.append(1))

        event = _mouse_event(QEvent.MouseButtonPress, (10, 10), (100, 100), Qt.LeftButton, Qt.LeftButton)
        btn.mousePressEvent(event)

        assert btn._dragging is True
        assert started == [1]
        assert btn.cursor().shape() == Qt.BlankCursor

        # Clean up the real grab so the test doesn't leak it to the next test.
        btn.mouseReleaseEvent(_mouse_event(QEvent.MouseButtonRelease, (10, 10), (100, 100), Qt.LeftButton, Qt.NoButton))

    def test_release_ends_dragging_and_restores_cursor(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        ended = []
        btn.scrubEnded.connect(lambda: ended.append(1))

        btn.mousePressEvent(_mouse_event(QEvent.MouseButtonPress, (10, 10), (100, 100), Qt.LeftButton, Qt.LeftButton))
        btn.mouseReleaseEvent(_mouse_event(QEvent.MouseButtonRelease, (10, 10), (100, 100), Qt.LeftButton, Qt.NoButton))

        assert btn._dragging is False
        assert ended == [1]
        assert btn.cursor().shape() != Qt.BlankCursor

    def test_release_without_a_prior_press_is_a_noop(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        ended = []
        btn.scrubEnded.connect(lambda: ended.append(1))

        # No mousePressEvent happened - must not raise or emit scrubEnded.
        btn.mouseReleaseEvent(_mouse_event(QEvent.MouseButtonRelease, (10, 10), (100, 100), Qt.LeftButton, Qt.NoButton))

        assert ended == []

    def test_drag_after_press_emits_step_requested_with_correct_sign(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        steps = []
        btn.stepRequested.connect(steps.append)

        btn.mousePressEvent(_mouse_event(QEvent.MouseButtonPress, (10, 10), (500, 500), Qt.LeftButton, Qt.LeftButton))

        # Drag far right -> fast rightward motion -> positive (forward) row steps. Use the
        # event's own timestamp (not wall-clock) for a deterministic elapsed time.
        btn.mouseMoveEvent(
            _mouse_event(QEvent.MouseMove, (600, 10), (1100, 500), Qt.LeftButton, Qt.LeftButton, timestamp_ms=1000)
        )

        btn.mouseReleaseEvent(
            _mouse_event(QEvent.MouseButtonRelease, (600, 10), (1100, 500), Qt.LeftButton, Qt.NoButton)
        )

        assert steps
        assert all(s > 0 for s in steps)

    def test_drag_left_emits_negative_steps(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        steps = []
        btn.stepRequested.connect(steps.append)

        btn.mousePressEvent(_mouse_event(QEvent.MouseButtonPress, (600, 10), (1100, 500), Qt.LeftButton, Qt.LeftButton))

        btn.mouseMoveEvent(
            _mouse_event(QEvent.MouseMove, (10, 10), (500, 500), Qt.LeftButton, Qt.LeftButton, timestamp_ms=1000)
        )

        btn.mouseReleaseEvent(_mouse_event(QEvent.MouseButtonRelease, (10, 10), (500, 500), Qt.LeftButton, Qt.NoButton))

        assert steps
        assert all(s < 0 for s in steps)

    def test_tiny_jitter_move_emits_no_steps(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        steps = []
        btn.stepRequested.connect(steps.append)

        btn.mousePressEvent(_mouse_event(QEvent.MouseButtonPress, (10, 10), (500, 500), Qt.LeftButton, Qt.LeftButton))

        # 1px moved over a full second = 1 px/s - genuine drift/jitter, well under DEAD_ZONE_PX_S,
        # not a fast flick (a *tiny* elapsed time would instead read as a *huge* velocity - this is
        # exactly why production code uses the event's own timestamp rather than wall-clock time
        # sampled when the handler happens to run).
        btn.mouseMoveEvent(
            _mouse_event(QEvent.MouseMove, (11, 10), (501, 500), Qt.LeftButton, Qt.LeftButton, timestamp_ms=1000)
        )

        btn.mouseReleaseEvent(_mouse_event(QEvent.MouseButtonRelease, (11, 10), (501, 500), Qt.LeftButton, Qt.NoButton))

        assert steps == []

    def test_slow_real_processing_does_not_inflate_velocity(self, qapp, qtbot):
        """Regression test for the "jumpy, sometimes jumps a very big amount" bug: if the elapsed
        time were measured by sampling the wall clock when mouseMoveEvent happens to run (instead
        of the event's own hardware timestamp), then a press immediately followed by a move -
        which in a test executes in microseconds - would read as a near-zero dt even though the
        events themselves are 100ms apart, producing a wildly inflated velocity and a huge spurious
        row jump for a drag that is actually only reference speed. This reproduces the same
        symptom a real app sees when the event loop briefly stalls (e.g. a repaint) and several
        queued mouse-move events then get processed back-to-back."""
        from qtpy.QtCore import QEvent, Qt

        btn = JogWheelButton()
        qtbot.addWidget(btn)

        steps = []
        btn.stepRequested.connect(steps.append)

        btn.mousePressEvent(_mouse_event(QEvent.MouseButtonPress, (10, 10), (500, 500), Qt.LeftButton, Qt.LeftButton))
        # Reference-speed drag over the event timestamps (100ms), delivered with no real wall-clock
        # delay between the press and move calls above.
        dx = int(btn.REFERENCE_PX_S * 0.1)
        btn.mouseMoveEvent(
            _mouse_event(
                QEvent.MouseMove,
                (10 + dx, 10),
                (500 + dx, 500),
                Qt.LeftButton,
                Qt.LeftButton,
                timestamp_ms=100,
            )
        )

        btn.mouseReleaseEvent(
            _mouse_event(QEvent.MouseButtonRelease, (10 + dx, 10), (500 + dx, 500), Qt.LeftButton, Qt.NoButton)
        )

        # A reference-speed drag should step ~1 row, never a huge jump.
        assert steps
        assert all(abs(s) <= 3 for s in steps)

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.playback_clock import PlaybackClock, PlaybackMode
from blinkview.ui.widgets.playback_control import PlaybackControlWidget, SpeedSliderWidget

BASE = 1_000_000_000_000
BOUNDS = (BASE, BASE + 10_000_000_000_000)  # wide span so ordinary ticks/seeks never hit an edge


class FakeLogPool:
    def get_time_bounds(self):
        return BOUNDS


class FakeCentral:
    log_pool = FakeLogPool()


class FakeRegistry:
    """Stands in for core.registry.Registry - only the surface PlaybackControlWidget touches."""

    def __init__(self):
        self.central = FakeCentral()
        self.playback_clock = PlaybackClock(self.central.log_pool)
        self._now_ns = 0

    def now_ns(self):
        return self._now_ns


class FakeGuiContext:
    """Stands in for ui.gui_context.GUIContext - only add_updatable/remove_updatable/registry."""

    def __init__(self):
        self.registry = FakeRegistry()
        self.updatable = []

    def add_updatable(self, w):
        self.updatable.append(w)

    def remove_updatable(self, w):
        if w in self.updatable:
            self.updatable.remove(w)


@pytest.fixture
def gui_context():
    return FakeGuiContext()


@pytest.fixture
def widget(qapp, qtbot, gui_context):
    w = PlaybackControlWidget(gui_context)
    qtbot.addWidget(w)
    yield w


def test_constructs_in_live_mode_registered_as_updatable(widget, gui_context):
    assert widget in gui_context.updatable
    assert widget.status_button.text() == "● LIVE"
    assert widget.play_button.isEnabled() is False


def test_apply_updates_ticks_the_clock_and_syncs_labels(widget, gui_context):
    gui_context.registry._now_ns = 1_000_000_000
    widget.apply_updates()

    assert widget.time_label.text() != "--:--:--.---"
    assert widget.seek_bar.current_ts_ns == gui_context.registry.playback_clock.current_ts_ns


def test_status_click_enters_and_leaves_replay(widget, gui_context, qtbot):
    from qtpy.QtCore import Qt

    qtbot.mouseClick(widget.status_button, Qt.LeftButton)
    assert gui_context.registry.playback_clock.mode is PlaybackMode.REPLAY
    assert widget.play_button.isEnabled() is True

    qtbot.mouseClick(widget.status_button, Qt.LeftButton)
    assert gui_context.registry.playback_clock.mode is PlaybackMode.LIVE
    assert widget.play_button.isEnabled() is False


def test_play_pause_click_drives_clock_is_playing(widget, gui_context, qtbot):
    from qtpy.QtCore import Qt

    qtbot.mouseClick(widget.status_button, Qt.LeftButton)  # must be in REPLAY before play() is meaningful

    qtbot.mouseClick(widget.play_button, Qt.LeftButton)
    assert gui_context.registry.playback_clock.is_playing is True

    qtbot.mouseClick(widget.play_button, Qt.LeftButton)
    assert gui_context.registry.playback_clock.is_playing is False


def test_seek_emit_with_large_nanosecond_timestamp_does_not_overflow(widget, gui_context):
    """Regression test: SeekBarWidget.seekRequested used to be Signal(int), a 32-bit C int in
    Qt, which overflowed for nanosecond epoch timestamps and raised at emit() time."""
    huge_ts = 1_784_465_400_483_618_560  # exceeds signed 32-bit int range many times over

    widget.seek_bar.seekRequested.emit(huge_ts)

    clock = gui_context.registry.playback_clock
    assert clock.mode is PlaybackMode.REPLAY
    assert clock.current_ts_ns == min(huge_ts, BOUNDS[1])


def test_seek_bar_scrub_signals_drive_clock_is_scrubbing(widget, gui_context, qtbot):
    """A real mouse press-then-release on the seek bar (not a direct scrubStarted/scrubEnded
    signal emit) must reach PlaybackClock.is_scrubbing, so a drag suspends tick()'s own
    auto-advance for the duration - see test_playback_clock.py's scrubbing tests for the
    underlying clock behavior.

    seek_bar is only enabled in REPLAY (see _sync_from_clock) - genuinely disabled while LIVE, so
    it must never receive real mouse events in that state; entering REPLAY first (via a real
    status_button click, not a direct call) mirrors the only way a user could actually reach a
    scrub. The old direct-signal-emit version of this test didn't need that step, since it
    bypassed the disabled-widget guard Qt itself enforces."""
    from qtpy.QtCore import Qt

    qtbot.mouseClick(widget.status_button, Qt.LeftButton)  # enter REPLAY - required to enable seek_bar

    clock = gui_context.registry.playback_clock
    assert clock.is_scrubbing is False

    qtbot.mousePress(widget.seek_bar, Qt.LeftButton)
    assert clock.is_scrubbing is True

    qtbot.mouseRelease(widget.seek_bar, Qt.LeftButton)
    assert clock.is_scrubbing is False


def test_speed_slider_right_click_resets_to_default_value(qapp, qtbot):
    from qtpy.QtCore import Qt

    slider = SpeedSliderWidget(Qt.Horizontal, default_value=10)
    qtbot.addWidget(slider)
    slider.setMinimum(-80)
    slider.setMaximum(80)
    slider.setValue(42)
    assert slider.value() == 42

    qtbot.mouseClick(slider, Qt.RightButton)

    assert slider.value() == 10


def test_speed_changed_updates_label_and_clock_speed(widget, gui_context):
    widget.speed_slider.setValue(20)  # 2.0x

    assert widget.speed_label.text() == "2.0x"
    assert gui_context.registry.playback_clock.speed == 2.0


def test_apply_updates_resyncs_when_tick_advances_playback_time(widget, gui_context, qtbot):
    from qtpy.QtCore import Qt

    qtbot.mouseClick(widget.status_button, Qt.LeftButton)  # enter REPLAY

    clock = gui_context.registry.playback_clock
    # Seek away from the live edge first - playing forward from current_ts_ns == bounds_max_ns
    # would immediately clamp back to the edge and auto-go-live, leaving nothing to observe.
    widget._on_seek_requested((clock.bounds_min_ns + clock.bounds_max_ns) // 2)

    qtbot.mouseClick(widget.play_button, Qt.LeftButton)  # start playing

    # The very first tick() call only establishes the wall-clock baseline (no prior timestamp to
    # diff against, so elapsed time is 0) - playback only visibly advances from the *next* tick.
    gui_context.registry._now_ns = 1_000_000_000
    widget.apply_updates()
    ts_before = clock.current_ts_ns

    gui_context.registry._now_ns = 2_000_000_000  # 1s of wall time elapses while playing
    widget.apply_updates()

    assert clock.current_ts_ns != ts_before
    assert widget.seek_bar.current_ts_ns == clock.current_ts_ns


def test_apply_updates_does_not_resync_when_tick_reports_no_change(widget, gui_context):
    # Constructing the widget already ran an initial _sync_from_clock(); a second
    # apply_updates() at the same wall time with nothing else changed (still LIVE, bounds
    # unchanged) must produce a no-op tick() and skip re-syncing.
    calls = []
    original = widget._sync_from_clock
    widget._sync_from_clock = lambda: (calls.append(True), original())[-1]

    widget.apply_updates()  # now_ns still 0 - nothing observable changes

    assert calls == []


class TestNoRegistryGuards:
    """gui_context.registry can be None (e.g. before a session is fully wired up) - every clock
    accessor must no-op rather than raise."""

    @pytest.fixture
    def widget_without_registry(self, qapp, qtbot):
        ctx = FakeGuiContext()
        ctx.registry = None
        w = PlaybackControlWidget(ctx)
        qtbot.addWidget(w)
        return w

    def test_construction_does_not_raise(self, widget_without_registry):
        assert widget_without_registry.time_label.text() == "--:--:--.---"

    def test_apply_updates_is_a_noop(self, widget_without_registry):
        widget_without_registry.apply_updates()  # must not raise

    def test_status_clicked_is_a_noop(self, widget_without_registry):
        widget_without_registry._on_status_clicked(True)  # must not raise

    def test_play_clicked_is_a_noop(self, widget_without_registry):
        widget_without_registry._on_play_clicked(True)  # must not raise

    def test_seek_requested_is_a_noop(self, widget_without_registry):
        widget_without_registry._on_seek_requested(123)  # must not raise

    def test_speed_changed_still_updates_the_label_but_not_a_clock(self, widget_without_registry):
        widget_without_registry._on_speed_changed(20)
        assert widget_without_registry.speed_label.text() == "2.0x"


class TestSeekBarHelpers:
    def test_ts_to_x_with_zero_span_returns_zero(self, qapp, qtbot):
        from blinkview.ui.widgets.playback_control import SeekBarWidget

        bar = SeekBarWidget()
        qtbot.addWidget(bar)
        bar.bounds_min_ns = 1000
        bar.bounds_max_ns = 1000  # zero span

        assert bar._ts_to_x(1000) == 0

    def test_x_to_ts_with_zero_span_returns_bounds_min(self, qapp, qtbot):
        from blinkview.ui.widgets.playback_control import SeekBarWidget

        bar = SeekBarWidget()
        qtbot.addWidget(bar)
        bar.bounds_min_ns = 500
        bar.bounds_max_ns = 500

        assert bar._x_to_ts(10) == 500

    def test_ts_to_x_and_x_to_ts_are_roughly_inverse(self, qapp, qtbot):
        from blinkview.ui.widgets.playback_control import SeekBarWidget

        bar = SeekBarWidget()
        qtbot.addWidget(bar)
        bar.resize(200, 30)
        bar.bounds_min_ns = 0
        bar.bounds_max_ns = 1000

        x = bar._ts_to_x(500)
        ts_back = bar._x_to_ts(x)
        assert abs(ts_back - 500) < 20  # rounding tolerance


class TestSeekBarMouseTracking:
    def test_hover_without_button_sets_hover_x_without_seeking(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, QPointF, Qt
        from qtpy.QtGui import QMouseEvent

        from blinkview.ui.widgets.playback_control import SeekBarWidget

        bar = SeekBarWidget()
        qtbot.addWidget(bar)
        bar.resize(200, 30)

        seeks = []
        bar.seekRequested.connect(seeks.append)

        event = QMouseEvent(QEvent.MouseMove, QPointF(50, 5), Qt.NoButton, Qt.NoButton, Qt.NoModifier)
        bar.mouseMoveEvent(event)

        assert bar._hover_x == 50
        assert seeks == []

    def test_drag_with_left_button_held_emits_seek_requested(self, qapp, qtbot):
        from qtpy.QtCore import QEvent, QPointF, Qt
        from qtpy.QtGui import QMouseEvent

        from blinkview.ui.widgets.playback_control import SeekBarWidget

        bar = SeekBarWidget()
        qtbot.addWidget(bar)
        bar.resize(200, 30)
        bar.bounds_min_ns = 0
        bar.bounds_max_ns = 1000

        seeks = []
        bar.seekRequested.connect(seeks.append)

        event = QMouseEvent(QEvent.MouseMove, QPointF(100, 5), Qt.LeftButton, Qt.LeftButton, Qt.NoModifier)
        bar.mouseMoveEvent(event)

        assert len(seeks) == 1

    def test_leave_event_clears_hover_x(self, qapp, qtbot):
        from qtpy.QtCore import QEvent

        from blinkview.ui.widgets.playback_control import SeekBarWidget

        bar = SeekBarWidget()
        qtbot.addWidget(bar)
        bar._hover_x = 42

        bar.leaveEvent(QEvent(QEvent.Leave))

        assert bar._hover_x is None

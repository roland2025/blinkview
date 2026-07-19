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
def widget(qapp, gui_context):
    w = PlaybackControlWidget(gui_context)
    yield w
    w.deleteLater()


def test_constructs_in_live_mode_registered_as_updatable(widget, gui_context):
    assert widget in gui_context.updatable
    assert widget.status_button.text() == "● LIVE"
    assert widget.play_button.isEnabled() is False


def test_apply_updates_ticks_the_clock_and_syncs_labels(widget, gui_context):
    gui_context.registry._now_ns = 1_000_000_000
    widget.apply_updates()

    assert widget.time_label.text() != "--:--:--.---"
    assert widget.seek_bar.current_ts_ns == gui_context.registry.playback_clock.current_ts_ns


def test_status_click_enters_and_leaves_replay(widget, gui_context):
    widget._on_status_clicked(True)
    assert gui_context.registry.playback_clock.mode is PlaybackMode.REPLAY
    assert widget.play_button.isEnabled() is True

    widget._on_status_clicked(False)
    assert gui_context.registry.playback_clock.mode is PlaybackMode.LIVE
    assert widget.play_button.isEnabled() is False


def test_play_pause_click_drives_clock_is_playing(widget, gui_context):
    widget._on_status_clicked(True)  # must be in REPLAY before play() is meaningful

    widget._on_play_clicked(True)
    assert gui_context.registry.playback_clock.is_playing is True

    widget._on_play_clicked(False)
    assert gui_context.registry.playback_clock.is_playing is False


def test_seek_emit_with_large_nanosecond_timestamp_does_not_overflow(widget, gui_context):
    """Regression test: SeekBarWidget.seekRequested used to be Signal(int), a 32-bit C int in
    Qt, which overflowed for nanosecond epoch timestamps and raised at emit() time."""
    huge_ts = 1_784_465_400_483_618_560  # exceeds signed 32-bit int range many times over

    widget.seek_bar.seekRequested.emit(huge_ts)

    clock = gui_context.registry.playback_clock
    assert clock.mode is PlaybackMode.REPLAY
    assert clock.current_ts_ns == min(huge_ts, BOUNDS[1])


def test_seek_bar_scrub_signals_drive_clock_is_scrubbing(widget, gui_context):
    """SeekBarWidget.scrubStarted/scrubEnded (emitted by mousePress/mouseRelease) must reach
    PlaybackClock.is_scrubbing, so a drag suspends tick()'s own auto-advance for the duration -
    see test_playback_clock.py's scrubbing tests for the underlying clock behavior."""
    clock = gui_context.registry.playback_clock
    assert clock.is_scrubbing is False

    widget.seek_bar.scrubStarted.emit()
    assert clock.is_scrubbing is True

    widget.seek_bar.scrubEnded.emit()
    assert clock.is_scrubbing is False


def test_speed_slider_right_click_resets_to_default_value(qapp):
    from qtpy.QtCore import Qt

    class FakeRightClickEvent:
        def button(self):
            return Qt.RightButton

        def accept(self):
            pass

    slider = SpeedSliderWidget(Qt.Horizontal, default_value=10)
    slider.setMinimum(-80)
    slider.setMaximum(80)
    slider.setValue(42)
    assert slider.value() == 42

    slider.mousePressEvent(FakeRightClickEvent())

    assert slider.value() == 10

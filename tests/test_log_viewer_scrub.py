# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Regression test for LogViewerWidget._on_scroll_value_changed's is_scrubbing guard.

Constructing a full LogViewerWidget requires a real Registry/GUIContext plus a whole tree of
sidebar/filter/telemetry-table children, which is out of proportion for exercising a single
branch of pure control-flow logic. Instead this drives the unbound method against a minimal
stand-in object exposing only the attributes/methods that branch actually touches. Since
plans/playback-follow-state-machine.md, the stub carries a real PlaybackFollowMachine (plain
Python, trivially constructible) rather than raw follow_playback/_playback_anchored booleans -
_playback.handle() mutates it for real, so assertions read its .state directly."""

from types import SimpleNamespace

from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.playback_follow import ClockSnapshot, FollowState, PlaybackFollowMachine
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.ui.widgets.log_viewer import LogViewerWidget


class FakeScrollbar:
    def __init__(self, value=50, minimum=0, maximum=100):
        self._value = value
        self._min = minimum
        self._max = maximum

    def value(self):
        return self._value

    def minimum(self):
        return self._min

    def maximum(self):
        return self._max


class FakeTextArea:
    def __init__(self, scrollbar):
        self._scrollbar = scrollbar

    def verticalScrollBar(self):
        return self._scrollbar


class FakeClock:
    def __init__(self, is_scrubbing, mode=PlaybackMode.REPLAY, current_ts_ns=999, is_playing=False):
        self.is_scrubbing = is_scrubbing
        self.mode = mode
        self.current_ts_ns = current_ts_ns
        self.is_playing = is_playing


def _clock_snapshot(clock):
    if clock is None:
        return ClockSnapshot(mode=PlaybackMode.LIVE, current_ts_ns=0)
    return ClockSnapshot(mode=clock.mode, current_ts_ns=clock.current_ts_ns, is_playing=clock.is_playing)


def _make_widget_stub(is_scrubbing):
    calls = {"pause": [], "reanchor": [], "freeze": []}
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FOLLOWING  # mirrors the old playback_anchored=True fixture setup

    stub = SimpleNamespace(
        _programmatic_scroll=False,
        view_mode=LogViewMode.HISTORY,
        text_area=FakeTextArea(FakeScrollbar(value=50)),
        _playback=machine,
        _playback_anchored=True,  # FOLLOWING and view_mode already HISTORY - see the real property
        history_anchor_ts_ns=123,
        history_reached_start=True,
        history_oldest_seq=None,
        history_newest_seq=None,
        _clock=lambda: FakeClock(is_scrubbing),
        _clock_snapshot=lambda clock: _clock_snapshot(clock),
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
        _apply_freeze=lambda action: calls["freeze"].append(action),
    )
    return stub, calls, machine


def test_scroll_change_while_scrubbing_does_not_detach_or_pause():
    """A drag on the global transport scrubber can trigger a scrollbar valueChanged here that
    slips past the _programmatic_scroll guard's synchronous window (see the comment at the call
    site) - while is_scrubbing is True this must never be treated as a genuine manual scroll."""
    stub, calls, machine = _make_widget_stub(is_scrubbing=True)

    LogViewerWidget._on_scroll_value_changed(stub, 10)

    assert machine.state is FollowState.FOLLOWING
    assert calls["pause"] == []
    assert calls["reanchor"] == []
    assert calls["freeze"] == []


def test_scroll_change_while_not_scrubbing_still_detaches_and_pauses():
    """The is_scrubbing guard must not swallow a genuine manual scroll once no drag is active."""
    stub, calls, machine = _make_widget_stub(is_scrubbing=False)

    LogViewerWidget._on_scroll_value_changed(stub, 10)

    assert machine.state is FollowState.FROZEN
    assert len(calls["freeze"]) == 1
    assert calls["freeze"][0].from_state is FollowState.FOLLOWING
    assert len(calls["reanchor"]) == 1

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Regression tests for LogTableViewerWidget's playback-clock following wiring (mirrors
tests/test_log_viewer_scrub.py's approach for LogViewerWidget).

Constructing a full LogTableViewerWidget requires a real Registry/GUIContext plus a whole tree of
sidebar/filter/canvas children, which is out of proportion for exercising pure control-flow logic.
Instead this drives the unbound methods against minimal stand-in objects exposing only the
attributes/methods each branch actually touches. Since plans/playback-follow-state-machine.md, the
stub carries a real PlaybackFollowMachine (plain Python, trivially constructible) rather than raw
follow_playback/_playback_anchored/is_paused booleans - _playback.handle() mutates it for real, so
assertions read its .state directly."""

from types import SimpleNamespace

from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.playback_follow import ClockSnapshot, FollowState, PlaybackFollowMachine
from blinkview.ui.widgets.log_table_viewer import LogTableViewerWidget
from blinkview.ui.widgets.log_view_mode import LogViewMode


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


class FakeView:
    def __init__(self, scrollbar):
        self._scrollbar = scrollbar

    def verticalScrollBar(self):
        return self._scrollbar


class FakeClock:
    def __init__(self, mode=PlaybackMode.REPLAY, is_scrubbing=False, is_playing=True, current_ts_ns=0):
        self.mode = mode
        self.is_scrubbing = is_scrubbing
        self.is_playing = is_playing
        self.current_ts_ns = current_ts_ns


def _clock_snapshot(clock):
    if clock is None:
        return ClockSnapshot(mode=PlaybackMode.LIVE, current_ts_ns=0)
    return ClockSnapshot(mode=clock.mode, current_ts_ns=clock.current_ts_ns, is_playing=clock.is_playing)


def _apply_freeze_fake(calls):
    def _apply(action):
        calls["freeze"].append(action)

    return _apply


def _make_widget_stub(is_scrubbing, playback_anchored=True):
    calls = {"pause": [], "reanchor": [], "go_live": [], "freeze": []}
    machine = PlaybackFollowMachine()
    if playback_anchored:
        machine.state = FollowState.FOLLOWING

    stub = SimpleNamespace(
        _programmatic_scroll=False,
        model=SimpleNamespace(mode=LogViewMode.HISTORY, anchor_seq=None, row_count=10),
        view=FakeView(FakeScrollbar(value=50)),
        _playback=machine,
        _playback_anchored=playback_anchored,
        _clock=lambda: FakeClock(is_scrubbing=is_scrubbing),
        _clock_snapshot=_clock_snapshot,
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
        _apply_freeze=_apply_freeze_fake(calls),
        _go_live=lambda: calls["go_live"].append(True),
        _at_live_edge=lambda: False,
        _history_newest_ref_seq=lambda: None,
    )
    return stub, calls, machine


def test_scroll_change_while_scrubbing_does_not_detach_or_pause():
    """A drag on the global transport scrubber can trigger a scrollbar valueChanged here that
    slips past the _programmatic_scroll guard's synchronous window (see the comment at the call
    site) - while is_scrubbing is True this must never be treated as a genuine manual scroll."""
    stub, calls, machine = _make_widget_stub(is_scrubbing=True)

    LogTableViewerWidget._on_scroll_value_changed(stub, 10)

    assert machine.state is FollowState.FOLLOWING
    assert calls["pause"] == []
    assert calls["reanchor"] == []
    assert calls["freeze"] == []


def test_scroll_change_while_not_scrubbing_still_detaches_and_pauses():
    """The is_scrubbing guard must not swallow a genuine manual scroll once no drag is active."""
    stub, calls, machine = _make_widget_stub(is_scrubbing=False)

    LogTableViewerWidget._on_scroll_value_changed(stub, 10)

    assert machine.state is FollowState.FROZEN
    assert len(calls["freeze"]) == 1
    assert calls["freeze"][0].from_state is FollowState.FOLLOWING


def test_scroll_change_not_playback_anchored_falls_through_to_ordinary_logic():
    """When not currently clock-anchored, scrolling must fall through to the ordinary
    top/bottom-edge history paging logic instead of being swallowed by the playback branch."""
    stub, calls, machine = _make_widget_stub(is_scrubbing=False, playback_anchored=False)

    LogTableViewerWidget._on_scroll_value_changed(stub, 50)

    # Neither the playback-detach path nor its pause/freeze call should have fired.
    assert calls["pause"] == []
    assert calls["freeze"] == []


def test_toggle_pause_checked_while_playback_anchored_freezes_without_go_live():
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FOLLOWING
    calls = {"freeze": [], "go_live": []}
    stub = SimpleNamespace(
        _playback=machine,
        model=SimpleNamespace(mode=LogViewMode.HISTORY),
        _clock_snapshot=_clock_snapshot,
        _clock=lambda: FakeClock(),
        _apply_freeze=_apply_freeze_fake(calls),
        _go_live=lambda: calls["go_live"].append(True),
    )

    LogTableViewerWidget._toggle_pause(stub, True)

    assert machine.state is FollowState.FROZEN
    assert len(calls["freeze"]) == 1
    assert calls["freeze"][0].from_state is FollowState.FOLLOWING  # must not anchor off the LIVE buffer
    assert calls["go_live"] == []


def test_toggle_pause_unchecked_while_clock_replay_rejoins_follow_without_go_live():
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FROZEN
    calls = {"pause": [], "go_live": [], "freeze": []}
    stub = SimpleNamespace(
        _playback=machine,
        model=SimpleNamespace(mode=LogViewMode.HISTORY),
        _clock_snapshot=_clock_snapshot,
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
        _apply_freeze=_apply_freeze_fake(calls),
        _clock=lambda: FakeClock(mode=PlaybackMode.REPLAY),
        _go_live=lambda: calls["go_live"].append(True),
    )

    LogTableViewerWidget._toggle_pause(stub, False)

    assert machine.state is FollowState.FOLLOWING
    assert calls["pause"] == [False]
    assert calls["go_live"] == []  # rejoin the clock, don't jump to the backend's live tail


def test_toggle_pause_unchecked_while_clock_live_goes_live():
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FROZEN
    calls = {"go_live": [], "freeze": []}
    stub = SimpleNamespace(
        _playback=machine,
        model=SimpleNamespace(mode=LogViewMode.HISTORY),
        _clock_snapshot=_clock_snapshot,
        _set_pause_ui=lambda paused, auto=False: None,
        _apply_freeze=_apply_freeze_fake(calls),
        _clock=lambda: FakeClock(mode=PlaybackMode.LIVE),
        _go_live=lambda: calls["go_live"].append(True),
    )

    LogTableViewerWidget._toggle_pause(stub, False)

    assert machine.state is FollowState.LIVE
    assert calls["go_live"] == [True]


def test_apply_updates_replay_to_live_exit_resets_follow_state_and_goes_live():
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FOLLOWING
    calls = {"go_live": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback=machine,
        _clock_snapshot=_clock_snapshot,
        _last_followed_ts_ns=123,
        prev_apply=0,
        _clock=lambda: FakeClock(mode=PlaybackMode.LIVE),
        _go_live=lambda: calls["go_live"].append(True),
        _sync_force_live_visibility=lambda clock: None,
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["go_live"] == [True]
    assert machine.state is FollowState.LIVE
    assert stub._last_followed_ts_ns is None


def test_apply_updates_is_paused_blocks_replay_to_live_exit():
    """is_paused always wins: a manually-paused tab stays frozen through a clock mode change and
    only unfreezes on its own Resume click."""
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FROZEN
    calls = {"go_live": [], "model_apply": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback=machine,
        _clock_snapshot=_clock_snapshot,
        _last_followed_ts_ns=123,
        prev_apply=0,
        model=SimpleNamespace(
            mode=LogViewMode.HISTORY,
            last_fetch_changed=False,
            apply_updates=lambda: calls["model_apply"].append(True),
        ),
        _clock=lambda: FakeClock(mode=PlaybackMode.LIVE),
        _go_live=lambda: calls["go_live"].append(True),
        _poll_history_tail=lambda: None,
        _sync_force_live_visibility=lambda clock: None,
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["go_live"] == []
    assert calls["model_apply"] == [True]  # falls through to the ordinary frozen-tab path
    assert machine.state is FollowState.FROZEN  # untouched - still frozen


def test_apply_updates_follows_clock_when_replay_and_following():
    machine = PlaybackFollowMachine()
    calls = {"reanchor": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback=machine,
        _clock_snapshot=_clock_snapshot,
        _last_followed_ts_ns=None,
        prev_apply=0,
        model=SimpleNamespace(mode=LogViewMode.HISTORY, row_count=5),
        _clock=lambda: FakeClock(mode=PlaybackMode.REPLAY, is_playing=True, current_ts_ns=999),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
        _sync_force_live_visibility=lambda clock: None,
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["reanchor"] == [{"anchor_ts": 999}]
    assert machine.state is FollowState.FOLLOWING
    assert stub._last_followed_ts_ns == 999


def test_apply_updates_follow_skips_refetch_when_paused_and_ts_unchanged():
    machine = PlaybackFollowMachine()
    machine.state = FollowState.FOLLOWING
    calls = {"reanchor": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback=machine,
        _clock_snapshot=_clock_snapshot,
        _last_followed_ts_ns=999,
        prev_apply=0,
        model=SimpleNamespace(mode=LogViewMode.HISTORY, row_count=5),
        _clock=lambda: FakeClock(mode=PlaybackMode.REPLAY, is_playing=False, current_ts_ns=999),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
        _sync_force_live_visibility=lambda clock: None,
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["reanchor"] == []  # clock hasn't moved and isn't playing - nothing to redo

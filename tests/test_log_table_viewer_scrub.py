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
attributes/methods each branch actually touches."""

from types import SimpleNamespace

from blinkview.core.playback_clock import PlaybackMode
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


def _make_widget_stub(is_scrubbing, playback_anchored=True):
    calls = {"pause": [], "reanchor": [], "go_live": []}
    stub = SimpleNamespace(
        _programmatic_scroll=False,
        model=SimpleNamespace(mode=LogViewMode.HISTORY, anchor_seq=None, row_count=10),
        view=FakeView(FakeScrollbar(value=50)),
        _playback_anchored=playback_anchored,
        follow_playback=True,
        _clock=lambda: FakeClock(is_scrubbing=is_scrubbing),
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
        _go_live=lambda: calls["go_live"].append(True),
        _at_live_edge=lambda: False,
        _history_newest_ref_seq=lambda: None,
    )
    return stub, calls


def test_scroll_change_while_scrubbing_does_not_detach_or_pause():
    """A drag on the global transport scrubber can trigger a scrollbar valueChanged here that
    slips past the _programmatic_scroll guard's synchronous window (see the comment at the call
    site) - while is_scrubbing is True this must never be treated as a genuine manual scroll."""
    stub, calls = _make_widget_stub(is_scrubbing=True)

    LogTableViewerWidget._on_scroll_value_changed(stub, 10)

    assert stub.follow_playback is True
    assert stub._playback_anchored is True
    assert calls["pause"] == []
    assert calls["reanchor"] == []


def test_scroll_change_while_not_scrubbing_still_detaches_and_pauses():
    """The is_scrubbing guard must not swallow a genuine manual scroll once no drag is active."""
    stub, calls = _make_widget_stub(is_scrubbing=False)

    LogTableViewerWidget._on_scroll_value_changed(stub, 10)

    assert stub.follow_playback is False
    assert stub._playback_anchored is False
    assert calls["pause"] == [True]


def test_scroll_change_not_playback_anchored_falls_through_to_ordinary_logic():
    """When not currently clock-anchored, scrolling must fall through to the ordinary
    top/bottom-edge history paging logic instead of being swallowed by the playback branch."""
    stub, calls = _make_widget_stub(is_scrubbing=False, playback_anchored=False)

    LogTableViewerWidget._on_scroll_value_changed(stub, 50)

    # Neither the playback-detach path nor its pause call should have fired.
    assert calls["pause"] == []


def test_toggle_pause_checked_while_playback_anchored_freezes_without_go_live():
    calls = {"pause": [], "enter_history": []}
    stub = SimpleNamespace(
        _playback_anchored=True,
        follow_playback=True,
        model=SimpleNamespace(mode=LogViewMode.HISTORY),
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
        _enter_history_at_top_row=lambda auto=False: calls["enter_history"].append(auto),
        _clock=lambda: FakeClock(),
        _go_live=lambda: (_ for _ in ()).throw(AssertionError("should not go live")),
    )

    LogTableViewerWidget._toggle_pause(stub, True)

    assert stub.follow_playback is False
    assert calls["pause"] == [True]
    assert calls["enter_history"] == []  # must not anchor off the LIVE buffer while following


def test_toggle_pause_unchecked_while_clock_replay_rejoins_follow_without_go_live():
    calls = {"pause": [], "go_live": []}
    stub = SimpleNamespace(
        _playback_anchored=False,
        follow_playback=False,
        model=SimpleNamespace(mode=LogViewMode.HISTORY),
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
        _clock=lambda: FakeClock(mode=PlaybackMode.REPLAY),
        _go_live=lambda: calls["go_live"].append(True),
    )

    LogTableViewerWidget._toggle_pause(stub, False)

    assert stub.follow_playback is True
    assert calls["pause"] == [False]
    assert calls["go_live"] == []  # rejoin the clock, don't jump to the backend's live tail


def test_toggle_pause_unchecked_while_clock_live_goes_live():
    calls = {"go_live": []}
    stub = SimpleNamespace(
        _playback_anchored=False,
        follow_playback=False,
        model=SimpleNamespace(mode=LogViewMode.HISTORY),
        _set_pause_ui=lambda paused, auto=False: None,
        _clock=lambda: FakeClock(mode=PlaybackMode.LIVE),
        _go_live=lambda: calls["go_live"].append(True),
    )

    LogTableViewerWidget._toggle_pause(stub, False)

    assert calls["go_live"] == [True]


def test_apply_updates_replay_to_live_exit_resets_follow_state_and_goes_live():
    calls = {"go_live": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback_anchored=True,
        follow_playback=False,
        is_paused=False,
        _last_followed_ts_ns=123,
        prev_apply=0,
        _clock=lambda: FakeClock(mode=PlaybackMode.LIVE),
        _go_live=lambda: calls["go_live"].append(True),
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["go_live"] == [True]
    assert stub._playback_anchored is False
    assert stub.follow_playback is True
    assert stub._last_followed_ts_ns is None


def test_apply_updates_is_paused_blocks_replay_to_live_exit():
    """is_paused always wins: a manually-paused tab stays frozen through a clock mode change and
    only unfreezes on its own Resume click."""
    calls = {"go_live": [], "model_apply": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback_anchored=True,
        follow_playback=False,
        is_paused=True,
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
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["go_live"] == []
    assert calls["model_apply"] == [True]  # falls through to the ordinary frozen-tab path
    assert stub._playback_anchored is True  # untouched - still frozen


def test_apply_updates_follows_clock_when_replay_and_following():
    calls = {"reanchor": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback_anchored=False,
        follow_playback=True,
        is_paused=False,
        _last_followed_ts_ns=None,
        prev_apply=0,
        model=SimpleNamespace(mode=LogViewMode.HISTORY, row_count=5),
        _clock=lambda: FakeClock(mode=PlaybackMode.REPLAY, is_playing=True, current_ts_ns=999),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["reanchor"] == [{"anchor_ts": 999}]
    assert stub._playback_anchored is True
    assert stub._last_followed_ts_ns == 999


def test_apply_updates_follow_skips_refetch_when_paused_and_ts_unchanged():
    calls = {"reanchor": []}
    stub = SimpleNamespace(
        gui_context=SimpleNamespace(registry=SimpleNamespace(now_ns=lambda: 1_000_000_000)),
        _playback_anchored=True,
        follow_playback=True,
        is_paused=False,
        _last_followed_ts_ns=999,
        prev_apply=0,
        model=SimpleNamespace(mode=LogViewMode.HISTORY, row_count=5),
        _clock=lambda: FakeClock(mode=PlaybackMode.REPLAY, is_playing=False, current_ts_ns=999),
        _reanchor_history=lambda **kw: calls["reanchor"].append(kw),
    )

    LogTableViewerWidget.apply_updates(stub)

    assert calls["reanchor"] == []  # clock hasn't moved and isn't playing - nothing to redo

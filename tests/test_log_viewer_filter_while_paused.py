# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Regression tests for editing a filter while LogViewerWidget is paused: a filter that matches
nothing must blank the frozen window instead of leaving the previous filter's rows on screen.

Same convention as test_log_viewer_scrub.py: drives the unbound methods against a minimal
stand-in object rather than constructing a full widget."""

from types import SimpleNamespace

from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.ui.widgets.log_viewer import LogViewerWidget


class FakeTextArea:
    def __init__(self, text):
        self.text = text
        self.max_block_count = None
        self.scrolled_to = None

    def set_max_block_count(self, count):
        self.max_block_count = count

    def setPlainText(self, text):
        self.text = text

    def scroll_to_block(self, block):
        self.scrolled_to = block

    def document(self):
        return None


def _make_stub(view_mode, fetch_result, latest_seq=1000):
    calls = {"pause": [], "redraw": 0}
    stub = SimpleNamespace(
        _programmatic_scroll=False,
        view_mode=view_mode,
        history_anchor_seq=500,
        history_anchor_ts_ns=None,
        history_oldest_seq=400,
        history_newest_seq=600,
        history_reached_start=False,
        text_area=FakeTextArea("old rows\n"),
        highlighter=SimpleNamespace(setDocument=lambda doc: None),
        gui_context=SimpleNamespace(
            registry=SimpleNamespace(
                central=SimpleNamespace(log_pool=SimpleNamespace(latest_sequence=lambda: latest_seq))
            )
        ),
        _fetch_history_window=lambda **kw: fetch_result,
        _set_pause_ui=lambda paused, auto=False: calls["pause"].append(paused),
    )
    stub._redraw_history = lambda: calls.__setitem__("redraw", calls["redraw"] + 1)
    stub._reanchor_history = lambda **kw: LogViewerWidget._reanchor_history(stub, **kw)
    return stub, calls


# (before_count, after_count, oldest_seq, newest_seq, reached_start, text) for a seq anchor of 500
NO_MATCHES = (0, 0, 500, 500, True, "")


def test_refresh_while_paused_blanks_view_when_filter_matches_nothing():
    stub, calls = _make_stub(LogViewMode.HISTORY, NO_MATCHES)

    LogViewerWidget._refresh_view(stub)

    assert stub.text_area.text == ""
    assert stub.text_area.max_block_count == 1  # 0 would mean "unlimited" to Qt
    # Still paused on the same anchor, so loosening the filter brings the rows back.
    assert stub.view_mode == LogViewMode.HISTORY
    assert stub.history_anchor_seq == 500
    assert calls["redraw"] == 0
    assert calls["pause"] == []


def test_refresh_while_paused_shows_rows_matching_new_filter():
    stub, calls = _make_stub(LogViewMode.HISTORY, (1, 1, 480, 520, True, "a\nb\n"))

    LogViewerWidget._refresh_view(stub)

    assert stub.text_area.text == "a\nb\n"
    assert stub.text_area.scrolled_to == 1
    assert (stub.history_oldest_seq, stub.history_newest_seq) == (480, 520)
    assert calls["redraw"] == 0


def test_empty_fetch_without_allow_empty_leaves_view_untouched():
    """Paging/playback-follow re-anchors that find nothing keep whatever is on screen."""
    stub, _ = _make_stub(LogViewMode.HISTORY, NO_MATCHES)

    LogViewerWidget._reanchor_history(stub, anchor_seq=500)

    assert stub.text_area.text == "old rows\n"


def test_empty_fetch_never_enters_history_from_live():
    stub, calls = _make_stub(LogViewMode.LIVE, NO_MATCHES)

    LogViewerWidget._reanchor_history(stub, anchor_seq=500, allow_empty=True)

    assert stub.view_mode == LogViewMode.LIVE
    assert stub.text_area.text == "old rows\n"
    assert calls["pause"] == []

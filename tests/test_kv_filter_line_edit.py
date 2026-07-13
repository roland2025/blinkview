# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.ui.widgets.kv_filter_line_edit import KvFilterLineEdit


def test_typing_restarts_the_debounce_timer_instead_of_committing_immediately(qapp):
    widget = KvFilterLineEdit()
    received = []
    widget.filterTextCommitted.connect(received.append)

    widget.setText("status=ok")

    assert received == []  # nothing committed yet - only the debounce timer was (re)started
    assert widget._timer.isActive()


def test_commits_when_debounce_fires_on_a_ready_query(qapp):
    widget = KvFilterLineEdit()
    received = []
    widget.filterTextCommitted.connect(received.append)

    widget.setText("status=ok")
    widget._timer.stop()
    widget._maybe_commit()  # simulate the debounce timer firing

    assert received == ["status=ok"]


def test_does_not_commit_mid_pair(qapp):
    widget = KvFilterLineEdit()
    received = []
    widget.filterTextCommitted.connect(received.append)

    widget.setText("status=")
    widget._timer.stop()
    widget._maybe_commit()

    assert received == []


def test_commits_once_a_value_character_follows(qapp):
    widget = KvFilterLineEdit()
    received = []
    widget.filterTextCommitted.connect(received.append)

    widget.setText("status=")
    widget._timer.stop()
    widget._maybe_commit()
    assert received == []

    widget.setText("status=o")
    widget._timer.stop()
    widget._maybe_commit()
    assert received == ["status=o"]


def test_empty_text_commits_to_clear_the_filter(qapp):
    widget = KvFilterLineEdit()
    received = []
    widget.filterTextCommitted.connect(received.append)

    widget.setText("")
    widget._timer.stop()
    widget._maybe_commit()

    assert received == [""]

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.ui.widgets.kv_filter_line_edit import KvFilterLineEdit

# DEBOUNCE_MS (200) plus headroom, so a slow CI box doesn't turn a real-timer wait into a flake.
WAIT_MS = KvFilterLineEdit.DEBOUNCE_MS + 300


def test_typing_restarts_the_debounce_timer_instead_of_committing_immediately(qapp, qtbot):
    widget = KvFilterLineEdit()
    qtbot.addWidget(widget)

    with qtbot.assertNotEmitted(widget.filterTextCommitted):
        qtbot.keyClicks(widget, "status=ok")

    assert widget._timer.isActive()  # nothing committed yet - only the debounce timer was (re)started


def test_commits_when_debounce_fires_on_a_ready_query(qapp, qtbot):
    widget = KvFilterLineEdit()
    qtbot.addWidget(widget)

    with qtbot.waitSignal(widget.filterTextCommitted, timeout=WAIT_MS) as blocker:
        qtbot.keyClicks(widget, "status=ok")  # real keystrokes; the real 200ms QTimer fires this

    assert blocker.args == ["status=ok"]


def test_does_not_commit_mid_pair(qapp, qtbot):
    widget = KvFilterLineEdit()
    qtbot.addWidget(widget)

    with qtbot.assertNotEmitted(widget.filterTextCommitted, wait=WAIT_MS):
        qtbot.keyClicks(widget, "status=")


def test_commits_once_a_value_character_follows(qapp, qtbot):
    widget = KvFilterLineEdit()
    qtbot.addWidget(widget)

    with qtbot.assertNotEmitted(widget.filterTextCommitted, wait=WAIT_MS):
        qtbot.keyClicks(widget, "status=")

    with qtbot.waitSignal(widget.filterTextCommitted, timeout=WAIT_MS) as blocker:
        qtbot.keyClicks(widget, "o")

    assert blocker.args == ["status=o"]


def test_empty_text_commits_to_clear_the_filter(qapp, qtbot):
    widget = KvFilterLineEdit()
    qtbot.addWidget(widget)
    widget.setText("status=ok")

    with qtbot.waitSignal(widget.filterTextCommitted, timeout=WAIT_MS) as blocker:
        widget.clear()  # the real user action for "empty the box" via the clear button

    assert blocker.args == [""]

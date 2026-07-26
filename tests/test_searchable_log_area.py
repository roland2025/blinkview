# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.searchable_log_area import SearchableLogArea


@pytest.fixture
def area(qapp, qtbot):
    w = SearchableLogArea()
    qtbot.addWidget(w)
    w.resize(400, 300)
    w.show()  # isVisible() (find_bar visibility, etc.) requires the whole ancestor chain shown
    return w


class TestConstruction:
    def test_find_bar_hidden_by_default(self, area):
        assert area.find_bar.isVisible() is False

    def test_editor_is_read_only_and_no_wrap(self, area):
        from qtpy.QtWidgets import QPlainTextEdit

        assert area.editor.isReadOnly() is True
        assert area.editor.lineWrapMode() == QPlainTextEdit.NoWrap

    def test_max_block_count_applied(self, qapp, qtbot):
        w = SearchableLogArea(maxlen=42)
        qtbot.addWidget(w)
        assert w.editor.maximumBlockCount() == 42


class TestAppendLog:
    def test_appends_a_single_string(self, area):
        area.append_log("hello")
        assert area.editor.toPlainText() == "hello"

    def test_appends_multiple_calls_on_new_lines(self, area):
        area.append_log("first")
        area.append_log("second")
        assert area.editor.toPlainText() == "first\nsecond"

    def test_appends_a_list_joined_by_newlines(self, area):
        area.append_log(["a", "b", "c"])
        assert area.editor.toPlainText() == "a\nb\nc"

    def test_empty_list_is_a_no_op(self, area):
        area.append_log([])
        assert area.editor.toPlainText() == ""

    def test_autoscrolls_when_already_at_bottom(self, area):
        area.append_log("\n".join(f"line{i}" for i in range(200)))
        scrollbar = area.editor.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

        area.append_log("new line")

        assert scrollbar.value() == scrollbar.maximum()

    def test_does_not_scroll_when_user_scrolled_away(self, area):
        area.append_log("\n".join(f"line{i}" for i in range(200)))
        scrollbar = area.editor.verticalScrollBar()
        scrollbar.setValue(0)

        area.append_log("new line")

        assert scrollbar.value() == 0


class TestClear:
    def test_clear_empties_text_and_selections(self, area):
        area.append_log("hello")
        area._find_text = "hello"
        area.refresh_highlights()

        area.clear()

        assert area.editor.toPlainText() == ""
        assert area.editor.extraSelections() == []


class TestDelegatedAccessors:
    def test_set_font_applies_to_editor(self, area):
        from qtpy.QtGui import QFont

        font = QFont("Arial", 20)
        area.set_font(font)
        assert area.editor.font().pointSize() == 20

    def test_document_returns_editor_document(self, area):
        assert area.document() is area.editor.document()

    def test_vertical_scroll_bar_returns_editor_scrollbar(self, area):
        assert area.verticalScrollBar() is area.editor.verticalScrollBar()

    def test_set_plain_text(self, area):
        area.setPlainText("replaced")
        assert area.editor.toPlainText() == "replaced"

    def test_set_max_block_count(self, area):
        area.set_max_block_count(7)
        assert area.editor.maximumBlockCount() == 7


class TestScrollHelpers:
    def test_scroll_to_end(self, area):
        area.append_log("\n".join(f"line{i}" for i in range(200)))
        area.editor.verticalScrollBar().setValue(0)

        area.scroll_to_end()

        scrollbar = area.editor.verticalScrollBar()
        assert scrollbar.value() == scrollbar.maximum()

    def test_scroll_to_block(self, area):
        area.append_log("\n".join(f"line{i}" for i in range(200)))
        area.scroll_to_block(10)
        assert area.editor.verticalScrollBar().value() == 10

    def test_first_visible_block_starts_at_zero(self, area):
        area.append_log("\n".join(f"line{i}" for i in range(50)))
        area.scroll_to_block(0)  # append_log auto-scrolled to the tail; scroll back to the top

        assert area.first_visible_block() == 0

    def test_visible_row_count_is_nonnegative(self, area):
        assert area.visible_row_count() >= 0


class TestFindBar:
    def test_show_find_bar_makes_it_visible(self, area):
        area.show_find_bar()
        assert area.find_bar.isVisible() is True

    def test_show_find_bar_prefills_from_selection(self, area):
        area.append_log("hello world")
        cursor = area.editor.textCursor()
        cursor.select(cursor.SelectionType.WordUnderCursor)
        area.editor.setTextCursor(cursor)

        area.show_find_bar()

        assert area.search_input.text() != ""

    def test_show_find_bar_ignores_multiline_selection(self, area):
        area.append_log("line1\nline2")
        cursor = area.editor.textCursor()
        cursor.movePosition(cursor.MoveOperation.Start)
        cursor.movePosition(cursor.MoveOperation.End, cursor.MoveMode.KeepAnchor)
        area.editor.setTextCursor(cursor)
        area.search_input.setText("")

        area.show_find_bar()

        assert area.search_input.text() == ""  # multi-line selection (contains U+2029) skipped

    def test_hide_find_bar_hides_it(self, area):
        area.show_find_bar()
        area.hide_find_bar()
        assert area.find_bar.isVisible() is False


class TestFindNextPrev:
    def test_find_next_no_op_without_search_text(self, area):
        area.append_log("hello world")
        area.find_next()  # must not raise

    def test_find_next_moves_cursor_to_match(self, area):
        area.append_log("hello world hello")
        area.search_input.setText("world")

        area.find_next()

        assert area.editor.textCursor().selectedText() == "world"

    def test_find_next_wraps_around(self, area):
        area.append_log("needle at start, then nothing else")
        area.search_input.setText("needle")
        area.find_next()  # lands on the only match
        area.find_next()  # must wrap back around to it again

        assert area.editor.textCursor().selectedText() == "needle"

    def test_find_prev_moves_backward(self, area):
        area.append_log("alpha beta alpha")
        area.search_input.setText("alpha")

        cursor = area.editor.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        area.editor.setTextCursor(cursor)

        area.find_prev()

        assert area.editor.textCursor().selectedText() == "alpha"


class TestSelectionAndSearchHandlers:
    def test_short_selection_does_not_set_manual_text(self, area):
        area.append_log("ab cd")
        cursor = area.editor.textCursor()
        cursor.movePosition(cursor.MoveOperation.Start)
        cursor.movePosition(cursor.MoveOperation.Right, cursor.MoveMode.KeepAnchor, 2)
        area.editor.setTextCursor(cursor)

        assert area._manual_text == ""

    def test_search_input_updates_find_text(self, area):
        area.search_input.setText("needle")
        assert area._find_text == "needle"


class TestJumpToFirstMatch:
    def test_no_op_without_search_text(self, area):
        area.append_log("hello")
        area.jump_to_first_match()  # must not raise

    def test_moves_cursor_to_first_match(self, area):
        area.append_log("prefix needle suffix")
        area.search_input.setText("needle")

        area.jump_to_first_match()

        assert area.editor.textCursor().selectedText() == "needle"

    def test_no_match_leaves_cursor_unchanged(self, area):
        area.append_log("nothing matches here")
        area.search_input.setText("zzz_not_found")

        area.jump_to_first_match()  # must not raise, silently does nothing


class TestRefreshHighlights:
    def test_no_search_text_clears_selections(self, area):
        area.append_log("hello")
        area.search_input.setText("hello")
        area.search_input.setText("")

        assert area.editor.extraSelections() == []

    def test_find_bar_visible_highlights_matches(self, area):
        area.append_log("aa bb aa")
        area.show_find_bar()
        area.search_input.setText("aa")

        assert len(area.editor.extraSelections()) >= 1

    def test_find_bar_hidden_does_not_highlight_find_matches(self, area):
        area.append_log("aa bb aa")
        area._find_text = "aa"
        area.find_bar.setVisible(False)

        area.refresh_highlights()

        assert area.editor.extraSelections() == []


class TestEventFilter:
    def test_enter_in_search_box_triggers_find_next(self, qapp, qtbot, area):
        from qtpy.QtCore import QEvent, Qt
        from qtpy.QtGui import QKeyEvent

        area.append_log("needle here")
        area.search_input.setText("needle")

        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Return, Qt.KeyboardModifier.NoModifier)
        handled = area.eventFilter(area.search_input, event)

        assert handled is True
        assert area.editor.textCursor().selectedText() == "needle"

    def test_shift_enter_in_search_box_triggers_find_prev(self, qapp, qtbot, area):
        from qtpy.QtCore import QEvent, Qt
        from qtpy.QtGui import QKeyEvent

        area.append_log("alpha beta alpha")
        area.search_input.setText("alpha")
        cursor = area.editor.textCursor()
        cursor.movePosition(cursor.MoveOperation.End)
        area.editor.setTextCursor(cursor)

        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_Return, Qt.KeyboardModifier.ShiftModifier)
        handled = area.eventFilter(area.search_input, event)

        assert handled is True
        assert area.editor.textCursor().selectedText() == "alpha"

    def test_non_keypress_event_falls_through_to_super(self, area):
        from qtpy.QtCore import QEvent

        event = QEvent(QEvent.Type.FocusIn)
        # Must not raise, and must not be swallowed as a navigation shortcut.
        area.eventFilter(area.search_input, event)

    def test_n_key_in_editor_navigates_when_unfocused_search(self, qapp, qtbot, area):
        from qtpy.QtCore import QEvent, Qt
        from qtpy.QtGui import QKeyEvent

        area.append_log("needle here needle")
        area._find_text = "needle"
        area.editor.setFocus()

        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_N, Qt.KeyboardModifier.NoModifier)
        handled = area.eventFilter(area.editor, event)

        assert handled is True

    def test_n_key_without_find_text_is_not_swallowed(self, qapp, qtbot, area):
        from qtpy.QtCore import QEvent, Qt
        from qtpy.QtGui import QKeyEvent

        area._find_text = ""
        event = QKeyEvent(QEvent.Type.KeyPress, Qt.Key.Key_N, Qt.KeyboardModifier.NoModifier)
        handled = area.eventFilter(area.editor, event)

        assert handled is False

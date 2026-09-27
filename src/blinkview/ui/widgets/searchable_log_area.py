# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import QEvent, QPoint, Qt, QTimer, Signal
from qtpy.QtGui import QColor, QFont, QKeySequence, QShortcut, QTextCharFormat, QTextCursor, QTextDocument
from qtpy.QtWidgets import QHBoxLayout, QLabel, QLineEdit, QPlainTextEdit, QTextEdit, QToolButton, QVBoxLayout, QWidget


class SearchableLogArea(QWidget):
    # Amber (#886622) - Used for the Find Bar (matches manual pause feel)
    COLOR_FIND_BAR = QColor(255, 190, 0, 140)  # Vibrant Amber (Translucent)
    COLOR_SELECTION = QColor(255, 80, 80, 140)  # Soft Coral/Red (Translucent)
    COLOR_CURRENT = QColor(40, 150, 40, 255)  # Solid Green for the active match

    # Fires on the edge from "no selection" to "has a selection" (mouse drag, Shift+arrows,
    # Ctrl+A, ...) - lets the owning viewer freeze its live tail so the selection doesn't
    # scroll/trim away underneath the user.
    selection_started = Signal()

    def __init__(self, parent=None, maxlen=10000):
        super().__init__(parent)

        # --- UI Layout ---
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        # The actual text editor
        self.editor = QPlainTextEdit()
        self.editor.setReadOnly(True)
        self.editor.setUndoRedoEnabled(False)
        self.editor.setLineWrapMode(QPlainTextEdit.NoWrap)

        self.editor.setFont(QFont("Consolas", 10))
        self.editor.setMaximumBlockCount(maxlen)
        self.editor.document().setDocumentMargin(0)

        # Pins the view to the newest line one event-loop turn after an append (see append_log). Restarting
        # a single-shot timer coalesces several appends within one turn into one scroll.
        self._pin_timer = QTimer(self)
        self._pin_timer.setSingleShot(True)
        self._pin_timer.setInterval(0)
        self._pin_timer.timeout.connect(self.scroll_to_end)
        # True while scroll_to_end() moves the scrollbar - see is_programmatic_scroll.
        self._pinning = False

        # The Find Bar (Hidden by default)
        self.find_bar = QWidget()
        find_layout = QHBoxLayout(self.find_bar)
        find_layout.setContentsMargins(5, 2, 5, 2)

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Find...")
        self.search_input.setMaximumWidth(300)

        self.btn_prev = QToolButton()
        self.btn_prev.setText("↑")
        self.btn_prev.clicked.connect(self.find_prev)

        self.btn_next = QToolButton()
        self.btn_next.setText("↓")
        self.btn_next.clicked.connect(self.find_next)

        close_btn = QToolButton()
        close_btn.setText("✕")
        close_btn.clicked.connect(self.hide_find_bar)

        find_layout.addWidget(QLabel("Find:"))
        find_layout.addWidget(self.search_input)
        find_layout.addWidget(self.btn_prev)
        find_layout.addWidget(self.btn_next)
        find_layout.addWidget(close_btn)

        find_layout.addStretch(1)

        self.find_bar.setVisible(False)

        # Assemble
        self.layout.addWidget(self.find_bar)
        self.layout.addWidget(self.editor)

        # --- Logic State ---
        # Internal state for both types
        self._find_text = ""
        self._manual_text = ""
        self._had_selection = False

        # Performance cache for formats
        self._fmt_find = QTextCharFormat()
        self._fmt_find.setBackground(self.COLOR_FIND_BAR)
        self._fmt_manual = QTextCharFormat()
        self._fmt_manual.setBackground(self.COLOR_SELECTION)
        self._fmt_current = QTextCharFormat()
        self._fmt_current.setBackground(self.COLOR_CURRENT)
        self._fmt_current.setForeground(Qt.white)

        # --- Connections ---
        self.editor.selectionChanged.connect(self._handle_selection_changed)
        self.search_input.textChanged.connect(self._handle_search_input)
        self.editor.verticalScrollBar().valueChanged.connect(self.refresh_highlights)

        # Standard Keyboard Shortcuts
        # Ctrl+F: Open/Focus Search
        QShortcut(QKeySequence("Ctrl+F"), self, self.show_find_bar)

        # Esc: Hide Search
        QShortcut(QKeySequence("Esc"), self, self.hide_find_bar)

        # F3: Find Next
        QShortcut(QKeySequence("F3"), self, self.find_next)
        # Shift+F3: Find Previous
        QShortcut(QKeySequence(Qt.SHIFT | Qt.Key_F3), self, self.find_prev)

        # Allow Enter/Shift+Enter specifically when the search box has focus
        self.search_input.installEventFilter(self)
        self.editor.installEventFilter(self)

    def eventFilter(self, obj, event):
        if event.type() != QEvent.KeyPress:
            return super().eventFilter(obj, event)

        # Logic for the Search Box (Input)
        if obj is self.search_input:
            if event.key() in (Qt.Key_Return, Qt.Key_Enter):
                if event.modifiers() & Qt.ShiftModifier:
                    self.find_prev()
                else:
                    self.find_next()
                return True

        # Logic for the Log Area (Navigation)
        elif obj is self.editor:
            # We only navigate with n/N if the search box isn't being typed in
            # and there is actually something to find.
            if not self.search_input.hasFocus() and self._find_text:
                if event.key() == Qt.Key_N:
                    if event.modifiers() & Qt.ShiftModifier:
                        self.find_prev()  # 'N' (Shift+n)
                    else:
                        self.find_next()  # 'n'
                    return True  # Swallow the event so 'n' isn't typed/processed

        return super().eventFilter(obj, event)

    # --- Public API ---
    def append_log(self, data):
        """
        Appends text to the editor.
        'data' can be a single string or a list of strings for batch processing.
        """
        # Convert list to a single string joined by newlines if necessary
        if isinstance(data, list):
            if not data:
                return
            text_to_append = "\n".join(data)
        else:
            text_to_append = data

        scrollbar = self.editor.verticalScrollBar()
        # Using a slightly larger buffer (40) is safer for high-DPI screens
        was_at_bottom = scrollbar.value() >= (scrollbar.maximum() - 5)

        self.editor.setUpdatesEnabled(False)
        self.editor.blockSignals(True)
        scrollbar.blockSignals(True)

        try:
            cursor = self.editor.textCursor()
            cursor.movePosition(QTextCursor.End)

            # If document isn't empty, ensure we start on a new line
            if not self.editor.document().isEmpty() and not cursor.atBlockStart():
                cursor.insertBlock()

            cursor.insertText(text_to_append)
        finally:
            scrollbar.blockSignals(False)
            self.editor.setUpdatesEnabled(True)
            self.editor.blockSignals(False)

        if was_at_bottom:
            # Not scrollbar.setValue(scrollbar.maximum()) here: with setMaximumBlockCount the insert above may
            # have trimmed blocks from the top, the scrollbar's range was updated while its signals were
            # blocked, and setting a value from that stale range makes QPlainTextEdit look up a block that no
            # longer exists (null QTextLayout in blockBoundingRect - a native crash, not an exception). Scroll
            # once the event loop has let the editor and its scrollbar settle.
            self._pin_timer.start()

    def prepend_lines(self, text: str) -> int:
        """Inserts `text` as whole lines above the current first line and returns how many lines
        were added. Existing QTextCursors - including the user's selection, even mid-drag - shift
        with the insert, and the scrollbar is moved by the same amount so the viewport stays on
        the same content. The caller must raise the max block count first, or Qt trims the top
        right back off."""
        text = text.rstrip("\n")
        if not text:
            return 0
        doc = self.editor.document()
        scrollbar = self.editor.verticalScrollBar()
        old_blocks = doc.blockCount()
        old_value = scrollbar.value()

        cursor = QTextCursor(doc)
        cursor.movePosition(QTextCursor.Start)
        cursor.insertText(text + "\n")

        added = doc.blockCount() - old_blocks
        scrollbar.setValue(old_value + added)
        return added

    def append_lines(self, text: str):
        """Appends `text` as whole lines after the last line, keeping the document's trailing
        empty block. Unlike append_log this never pins the view to the bottom, and it keeps the
        user's selection exactly where it was - a selection ending at the document end would
        otherwise grow to swallow the inserted text, since cursors at the insert point move with
        it."""
        text = text.rstrip("\n")
        if not text:
            return
        doc = self.editor.document()
        scrollbar = self.editor.verticalScrollBar()
        scroll_value = scrollbar.value()
        selection = self.editor.textCursor()
        anchor, position = selection.anchor(), selection.position()

        cursor = QTextCursor(doc)
        cursor.movePosition(QTextCursor.End)
        if not cursor.atBlockStart():
            cursor.insertBlock()
        cursor.insertText(text + "\n")

        selection = self.editor.textCursor()
        if (selection.anchor(), selection.position()) != (anchor, position):
            selection.setPosition(anchor)
            selection.setPosition(position, QTextCursor.KeepAnchor)
            self.editor.setTextCursor(selection)
        scrollbar.setValue(scroll_value)

    def clear(self):
        self._pin_timer.stop()  # a pending pin belongs to the content being thrown away
        self.editor.clear()
        self.editor.setExtraSelections([])

    def line_count(self) -> int:
        """Number of real lines - the block count minus the empty block a trailing newline
        leaves at the end."""
        doc = self.editor.document()
        count = doc.blockCount()
        if count and not doc.lastBlock().text():
            count -= 1
        return count

    def has_selection(self) -> bool:
        return self.editor.textCursor().hasSelection()

    def set_font(self, font):
        self.editor.setFont(font)

    def document(self):
        return self.editor.document()

    def verticalScrollBar(self):
        return self.editor.verticalScrollBar()

    def setPlainText(self, text):
        self._pin_timer.stop()  # a pending pin belongs to the content being replaced
        self.editor.setPlainText(text)

    def set_max_block_count(self, maxlen):
        """Adjusts how many blocks the editor retains before trimming from the top - used to
        switch between live mode's small tail window and history mode's larger static window."""
        self.editor.setMaximumBlockCount(maxlen)

    def scroll_to_end(self):
        """Scrolls to the last line. Re-widening/heightening the viewport (e.g. a splitter or
        window resize) changes how many lines fit without moving the scrollbar's value, so a
        resize while pinned to the tail can otherwise leave the view sitting above the new
        bottom."""
        scrollbar = self.editor.verticalScrollBar()
        self._pinning = True
        try:
            scrollbar.setValue(scrollbar.maximum())
        finally:
            self._pinning = False

    @property
    def is_programmatic_scroll(self) -> bool:
        """True while scroll_to_end() is moving the scrollbar. valueChanged listeners must ignore
        the scroll then: the value they see can be off the bottom (e.g. long lines toggling the
        horizontal scrollbar shrink the viewport mid-setValue), and treating it as a user scroll
        rebuilds the document (setPlainText) re-entrantly inside QScrollBar.setValue - Qt then
        continues on the discarded layout and crashes natively (access violation)."""
        return self._pinning

    def scroll_to_block(self, block_number):
        """Scrolls so `block_number` becomes the first visible line. Valid because the editor
        uses NoWrap line wrapping, so each block is exactly one visual line and the vertical
        scrollbar's value directly addresses block indices."""
        self.editor.verticalScrollBar().setValue(block_number)

    def first_visible_block(self):
        """Index of the topmost currently-visible block/line."""
        return self.editor.firstVisibleBlock().blockNumber()

    def visible_row_count(self):
        """How many lines actually fit in the current viewport height - each block is exactly
        one visual line (NoWrap), so this is viewport height / line height. Used to size
        live/follow fetch budgets to what the screen can show rather than a fixed guess."""
        line_height = self.editor.fontMetrics().lineSpacing()
        if line_height <= 0:
            return 0
        return self.editor.viewport().height() // line_height

    # --- Logic ---
    def show_find_bar(self):
        """Opens find bar and pre-fills with current selection."""
        # Grab text from cursor
        cursor = self.editor.textCursor()
        if cursor.hasSelection():
            text = cursor.selectedText()
            # \u2029 is the paragraph separator in Qt, avoid multi-line search strings
            if text and "\u2029" not in text:
                self.search_input.setText(text)

        self.find_bar.setVisible(True)
        self.search_input.setFocus()
        self.search_input.selectAll()
        self.refresh_highlights()

    def find_next(self):
        """Moves to the next occurrence of search text."""
        if not self._find_text:
            return
        # Search forward from current cursor
        found = self.editor.find(self._find_text)
        if not found:
            # Wrap around to the start
            cursor = self.editor.textCursor()
            cursor.movePosition(QTextCursor.Start)
            self.editor.setTextCursor(cursor)
            self.editor.find(self._find_text)

        if found:
            self.editor.centerCursor()

        self.refresh_highlights()

    def find_prev(self):
        """Moves to the previous occurrence of search text."""
        if not self._find_text:
            return
        # Search backward
        found = self.editor.find(self._find_text, QTextDocument.FindBackward)
        if not found:
            # Wrap around to the end
            cursor = self.editor.textCursor()
            cursor.movePosition(QTextCursor.End)
            self.editor.setTextCursor(cursor)
            self.editor.find(self._find_text, QTextDocument.FindBackward)

        if found:
            self.editor.centerCursor()

        self.refresh_highlights()

    def hide_find_bar(self):
        self.find_bar.setVisible(False)
        self.editor.setFocus()

    def _handle_selection_changed(self):
        """Updates the 'Manual Selection' text."""
        cursor = self.editor.textCursor()
        has_selection = cursor.hasSelection()
        if has_selection and not self._had_selection:
            self._had_selection = True
            self.selection_started.emit()
        elif not has_selection:
            self._had_selection = False

        sel_text = cursor.selectedText()
        # If user clears selection, reset manual text
        if len(sel_text) > 2 and not sel_text.isspace():
            self._manual_text = sel_text
        else:
            self._manual_text = ""

        self.refresh_highlights()

    def _handle_search_input(self, text):
        """Updates the 'Global Search' text."""
        self._find_text = text
        self.refresh_highlights()

    def jump_to_first_match(self):
        """
        Searches the entire document and scrolls to the first occurrence
        of the current search text.
        """
        if not self._find_text:
            return

        doc = self.editor.document()
        # Search the whole document from the beginning
        cursor = doc.find(self._find_text)

        if not cursor.isNull():
            # Move the editor's cursor to this match and scroll it into view
            self.editor.setTextCursor(cursor)
            self.editor.ensureCursorVisible()
            # Trigger a refresh so the highlights appear immediately
            self.refresh_highlights()

    def refresh_highlights(self):
        """Visually renders three layers: Global Find, Manual Selection, and Current Match."""
        if not self._find_text and not self._manual_text:
            # If nothing is being searched, ensure selections are clear and do zero math
            if self.editor.extraSelections():
                self.editor.setExtraSelections([])
            return

        if not self.editor.viewport():
            return
        doc = self.editor.document()
        combined_selections = []

        # Viewport boundaries for visible-only optimization
        start_pos = self.editor.cursorForPosition(QPoint(0, 0)).position()
        view_rect = self.editor.viewport().rect()
        end_pos = self.editor.cursorForPosition(view_rect.bottomRight()).position()
        if end_pos <= start_pos:
            end_pos = doc.characterCount()

        # Get current cursor to highlight the "active" match differently
        current_cursor = self.editor.textCursor()

        def find_visible_matches(text, fmt, limit=500):
            if not text or text.isspace():
                return
            cursor = doc.find(text, start_pos)
            count = 0
            while not cursor.isNull() and cursor.position() <= end_pos and count < limit:
                # Determine which format to use
                is_active = (
                    cursor.selectionStart() == current_cursor.selectionStart()
                    and cursor.selectionEnd() == current_cursor.selectionEnd()
                )

                sel = QTextEdit.ExtraSelection()
                sel.format = self._fmt_current if is_active else fmt
                sel.cursor = cursor
                combined_selections.append(sel)
                cursor = doc.find(text, cursor)
                count += 1

        if self.find_bar.isVisible():
            find_visible_matches(self._find_text, self._fmt_find)

        if self._manual_text and self._manual_text != self._find_text:
            find_visible_matches(self._manual_text, self._fmt_manual)

        self.editor.setExtraSelections(combined_selections)

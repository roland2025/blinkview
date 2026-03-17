# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtWidgets import (
    QPlainTextEdit, QTextEdit, QWidget, QVBoxLayout,
    QLineEdit, QHBoxLayout, QLabel, QToolButton
)
from PySide6.QtGui import QTextCharFormat, QColor, QTextDocument, QKeySequence, QShortcut, QFont, QTextCursor
from PySide6.QtCore import Qt, QTimer, Slot, QPoint, QEvent


class SearchableLogArea(QWidget):
    # Amber (#886622) - Used for the Find Bar (matches manual pause feel)
    COLOR_FIND_BAR = QColor(255, 190, 0, 140)  # Vibrant Amber (Translucent)
    COLOR_SELECTION = QColor(255, 80, 80, 140)  # Soft Coral/Red (Translucent)
    COLOR_CURRENT = QColor(40, 150, 40, 255)  # Solid Green for the active match

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
            if not data: return
            text_to_append = "\n".join(data)
        else:
            text_to_append = data

        scrollbar = self.editor.verticalScrollBar()
        # Using a slightly larger buffer (40) is safer for high-DPI screens
        was_at_bottom = scrollbar.value() >= (scrollbar.maximum() - 5)

        self.editor.setUpdatesEnabled(False)
        self.editor.blockSignals(True)

        try:
            cursor = self.editor.textCursor()
            cursor.movePosition(QTextCursor.End)

            # If document isn't empty, ensure we start on a new line
            if not self.editor.document().isEmpty():
                cursor.insertBlock()

            cursor.insertText(text_to_append)
        finally:
            self.editor.setUpdatesEnabled(True)
            self.editor.blockSignals(False)

        if was_at_bottom:
            scrollbar.setValue(scrollbar.maximum())

    def clear(self):
        self.editor.clear()
        self.editor.setExtraSelections([])

    def set_font(self, font):
        self.editor.setFont(font)

    def document(self):
        return self.editor.document()

    def verticalScrollBar(self):
        return self.editor.verticalScrollBar()

    def setPlainText(self, text):
        self.editor.setPlainText(text)

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
        if not self._find_text: return
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
        if not self._find_text: return
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
        sel_text = self.editor.textCursor().selectedText()
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
        if not self.editor.viewport(): return
        doc = self.editor.document()
        combined_selections = []

        # Viewport boundaries for visible-only optimization
        start_pos = self.editor.cursorForPosition(QPoint(0, 0)).position()
        view_rect = self.editor.viewport().rect()
        end_pos = self.editor.cursorForPosition(view_rect.bottomRight()).position()
        if end_pos <= start_pos: end_pos = doc.characterCount()

        # Get current cursor to highlight the "active" match differently
        current_cursor = self.editor.textCursor()

        def find_visible_matches(text, fmt, limit=500):
            if not text or text.isspace(): return
            cursor = doc.find(text, start_pos)
            count = 0
            while not cursor.isNull() and cursor.position() <= end_pos and count < limit:
                # Determine which format to use
                is_active = (cursor.selectionStart() == current_cursor.selectionStart() and
                             cursor.selectionEnd() == current_cursor.selectionEnd())

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

    def scroll_to_end(self):
        """Forces the editor to scroll to the very bottom."""
        scrollbar = self.editor.verticalScrollBar()
        if scrollbar.value() != scrollbar.maximum():
            scrollbar.setValue(scrollbar.maximum())


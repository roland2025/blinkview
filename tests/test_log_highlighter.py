# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""LogHighlighter formats a block by setting on the QTextLayout's format ranges - this is a
render-layer overlay, not part of the document's own character-fragment formatting, so
inspecting it requires reading block.layout().formats() (not fragment().charFormat()) after an
explicit highlighter.rehighlight() call (setPlainText's automatic rehighlight is scheduled
asynchronously and doesn't run without pumping the event loop)."""

import pytest

from blinkview.ui.widgets.log_highlighter import LogHighlighter
from blinkview.utils.log_level import LogLevel


def _formats(document, block=0):
    return document.findBlockByNumber(block).layout().formats()


@pytest.fixture
def document(qapp):
    from qtpy.QtGui import QTextDocument

    return QTextDocument()


@pytest.fixture
def highlighter(document):
    hl = LogHighlighter(document)
    hl.set_index(2)  # level is the 3rd space-delimited token, e.g. "17:28:35.459 ABC E asi: ..."
    return hl


def _highlight(document, highlighter, text):
    document.setPlainText(text)
    highlighter.rehighlight()
    return _formats(document)


class TestConstruction:
    def test_builds_a_format_for_every_configured_level(self, highlighter):
        for level in LogLevel.LIST_CONF:
            assert level.name in highlighter.formats

    def test_warn_and_above_are_bold_below_are_not(self, highlighter):
        for level in LogLevel.LIST_CONF:
            fmt = highlighter.formats[level.name]
            is_bold = fmt.fontWeight() > 400  # QFont.Normal == 400, QFont.Bold == 700
            assert is_bold == (level >= LogLevel.WARN)

    def test_format_uses_the_level_color(self, highlighter):
        from qtpy.QtGui import QColor

        assert highlighter.formats["E"].foreground().color() == QColor(LogLevel.ERROR.color)


class TestHighlightBlock:
    def test_colors_the_whole_line_by_its_level_token(self, document, highlighter):
        text = "17:28:35.459 ABC E asi: something went wrong"
        formats = _highlight(document, highlighter, text)

        assert len(formats) == 1
        f = formats[0]
        assert f.start == 0
        assert f.length == len(text)
        assert f.format.foreground().color().name().lower() == LogLevel.ERROR.color.lower()
        assert f.format.fontWeight() > 400  # ERROR is bold

    def test_different_level_token_uses_a_different_format(self, document, highlighter):
        formats = _highlight(document, highlighter, "17:28:35.459 ABC I asi: informational")

        assert len(formats) == 1
        assert formats[0].format.foreground().color().name().lower() == LogLevel.INFO.color.lower()
        assert formats[0].format.fontWeight() == 400  # INFO is not bold

    def test_unknown_level_token_leaves_line_unformatted(self, document, highlighter):
        """The bare except around the KeyError lookup means a token that isn't a recognized
        level name (log line format changed, or genuinely not a log line) is silently skipped
        rather than raising - this locks in that silent-skip behavior."""
        formats = _highlight(document, highlighter, "17:28:35.459 ABC ZZZ asi: unknown token")
        assert formats == []

    def test_negative_level_index_leaves_line_unformatted(self, document, highlighter):
        highlighter.set_index(-1)
        formats = _highlight(document, highlighter, "17:28:35.459 ABC E asi: something")
        assert formats == []

    def test_line_with_fewer_words_than_level_index_leaves_line_unformatted(self, document, highlighter):
        """level_index=2 expects at least 3 space-delimited tokens; a shorter line must not
        raise (the `start == 0` / "space not found" guard)."""
        formats = _highlight(document, highlighter, "only two")
        assert formats == []

    def test_set_index_changes_which_token_is_treated_as_the_level(self, document, highlighter):
        highlighter.set_index(0)  # level is now the very first token
        formats = _highlight(document, highlighter, "E asi: something went wrong")

        assert len(formats) == 1
        assert formats[0].format.foreground().color().name().lower() == LogLevel.ERROR.color.lower()

    def test_level_token_at_end_of_line_with_no_trailing_space(self, document, highlighter):
        """text.find(' ', start) returning -1 (no trailing space after the level token) must
        fall back to len(text) instead of slicing incorrectly."""
        formats = _highlight(document, highlighter, "17:28:35.459 ABC E")
        assert len(formats) == 1
        assert formats[0].format.foreground().color().name().lower() == LogLevel.ERROR.color.lower()

    def test_empty_line_leaves_unformatted(self, document, highlighter):
        formats = _highlight(document, highlighter, "")
        assert formats == []

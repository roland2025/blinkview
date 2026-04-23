# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtGui import QColor, QFont, QSyntaxHighlighter, QTextCharFormat

from blinkview.utils.level_map import LevelMap
from blinkview.utils.log_level import LogLevel


class LogHighlighter(QSyntaxHighlighter):
    def __init__(self, parent):
        super().__init__(parent)

        # Define formats for each level
        # self.formats = {
        #     'I': self._create_format("#808080"),  # Gray for Info
        #     'W': self._create_format("#FFCC00", bold=True),  # Amber for Warning
        #     'E': self._create_format("#FF3333", bold=True),  # Red for Error
        # }
        self.formats = {}

        self.level_index = 0

        for level in LogLevel.LIST_CONF:
            self.formats[level.name] = self._create_format(level.color, bold=level >= LogLevel.WARN)

    def _create_format(self, color_hex, bold=False):
        fmt = QTextCharFormat()
        fmt.setForeground(QColor(color_hex))
        if bold:
            fmt.setFontWeight(QFont.Bold)
        return fmt

    def set_index(self, idx):
        self.level_index = idx

    def highlightBlock(self, text):
        """Called automatically by Qt when a line needs rendering."""

        try:
            idx = self.level_index
            if idx < 0:
                return
            # Assuming the level is the 3rd 'word' in your string:
            # "17:28:35.459 ABC E asi: ..."
            start = 0
            for _ in range(self.level_index):
                start = text.find(" ", start) + 1
                if start == 0:  # Space not found
                    return

            # Find the end of the level token
            end = text.find(" ", start)
            if end == -1:
                end = len(text)

            level_token = text[start:end]
            # if len(parts) > idx:
            fmt = self.formats[level_token]
            self.setFormat(0, len(text), fmt)
        except (KeyError, IndexError):
            # If the expected level part is missing or not recognized, we can skip formatting
            pass

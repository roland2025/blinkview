# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtGui import QColor, QFont, QSyntaxHighlighter, QTextCharFormat

from blinkview.utils.level_map import LevelMap
from blinkview.utils.log_level import LogLevel


class LogHighlighter(QSyntaxHighlighter):
    def __init__(self, parent, level_map: LevelMap):
        super().__init__(parent)

        self.level_map: LevelMap = level_map

        # Define formats for each level
        # self.formats = {
        #     'I': self._create_format("#808080"),  # Gray for Info
        #     'W': self._create_format("#FFCC00", bold=True),  # Amber for Warning
        #     'E': self._create_format("#FF3333", bold=True),  # Red for Error
        # }
        self.formats = {}

        self.level_index = 0

        for level in level_map.levels():
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
            # Assuming the level is the 3rd 'word' in your string:
            # "17:28:35.459 ABC E asi: ..."
            parts = text.split(maxsplit=idx + 1)  # Split into at most idx+1 parts to avoid unnecessary splitting
            # if len(parts) > idx:
            fmt = self.formats[parts[idx]]
            self.setFormat(0, len(text), fmt)
        except (KeyError, IndexError):
            # If the expected level part is missing or not recognized, we can skip formatting
            pass

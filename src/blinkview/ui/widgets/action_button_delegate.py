# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import perf_counter

from PySide6.QtGui import QColor, QBrush, QFont
from PySide6.QtWidgets import QStyledItemDelegate, QStyle, QStyleOptionButton, QApplication

from blinkview.ui.widgets.config.style_config import StyleConfig


class ActionButtonDelegate(QStyledItemDelegate):
    def __init__(self, parent=None, callback=None):
        super().__init__(parent)
        self.callback = callback

    def paint(self, painter, option, index):
        if index.column() == 2:  # The Action Column
            button_option = QStyleOptionButton()
            button_option.rect = option.rect.adjusted(2, 2, -2, -2)
            button_option.text = "Action"
            button_option.state = QStyle.State_Enabled

            QApplication.style().drawControl(QStyle.CE_PushButton, button_option, painter)
        else:
            super().paint(painter, option, index)

    def editorEvent(self, event, model, option, index):
        # Handle the click event
        if event.type() == event.MouseButtonRelease and index.column() == 2:
            if self.callback:
                self.callback(model.keys[index.row()])
            return True
        return False


from time import perf_counter
from PySide6.QtWidgets import QStyledItemDelegate, QStyle, QApplication
from PySide6.QtGui import QColor, QBrush, QPalette
from PySide6.QtCore import Qt


from enum import IntEnum, auto


class TelemetryCol(IntEnum):
    DEVICE = 0
    NAME = auto()
    VALUE = auto()
    ACTIONS = auto()
    # To add a column, just add it here and the rest of the code stays sane
    # TIMESTAMP = 3


class TelemetryDelegate(QStyledItemDelegate):
    def __init__(self, theme: StyleConfig, parent=None):
        super().__init__(parent)
        self.theme = theme
        self._flash_brushes = []
        self.indent_width = 20
        self._rebuild_cache()
        self.steps = len(self._flash_brushes)

        self.value_font = QFont("Consolas, monospace")
        self.value_font.setPointSizeF(10.5)
        self.value_font.setBold(True)

    def _rebuild_cache(self):
        """Pre-calculates brushes using the current THEME values."""
        self._flash_brushes = []
        steps = 100
        for i in range(steps):
            t = i / steps
            strength = 1.0 - (t * t)
            c = QColor(self.theme.color_flash_base)
            c.setAlphaF(strength * self.theme.flash_max_opacity)
            self._flash_brushes.append(QBrush(c))

    def paint(self, painter, option, index):
        # 1. Setup Source Model Access
        theme = self.theme
        model = index.model()
        if hasattr(model, 'mapToSource'):
            source_index = model.mapToSource(index)
            actual_model = model.sourceModel()
        else:
            source_index = index
            actual_model = model

        state = actual_model._row_states[source_index.row()]
        now = perf_counter()

        elapsed_since_arrival = now - state.last_arrival_time
        is_stale = state.last_painted_row and (elapsed_since_arrival > theme.stale_threshold)

        # 2. Use change time for the flash
        elapsed_since_change = now - state.last_change_time

        painter.save()

        col = index.column()

        if col == TelemetryCol.VALUE:
            painter.setFont(self.value_font)
        else:
            # Use the default font provided by the View/Option
            painter.setFont(option.font)

        # --- DRAW BACKGROUND FLASH ---
        if col == TelemetryCol.VALUE and elapsed_since_change < theme.fade_duration:
            idx = int((elapsed_since_change / theme.fade_duration) * self.steps)
            if 0 <= idx < self.steps:
                painter.fillRect(option.rect, self._flash_brushes[idx])

        # --- CONFIGURE TEXT COLOR ---
        if not state.last_painted_row or is_stale:
            color = theme.color_text_stale

        elif col == TelemetryCol.NAME:
            color = theme.color_text_name

        elif col == TelemetryCol.VALUE:
            # Safe access to the level color
            color = getattr(state.last_painted_row.level, 'color', theme.color_text_default)

        else:
            color = self.theme.color_text_default

        # --- DRAW TEXT ---
        # We use the 'option' to handle selection highlights and focus rects
        painter.setPen(color)

        # # Alignment logic
        # alignment = Qt.AlignVCenter
        # alignment |= (Qt.AlignLeft if col == TelemetryCol.VALUE else Qt.AlignCenter)
        #
        # # Calculate text rectangle with a small margin
        # text_rect = option.rect.adjusted(5, 0, -5, 0)
        # text = str(index.data(Qt.DisplayRole))
        #
        # painter.drawText(text_rect, alignment, text)
        #
        # painter.restore()
        text_rect = option.rect.adjusted(5, 0, -5, 0)

        # Default alignment
        alignment = Qt.AlignVCenter

        if col == TelemetryCol.NAME:
            # Shift text based on depth
            indent = state.module.depth * self.indent_width
            text_rect.setLeft(text_rect.left() + indent)
            alignment |= Qt.AlignLeft  # Trees must be left-aligned to look right
        elif col == TelemetryCol.VALUE:
            alignment |= Qt.AlignLeft
        else:
            alignment |= Qt.AlignCenter

        # --- DRAW TEXT ---
        text = str(index.data(Qt.DisplayRole))
        painter.drawText(text_rect, alignment, text)

        painter.restore()

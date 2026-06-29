# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from enum import IntEnum, auto
from time import perf_counter

from PySide6.QtCore import QSize
from qtpy.QtCore import Qt
from qtpy.QtGui import QBrush, QColor, QFont, QPalette
from qtpy.QtWidgets import QApplication, QStyle, QStyledItemDelegate, QStyleOptionButton

from blinkview.ui.widgets.config.style_config import StyleConfig
from blinkview.utils.log_level import LogLevel


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
        self.value_font.setBold(False)

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
        try:
            # Setup Source Model Access
            theme = self.theme
            model = index.model()

            row = index.row()
            if row >= len(model.visible_mod_ids):
                return

            mod_id = model.visible_mod_ids[row]
            module = model.modules[mod_id]

            # --- OPTIMIZATION FIX ---
            # Read directly from the memoryviews to prevent numpy scalar boxing
            last_arrival_time = model.arr_mv[mod_id]
            last_change_time = model.chg_mv[mod_id]
            last_painted_seq = model.seqs_mv[mod_id]

            last_painted_level = model.levels_mv[mod_id]

            now = perf_counter()

            elapsed_since_arrival = now - last_arrival_time
            is_stale = (last_painted_seq > 0) and (elapsed_since_arrival > theme.stale_threshold)

            # Use change time for the flash
            elapsed_since_change = now - last_change_time

            painter.save()

            col = index.column()

            if col == TelemetryCol.VALUE:
                painter.setFont(self.value_font)
            else:
                # Use the default font provided by the View/Option
                painter.setFont(option.font)

            # if option.state & QStyle.State_MouseOver:
            #     option.state |= QStyle.State_Selected

            # option.palette.setCurrentColorGroup(QPalette.Active)
            # QApplication.style().drawPrimitive(QStyle.PE_PanelItemViewItem, option, painter)

            # --- DRAW BACKGROUND FLASH ---
            # Restored the smooth gradient fade now that the model is fast enough to handle it
            if col == TelemetryCol.VALUE and elapsed_since_change < theme.fade_duration:
                # idx = int((elapsed_since_change / theme.fade_duration) * self.steps)
                # if 0 <= idx < self.steps:
                #     painter.fillRect(option.rect, self._flash_brushes[idx])
                painter.fillRect(option.rect, self.theme.color_flash_base)

            # --- CONFIGURE TEXT COLOR ---
            if last_painted_seq == 0 or is_stale:
                color = theme.color_text_stale

            elif col == TelemetryCol.NAME:
                color = theme.color_text_name

            elif col == TelemetryCol.VALUE:
                # Safe access to the level color
                lvl_obj = LogLevel.from_value(last_painted_level)
                color = QColor(lvl_obj.color)

            else:
                color = self.theme.color_text_default

            # --- DRAW TEXT ---
            # We use the 'option' to handle selection highlights and focus rects
            painter.setPen(color)

            text_rect = option.rect.adjusted(5, 0, -5, 0)

            # Default alignment
            alignment = Qt.AlignVCenter

            if col == TelemetryCol.NAME:
                # Shift text based on depth using the actual module object
                indent = module.depth * self.indent_width
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
        except Exception as e:
            # If ANYTHING fails here, we catch it instead of letting C++ kill the app
            print(f"CRASH AVERTED IN DELEGATE: {e}")
            import traceback

            traceback.print_exc()

    # def sizeHint(self, option, index):
    #     # Get the original size hint
    #     size = super().sizeHint(option, index)
    #     # Force the height to the minimum required by the font
    #     # or a hardcoded small value
    #     size.setHeight(10)
    #     return size
    def sizeHint(self, option, index):
        # Bypass the expensive font-metric calculation completely
        return QSize(50, 10)

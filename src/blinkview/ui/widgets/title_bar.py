# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtCore import Qt, QTimer, Signal, Slot
from PySide6.QtGui import QAction, QCloseEvent, QFont, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QDockWidget,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QListWidget,
    QMainWindow,
    QMenu,
    QPlainTextEdit,
    QPushButton,
    QTabWidget,
    QToolBar,
    QVBoxLayout,
    QWidget,
)


class TitleBar(QWidget):
    def __init__(self, parent):
        super().__init__(parent)
        self.parent = parent
        self.setFixedHeight(40)
        self.setStyleSheet("background-color: #2b2d30; color: #dfe1e5;")  # PyCharm Dark colors

        layout = QHBoxLayout(self)
        layout.setContentsMargins(10, 0, 10, 0)

        # Menu Button (The "Hamburger")
        self.menu_btn = QPushButton("☰")
        self.menu_btn.setFlat(True)
        self.menu_btn.setFixedSize(30, 30)

        # Window Title
        self.title_label = QLabel("BlinkView")
        self.title_label.setStyleSheet("font-weight: bold; margin-left: 10px;")

        # Window Controls (Min/Max/Close)
        self.btn_min = QPushButton("─")
        self.btn_max = QPushButton("▢")
        self.btn_close = QPushButton("✕")

        for btn in [self.btn_min, self.btn_max, self.btn_close]:
            btn.setFixedSize(35, 40)
            btn.setFlat(True)

        layout.addWidget(self.menu_btn)
        layout.addWidget(self.title_label)
        layout.addStretch()  # Push controls to the right
        layout.addWidget(self.btn_min)
        layout.addWidget(self.btn_max)
        layout.addWidget(self.btn_close)

        # Connect window actions
        self.btn_min.clicked.connect(self.parent.showMinimized)
        self.btn_max.clicked.connect(self._toggle_maximize)
        self.btn_close.clicked.connect(self.parent.close)

    def _toggle_maximize(self):
        if self.parent.isMaximized():
            self.parent.showNormal()
        else:
            self.parent.showMaximized()

    # CRITICAL: Allow dragging the window since the native title bar is gone
    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.drag_pos = event.globalPosition().toPoint() - self.parent.frameGeometry().topLeft()
            event.accept()

    def mouseMoveEvent(self, event):
        if event.buttons() == Qt.LeftButton:
            self.parent.move(event.globalPosition().toPoint() - self.drag_pos)
            event.accept()

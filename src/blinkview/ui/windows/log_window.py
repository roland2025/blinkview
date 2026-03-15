# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtWidgets import (
    QMainWindow, QPlainTextEdit, QVBoxLayout, QWidget, QToolBar
)
from PySide6.QtGui import QFont, QAction, QCloseEvent

from blinkview.ui.native_dark_mode import set_native_dark_mode
from blinkview.utils.time_utils import ConsoleTimestampFormatter


class LogViewerWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("BlinkView - Live Logs")
        self.resize(1000, 600)

        set_native_dark_mode(self)

        self.timestamp_formatter = ConsoleTimestampFormatter()

        # Setup the text engine
        self.text_edit = QPlainTextEdit()
        self.text_edit.setReadOnly(True)
        self.text_edit.setFont(QFont("Consolas", 10))
        self.text_edit.setMaximumBlockCount(10000)
        self.text_edit.setLineWrapMode(QPlainTextEdit.LineWrapMode.NoWrap)

        central_widget = QWidget()
        layout = QVBoxLayout(central_widget)
        layout.addWidget(self.text_edit)
        layout.setContentsMargins(5, 5, 5, 5)
        self.setCentralWidget(central_widget)

    # def append_logs(self, text: str):
    #     """Thread-safe standard append called by the Main Window poller."""
    #     self.text_edit.appendPlainText(text)

    def closeEvent(self, event: QCloseEvent):
        """Intercept the close button. Hide the window instead of destroying it."""
        self.hide()
        event.ignore()

    def process_log_batch(self, batch: list):
        """Receives a raw list of log objects and formats them."""
        format_ts = self.timestamp_formatter.format
        rows = []

        for msg in batch:
            # Format however this specific window wants it
            formatted_line = f"{format_ts(msg.timestamp_ns)} {msg.level} {msg.module.device} {msg.module.name}: {msg.message}"
            rows.append(formatted_line)

        if rows:
            # Assuming you have an append_logs method handling the UI insertion
            self.text_edit.appendPlainText("\n".join(rows))

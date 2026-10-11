# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import threading
from time import perf_counter
from typing import Callable, Optional

from qtpy.QtCore import QObject, Qt, QTimer, Signal
from qtpy.QtGui import QGuiApplication, QPixmap
from qtpy.QtWidgets import QFrame, QHBoxLayout, QLabel, QProgressBar, QPushButton, QVBoxLayout

from blinkview import ICON_PATH, __app_name__, __version__

PROGRESS_STEPS = 1000

# compile(on_progress, cancel) -> error text, or None when every kernel compiled.
CompileFn = Callable[[Callable[[float, str], None], threading.Event], Optional[str]]


class _CompileBridge(QObject):
    """Marshals the compile thread's progress and result onto the main thread (queued signals),
    like main_window's _WarmupBridge."""

    progress = Signal(float, str)  # fraction of the whole warmup, callback label
    finished = Signal(str)  # error text, "" on success


class SetupWindow(QFrame):
    """`blink setup`'s splash: a progress bar over the Numba warmup, which runs on its own thread
    so the window keeps painting (plans/background-warmup.md measured that). Closes itself when
    the compile succeeds; stays up with the message when it fails."""

    def __init__(self, compile_fn: CompileFn, parent=None):
        super().__init__(parent)
        self._compile_fn = compile_fn
        self._cancel = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._started_at = 0.0
        # For the process once the window has closed: 0 compiled, 1 failed or cancelled.
        self.exit_code: Optional[int] = None
        self.error: Optional[str] = None

        self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint)
        self.setWindowTitle(f"{__app_name__} setup")
        self.setObjectName("SetupWindow")
        self.setStyleSheet("#SetupWindow { border: 1px solid #4e5157; }")
        self.setFixedWidth(460)

        icon = QLabel()
        icon.setPixmap(QPixmap(str(ICON_PATH)).scaled(40, 40, Qt.KeepAspectRatio, Qt.SmoothTransformation))

        title = QLabel(f"{__app_name__} v{__version__}")
        title_font = title.font()
        title_font.setPointSize(title_font.pointSize() + 3)
        title_font.setBold(True)
        title.setFont(title_font)
        subtitle = QLabel("Compiling shaders. This is needed once after an update.")

        titles = QVBoxLayout()
        titles.setSpacing(2)
        titles.addWidget(title)
        titles.addWidget(subtitle)

        header = QHBoxLayout()
        header.setSpacing(12)
        header.addWidget(icon)
        header.addLayout(titles, 1)

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, PROGRESS_STEPS)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(8)

        self.status_label = QLabel("Starting...")
        self.status_label.setWordWrap(True)
        self.elapsed_label = QLabel("")
        self.button = QPushButton("Cancel")
        self.button.clicked.connect(self._on_button)

        footer = QHBoxLayout()
        footer.addWidget(self.status_label, 1)
        footer.addWidget(self.elapsed_label)
        footer.addWidget(self.button)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 16)
        layout.setSpacing(12)
        layout.addLayout(header)
        layout.addWidget(self.progress_bar)
        layout.addLayout(footer)

        self._bridge = _CompileBridge(self)
        self._bridge.progress.connect(self._on_progress)
        self._bridge.finished.connect(self._on_finished)

        self._elapsed_timer = QTimer(self)
        self._elapsed_timer.setInterval(1000)
        self._elapsed_timer.timeout.connect(self._update_elapsed)

    def start(self):
        self._started_at = perf_counter()
        self._elapsed_timer.start()
        self._thread = threading.Thread(target=self._compile, name="blink-setup-compile")
        self._thread.start()

    def center_on_screen(self):
        screen = QGuiApplication.primaryScreen()
        if screen is not None:
            self.adjustSize()
            self.move(screen.availableGeometry().center() - self.rect().center())

    def _compile(self):
        # Compile thread: only emit, the slots run on the main thread.
        try:
            error = self._compile_fn(self._bridge.progress.emit, self._cancel)
        except Exception as e:
            error = str(e) or type(e).__name__
        self._bridge.finished.emit(error or "")

    def _on_progress(self, fraction: float, label: str):
        self.progress_bar.setValue(int(fraction * PROGRESS_STEPS))
        if label and not self._cancel.is_set():
            self.status_label.setText(f"{int(fraction * 100)}% - {label}")

    def _update_elapsed(self):
        self.elapsed_label.setText(f"{perf_counter() - self._started_at:.0f} s")

    def _on_finished(self, error: str):
        self._elapsed_timer.stop()
        self._update_elapsed()
        if not error:
            self._finish(0)
        elif self._cancel.is_set():
            self._finish(1)
        else:
            self.fail(error)

    def fail(self, error: str):
        """Keeps the window up with the message until the user closes it."""
        self._elapsed_timer.stop()
        self.error = error
        self.exit_code = 1
        self.status_label.setText(f"Compilation failed: {error}")
        self.button.setText("Close")
        self.button.setEnabled(True)

    def _on_button(self):
        if self.exit_code is not None:
            self._finish(self.exit_code)
            return
        if self._thread is None:
            self._finish(1)  # cancelled before the compile started
            return
        # A Numba compile that is already running can't be interrupted - the thread stops at the
        # next kernel.
        self._cancel.set()
        self.button.setEnabled(False)
        self.status_label.setText("Cancelling after the current kernel...")

    def _finish(self, exit_code: int):
        self.exit_code = exit_code
        self._thread = None
        self.close()

    def closeEvent(self, event):
        if self._thread is not None and self.exit_code is None:
            # Alt+F4 mid-compile is a cancel; the window closes once the thread has stopped.
            event.ignore()
            self._on_button()
            return
        if self.exit_code is None:
            self.exit_code = 1  # closed before the compile started
        event.accept()

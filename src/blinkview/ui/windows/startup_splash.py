# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Callable, Optional

from qtpy.QtCore import QCoreApplication, QRect, Qt
from qtpy.QtGui import QCursor, QGuiApplication
from qtpy.QtWidgets import QFrame, QHBoxLayout, QLabel, QProgressBar, QVBoxLayout, QWidget

from blinkview import __app_name__
from blinkview.ui.windows.setup_window import PROGRESS_STEPS, app_header

# The status line while the warmup is compiling rather than loading (BlinkMainWindow decides).
COMPILING_STATUS = "Compiling kernels. Needed once after an install or update."


class StartupSplash(QFrame):
    """The window over a normal start (plans/startup-splash.md): up from right after the
    QApplication until the main window and every restored floating window are revealed at once.
    run.py drives it through the imports, BlinkMainWindow through the warmup and the restore."""

    def __init__(self, parent=None):
        super().__init__(parent)
        # Set by the main window: closing the splash (Alt+F4) closes the app.
        self.close_requested: Optional[Callable[[], None]] = None
        self._finished = False

        # A splash screen has no taskbar button of its own; the (still invisible) main window's
        # raises it again (BlinkMainWindow.changeEvent).
        self.setWindowFlags(Qt.SplashScreen | Qt.FramelessWindowHint)
        self.setWindowTitle(__app_name__)
        self.setObjectName("StartupSplash")
        self.setStyleSheet("#StartupSplash { border: 1px solid #4e5157; }")
        self.setFixedWidth(460)

        # Under the title: what the start is doing now.
        self.status_label = QLabel("Loading...")

        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, PROGRESS_STEPS)
        self.progress_bar.setTextVisible(False)
        self.progress_bar.setFixedHeight(8)

        # Under the bar: "42% - BinaryParser", and on the right the seconds a compile has taken.
        self.step_label = QLabel()
        self.elapsed_label = QLabel()
        footer = QHBoxLayout()
        footer.setContentsMargins(0, 0, 0, 0)
        footer.addWidget(self.step_label, 1)
        footer.addWidget(self.elapsed_label)

        # The bar and the line under it, hidden until a step with real progress (the warmup)
        # reports some. retainSizeWhenHidden keeps the window from changing height then.
        self.progress_area = QWidget()
        progress_layout = QVBoxLayout(self.progress_area)
        progress_layout.setContentsMargins(0, 0, 0, 0)
        progress_layout.setSpacing(8)
        progress_layout.addWidget(self.progress_bar)
        progress_layout.addLayout(footer)
        policy = self.progress_area.sizePolicy()
        policy.setRetainSizeWhenHidden(True)
        self.progress_area.setSizePolicy(policy)
        self.progress_area.hide()

        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 18, 20, 16)
        layout.setSpacing(12)
        layout.addLayout(app_header(self.status_label))
        layout.addWidget(self.progress_area)

    def set_status(self, text: str):
        self.status_label.setText(text)

    def set_progress(self, fraction: Optional[float], step: str = ""):
        """0.0-1.0 shows the bar at that point, with "NN% - step" under it; None hides both."""
        if fraction is None:
            self.progress_area.hide()
            return
        self.progress_bar.setValue(int(fraction * PROGRESS_STEPS))
        percent = f"{int(fraction * 100)}%"
        self.step_label.setText(f"{percent} - {step}" if step else percent)
        self.progress_area.show()

    def set_elapsed(self, seconds: Optional[float]):
        """Seconds on the right under the bar; None clears it."""
        self.elapsed_label.setText("" if seconds is None else f"{seconds:.0f} s")

    def paint_now(self):
        """For run.py, where no event loop is running yet between its blocking steps: lets the
        splash show and repaint its status line before the next one."""
        QCoreApplication.processEvents()

    def center_on_cursor_screen(self):
        """Before the main window exists its saved position isn't known, so start where the
        user is - the screen under the mouse."""
        screen = QGuiApplication.screenAt(QCursor.pos()) or QGuiApplication.primaryScreen()
        if screen is not None:
            self.center_over(screen.availableGeometry())

    def center_over(self, area: QRect):
        self.adjustSize()
        self.move(area.center() - self.rect().center())

    def finish(self):
        """Closes the splash for good (the reveal, or the app's final close)."""
        self._finished = True
        self.close()

    def closeEvent(self, event):
        if not self._finished and self.close_requested is not None:
            # Alt+F4 on the splash quits the app; the main window's close path decides when the
            # splash actually goes (it says "Closing..." until then).
            event.ignore()
            self.close_requested()
            return
        event.accept()


def _demo():
    """`python -m blinkview.ui.windows.startup_splash`: the splash on its own, stepping through
    the real start's status lines on a loop. Alt+F4 closes it."""
    import sys

    import qdarktheme
    from qtpy.QtCore import QTimer
    from qtpy.QtGui import QIcon
    from qtpy.QtWidgets import QApplication

    from blinkview import ICON_PATH

    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    qdarktheme.setup_theme("dark", corner_shape="sharp")
    app.setWindowIcon(QIcon(str(ICON_PATH)))

    splash = StartupSplash()
    splash.close_requested = app.quit
    splash.center_on_cursor_screen()
    splash.show()

    # (status, progress, step, elapsed) per 100 ms tick; the kernel steps stand in for the
    # warmup's progress, the second half as a compile.
    steps = [
        (status, None, "", None)
        for status in ["Loading...", "Loading profile...", "Loading interface..."]
        for _ in range(5)
    ]
    steps += [("Loading kernels...", i / 30, f"Kernel {i // 2 + 1}", None) for i in range(10)]
    steps += [(COMPILING_STATUS, i / 30, f"Kernel {i // 2 + 1}", i / 10) for i in range(10, 31)]
    steps += [("Restoring windows...", None, "", None)] * 10
    tick = 0

    def advance():
        nonlocal tick
        status, progress, step, elapsed = steps[tick % len(steps)]
        splash.set_status(status)
        splash.set_progress(progress, step)
        splash.set_elapsed(elapsed)
        tick += 1

    timer = QTimer()
    timer.timeout.connect(advance)
    timer.start(100)
    advance()
    sys.exit(app.exec())


if __name__ == "__main__":
    _demo()

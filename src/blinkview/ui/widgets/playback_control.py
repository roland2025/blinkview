# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import Qt, Signal
from qtpy.QtGui import QPainter, QPen
from qtpy.QtWidgets import QHBoxLayout, QLabel, QSlider, QToolButton, QWidget

from blinkview.core.playback_clock import PlaybackMode
from blinkview.ui.gui_context import GUIContext
from blinkview.utils.time_utils import ConsoleTimestampFormatter

# Speed slider maps integer steps to a signed multiplier (magnitude/direction), so the
# scrubber never has to represent an unrepresentable "0 direction" state on its own.
_SPEED_SCALE = 10.0
_SPEED_SLIDER_MIN = -80  # -8.0x
_SPEED_SLIDER_MAX = 80  # +8.0x
_SPEED_SLIDER_DEFAULT = int(_SPEED_SCALE)  # 1.0x


class SpeedSliderWidget(QSlider):
    """QSlider that resets to a default value on right-click - dragging to land exactly on
    1.0x is fiddly, so right-click is a quick way back to the common case."""

    def __init__(self, *args, default_value: int = 0, **kwargs):
        super().__init__(*args, **kwargs)
        self._default_value = default_value

    def mousePressEvent(self, event):
        if event.button() == Qt.RightButton:
            self.setValue(self._default_value)
            event.accept()
            return
        super().mousePressEvent(event)


class SeekBarWidget(QWidget):
    """Simple custom-painted horizontal scrubber: track + playhead + click/drag-to-seek.

    Deliberately plain for phase 1 (flat track, no jog-wheel styling) - there's no existing
    QSlider precedent in this codebase to build on for a DaVinci-style scrubber, so this keeps
    the paint surface minimal until the backend clock is validated.
    """

    seekRequested = Signal(object)  # ts_ns (int) - plain `int` is a 32-bit C int in Qt and
    # overflows for nanosecond epoch timestamps, so this must carry a Python object instead.
    scrubStarted = Signal()  # left mouse button pressed down on the track
    scrubEnded = Signal()  # left mouse button released (anywhere - Qt implicitly grabs the
    # mouse for the widget that received the press, so this fires even if the release happens
    # outside the widget's bounds)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setMinimumHeight(28)
        self.setMouseTracking(True)

        self.bounds_min_ns = 0
        self.bounds_max_ns = 0
        self.current_ts_ns = 0
        self._hover_x = None

    def set_state(self, bounds_min_ns: int, bounds_max_ns: int, current_ts_ns: int):
        self.bounds_min_ns = bounds_min_ns
        self.bounds_max_ns = bounds_max_ns
        self.current_ts_ns = current_ts_ns
        self.update()

    def _ts_to_x(self, ts_ns: int) -> int:
        span = self.bounds_max_ns - self.bounds_min_ns
        if span <= 0:
            return 0
        frac = (ts_ns - self.bounds_min_ns) / span
        return int(frac * self.width())

    def _x_to_ts(self, x: int) -> int:
        span = self.bounds_max_ns - self.bounds_min_ns
        if span <= 0 or self.width() <= 0:
            return self.bounds_min_ns
        frac = max(0.0, min(1.0, x / self.width()))
        return int(self.bounds_min_ns + frac * span)

    def mousePressEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.scrubStarted.emit()
        self.seekRequested.emit(self._x_to_ts(int(event.position().x())))

    def mouseMoveEvent(self, event):
        x = int(event.position().x())
        self._hover_x = x
        if event.buttons() & Qt.LeftButton:
            self.seekRequested.emit(self._x_to_ts(x))
        self.update()

    def mouseReleaseEvent(self, event):
        if event.button() == Qt.LeftButton:
            self.scrubEnded.emit()

    def leaveEvent(self, event):
        self._hover_x = None
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        h = self.height()
        w = self.width()
        mid_y = h // 2

        # Track
        painter.setPen(QPen(Qt.gray, 3))
        painter.drawLine(0, mid_y, w, mid_y)

        # Hover marker (faint)
        if self._hover_x is not None:
            painter.setPen(QPen(Qt.darkGray, 1))
            painter.drawLine(self._hover_x, 0, self._hover_x, h)

        # Playhead
        x = self._ts_to_x(self.current_ts_ns)
        painter.setPen(QPen(Qt.cyan, 2))
        painter.setBrush(Qt.cyan)
        painter.drawLine(x, 0, x, h)
        painter.drawEllipse(x - 5, mid_y - 5, 10, 10)

        painter.end()


class PlaybackControlWidget(QWidget):
    """Transport toolbar for the session's PlaybackClock (registry.playback_clock).

    One global instance per session (mirrors the Registry's one-per-session lifecycle) - not
    per log-viewer tab. Reads clock state each heartbeat via apply_updates() (same
    add_updatable/GUIContext.on_update() pattern LogViewerWidget uses) rather than any
    cross-thread signal, since nothing in this codebase bridges threads with Qt signals.

    Phase 1 only: doesn't yet drive LogViewerWidget/LogTableViewerWidget row fetching.
    """

    def __init__(self, gui_context: GUIContext, parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self._time_formatter = ConsoleTimestampFormatter()

        layout = QHBoxLayout(self)
        layout.setContentsMargins(4, 2, 4, 2)

        self.status_button = QToolButton(self)
        self.status_button.setCheckable(True)
        self.status_button.clicked.connect(self._on_status_clicked)
        layout.addWidget(self.status_button)

        self.play_button = QToolButton(self)
        self.play_button.setText("⏵")  # play
        self.play_button.setCheckable(True)
        self.play_button.clicked.connect(self._on_play_clicked)
        layout.addWidget(self.play_button)

        self.seek_bar = SeekBarWidget(self)
        self.seek_bar.seekRequested.connect(self._on_seek_requested)
        self.seek_bar.scrubStarted.connect(self._on_scrub_started)
        self.seek_bar.scrubEnded.connect(self._on_scrub_ended)
        layout.addWidget(self.seek_bar, stretch=1)

        self.speed_slider = SpeedSliderWidget(Qt.Horizontal, self, default_value=_SPEED_SLIDER_DEFAULT)
        self.speed_slider.setMinimum(_SPEED_SLIDER_MIN)
        self.speed_slider.setMaximum(_SPEED_SLIDER_MAX)
        self.speed_slider.setValue(_SPEED_SLIDER_DEFAULT)
        self.speed_slider.setMaximumWidth(120)
        self.speed_slider.valueChanged.connect(self._on_speed_changed)
        layout.addWidget(self.speed_slider)

        self.speed_label = QLabel("1.0x", self)
        self.speed_label.setMinimumWidth(40)
        layout.addWidget(self.speed_label)

        self.time_label = QLabel("--:--:--.---", self)
        self.time_label.setMinimumWidth(90)
        self.time_label.setAlignment(Qt.AlignRight | Qt.AlignVCenter)
        layout.addWidget(self.time_label)

        self._sync_from_clock()

        self.gui_context.add_updatable(self)

    def _clock(self):
        registry = self.gui_context.registry
        return registry.playback_clock if registry is not None else None

    def closeEvent(self, event):
        self.gui_context.remove_updatable(self)
        super().closeEvent(event)

    def apply_updates(self):
        clock = self._clock()
        if clock is None:
            return

        if clock.tick(self.gui_context.registry.now_ns()):
            self._sync_from_clock()

    def _sync_from_clock(self):
        clock = self._clock()
        if clock is None:
            return

        is_live = clock.mode is PlaybackMode.LIVE
        self.status_button.setChecked(not is_live)
        self.status_button.setText("● LIVE" if is_live else "⏵ REPLAY")

        self.play_button.setEnabled(not is_live)
        self.play_button.setChecked(clock.is_playing)
        self.play_button.setText("⏸" if clock.is_playing else "⏵")

        self.seek_bar.setEnabled(not is_live)
        self.seek_bar.set_state(clock.bounds_min_ns, clock.bounds_max_ns, clock.current_ts_ns)

        self.time_label.setText(self._time_formatter.format(clock.current_ts_ns))

    def _on_status_clicked(self, checked: bool):
        clock = self._clock()
        if clock is None:
            return
        if checked:
            clock.enter_replay()
        else:
            clock.go_live()
        self._sync_from_clock()

    def _on_play_clicked(self, checked: bool):
        clock = self._clock()
        if clock is None:
            return
        if checked:
            clock.play()
        else:
            clock.pause()
        self._sync_from_clock()

    def _on_seek_requested(self, ts_ns: int):
        clock = self._clock()
        if clock is None:
            return
        clock.seek(ts_ns)
        self._sync_from_clock()

    def _on_scrub_started(self):
        clock = self._clock()
        if clock is not None:
            clock.begin_scrub()

    def _on_scrub_ended(self):
        clock = self._clock()
        if clock is not None:
            clock.end_scrub()

    def _on_speed_changed(self, value: int):
        speed = value / _SPEED_SCALE
        self.speed_label.setText(f"{speed:.1f}x")

        clock = self._clock()
        if clock is None:
            return
        clock.set_speed(speed)

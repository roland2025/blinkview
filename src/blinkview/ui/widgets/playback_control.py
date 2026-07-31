# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import Qt, Signal
from qtpy.QtGui import QColor, QPainter, QPen
from qtpy.QtWidgets import (
    QComboBox,
    QHBoxLayout,
    QInputDialog,
    QLabel,
    QSlider,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from blinkview.core.playback_clock import PlaybackMode
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.jog_wheel_button import JogWheelButton
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
        self._ranges = []  # list[PlaybackRange], drawn as bands under the track

    def set_state(self, bounds_min_ns: int, bounds_max_ns: int, current_ts_ns: int):
        self.bounds_min_ns = bounds_min_ns
        self.bounds_max_ns = bounds_max_ns
        self.current_ts_ns = current_ts_ns
        self.update()

    def set_ranges(self, ranges):
        self._ranges = ranges
        self.update()

    def _ts_to_x(self, ts_ns: int) -> int:
        span = self.bounds_max_ns - self.bounds_min_ns
        if span <= 0:
            return 0
        # Clamp to [0, 1] - a range/timestamp outside [bounds_min_ns, bounds_max_ns] (e.g. a
        # named range saved under different bounds) must draw pinned at the track's edge, not
        # at an arbitrarily large pixel offset: Qt's drawRect takes a C++ int (32-bit), and an
        # unclamped frac (e.g. ts_ns=0 against a real epoch-ns bounds window) overflows it.
        frac = max(0.0, min(1.0, (ts_ns - self.bounds_min_ns) / span))
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
        try:
            painter.setRenderHint(QPainter.Antialiasing)

            h = self.height()
            w = self.width()
            mid_y = h // 2

            # Named range bands - drawn first so the track/playhead/hover marker paint on top.
            if self._ranges:
                band_color = QColor(80, 180, 255, 90)
                painter.setPen(Qt.NoPen)
                painter.setBrush(band_color)
                for rng in self._ranges:
                    rng = rng.normalized()
                    x0 = self._ts_to_x(rng.start_ts_ns)
                    x1 = self._ts_to_x(rng.end_ts_ns)
                    painter.drawRect(x0, 2, max(1, x1 - x0), h - 4)

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
        finally:
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
        self._pending_mark_in_ts = None
        self._ranges_combo_ids = []  # tracks what's currently populated, to avoid rebuilding
        # (and losing the user's current selection) on every heartbeat when nothing changed
        self._active_range_id = None  # range currently shown zoomed-in on the second row, if any

        outer_layout = QVBoxLayout(self)
        outer_layout.setContentsMargins(0, 0, 0, 0)
        outer_layout.setSpacing(0)

        layout = QHBoxLayout()
        layout.setContentsMargins(4, 2, 4, 2)
        outer_layout.addLayout(layout)

        self.status_button = QToolButton(self)
        self.status_button.setCheckable(True)
        self.status_button.clicked.connect(self._on_status_clicked)
        layout.addWidget(self.status_button)

        self.play_button = QToolButton(self)
        self.play_button.setText("⏵")  # play
        self.play_button.setCheckable(True)
        self.play_button.clicked.connect(self._on_play_clicked)
        layout.addWidget(self.play_button)

        self.jog_wheel = JogWheelButton(self)
        self.jog_wheel.stepRequested.connect(self._on_jog_step)
        layout.addWidget(self.jog_wheel)

        self.seek_bar = SeekBarWidget(self)
        self.seek_bar.seekRequested.connect(self._on_seek_requested)
        self.seek_bar.scrubStarted.connect(self._on_scrub_started)
        self.seek_bar.scrubEnded.connect(self._on_scrub_ended)
        layout.addWidget(self.seek_bar, stretch=1)

        self.mark_in_button = QToolButton(self)
        self.mark_in_button.setText("[")
        self.mark_in_button.setToolTip("Mark range start at the current position")
        self.mark_in_button.clicked.connect(self._on_mark_in_clicked)
        layout.addWidget(self.mark_in_button)

        self.mark_out_button = QToolButton(self)
        self.mark_out_button.setText("]")
        self.mark_out_button.setToolTip("Mark range end at the current position and name it")
        self.mark_out_button.setEnabled(False)
        self.mark_out_button.clicked.connect(self._on_mark_out_clicked)
        layout.addWidget(self.mark_out_button)

        self.ranges_combo = QComboBox(self)
        self.ranges_combo.setMinimumWidth(120)
        self.ranges_combo.setToolTip("Named ranges - select to jump to its start")
        self.ranges_combo.activated.connect(self._on_range_selected)
        layout.addWidget(self.ranges_combo)

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

        # Second row: a zoomed-in scrubber spanning just the active named range, so a range
        # too short to scrub precisely on the full-session seek bar above can still be
        # navigated row-by-row. Hidden entirely until a range is selected from the combo.
        self.zoom_row = QWidget(self)
        zoom_layout = QHBoxLayout(self.zoom_row)
        zoom_layout.setContentsMargins(4, 0, 4, 2)

        self.zoom_label = QLabel(self.zoom_row)
        self.zoom_label.setMinimumWidth(120)
        zoom_layout.addWidget(self.zoom_label)

        self.zoom_seek_bar = SeekBarWidget(self.zoom_row)
        self.zoom_seek_bar.seekRequested.connect(self._on_seek_requested)
        self.zoom_seek_bar.scrubStarted.connect(self._on_scrub_started)
        self.zoom_seek_bar.scrubEnded.connect(self._on_scrub_ended)
        zoom_layout.addWidget(self.zoom_seek_bar, stretch=1)

        outer_layout.addWidget(self.zoom_row)
        self.zoom_row.setVisible(False)

        # Third row: a persistent "live system logs" scrubber spanning program-start-to-now
        # (registry.start_ts_ns to the clock's live edge), always visible regardless of mode or
        # which replay session (if any) is loaded - the system's own self-logging keeps running
        # live throughout a replay, so this stays a stable reference to "where is live right now"
        # even while the main seek bar above is showing a fixed-length recorded session.
        self.live_row = QWidget(self)
        live_layout = QHBoxLayout(self.live_row)
        live_layout.setContentsMargins(4, 0, 4, 2)

        self.live_label = QLabel("Live", self.live_row)
        self.live_label.setMinimumWidth(120)
        live_layout.addWidget(self.live_label)

        self.live_seek_bar = SeekBarWidget(self.live_row)
        self.live_seek_bar.seekRequested.connect(self._on_seek_requested)
        self.live_seek_bar.scrubStarted.connect(self._on_scrub_started)
        self.live_seek_bar.scrubEnded.connect(self._on_scrub_ended)
        live_layout.addWidget(self.live_seek_bar, stretch=1)

        outer_layout.addWidget(self.live_row)

        self._sync_from_clock()

        self.gui_context.add_updatable(self)

    def _clock(self):
        registry = self.gui_context.registry
        return registry.playback_clock if registry is not None else None

    def _ranges_store(self):
        registry = self.gui_context.registry
        return registry.playback_ranges if registry is not None else None

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
        seek_min_ns, seek_max_ns = self._seek_bar_bounds(clock)
        self.seek_bar.set_state(seek_min_ns, seek_max_ns, clock.current_ts_ns)

        self.time_label.setText(self._time_formatter.format(clock.current_ts_ns))

        self.mark_out_button.setEnabled(self._pending_mark_in_ts is not None)

        self._sync_ranges()
        self._sync_zoom_bar()
        self._sync_live_bar(clock)

    def _seek_bar_bounds(self, clock):
        """Bounds for the main seek bar: while replaying a loaded session with known metadata
        (registry.replay_session_bounds_ns, set from metadata.json created_at/finished_at - see
        Registry.load_replay_session), use that session's own fixed length instead of the raw
        clock bounds. Otherwise falls back to the clock's own bounds unchanged - covers plain
        LIVE mode and a REPLAY entered by scrubbing back into the live buffer with no loaded
        session (no metadata to fix a length to), same as before this distinction existed.
        Without this, a session's seek bar would keep growing for as long as the system's own
        self-logging keeps appending rows to the shared central pool during replay, even though
        the recorded session itself has a fixed length. Deliberately not a selectable named
        PlaybackRange (it used to be one) - the seek bar already shows "the whole recording" by
        default, so a dedicated dropdown entry for the same thing would be redundant."""
        if clock.mode is PlaybackMode.REPLAY:
            registry = self.gui_context.registry
            bounds = getattr(registry, "replay_session_bounds_ns", None) if registry is not None else None
            if bounds is not None:
                return bounds
        return clock.bounds_min_ns, clock.bounds_max_ns

    def _sync_live_bar(self, clock):
        """Always-visible second scrubber spanning registry.start_ts_ns (program start) to the
        clock's live edge (bounds_max_ns) - independent of _seek_bar_bounds()'s session-length
        window above, so the live system log stream stays scrubbable even while the main bar is
        pinned to a fixed recorded session's length."""
        registry = self.gui_context.registry
        start_ts_ns = getattr(registry, "start_ts_ns", clock.bounds_min_ns) if registry is not None else 0
        self.live_seek_bar.setEnabled(True)
        self.live_seek_bar.set_state(start_ts_ns, clock.bounds_max_ns, clock.current_ts_ns)

    def _sync_ranges(self):
        store = self._ranges_store()
        ranges = store.ranges if store is not None else []

        self.seek_bar.set_ranges(ranges)

        current_ids = [r.id for r in ranges]
        if current_ids == self._ranges_combo_ids:
            return  # nothing changed - rebuilding would reset the user's current selection

        self._ranges_combo_ids = current_ids
        self.ranges_combo.blockSignals(True)
        self.ranges_combo.clear()
        for r in ranges:
            self.ranges_combo.addItem(r.name, r.id)
        self.ranges_combo.blockSignals(False)

    def _sync_zoom_bar(self):
        clock = self._clock()
        store = self._ranges_store()
        rng = None
        if clock is not None and store is not None and self._active_range_id is not None:
            rng = store.get(self._active_range_id)

        if rng is None:
            self._active_range_id = None
            self.zoom_row.setVisible(False)
            return

        rng = rng.normalized()
        self.zoom_row.setVisible(True)
        self.zoom_label.setText(rng.name)
        self.zoom_seek_bar.setEnabled(self.seek_bar.isEnabled())
        self.zoom_seek_bar.set_state(rng.start_ts_ns, rng.end_ts_ns, clock.current_ts_ns)

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

    def _on_jog_step(self, delta_rows: int):
        clock = self._clock()
        if clock is None:
            return
        clock.step_rows(delta_rows)
        self._sync_from_clock()

    def _on_mark_in_clicked(self):
        clock = self._clock()
        if clock is None:
            return
        self._pending_mark_in_ts = clock.current_ts_ns
        self.mark_out_button.setEnabled(True)

    def _on_mark_out_clicked(self):
        clock = self._clock()
        store = self._ranges_store()
        if clock is None or store is None or self._pending_mark_in_ts is None:
            return

        name, ok = QInputDialog.getText(self, "Name range", "Range name:")
        if not ok or not name:
            self._pending_mark_in_ts = None
            self.mark_out_button.setEnabled(False)
            return

        rng = store.add(name, self._pending_mark_in_ts, clock.current_ts_ns)
        self._pending_mark_in_ts = None
        self.mark_out_button.setEnabled(False)
        self._active_range_id = rng.id
        self._sync_ranges()
        self._sync_zoom_bar()

    def _on_range_selected(self, index: int):
        clock = self._clock()
        store = self._ranges_store()
        if clock is None or store is None or index < 0:
            return

        range_id = self.ranges_combo.itemData(index)
        rng = store.get(range_id)
        if rng is None:
            return

        self._active_range_id = range_id
        clock.seek(rng.normalized().start_ts_ns)
        self._sync_from_clock()

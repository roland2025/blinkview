# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from PySide6.QtCore import QEasingCurve, QObject, QPropertyAnimation, QRectF, Qt, QVariantAnimation, Signal, Slot
from PySide6.QtGui import QColor, QPainter, QPen
from PySide6.QtWidgets import QApplication, QGraphicsOpacityEffect, QHBoxLayout, QLabel, QPushButton, QWidget


class ToastType:
    INFO = {"color": "#2b2d30", "icon": "ⓘ", "text": "#bcbec4"}
    SUCCESS = {"color": "#2e3c2e", "icon": "✔", "text": "#addb67"}
    WARNING = {"color": "#3e3925", "icon": "⚠", "text": "#e2c08d"}
    ERROR = {"color": "#482323", "icon": "✖", "text": "#ff5555"}


class ToastIcon(QLabel):
    """A custom label that draws a circular progress ring around the icon."""

    def __init__(self, icon_text, color, parent=None):
        super().__init__(icon_text, parent)
        self._progress = 1.0  # 1.0 to 0.0
        self._color = QColor(color)
        self.setMargin(0)
        self.setContentsMargins(0, 0, 0, 0)
        # Ensure the label doesn't try to grow
        self.setFixedSize(24, 24)

        self.setAlignment(Qt.AlignCenter)
        self.setStyleSheet("background: transparent; font-weight: bold; font-size: 14px;")

    def set_progress(self, value):
        self._progress = value
        self.update()

    def paintEvent(self, event):
        # Create the painter
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        # Calculate a perfectly centered square for the circle
        # Using adjusted() is cleaner than hardcoded (2, 2, w-4, h-4)
        rect = QRectF(self.rect()).adjusted(2, 2, -2, -2)

        # Draw background track
        pen = QPen(QColor(255, 255, 255, 30))
        pen.setWidth(2)
        painter.setPen(pen)
        painter.drawEllipse(rect)

        # Draw progress arc
        pen.setColor(self._color)
        painter.setPen(pen)
        start_angle = 90 * 16
        span_angle = int(self._progress * 360 * 16)
        painter.drawArc(rect, start_angle, span_angle)

        # End the painter before calling super().paintEvent (the text)
        # This prevents the text from being "clipped" or affected by painter settings
        painter.end()

        # Now draw the text icon (ⓘ, ✔, etc.) centered in the widget
        super().paintEvent(event)


class ToastWidget(QWidget):
    def __init__(
        self,
        message,
        toast_type=ToastType.INFO,
        duration=5000,
        action_text=None,
        action_callback=None,
        click_callback=None,
        parent=None,
    ):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.ToolTip | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)

        self.setFixedWidth(300)

        self.action_callback = action_callback
        self.click_callback = click_callback

        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)

        self.bg_frame = QWidget(self)
        self.bg_frame.setObjectName("toastFrame")

        # Cursor & Hover logic
        if self.click_callback:
            self.bg_frame.setCursor(Qt.PointingHandCursor)
        hover_style = "background-color: #3e4043;" if self.click_callback else ""

        self.bg_frame.setStyleSheet(f"""
            QWidget#toastFrame {{
                background-color: {toast_type["color"]};
                border: 1px solid #4e5157;
                border-radius: 4px;
                min-height: 40px; 
            }}
            QWidget#toastFrame:hover {{
                {hover_style}
            }}
            QLabel {{
                color: {toast_type["text"]};
                font-size: 13px;
                background: transparent;
            }}
            QPushButton#actionBtn {{
                background: transparent;
                color: #3592c4;
                border: none;
                font-weight: bold;
                padding: 2px 8px;
                font-size: 12px;
            }}
            QPushButton#actionBtn:hover {{
                text-decoration: underline;
                color: #43a3d3;
            }}
            QPushButton#closeBtn {{
                background: transparent;
                color: #6e7075;
                border: none;
                font-size: 16px;
                font-weight: bold;
                padding: 0px 5px;
            }}
            QPushButton#closeBtn:hover {{
                color: #bcbec4;
            }}
        """)

        frame_layout = QHBoxLayout(self.bg_frame)
        frame_layout.setContentsMargins(12, 8, 12, 8)
        frame_layout.setSpacing(12)

        # Custom Progress Icon
        self.icon_widget = ToastIcon(toast_type["icon"], toast_type["text"])
        frame_layout.addWidget(self.icon_widget, 0, Qt.AlignVCenter)

        # Message Body
        self.msg_label = QLabel(message)
        self.msg_label.setWordWrap(True)
        frame_layout.addWidget(self.msg_label, 1, Qt.AlignVCenter)

        # Action Button (Optional)
        if action_text and action_callback:
            self.action_btn = QPushButton(action_text)
            self.action_btn.setObjectName("actionBtn")
            self.action_btn.setCursor(Qt.PointingHandCursor)
            self.action_btn.clicked.connect(self._handle_action)
            frame_layout.addWidget(self.action_btn, 0, Qt.AlignVCenter)

        # Close Button
        self.close_btn = QPushButton("×")
        self.close_btn.setObjectName("closeBtn")
        self.close_btn.setCursor(Qt.PointingHandCursor)
        self.close_btn.setToolTip("Dismiss")
        self.close_btn.clicked.connect(self.hide_toast)
        frame_layout.addWidget(self.close_btn, 0, Qt.AlignVCenter)

        layout.addWidget(self.bg_frame)

        # --- Animations ---
        # Master Timer & Progress Ring
        self.prog_anim = QVariantAnimation(self)
        self.prog_anim.setStartValue(1.0)
        self.prog_anim.setEndValue(0.0)
        self.prog_anim.setDuration(duration)
        self.prog_anim.valueChanged.connect(self.icon_widget.set_progress)
        self.prog_anim.finished.connect(self.hide_toast)

        # Fade In/Out
        self.opacity_effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.opacity_effect)
        self.fade_anim = QPropertyAnimation(self.opacity_effect, b"opacity")
        self.fade_anim.setDuration(300)

    # --- Event Handlers ---

    def enterEvent(self, event):
        """Pause the countdown when the user hovers over the toast."""
        self.prog_anim.pause()
        super().enterEvent(event)

    def leaveEvent(self, event):
        """Resume the countdown when the mouse leaves."""
        self.prog_anim.resume()
        super().leaveEvent(event)

    def mousePressEvent(self, event):
        """Handle global click on the toast body."""
        if self.click_callback and event.button() == Qt.LeftButton:
            self.click_callback()
            self.hide_toast()
        super().mousePressEvent(event)

    def _handle_action(self):
        """Execute specific action button callback."""
        if self.action_callback:
            self.action_callback()
        self.hide_toast()

    # --- Lifecycle ---

    def show_toast(self):
        self.show()
        self.fade_anim.setStartValue(0)
        self.fade_anim.setEndValue(1)
        self.fade_anim.setEasingCurve(QEasingCurve.OutCubic)
        self.fade_anim.start()
        self.prog_anim.start()

    def hide_toast(self):
        if self.fade_anim.state() == QPropertyAnimation.Running and self.fade_anim.endValue() == 0:
            return  # Already hiding

        self.prog_anim.stop()  # Stop progress ring
        self.fade_anim.setStartValue(self.opacity_effect.opacity())
        self.fade_anim.setEndValue(0)
        self.fade_anim.finished.connect(self.deleteLater)
        self.fade_anim.start()


class ToastManager:
    _toasts = []

    @classmethod
    def show(
        cls,
        message,
        toast_type=ToastType.INFO,
        duration=5000,
        action_text=None,
        action_callback=None,
        click_callback=None,
        parent=None,
    ):

        # 1. Resolve Parent: Use provided, or fallback to active, or abort
        target_parent = parent or QApplication.activeWindow()
        if not target_parent:
            return

        # Ensure we anchor to the top-level window if a child widget was passed
        target_window = target_parent.window()

        toast = ToastWidget(message, toast_type, duration, action_text, action_callback, click_callback, target_window)

        cls._toasts.append(toast)

        # Cleanup connections
        toast.destroyed.connect(lambda: cls._toasts.remove(toast) if toast in cls._toasts else None)
        toast.destroyed.connect(cls._reposition_toasts)

        cls._reposition_toasts()
        toast.show_toast()

    @classmethod
    def _reposition_toasts(cls):
        # We use a dict to keep track of the 'current_y' for EACH window separately
        # Key: Window Object, Value: current_y_anchor
        anchors = {}

        margin_right = 20
        margin_bottom = 20
        spacing = 10

        # Process from newest to oldest
        for toast in reversed(cls._toasts):
            parent = toast.parentWidget()
            if not parent:
                continue

            parent_geo = parent.geometry()

            # Initialize the anchor for this specific window if not already present
            if parent not in anchors:
                anchors[parent] = parent_geo.bottom() - margin_bottom

            toast.adjustSize()

            # Calculate X (right aligned to this specific parent)
            x = parent_geo.right() - toast.width() - margin_right

            # Calculate Y (stacked up from this parent's specific anchor)
            y = anchors[parent] - toast.height()

            toast.move(x, y)

            # Update the anchor for the NEXT toast in THIS window
            anchors[parent] = y - spacing

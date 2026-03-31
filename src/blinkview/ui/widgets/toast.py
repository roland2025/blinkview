# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import QEasingCurve, QObject, QPropertyAnimation, QRectF, Qt, QVariantAnimation, Signal, Slot
from qtpy.QtGui import QColor, QCursor, QPainter, QPen
from qtpy.QtWidgets import QApplication, QGraphicsOpacityEffect, QHBoxLayout, QLabel, QPushButton, QWidget


class ToastType:
    # (x, y) offset: +x moves right, +y moves down
    INFO = {"color": "#2b2d30", "icon": "ⓘ", "text": "#bcbec4", "offset": (0, 0)}
    SUCCESS = {"color": "#2e3c2e", "icon": "✔", "text": "#addb67", "offset": (0, 0)}
    WARNING = {"color": "#3e3925", "icon": "⚠", "text": "#e2c08d", "offset": (0, 2)}
    ERROR = {"color": "#482323", "icon": "✖", "text": "#ff5555", "offset": (0, 0)}


class ToastIcon(QLabel):
    def __init__(self, config, parent=None):
        super().__init__(config["icon"], parent)
        self._progress = 1.0
        self._color = QColor(config["text"])

        # Pull offset directly from the ToastType dict
        self.ring_offset = config.get("offset", (0, 0))

        self.setFixedSize(32, 36)
        self.setAlignment(Qt.AlignCenter)
        self.setStyleSheet(f"""
            background: transparent; 
            font-weight: bold; 
            font-size: 14px; 
            color: {config["text"]}; 
            padding-top: 2px;
        """)

    def set_progress(self, value):
        self._progress = value
        self.update()

    def paintEvent(self, event):
        painter = QPainter(self)
        painter.setRenderHint(QPainter.Antialiasing)

        # Create the circle rect and slide it by the per-type offset
        rect = QRectF(3, 3, 26, 26).translated(0.5 + self.ring_offset[0], 4 + self.ring_offset[1])

        # Draw track
        pen = QPen(QColor(255, 255, 255, 30))
        pen.setWidth(3)
        painter.setPen(pen)
        painter.drawEllipse(rect)

        # Draw progress
        pen.setColor(self._color)
        painter.setPen(pen)
        start_angle = 90 * 16
        span_angle = int(self._progress * 360 * 16)
        painter.drawArc(rect, start_angle, span_angle)

        painter.end()
        super().paintEvent(event)


class ToastWidget(QWidget):
    def __init__(
        self,
        message,
        toast_type=ToastType.INFO,
        duration=5.0,
        action_text=None,
        action_callback=None,
        click_callback=None,
        parent=None,
    ):
        super().__init__(parent)
        self.setWindowFlags(Qt.FramelessWindowHint | Qt.ToolTip | Qt.WindowStaysOnTopHint)
        self.setAttribute(Qt.WA_TranslucentBackground)

        self.setFixedWidth(300)

        self.is_hovered = False

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
        self.icon_widget = ToastIcon(toast_type)
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
        self.prog_anim.setDuration(int((duration * 1000)))
        self.prog_anim.valueChanged.connect(self.icon_widget.set_progress)
        self.prog_anim.finished.connect(self.hide_toast)

        # Fade In/Out
        self.opacity_effect = QGraphicsOpacityEffect(self)
        self.setGraphicsEffect(self.opacity_effect)
        self.fade_anim = QPropertyAnimation(self.opacity_effect, b"opacity")
        self.fade_anim.setDuration(300)

        self.setAttribute(Qt.WA_Hover)
        self.setMouseTracking(True)

    # --- Event Handlers ---

    def enterEvent(self, event):
        """Pause the countdown when the user hovers over the toast."""
        self.is_hovered = True
        self.prog_anim.pause()
        super().enterEvent(event)

    def leaveEvent(self, event):
        """Resume the countdown when the mouse leaves."""
        self.is_hovered = False
        self.prog_anim.resume()
        super().leaveEvent(event)

        ToastManager._reposition_toasts()

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
        duration=5.0,
        action_text=None,
        action_callback=None,
        click_callback=None,
        parent=None,
    ):
        print(f"[ToastManager]: show: {message}")
        # Resolve Parent: Use provided, or fallback to active, or abort
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
        anchors = {}
        margin_right = 20
        margin_bottom = 20
        spacing = 10

        for toast in cls._toasts:
            parent = toast.parentWidget()
            if not parent:
                continue

            parent_geo = parent.geometry()
            if parent not in anchors:
                anchors[parent] = parent_geo.bottom() - margin_bottom

            toast.adjustSize()
            x = parent_geo.right() - toast.width() - margin_right
            y = anchors[parent] - toast.height()

            # The clean, reliable hover check
            if not toast.is_hovered:
                toast.move(x, y)
                # Update the anchor based on where it safely moved
                anchors[parent] = y - spacing
            else:
                # The toast is frozen. Update the anchor based on its ACTUAL
                # physical position so the toasts above it don't collapse into it!
                anchors[parent] = toast.y() - spacing

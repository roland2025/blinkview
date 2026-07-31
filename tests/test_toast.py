# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.toast import ToastManager, ToastType, ToastWidget


@pytest.fixture(autouse=True)
def _clean_toast_manager_list():
    """ToastManager._toasts is a shared class-level list - isolate each test from whatever a
    previous one left behind."""
    ToastManager._toasts.clear()
    yield
    ToastManager._toasts.clear()


class TestToastWidgetConstruction:
    def test_message_text_is_set(self, qapp, qtbot):
        toast = ToastWidget("hello world")
        qtbot.addWidget(toast)
        assert toast.msg_label.text() == "hello world"

    def test_action_button_present_when_text_and_callback_given(self, qapp, qtbot):
        toast = ToastWidget("msg", action_text="Undo", action_callback=lambda: None)
        qtbot.addWidget(toast)
        assert hasattr(toast, "action_btn")
        assert toast.action_btn.text() == "Undo"

    def test_action_button_absent_without_action_text(self, qapp, qtbot):
        toast = ToastWidget("msg", action_callback=lambda: None)
        qtbot.addWidget(toast)
        assert not hasattr(toast, "action_btn")

    def test_action_button_absent_without_action_callback(self, qapp, qtbot):
        toast = ToastWidget("msg", action_text="Undo")
        qtbot.addWidget(toast)
        assert not hasattr(toast, "action_btn")

    def test_close_button_always_present(self, qapp, qtbot):
        toast = ToastWidget("msg")
        qtbot.addWidget(toast)
        assert toast.close_btn.toolTip() == "Dismiss"


class TestToastWidgetInteraction:
    def test_mouse_press_triggers_click_callback_and_hides(self, qapp, qtbot):
        from qtpy.QtCore import Qt

        clicked = []
        toast = ToastWidget("msg", click_callback=lambda: clicked.append(True))
        qtbot.addWidget(toast)

        qtbot.mouseClick(toast, Qt.LeftButton)

        assert clicked == [True]
        assert toast.fade_anim.endValue() == 0  # hide_toast was triggered

    def test_action_button_click_runs_callback_and_hides(self, qapp, qtbot):
        from qtpy.QtCore import Qt

        ran = []
        toast = ToastWidget("msg", action_text="Retry", action_callback=lambda: ran.append(True))
        qtbot.addWidget(toast)

        qtbot.mouseClick(toast.action_btn, Qt.LeftButton)

        assert ran == [True]
        assert toast.fade_anim.endValue() == 0

    def test_close_button_click_hides_without_running_any_callback(self, qapp, qtbot):
        from qtpy.QtCore import Qt

        clicked = []
        toast = ToastWidget("msg", click_callback=lambda: clicked.append(True))
        qtbot.addWidget(toast)

        qtbot.mouseClick(toast.close_btn, Qt.LeftButton)

        assert clicked == []  # close button must not trigger the toast-body click callback
        assert toast.fade_anim.endValue() == 0

    def test_hover_pauses_and_leave_resumes_the_progress_animation(self, qapp, qtbot):
        toast = ToastWidget("msg")
        qtbot.addWidget(toast)
        toast.show_toast()

        assert toast.is_hovered is False

        toast.enterEvent(_fake_enter_event())
        assert toast.is_hovered is True

        toast.leaveEvent(_fake_leave_event())
        assert toast.is_hovered is False

    def test_hide_toast_is_idempotent_while_already_hiding(self, qapp, qtbot):
        """Calling hide_toast a second time while the fade-out animation is already running
        toward 0 must be a no-op, not restart/duplicate the fade."""
        toast = ToastWidget("msg")
        qtbot.addWidget(toast)
        toast.show_toast()

        toast.hide_toast()
        start_value_after_first = toast.fade_anim.startValue()

        toast.hide_toast()  # should be swallowed by the "already hiding" guard

        assert toast.fade_anim.startValue() == start_value_after_first


class TestPersistentToast:
    """A persistent toast (e.g. shutdown-compression progress) must not auto-dismiss just
    because its visual countdown-ring animation finished - only an explicit dismiss()/
    hide_toast() (or the close button) should hide it. See plans/expressive-sauteeing-sun.md."""

    def test_non_persistent_toast_hides_when_its_animation_finishes(self, qapp, qtbot):
        toast = ToastWidget("msg", duration=0.05, persistent=False)
        qtbot.addWidget(toast)
        toast.show_toast()

        toast.prog_anim.finished.emit()  # simulate the ring animation completing naturally

        assert toast.fade_anim.endValue() == 0  # hide_toast was triggered

    def test_persistent_toast_does_not_hide_when_its_animation_finishes(self, qapp, qtbot):
        toast = ToastWidget("msg", duration=0.05, persistent=True)
        qtbot.addWidget(toast)
        toast.show_toast()

        toast.prog_anim.finished.emit()  # ring finished, but the represented work might not be

        assert toast.fade_anim.state() != toast.fade_anim.State.Running or toast.fade_anim.endValue() != 0

    def test_persistent_toast_still_hides_via_explicit_dismiss(self, qapp, qtbot):
        toast = ToastWidget("msg", persistent=True)
        qtbot.addWidget(toast)
        toast.show_toast()

        toast.dismiss()

        assert toast.fade_anim.endValue() == 0

    def test_set_message_updates_the_label_text(self, qapp, qtbot):
        toast = ToastWidget("initial", persistent=True)
        qtbot.addWidget(toast)

        toast.set_message("updated text")

        assert toast.msg_label.text() == "updated text"

    def test_set_message_triggers_a_reposition(self, qapp, qtbot, monkeypatch):
        toast = ToastWidget("short", persistent=True)
        qtbot.addWidget(toast)

        calls = []
        monkeypatch.setattr(ToastManager, "_reposition_toasts", classmethod(lambda cls: calls.append(True)))

        toast.set_message("a much longer message that changes the widget's size")

        assert calls == [True]


class TestShowPersistent:
    def test_returns_the_toast_widget(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.resize(400, 300)

        toast = ToastManager.show_persistent("compressing...", parent=parent)

        assert isinstance(toast, ToastWidget)
        assert toast.msg_label.text() == "compressing..."
        assert toast.persistent is True

    def test_returned_toast_can_be_updated_and_dismissed(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.resize(400, 300)

        toast = ToastManager.show_persistent("compressing...", parent=parent)
        toast.set_message("compressing... 2 of 5")
        assert toast.msg_label.text() == "compressing... 2 of 5"

        toast.dismiss()
        assert toast.fade_anim.endValue() == 0

    def test_returns_none_without_a_resolvable_parent(self, qapp, monkeypatch):
        from qtpy.QtWidgets import QApplication

        monkeypatch.setattr(QApplication, "activeWindow", staticmethod(lambda: None))

        assert ToastManager.show_persistent("no parent available") is None


def _fake_enter_event():
    from qtpy.QtCore import QPointF
    from qtpy.QtGui import QEnterEvent

    return QEnterEvent(QPointF(0, 0), QPointF(0, 0), QPointF(0, 0))


def _fake_leave_event():
    from qtpy.QtCore import QEvent

    return QEvent(QEvent.Type.Leave)


class TestToastManager:
    def test_show_is_a_no_op_without_a_resolvable_parent(self, qapp, monkeypatch):
        from qtpy.QtWidgets import QApplication

        monkeypatch.setattr(QApplication, "activeWindow", staticmethod(lambda: None))

        ToastManager.show("no parent available")

        assert ToastManager._toasts == []

    def test_show_with_explicit_parent_registers_and_shows_the_toast(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.resize(400, 300)

        ToastManager.show("hello", parent=parent)

        assert len(ToastManager._toasts) == 1
        toast = ToastManager._toasts[0]
        assert toast.msg_label.text() == "hello"
        assert toast.isVisible()

    def test_toast_removed_from_list_once_destroyed(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.resize(400, 300)

        ToastManager.show("hello", parent=parent)
        toast = ToastManager._toasts[0]

        toast.deleteLater()
        qtbot.wait(50)  # let the deferred deletion + destroyed signal actually fire

        assert toast not in ToastManager._toasts

    def test_reposition_toasts_anchors_to_bottom_right_of_parent(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.setGeometry(0, 0, 400, 300)

        ToastManager.show("hello", parent=parent)
        toast = ToastManager._toasts[0]
        toast.adjustSize()

        ToastManager._reposition_toasts()

        expected_x = parent.geometry().right() - toast.width() - 20
        assert toast.x() == expected_x

    def test_reposition_toasts_stacks_multiple_toasts_upward(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.setGeometry(0, 0, 400, 300)

        ToastManager.show("first", parent=parent)
        ToastManager.show("second", parent=parent)

        assert len(ToastManager._toasts) == 2
        older, newer = ToastManager._toasts
        assert newer.y() < older.y()  # the newer toast stacks above the older one

    def test_reposition_toasts_skips_a_hovered_toast(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)
        parent.setGeometry(0, 0, 400, 300)

        ToastManager.show("hello", parent=parent)
        toast = ToastManager._toasts[0]
        toast.adjustSize()
        ToastManager._reposition_toasts()
        original_pos = (toast.x(), toast.y())

        toast.is_hovered = True
        toast.move(999, 999)  # simulate wherever it "safely" ended up while frozen
        ToastManager._reposition_toasts()

        assert (toast.x(), toast.y()) == (999, 999)  # untouched - reposition must skip it
        assert original_pos != (999, 999)

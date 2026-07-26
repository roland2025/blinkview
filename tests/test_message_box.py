# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""MessageBox.info/question/warning/critical all delegate to _show(), which blocks on
QMessageBox.exec() until a button is clicked - a QTimer.singleShot fires after exec() starts to
find the live QMessageBox instance and click a specific button, driving it the same way a real
user would rather than just closing the window (which wouldn't exercise button-return values)."""

from qtpy.QtCore import QTimer
from qtpy.QtWidgets import QApplication, QMessageBox

from blinkview.ui.widgets.message_box import MessageBox


def _click_button(role):
    """Schedules a click on the given StandardButton the instant the (only) live QMessageBox
    appears - must be scheduled before calling the blocking MessageBox.* method."""

    def _do_click():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox):
                w.button(role).click()
                return

    QTimer.singleShot(50, _do_click)


def test_btn_proxy_exposes_qmessagebox_standard_buttons(qapp):
    assert MessageBox.Btn.Yes == QMessageBox.StandardButton.Yes
    assert MessageBox.Btn.No == QMessageBox.StandardButton.No
    assert MessageBox.Btn.Ok == QMessageBox.StandardButton.Ok


def test_info_defaults_to_ok_button_and_returns_it_when_clicked(qapp):
    _click_button(QMessageBox.StandardButton.Ok)

    result = MessageBox.info(None, "Title", "Some info")

    assert result == QMessageBox.StandardButton.Ok


def test_question_defaults_to_yes_no_and_returns_the_clicked_button(qapp):
    _click_button(QMessageBox.StandardButton.Yes)

    result = MessageBox.question(None, "Confirm", "Are you sure?")

    assert result == QMessageBox.StandardButton.Yes


def test_question_no_click_returns_no(qapp):
    _click_button(QMessageBox.StandardButton.No)

    result = MessageBox.question(None, "Confirm", "Are you sure?")

    assert result == QMessageBox.StandardButton.No


def test_question_custom_buttons_and_default_are_honored(qapp):
    custom_buttons = QMessageBox.StandardButton.Abort | QMessageBox.StandardButton.Retry

    def _check_default_then_click():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox):
                assert w.defaultButton() is w.button(QMessageBox.StandardButton.Retry)
                w.button(QMessageBox.StandardButton.Abort).click()
                return

    QTimer.singleShot(50, _check_default_then_click)

    result = MessageBox.question(
        None, "Retry?", "Operation failed", buttons=custom_buttons, default_btn=QMessageBox.StandardButton.Retry
    )

    assert result == QMessageBox.StandardButton.Abort


def test_warning_defaults_to_ok_button(qapp):
    _click_button(QMessageBox.StandardButton.Ok)

    result = MessageBox.warning(None, "Warning", "Something looks off")

    assert result == QMessageBox.StandardButton.Ok


def test_critical_defaults_to_ok_button(qapp):
    _click_button(QMessageBox.StandardButton.Ok)

    result = MessageBox.critical(None, "Critical", "Something broke")

    assert result == QMessageBox.StandardButton.Ok


def test_info_sets_window_title_and_text(qapp):
    def _check_and_close():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox):
                assert w.windowTitle() == "My Title"
                assert w.text() == "My Text"
                w.button(QMessageBox.StandardButton.Ok).click()
                return

    QTimer.singleShot(50, _check_and_close)

    MessageBox.info(None, "My Title", "My Text")


def test_info_uses_information_icon(qapp):
    def _check_and_close():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox):
                assert w.icon() == QMessageBox.Icon.Information
                w.button(QMessageBox.StandardButton.Ok).click()
                return

    QTimer.singleShot(50, _check_and_close)

    MessageBox.info(None, "Title", "Text")


def test_critical_uses_critical_icon(qapp):
    def _check_and_close():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox):
                assert w.icon() == QMessageBox.Icon.Critical
                w.button(QMessageBox.StandardButton.Ok).click()
                return

    QTimer.singleShot(50, _check_and_close)

    MessageBox.critical(None, "Title", "Text")

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.utils import in_development as module
from blinkview.ui.utils.in_development import GITHUB_PROJECT, set_as_in_development


@pytest.fixture
def action(qapp):
    from qtpy.QtGui import QAction

    return QAction("My Feature")


class TestSetAsInDevelopment:
    def test_appends_soon_marker_to_the_text(self, action):
        set_as_in_development(action, parent_widget=None)
        assert action.text() == "My Feature (Soon™)"

    def test_does_not_double_append_the_marker(self, action):
        set_as_in_development(action, parent_widget=None)
        set_as_in_development(action, parent_widget=None)
        assert action.text().count("(Soon™)") == 1

    def test_keeps_the_action_enabled(self, action):
        action.setEnabled(False)
        set_as_in_development(action, parent_widget=None)
        assert action.isEnabled() is True

    def test_returns_the_target(self, action):
        assert set_as_in_development(action, parent_widget=None) is action

    def test_triggering_shows_the_teaser_with_original_text_as_feature_name(self, qapp, action, monkeypatch):
        calls = []
        monkeypatch.setattr(
            module, "show_feature_teaser", lambda parent, name, issue_no=None: calls.append((parent, name, issue_no))
        )

        parent = object()
        set_as_in_development(action, parent, issue_no=42)
        action.trigger()

        assert calls == [(parent, "My Feature", 42)]  # name falls back to the pre-suffix text

    def test_triggering_uses_explicit_feature_name_over_the_action_text(self, qapp, action, monkeypatch):
        calls = []
        monkeypatch.setattr(
            module, "show_feature_teaser", lambda parent, name, issue_no=None: calls.append((parent, name, issue_no))
        )

        set_as_in_development(action, None, feature_name="Custom Name")
        action.trigger()

        assert calls[0][1] == "Custom Name"


class TestShowFeatureTeaser:
    def _click_button(self, predicate):
        from qtpy.QtCore import QTimer
        from qtpy.QtWidgets import QApplication, QMessageBox

        def _do_click():
            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMessageBox):
                    for btn in w.buttons():
                        if predicate(btn):
                            btn.click()
                            return

        QTimer.singleShot(50, _do_click)

    def test_ok_button_does_not_open_a_url(self, qapp, qtbot, monkeypatch):
        from qtpy.QtGui import QDesktopServices
        from qtpy.QtWidgets import QMessageBox, QWidget

        opened = []
        monkeypatch.setattr(QDesktopServices, "openUrl", staticmethod(lambda url: opened.append(url)))

        parent = QWidget()
        qtbot.addWidget(parent)
        self._click_button(lambda b: b.text() == "OK")

        module.show_feature_teaser(parent, "My Feature")

        assert opened == []

    def test_github_button_without_issue_no_opens_the_issues_list(self, qapp, qtbot, monkeypatch):
        from qtpy.QtGui import QDesktopServices
        from qtpy.QtWidgets import QWidget

        opened = []
        monkeypatch.setattr(QDesktopServices, "openUrl", staticmethod(lambda url: opened.append(url.toString())))

        parent = QWidget()
        qtbot.addWidget(parent)
        self._click_button(lambda b: b.text() == "Open GitHub Issues")

        module.show_feature_teaser(parent, "My Feature")

        assert opened == [f"{GITHUB_PROJECT}/issues"]

    def test_github_button_with_issue_no_opens_the_specific_issue(self, qapp, qtbot, monkeypatch):
        from qtpy.QtGui import QDesktopServices
        from qtpy.QtWidgets import QWidget

        opened = []
        monkeypatch.setattr(QDesktopServices, "openUrl", staticmethod(lambda url: opened.append(url.toString())))

        parent = QWidget()
        qtbot.addWidget(parent)
        self._click_button(lambda b: b.text() == "View Issue #123")

        module.show_feature_teaser(parent, "My Feature", issue_no=123)

        assert opened == [f"{GITHUB_PROJECT}/issues/123"]

    def test_window_title_and_feature_name_are_set(self, qapp, qtbot, monkeypatch):
        from qtpy.QtGui import QDesktopServices
        from qtpy.QtWidgets import QApplication, QMessageBox, QWidget

        monkeypatch.setattr(QDesktopServices, "openUrl", staticmethod(lambda url: None))

        seen = {}

        def _check_and_click_ok():
            for w in QApplication.topLevelWidgets():
                if isinstance(w, QMessageBox):
                    seen["title"] = w.windowTitle()
                    seen["text"] = w.text()
                    for btn in w.buttons():
                        if btn.text() == "OK":
                            btn.click()
                            return

        from qtpy.QtCore import QTimer

        QTimer.singleShot(50, _check_and_click_ok)

        parent = QWidget()
        qtbot.addWidget(parent)
        module.show_feature_teaser(parent, "Amazing Feature")

        assert seen["title"] == "Work in Progress"
        assert "Amazing Feature" in seen["text"]

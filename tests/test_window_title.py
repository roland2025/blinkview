# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Every window/dialog title starts with "{project} / {profile} - " so several BlinkView instances
can be told apart in the taskbar / Alt+Tab (ui/utils/window_title.py)."""

import pytest
from qtpy.QtCore import QTimer
from qtpy.QtWidgets import QApplication, QMessageBox, QWidget

from blinkview.ui.main_window import BlinkMainWindow
from blinkview.ui.utils.window_title import set_title_prefix, titled
from blinkview.ui.widgets.message_box import MessageBox
from blinkview.ui.windows.detached_tab_window import DetachedTabWindow
from tests.fakes.real_registry import make_real_registry


def test_titled_is_unchanged_until_a_prefix_is_set():
    assert titled("Save layout") == "Save layout"


def test_titled_prepends_project_and_profile():
    set_title_prefix("proj", "can")
    assert titled("Save layout") == "proj / can - Save layout"


@pytest.fixture
def main_window(qapp, qtbot, tmp_path):
    registry = make_real_registry(tmp_path, "window_title_test")
    w = BlinkMainWindow(registry)
    qtbot.addWidget(w)
    yield w
    registry.stop()


def test_main_window_and_floating_windows_share_the_prefix(main_window, qtbot):
    fm = main_window.gui_context.registry.file_manager
    prefix = f"{fm.project_name} / {fm.profile_name} - "
    assert main_window.windowTitle().startswith(prefix)

    content = QWidget()
    floating = DetachedTabWindow(main_window.gui_context, content, "Live Logs")
    qtbot.addWidget(floating)
    assert floating.windowTitle() == f"{prefix}Live Logs - BlinkView"


def test_message_box_title_gets_the_prefix(qapp, qtbot):
    set_title_prefix("proj", "can")
    seen = {}

    def read_title_and_close():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox) and w.isVisible():
                seen["title"] = w.windowTitle()
                w.done(0)

    QTimer.singleShot(50, read_title_and_close)
    parent = QWidget()
    qtbot.addWidget(parent)
    MessageBox.info(parent, "Delete layout", "text")

    assert seen["title"] == "proj / can - Delete layout"

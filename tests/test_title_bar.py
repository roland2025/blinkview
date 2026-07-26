# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from qtpy.QtCore import QPoint, Qt

from blinkview.ui.widgets.title_bar import TitleBar


def _make_title_bar(qtbot, parent):
    qtbot.addWidget(parent)
    tb = TitleBar(parent)
    return tb


class TestConstruction:
    def test_title_label_shows_app_name(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        tb = _make_title_bar(qtbot, parent)
        assert tb.title_label.text() == "BlinkView"

    def test_window_control_buttons_exist(self, qapp, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        tb = _make_title_bar(qtbot, parent)
        assert tb.btn_min.text() == "─"
        assert tb.btn_max.text() == "▢"
        assert tb.btn_close.text() == "✕"


class TestWindowControls:
    def test_min_button_minimizes_the_parent(self, qapp, qtbot):
        from qtpy.QtWidgets import QMainWindow

        parent = QMainWindow()
        tb = _make_title_bar(qtbot, parent)
        parent.show()

        qtbot.mouseClick(tb.btn_min, Qt.LeftButton)

        assert parent.isMinimized()

    def test_close_button_closes_the_parent(self, qapp, qtbot):
        from qtpy.QtWidgets import QMainWindow

        parent = QMainWindow()
        tb = _make_title_bar(qtbot, parent)
        parent.show()

        qtbot.mouseClick(tb.btn_close, Qt.LeftButton)

        assert not parent.isVisible()

    def test_max_button_maximizes_then_restores(self, qapp, qtbot):
        from qtpy.QtWidgets import QMainWindow

        parent = QMainWindow()
        tb = _make_title_bar(qtbot, parent)
        parent.show()
        assert not parent.isMaximized()

        qtbot.mouseClick(tb.btn_max, Qt.LeftButton)
        assert parent.isMaximized()

        qtbot.mouseClick(tb.btn_max, Qt.LeftButton)
        assert not parent.isMaximized()


class TestDragToMove:
    def test_press_then_move_drags_the_parent_window(self, qapp, qtbot):
        from qtpy.QtWidgets import QMainWindow

        parent = QMainWindow()
        tb = _make_title_bar(qtbot, parent)
        parent.resize(800, 600)
        parent.move(100, 100)
        parent.show()

        qtbot.mousePress(tb, Qt.LeftButton, pos=QPoint(10, 10))
        start_pos = parent.pos()

        qtbot.mouseMove(tb, QPoint(60, 60))

        # The window should have moved by the same delta the cursor moved (50, 50).
        assert parent.pos() == start_pos + QPoint(50, 50)

    def test_mouse_press_without_left_button_does_not_set_drag_pos(self, qapp, qtbot):
        from qtpy.QtWidgets import QMainWindow

        parent = QMainWindow()
        tb = _make_title_bar(qtbot, parent)
        parent.show()

        qtbot.mousePress(tb, Qt.RightButton, pos=QPoint(10, 10))

        assert not hasattr(tb, "drag_pos")

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Multi-row selection/copy and the Filter button's active-filter border on a real
LogTableViewerWidget (real Registry/GUIContext, real mouse/key events on the canvas)."""

import pytest
from qtpy.QtCore import QEvent, QPoint, QPointF, Qt
from qtpy.QtGui import QMouseEvent
from qtpy.QtWidgets import QApplication

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.log_table_viewer import HEADER_HEIGHT, ROW_HEIGHT, LogTableViewerWidget
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_gui_context, make_real_registry

ROW_COUNT = 10


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "log_table_viewer_selection_test")
    yield reg
    reg.stop()


def _push_messages(registry, device, module, count):
    array_pool = registry.system_ctx.array_pool
    base = registry.now_ns()
    src = array_pool.create(PooledLogBatch, count, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(count):
            ts = base + i * 1_000_000
            src.insert_any(ts, ts, f"message{i}".encode("ascii"), level=LogLevel.INFO.value, module=module.id, device=device.id)
        registry.central.log_pool.batch_append(src)


def _make_viewer(qtbot, registry, state=None):
    viewer = LogTableViewerWidget(make_real_gui_context(registry), state=state)
    qtbot.addWidget(viewer)
    viewer.resize(900, 600)  # ~36 visible rows: all ROW_COUNT rows fit, row i == message i
    viewer.show()
    qtbot.waitExposed(viewer)
    return viewer


@pytest.fixture
def viewer(qapp, qtbot, registry):
    device = registry.id_registry.get_device("seldev")
    module = device.get_module("selmod")
    _push_messages(registry, device, module, ROW_COUNT)
    w = _make_viewer(qtbot, registry)
    w.model.prev_apply = 0
    w.apply_updates()
    assert w.model.row_count == ROW_COUNT
    return w


def _row_pos(viewer, row):
    """Viewport point inside visible row `row` (0 = top row on screen)."""
    return QPoint(20, HEADER_HEIGHT + row * ROW_HEIGHT + ROW_HEIGHT // 2)


def _click(qtbot, viewer, row, modifier=Qt.NoModifier):
    qtbot.mouseClick(viewer.view.viewport(), Qt.LeftButton, modifier, _row_pos(viewer, row))


def _drag(viewer, from_row, to_row):
    viewport = viewer.view.viewport()
    start, end = _row_pos(viewer, from_row), _row_pos(viewer, to_row)
    for etype, pos, buttons in (
        (QEvent.MouseButtonPress, start, Qt.LeftButton),
        (QEvent.MouseMove, end, Qt.LeftButton),
        (QEvent.MouseButtonRelease, end, Qt.NoButton),
    ):
        QApplication.sendEvent(
            viewport,
            QMouseEvent(etype, QPointF(pos), QPointF(viewport.mapToGlobal(pos)), Qt.LeftButton, buttons, Qt.NoModifier),
        )


def _copied_lines():
    return QApplication.clipboard().text().splitlines()


def _assert_lines_are_messages(lines, indices):
    assert len(lines) == len(indices)
    for line, i in zip(lines, indices):
        assert line.endswith(f"selmod: message{i}")
        assert " seldev " in line


class TestMultiRowSelection:
    def test_shift_click_selects_range_and_ctrl_c_copies_rows(self, qtbot, viewer):
        _click(qtbot, viewer, 2)
        _click(qtbot, viewer, 5, Qt.ShiftModifier)

        qtbot.keyClick(viewer.view, Qt.Key_C, Qt.ControlModifier)

        _assert_lines_are_messages(_copied_lines(), [2, 3, 4, 5])

    def test_shift_click_upwards_copies_in_log_order(self, qtbot, viewer):
        _click(qtbot, viewer, 6)
        _click(qtbot, viewer, 3, Qt.ShiftModifier)

        qtbot.keyClick(viewer.view, Qt.Key_C, Qt.ControlModifier)

        _assert_lines_are_messages(_copied_lines(), [3, 4, 5, 6])

    def test_drag_selects_range(self, qtbot, viewer):
        _drag(viewer, 1, 3)

        qtbot.keyClick(viewer.view, Qt.Key_C, Qt.ControlModifier)

        _assert_lines_are_messages(_copied_lines(), [1, 2, 3])

    def test_ctrl_a_selects_every_loaded_row(self, qtbot, viewer):
        qtbot.keyClick(viewer.view, Qt.Key_A, Qt.ControlModifier)
        qtbot.keyClick(viewer.view, Qt.Key_C, Qt.ControlModifier)

        _assert_lines_are_messages(_copied_lines(), list(range(ROW_COUNT)))

    def test_single_click_copies_one_row_and_does_not_pause(self, qtbot, viewer):
        _click(qtbot, viewer, 4)

        qtbot.keyClick(viewer.view, Qt.Key_C, Qt.ControlModifier)

        _assert_lines_are_messages(_copied_lines(), [4])
        assert viewer.is_paused is False
        assert viewer.model.mode == LogViewMode.LIVE

    def test_range_selection_pauses_live_tail(self, qtbot, viewer):
        """Like the text viewer's selection freeze: a multi-row selection must not scroll away."""
        _click(qtbot, viewer, 2)
        _click(qtbot, viewer, 5, Qt.ShiftModifier)

        assert viewer.is_paused is True
        assert viewer.model.mode == LogViewMode.HISTORY

    def test_selection_survives_the_pause_rebuild(self, qtbot, viewer):
        """The freeze rebuilds the store into a history window; the selection is keyed by seq,
        so the same rows stay selected (and still paint/copy) afterwards."""
        _click(qtbot, viewer, 2)
        _click(qtbot, viewer, 5, Qt.ShiftModifier)
        assert viewer.model.mode == LogViewMode.HISTORY

        viewer.view.copy_selected_rows()

        _assert_lines_are_messages(_copied_lines(), [2, 3, 4, 5])

    def test_clear_logs_drops_selection(self, qtbot, viewer):
        _click(qtbot, viewer, 2)
        _click(qtbot, viewer, 5, Qt.ShiftModifier)

        viewer.clear_logs()

        assert viewer.view.selected_range() is None


class TestContextMenu:
    def test_menu_copies_selected_rows_and_messages(self, qtbot, viewer):
        _click(qtbot, viewer, 1)
        _click(qtbot, viewer, 2, Qt.ShiftModifier)

        menu = viewer.view.build_context_menu(viewer.view._row_at(_row_pos(viewer, 2)))
        actions = {a.text(): a for a in menu.actions() if a.text()}

        actions["Copy 2 Rows"].trigger()
        _assert_lines_are_messages(_copied_lines(), [1, 2])

        actions["Copy 2 Messages"].trigger()
        assert _copied_lines() == ["message1", "message2"]

    def test_right_click_outside_selection_selects_that_row(self, qtbot, viewer):
        _click(qtbot, viewer, 1)
        _click(qtbot, viewer, 2, Qt.ShiftModifier)

        menu = viewer.view.build_context_menu(viewer.view._row_at(_row_pos(viewer, 7)))
        actions = {a.text(): a for a in menu.actions() if a.text()}

        actions["Copy Message"].trigger()
        assert _copied_lines() == ["message7"]


class TestFilterButtonBorder:
    def _filter_button(self, viewer):
        return viewer.toolbar.widgetForAction(viewer.action_toggle_filter)

    def test_border_follows_enable_toggle(self, qtbot, viewer):
        button = self._filter_button(viewer)
        assert not button.property("filterEnabled")

        viewer.filter_sidebar.action_enable.setChecked(True)
        assert button.property("filterEnabled") is True

        viewer.filter_sidebar.action_enable.setChecked(False)
        assert button.property("filterEnabled") is False

    def test_border_restored_from_saved_state(self, qapp, qtbot, registry):
        viewer = _make_viewer(qtbot, registry, state={"filter_sidebar": {"enabled": True}})

        assert self._filter_button(viewer).property("filterEnabled") is True

    def test_stylesheet_has_the_border_rule(self, viewer):
        assert 'QToolButton[filterEnabled="true"]' in viewer.styleSheet()

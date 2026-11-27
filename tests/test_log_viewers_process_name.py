# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The process-name column and process filtering on the real LogViewerWidget and
LogTableViewerWidget (real Registry/GUIContext, real pid_history)."""

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.log_table_viewer import LogTableCol, LogTableViewerWidget
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "log_viewers_process_name_test")
    device = reg.id_registry.get_device("phone")
    module = device.get_module("tag")
    reg.pid_history.update((device.id << 32) | 100, "com.example.app", 1_000)
    reg.pid_history.update((device.id << 32) | 200, "system_server", 1_000)

    src = reg.system_ctx.array_pool.create(
        PooledLogBatch, 4, 4096, has_levels=True, has_modules=True, has_devices=True, has_pids=True
    )
    with src:
        for ts, pid, msg in [(2_000, 100, "app one"), (2_001, 200, "server one"), (2_002, 100, "app two")]:
            src.insert_any(ts, ts, msg.encode(), level=LogLevel.INFO.value, module=module.id, device=device.id, pid=pid)
        reg.central.log_pool.batch_append(src)
    yield reg
    reg.stop()


def _text_viewer(qtbot, registry, state=None):
    viewer = LogViewerWidget(make_real_gui_context(registry), state=state)
    qtbot.addWidget(viewer)
    viewer.resize(1000, 600)
    viewer.show()
    qtbot.waitExposed(viewer)
    viewer.prev_apply = 0
    viewer.apply_updates()
    return viewer


def _text_lines(viewer):
    """After the next heartbeat: a settings change clears the view and refills it through the
    100 ms-throttled apply_updates(), so run that tick first."""
    viewer.prev_apply = 0
    viewer.apply_updates()
    return [line for line in viewer.text_area.document().toPlainText().splitlines() if line]


def _table_viewer(qtbot, registry):
    viewer = LogTableViewerWidget(make_real_gui_context(registry))
    qtbot.addWidget(viewer)
    viewer.resize(1000, 600)
    viewer.show()
    qtbot.waitExposed(viewer)
    viewer.model.prev_apply = 0
    viewer.apply_updates()
    return viewer


def _table_messages(viewer):
    return [viewer.model.get_cell(r, LogTableCol.MESSAGE) for r in range(viewer.model.row_count)]


class TestTextViewerProcessColumn:
    def test_proc_toggle_is_off_by_default(self, qapp, qtbot, registry):
        viewer = _text_viewer(qtbot, registry)

        assert viewer.column_actions["show_proc"].isChecked() is False
        assert not any("com.example.app" in line for line in _text_lines(viewer))

    def test_turning_proc_on_shows_process_names(self, qapp, qtbot, registry):
        viewer = _text_viewer(qtbot, registry)

        viewer.column_actions["show_proc"].setChecked(True)

        lines = _text_lines(viewer)
        assert len(lines) == 3
        assert lines[0].endswith("com.example.app tag: app one")
        assert lines[1].endswith("system_server tag: server one")

    def test_proc_column_is_saved_and_restored(self, qapp, qtbot, registry):
        viewer = _text_viewer(qtbot, registry)
        viewer.column_actions["show_proc"].setChecked(True)
        state = viewer.get_state()
        assert state["view_state"]["show_proc"] is True

        restored = _text_viewer(qtbot, registry, state=state)

        assert restored.column_actions["show_proc"].isChecked() is True
        assert _text_lines(restored)[0].endswith("com.example.app tag: app one")

    def test_all_toggle_leaves_proc_alone(self, qapp, qtbot, registry):
        """PROC is optional, like Date and Rx time: ALL doesn't switch it on."""
        viewer = _text_viewer(qtbot, registry)
        viewer.action_all.setChecked(False)
        viewer.action_all.setChecked(True)

        assert viewer.column_actions["show_proc"].isChecked() is False

    def test_level_colouring_still_finds_the_level_with_proc_on(self, qapp, qtbot, registry):
        """The highlighter locates the level by word position; the process name comes after the
        level, so turning PROC on must not move it."""
        viewer = _text_viewer(qtbot, registry)
        before = viewer.highlighter.level_index
        viewer.column_actions["show_proc"].setChecked(True)

        assert viewer.highlighter.level_index == before


class TestTextViewerProcessFilter:
    def test_kv_process_condition(self, qapp, qtbot, registry):
        viewer = _text_viewer(qtbot, registry)

        viewer._apply_kv_filter_text("process=example")

        lines = _text_lines(viewer)
        assert [line.rsplit(": ", 1)[1] for line in lines] == ["app one", "app two"]

    def test_search_box_matches_process_name(self, qapp, qtbot, registry):
        viewer = _text_viewer(qtbot, registry)

        viewer.search_box.setText("system_server")
        viewer._apply_search_text()

        assert [line.rsplit(": ", 1)[1] for line in _text_lines(viewer)] == ["server one"]


class TestTableViewerProcessFilter:
    def test_kv_process_condition(self, qapp, qtbot, registry):
        viewer = _table_viewer(qtbot, registry)

        viewer._apply_kv_filter_text("process=example")

        assert _table_messages(viewer) == ["app one", "app two"]

    def test_search_box_matches_process_name(self, qapp, qtbot, registry):
        viewer = _table_viewer(qtbot, registry)

        viewer.search_box.setText("system_server")
        viewer._apply_search_text()

        assert _table_messages(viewer) == ["server one"]

    def test_clearing_the_process_condition_brings_rows_back(self, qapp, qtbot, registry):
        viewer = _table_viewer(qtbot, registry)
        viewer._apply_kv_filter_text("process=example")
        viewer._apply_kv_filter_text("")

        assert _table_messages(viewer) == ["app one", "server one", "app two"]

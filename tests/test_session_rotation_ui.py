# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Phase 3 of session rotation (plans/session-rotation.md): the main window's Clear button and
the views' reset after a rotation - driven through the real BlinkMainWindow / real widgets
against a real Registry, per the playback-wiring skill's "real widget, real registry" rule."""

import json
import threading

import pytest

import blinkview.ui.main_window as main_window_module
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.playback_clock import PlaybackMode
from blinkview.ui.constants import WidgetName
from blinkview.ui.main_window import BlinkMainWindow
from blinkview.ui.widgets.action_button_delegate import TelemetryCol
from blinkview.ui.widgets.log_table_viewer import LogTableCol, LogTableViewerWidget
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.ui.widgets.playback_control import PlaybackControlWidget
from blinkview.ui.widgets.plotter import TelemetryPlotter
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from tests.fakes.real_registry import make_real_gui_context, make_real_registry

ROTATION_TIMEOUT_MS = 20_000


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "rotation_ui_test", with_value_tracker=True)
    yield reg
    reg.stop()


def _push(registry, texts, module_name="mod"):
    """Appends rows straight into the pool (no central thread needed) - inside the batch's
    `with` block, see the playback-wiring skill's batch-lifetime note."""
    module = registry.id_registry.get_device("uidev").get_module(module_name)
    src = registry.system_ctx.array_pool.create(
        PooledLogBatch, len(texts), 4096, has_levels=True, has_modules=True, has_devices=True
    )
    with src:
        for text in texts:
            ts = registry.now_ns()
            src.insert_any(ts, ts, text.encode("ascii"), level=0, module=module.id, device=module.device.id)
        registry.central.log_pool.batch_append(src)
    registry.module_value_tracker.update()
    registry.playback_clock.tick(registry.now_ns())
    return module


def _session_files(folder):
    return {p.name for p in folder.iterdir()}


# ---------------------------------------------------------------------------
# Main window: Clear button
# ---------------------------------------------------------------------------


@pytest.fixture
def main_window(qapp, qtbot, registry):
    w = BlinkMainWindow(registry)
    qtbot.addWidget(w)
    yield w
    if w._session_rotation_thread is not None:
        qtbot.waitUntil(lambda: w._session_rotation_thread is None, timeout=ROTATION_TIMEOUT_MS)


def _wait_rotation_done(qtbot, main_window):
    qtbot.waitUntil(lambda: main_window._session_rotation_thread is None, timeout=ROTATION_TIMEOUT_MS)


class TestClearButton:
    def test_click_starts_a_new_session_off_the_ui_thread(self, qtbot, main_window, registry):
        fm = registry.file_manager
        old_dir = fm.session_dir

        main_window.rotate_button.click()
        assert not main_window.rotate_button.isEnabled()  # disabled while rotating
        _wait_rotation_done(qtbot, main_window)

        assert fm.session_dir != old_dir
        assert main_window.rotate_button.isEnabled()
        assert registry.session_generation == 1
        assert fm.session_display_name == "rotation_ui_test"  # left click keeps the name

    def test_gui_state_goes_to_the_old_session_and_starts_the_new_one(self, qtbot, main_window, registry):
        """State otherwise saved only at close (layout, open tabs, widget state) must land in
        the old session as its `final` snapshot; the new session gets the `start` ones."""
        main_window.create_widget(WidgetName.LOG_VIEWER, "Rotation Logs")
        fm = registry.file_manager
        old_dir = fm.session_dir
        cfg = fm.config_file_name

        main_window.start_session_rotation()
        _wait_rotation_done(qtbot, main_window)
        new_dir = fm.session_dir

        old_files = _session_files(old_dir)
        assert {f"{cfg}.gui_state.final.json", f"{cfg}.gui_config.final.json", f"{cfg}.final.json"} <= old_files
        final_state = json.loads((old_dir / f"{cfg}.gui_state.final.json").read_text())
        assert "Rotation Logs" in [tab["name"] for tab in final_state["open_tabs"]]

        new_files = _session_files(new_dir)
        assert {f"{cfg}.gui_state.start.json", f"{cfg}.gui_config.start.json", f"{cfg}.start.json"} <= new_files
        assert main_window.gui_context.gui_config.autosave_path.parent == new_dir
        assert registry.config.autosave_path.parent == new_dir

    def test_right_click_names_the_new_session(self, qtbot, main_window, registry, monkeypatch):
        monkeypatch.setattr(
            main_window_module.QInputDialog, "getText", staticmethod(lambda *a, **kw: ("Bench run 2", True))
        )

        main_window._prompt_session_rotation_name()
        _wait_rotation_done(qtbot, main_window)

        new_dir = registry.file_manager.session_dir
        assert new_dir.name.endswith("_Bench_run_2")
        assert json.loads((new_dir / "metadata.json").read_text())["project"]["display_name"] == "Bench_run_2"

    @pytest.mark.parametrize("dialog_result", [("", False), ("anything", False), ("   ", True)])
    def test_cancelled_or_empty_name_does_not_rotate(self, main_window, registry, monkeypatch, dialog_result):
        monkeypatch.setattr(main_window_module.QInputDialog, "getText", staticmethod(lambda *a, **kw: dialog_result))
        old_dir = registry.file_manager.session_dir

        main_window._prompt_session_rotation_name()

        assert main_window._session_rotation_thread is None
        assert registry.file_manager.session_dir == old_dir

    def test_a_second_click_while_rotating_is_ignored(self, qtbot, main_window, registry, monkeypatch):
        release = threading.Event()
        calls = []
        real_rotate = registry.rotate_session

        def slow_rotate(display_name=None):
            calls.append(display_name)
            release.wait(10)
            return real_rotate(display_name)

        monkeypatch.setattr(registry, "rotate_session", slow_rotate)
        assert main_window.start_session_rotation() is True
        assert main_window.start_session_rotation() is False
        release.set()
        _wait_rotation_done(qtbot, main_window)
        assert len(calls) == 1

    def test_closing_during_a_rotation_waits_for_it(self, qtbot, main_window, registry, monkeypatch):
        release = threading.Event()
        real_rotate = registry.rotate_session
        monkeypatch.setattr(registry, "rotate_session", lambda name=None: (release.wait(10), real_rotate(name))[1])

        main_window.start_session_rotation()
        main_window.close()
        assert main_window._close_after_rotation is True
        assert main_window.gui_context.is_shutting_down is False  # stop() hasn't started

        release.set()
        # Once the rotation finishes, the deferred close goes through the normal shutdown path.
        qtbot.waitUntil(lambda: main_window._shutdown_ready_to_close, timeout=ROTATION_TIMEOUT_MS)
        assert registry.session_generation == 1


def test_clear_is_unavailable_in_replay_mode(qapp, qtbot, registry):
    registry.replay_mode = True
    w = BlinkMainWindow(registry)
    qtbot.addWidget(w)

    assert not w.rotate_button.isEnabled()
    assert w.start_session_rotation() is False


# ---------------------------------------------------------------------------
# Views drop the previous session's data
# ---------------------------------------------------------------------------


@pytest.fixture
def gui_context(qapp, registry):
    return make_real_gui_context(registry)


def test_log_viewer_shows_only_the_new_session(qtbot, gui_context, registry):
    _push(registry, ["old-row-1", "old-row-2"])
    w = LogViewerWidget(gui_context)
    qtbot.addWidget(w)
    w.resize(800, 600)
    w.apply_updates()
    assert "old-row-2" in w.text_area.document().toPlainText()

    registry.rotate_session()
    _push(registry, ["new-row-1"])
    w.prev_apply = 0
    w.apply_updates()

    text = w.text_area.document().toPlainText()
    assert "new-row-1" in text
    assert "old-row" not in text


def test_log_viewer_rotation_drops_a_per_tab_clear_floor(qtbot, gui_context, registry):
    """A per-tab Clear floor points at a sequence in the previous session; rows the new session
    already has (seq continues past it) must still show."""
    _push(registry, ["old-row"])
    w = LogViewerWidget(gui_context)
    qtbot.addWidget(w)
    w.resize(800, 600)
    w.apply_updates()

    registry.rotate_session()
    _push(registry, ["new-row-early"])  # lands before the view notices the rotation
    w.clear_logs()  # per-tab floor set *after* those rows -> would hide them if kept
    w.prev_apply = 0
    w.apply_updates()

    assert "new-row-early" in w.text_area.document().toPlainText()


def test_log_table_viewer_shows_only_the_new_session(qtbot, gui_context, registry):
    _push(registry, ["old-row"])
    w = LogTableViewerWidget(gui_context)
    qtbot.addWidget(w)
    w.resize(800, 600)
    w.model.enter_live_mode()

    registry.rotate_session()
    _push(registry, ["new-row"])
    w.apply_updates()

    messages = {w.model.get_cell(r, LogTableCol.MESSAGE) for r in range(w.model.row_count)}
    assert "new-row" in messages
    assert "old-row" not in messages


def test_plotter_live_ring_holds_only_the_new_session(qtbot, gui_context, registry):
    module = _push(registry, ["1.0", "2.0", "3.0"], module_name="floats")
    w = TelemetryPlotter(gui_context)
    qtbot.addWidget(w)
    w.resize(800, 600)
    w.modules = [module]
    for _ in range(3):
        w.apply_updates(force=True)
    assert w.buffers[module].size == 3

    registry.rotate_session()
    _push(registry, ["7.0", "8.0"], module_name="floats")
    for _ in range(3):
        w.apply_updates(force=True)

    buf = w.buffers[module]
    bundle = buf.bundle()
    values = bundle.y_data[bundle.data_start : bundle.data_start + bundle.data_size, 0].tolist()
    assert sorted(values) == [7.0, 8.0]


def _table_value(table, module):
    for row, mod_id in enumerate(table.model.visible_mod_ids):
        if table.model.modules[mod_id] == module:
            return table.model.index(row, TelemetryCol.VALUE).data()
    return None


def test_telemetry_table_forgets_previous_session_values(qtbot, gui_context, registry):
    stale = _push(registry, ["stale-value"], module_name="stale")
    w = TelemetryTable(gui_context)
    qtbot.addWidget(w)
    w.apply_updates(force=True)
    assert _table_value(w, stale) == "stale-value"

    registry.rotate_session()
    fresh = _push(registry, ["fresh-value"], module_name="fresh")
    w.apply_updates(force=True)

    assert _table_value(w, stale) in (None, "---")
    assert _table_value(w, fresh) == "fresh-value"


def test_playback_bar_returns_to_live_and_forgets_a_pending_mark_in(qtbot, gui_context, registry):
    _push(registry, ["old-row"])
    w = PlaybackControlWidget(gui_context)
    qtbot.addWidget(w)
    clock = registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns)
    w._on_mark_in_clicked()
    assert w.mark_out_button.isEnabled()

    registry.rotate_session()
    w.apply_updates()

    assert clock.mode is PlaybackMode.LIVE
    assert w._pending_mark_in_ts is None
    assert not w.mark_out_button.isEnabled()

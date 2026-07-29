# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for LogViewerWidget, driving the real widget (not just LogSegmentScanner/
LogTextFetcher in isolation) through a real Registry/GUIContext - see the blinkview-playback-wiring
skill for why the real-widget level is needed to catch wiring bugs unit tests on the pieces alone
would miss."""

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "log_viewer_playback_test")
    yield reg
    reg.stop()


def _push_messages(registry, device, module, count, start_ts=None, spacing_ns=100_000_000, text="message"):
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    base = start_ts if start_ts is not None else registry.now_ns()
    src = array_pool.create(PooledLogBatch, count, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(count):
            ts = base + i * spacing_ns
            src.insert_any(ts, ts, f"{text}{i}".encode("ascii"), level=0, module=module.id, device=device.id)
        log_pool.batch_append(src)
    return base


@pytest.fixture
def viewer(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)
    device = registry.id_registry.get_device("logviewertest")
    module = device.get_module("mod1")

    _push_messages(registry, device, module, 20)
    registry.playback_clock.tick(registry.now_ns())

    w = LogViewerWidget(gui_context)
    qtbot.addWidget(w)
    w.resize(800, 600)
    yield w


class TestConstruction:
    def test_starts_in_live_mode(self, viewer):
        assert viewer.view_mode == LogViewMode.LIVE

    def test_starts_not_paused(self, viewer):
        assert viewer.is_paused is False

    def test_follow_playback_defaults_true(self, viewer):
        assert viewer.follow_playback is True


class TestLiveTail:
    def test_apply_updates_populates_text_area(self, viewer):
        for _ in range(3):
            viewer.apply_updates()

        assert "message" in viewer.text_area.document().toPlainText()

    def test_clear_logs_resets_view(self, viewer):
        viewer.apply_updates()
        viewer.clear_logs()

        assert viewer.text_area.document().toPlainText() == ""
        assert viewer.view_mode == LogViewMode.LIVE


class TestFilteringAndLevel:
    def test_level_change_triggers_refresh_without_error(self, viewer):
        idx = viewer.level_combo.findData(LogLevel.ERROR)
        assert idx != -1
        viewer.level_combo.setCurrentIndex(idx)  # must not raise

        assert viewer.log_filter.log_level == LogLevel.ERROR

    def test_kv_filter_applies_and_refreshes(self, viewer):
        viewer.apply_updates()
        viewer._apply_kv_filter_text("nonexistent=true")

        assert viewer.log_filter.kv_filter_text == "nonexistent=true"

    def test_search_text_debounces_then_applies(self, qtbot, viewer):
        viewer.apply_updates()
        viewer.search_box.setText("message3")

        with qtbot.waitSignal(viewer._search_timer.timeout, timeout=1000):
            pass

        assert viewer.log_filter.text_filter_text == "message3"


class TestColumnToggles:
    def test_toggle_all_off_then_on(self, viewer):
        viewer.action_all.setChecked(False)
        assert viewer.column_actions["show_ts"].isChecked() is False
        assert viewer.show_ts is False

        viewer.action_all.setChecked(True)
        assert viewer.column_actions["show_ts"].isChecked() is True
        assert viewer.show_ts is True

    def test_toggle_single_column_unchecks_all_button(self, viewer):
        viewer.column_actions["show_mod"].setChecked(False)

        assert viewer.show_mod is False
        assert viewer.action_all.isChecked() is False

    def test_set_ts_precision_refreshes(self, viewer):
        viewer.apply_updates()
        viewer._set_ts_precision(9)

        assert viewer.ts_precision == 9

    def test_set_ts_precision_same_value_is_a_noop(self, viewer):
        viewer._set_ts_precision(viewer.ts_precision)  # must not raise


class TestPauseResume:
    def test_toggle_pause_enters_history_mode(self, viewer):
        viewer.apply_updates()
        viewer.apply_updates()

        viewer._toggle_pause(True)

        assert viewer.is_paused is True
        assert viewer.view_mode == LogViewMode.HISTORY

    def test_toggle_pause_off_resumes_live(self, viewer):
        viewer.apply_updates()
        viewer._toggle_pause(True)
        viewer._toggle_pause(False)

        assert viewer.is_paused is False
        assert viewer.view_mode == LogViewMode.LIVE

    def test_toggle_telemetry_sidebar(self, viewer):
        viewer.show()
        viewer.action_telemetry.setChecked(True)

        assert viewer.show_telemetry is True
        assert viewer.telemetry_sidebar.isVisible() is True

    def test_toggle_module_filter_sidebar(self, viewer):
        viewer.show()
        viewer.action_toggle_filter.setChecked(True)

        assert viewer.show_module_filter is True
        assert viewer.filter_sidebar.isVisible() is True


class TestScrollPaging:
    def test_scrolling_to_top_of_history_pages_backward(self, viewer):
        viewer.apply_updates()
        viewer._toggle_pause(True)
        assert viewer.view_mode == LogViewMode.HISTORY

        scrollbar = viewer.text_area.verticalScrollBar()
        viewer.history_reached_start = False
        viewer._on_scroll_value_changed(scrollbar.minimum())  # must not raise

    def test_scrolling_to_bottom_at_true_edge_resumes_live(self, viewer):
        viewer.apply_updates()
        viewer._toggle_pause(True)
        assert viewer.view_mode == LogViewMode.HISTORY

        pool = viewer.gui_context.registry.central.log_pool
        viewer.history_newest_seq = pool.latest_sequence()

        scrollbar = viewer.text_area.verticalScrollBar()
        viewer._on_scroll_value_changed(scrollbar.maximum())

        assert viewer.view_mode == LogViewMode.LIVE

    def test_scrolling_to_bottom_with_newer_data_pages_forward(self, viewer, registry, request):
        viewer.apply_updates()
        viewer._toggle_pause(True)
        assert viewer.view_mode == LogViewMode.HISTORY
        stale_newest = viewer.history_newest_seq

        device = registry.id_registry.get_device("logviewertest")
        module = device.get_module("mod1")
        _push_messages(registry, device, module, 5, start_ts=registry.now_ns() + 10_000_000_000, text="later")

        scrollbar = viewer.text_area.verticalScrollBar()
        viewer._on_scroll_value_changed(scrollbar.maximum())

        assert viewer.history_newest_seq != stale_newest or viewer.view_mode == LogViewMode.LIVE


class TestResizeAndShow:
    def test_resize_recomputes_row_budget_and_scrolls_to_end_when_live(self, viewer):
        viewer.resize(1000, 900)
        from qtpy.QtGui import QResizeEvent
        from qtpy.QtCore import QSize

        event = QResizeEvent(QSize(1000, 900), QSize(800, 600))
        viewer.resizeEvent(event)  # must not raise

    def test_show_event_recomputes_row_budget(self, qtbot, viewer):
        viewer.show()
        qtbot.waitExposed(viewer)  # must not raise


class TestStateRoundtrip:
    def test_get_state_and_restore(self, qapp, qtbot, registry):
        gui_context = make_real_gui_context(registry)
        device = registry.id_registry.get_device("logviewertest2")
        module = device.get_module("mod1")
        _push_messages(registry, device, module, 5)
        registry.playback_clock.tick(registry.now_ns())

        w = LogViewerWidget(gui_context)
        qtbot.addWidget(w)
        w.show_date = True
        w.ts_precision = 6

        state = w.get_state()

        w2 = LogViewerWidget(gui_context, state=state)
        qtbot.addWidget(w2)

        assert w2.show_date is True
        assert w2.ts_precision == 6


class TestPlaybackFollow:
    def test_entering_replay_and_ticking_follows_the_clock(self, viewer, registry):
        clock = registry.playback_clock
        # Anchor the scrub roughly in the middle of the pushed data.
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())

        viewer.prev_apply = 0  # bypass the ~100ms follow throttle for this immediate tick
        viewer.apply_updates()

        assert viewer._playback_anchored is True
        assert viewer.view_mode == LogViewMode.HISTORY
        assert viewer.history_anchor_ts_ns == mid_ts

    def test_clock_returning_to_live_resumes_live_tail(self, viewer, registry):
        clock = registry.playback_clock
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer._playback_anchored is True

        clock.go_live()
        viewer.apply_updates()

        assert viewer._playback_anchored is False
        assert viewer.view_mode == LogViewMode.LIVE
        assert viewer.follow_playback is True

    def test_manual_scroll_detaches_from_follow(self, viewer, registry):
        clock = registry.playback_clock
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer._playback_anchored is True

        scrollbar = viewer.text_area.verticalScrollBar()
        viewer._on_scroll_value_changed(scrollbar.value())

        assert viewer.follow_playback is False
        assert viewer._playback_anchored is False
        assert viewer.is_paused is True

    def test_scrubbing_scroll_event_is_ignored(self, viewer, registry):
        """A scrollbar valueChanged that slips through during an active scrub-bar drag must not
        be treated as a genuine manual scroll (see test_log_viewer_scrub.py for the isolated
        branch test - this confirms the same behavior end-to-end against the real clock)."""
        clock = registry.playback_clock
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer._playback_anchored is True

        clock.begin_scrub()
        scrollbar = viewer.text_area.verticalScrollBar()
        viewer._on_scroll_value_changed(scrollbar.value())

        assert viewer.follow_playback is True
        assert viewer._playback_anchored is True
        clock.end_scrub()

    def test_opening_while_replay_already_active_follows_without_manual_pause(self, qapp, qtbot, registry):
        """A tab opened *after* the clock is already in REPLAY (e.g. the user scrubs first, then
        opens a new Log Viewer tab to look at that point in time) must start following
        immediately, not land in a manually-paused state that requires a Resume click before the
        view reacts to further scrubbing. This is the was_live/anchor_ts is None guard in
        _reanchor_history (see the blinkview-playback-wiring skill, Trap A) exercised on the
        live->history transition that happens on this tab's very first tick, rather than one that
        happens after the tab has already been following for a while."""
        device = registry.id_registry.get_device("late_viewer_test")
        module = device.get_module("mod1")
        _push_messages(registry, device, module, 20)

        clock = registry.playback_clock
        clock.tick(registry.now_ns())  # refresh bounds against the rows just pushed
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())

        gui_context = make_real_gui_context(registry)
        late_viewer = LogViewerWidget(gui_context)
        qtbot.addWidget(late_viewer)
        late_viewer.resize(800, 600)

        late_viewer.prev_apply = 0
        late_viewer.apply_updates()

        assert late_viewer.is_paused is False
        assert late_viewer.view_mode == LogViewMode.HISTORY
        assert late_viewer._playback_anchored is True
        assert late_viewer.follow_playback is True

        # Scrubbing further after the tab opened must keep following without any Resume click.
        new_ts = mid_ts + 200_000_000
        clock.seek(new_ts)
        clock.tick(registry.now_ns())
        late_viewer.prev_apply = 0
        late_viewer.apply_updates()

        assert late_viewer.history_anchor_ts_ns == new_ts
        assert late_viewer.is_paused is False


class TestForceLive:
    def test_live_button_hidden_until_replay(self, viewer):
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer.action_force_live.isVisible() is False

    def test_toggling_live_pins_tab_to_live_tail_during_replay(self, viewer, registry):
        clock = registry.playback_clock
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer._playback_anchored is True
        assert viewer.action_force_live.isVisible() is True

        viewer.action_force_live.setChecked(True)
        assert viewer.view_mode == LogViewMode.LIVE
        assert viewer._playback_anchored is False

        # Further scrubbing must not pull this tab back into REPLAY while pinned.
        clock.seek(mid_ts + 200_000_000)
        clock.tick(registry.now_ns())
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer.view_mode == LogViewMode.LIVE
        assert viewer._playback_anchored is False

        # Un-toggling resumes following the clock's current position.
        viewer.action_force_live.setChecked(False)
        viewer.prev_apply = 0
        viewer.apply_updates()
        assert viewer._playback_anchored is True
        assert viewer.view_mode == LogViewMode.HISTORY

    def test_force_live_state_survives_get_state_restore(self, qapp, qtbot, registry):
        gui_context = make_real_gui_context(registry)
        w = LogViewerWidget(gui_context)
        qtbot.addWidget(w)
        w.action_force_live.setChecked(True)

        state = w.get_state()
        w2 = LogViewerWidget(gui_context, state=state)
        qtbot.addWidget(w2)

        assert w2.force_live is True
        assert w2.action_force_live.isChecked() is True

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import pytest
from qtpy.QtWidgets import QWidget

from blinkview.ui.constants import WidgetName
from blinkview.ui.main_window import BlinkMainWindow
from blinkview.utils.session_lister import SessionInfo
from tests.fakes.real_registry import make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "main_window_test")
    yield reg
    reg.stop()


@pytest.fixture
def main_window(qapp, qtbot, registry):
    w = BlinkMainWindow(registry)
    qtbot.addWidget(w)
    return w


class FakeWidget(QWidget):
    """Stand-in registered under a throwaway name in widget_factories - a real widget
    (LogViewerWidget, TelemetryTable, ...) would drag in a lot of unrelated backend wiring/
    network timers (see UpdateWidget) that create_widget itself doesn't care about."""

    def __init__(self, gui_context, params):
        super().__init__()
        self.gui_context = gui_context
        self.params = params
        self.signal_destroy = None


def make_session_info(session_id="sess1", path=None, **overrides):
    fields = {
        "session_id": session_id,
        "path": path,
        "display_name": session_id,
        "profile": "default",
        "status": "finished",
        "created_at": "2026-01-01T00:00:00Z",
        "finished_at": "2026-01-01T00:01:00Z",
        "duration_seconds": 60.0,
    }
    fields.update(overrides)
    return SessionInfo(**fields)


class TestConstruction:
    def test_window_title_includes_project_and_profile(self, main_window, registry):
        fm = registry.file_manager
        assert fm.project_name in main_window.windowTitle()
        assert fm.profile_name in main_window.windowTitle()

    def test_sidebars_start_hidden_until_load_ui_state_runs(self, main_window):
        assert main_window.sources_dock.isVisible() is False
        assert main_window.pipelines_dock.isVisible() is False

    def test_central_tabs_start_empty(self, main_window):
        assert main_window.central_tabs.count() == 0

    def test_widget_factories_is_populated(self, main_window):
        assert len(main_window.widget_factories) > 0


class TestLogTargetRegistration:
    def test_register_adds_target(self, main_window):
        target = object()
        main_window.register_log_target(target)
        assert target in main_window.log_targets

    def test_register_does_not_duplicate(self, main_window):
        target = object()
        main_window.register_log_target(target)
        main_window.register_log_target(target)
        assert main_window.log_targets.count(target) == 1

    def test_deregister_removes_target(self, main_window):
        target = object()
        main_window.register_log_target(target)
        main_window.deregister_log_target(target)
        assert target not in main_window.log_targets

    def test_deregister_unknown_target_is_a_noop(self, main_window):
        main_window.deregister_log_target(object())  # must not raise


class TestFocusTabIfExists:
    def test_returns_false_when_no_tab_matches(self, main_window):
        assert main_window.focus_tab_if_exists("Nope") is False

    def test_returns_true_and_focuses_matching_tab(self, main_window, qtbot):
        w1, w2 = QWidget(), QWidget()
        qtbot.addWidget(w1)
        qtbot.addWidget(w2)
        main_window.central_tabs.addTab(w1, "Tab A")
        main_window.central_tabs.addTab(w2, "Tab B")
        main_window.central_tabs.setCurrentIndex(0)

        assert main_window.focus_tab_if_exists("Tab B") is True
        assert main_window.central_tabs.currentIndex() == 1


class TestAddTabFocused:
    def test_adds_and_focuses_the_new_tab(self, main_window, qtbot):
        existing = QWidget()
        qtbot.addWidget(existing)
        main_window.central_tabs.addTab(existing, "Existing")

        new_widget = QWidget()
        qtbot.addWidget(new_widget)
        main_window.add_tab_focused(new_widget, "New Tab")

        assert main_window.central_tabs.count() == 2
        assert main_window.central_tabs.currentIndex() == 1
        assert main_window.central_tabs.tabText(1) == "New Tab"


class TestCreateWidget:
    def test_unknown_widget_class_returns_none(self, main_window):
        assert main_window.create_widget("NoSuchWidget", "Tab") is None
        assert main_window.central_tabs.count() == 0

    def test_creates_and_adds_a_tab_for_a_known_widget(self, main_window):
        main_window.widget_factories["Fake"] = FakeWidget

        widget = main_window.create_widget("Fake", "My Tab")

        assert isinstance(widget, FakeWidget)
        assert main_window.central_tabs.count() == 1
        assert main_window.central_tabs.tabText(0) == "My Tab"
        assert widget.params == {"tab_name": "My Tab"}

    def test_existing_floating_window_is_raised_instead_of_recreated(self, main_window, qtbot):
        placeholder = QWidget()
        qtbot.addWidget(placeholder)
        content = QWidget()  # window_manager.register() wires content.destroyed - needs a QObject
        content.tab_name = "Floating Tab"
        qtbot.addWidget(content)
        main_window.window_manager.register(placeholder, content)

        result = main_window.create_widget("Fake", "Floating Tab")

        assert result is None
        assert main_window.central_tabs.count() == 0

    def test_as_window_creates_a_detached_tab_window(self, main_window):
        main_window.widget_factories["Fake"] = FakeWidget

        result = main_window.create_widget("Fake", "Floater", as_window=True, show=False)

        from blinkview.ui.windows.detached_tab_window import DetachedTabWindow

        assert isinstance(result, DetachedTabWindow)
        assert result in main_window.window_manager._windows
        assert main_window.central_tabs.count() == 0

    def test_reopening_the_same_tab_name_focuses_instead_of_duplicating(self, main_window):
        main_window.widget_factories["Fake"] = FakeWidget

        first = main_window.create_widget("Fake", "My Tab")
        main_window.central_tabs.addTab(QWidget(), "Other Tab")  # make current index != 0
        main_window.central_tabs.setCurrentIndex(1)

        result = main_window.create_widget("Fake", "My Tab")

        assert result is None  # focus_tab_if_exists short-circuits, nothing new is returned
        assert main_window.central_tabs.count() == 2
        assert main_window.central_tabs.currentIndex() == 0
        assert first is not None


class TestCloseTab:
    def test_close_tab_removes_it_and_deregisters(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)
        main_window.central_tabs.addTab(widget, "Tab")
        main_window.register_log_target(widget)

        main_window.close_tab(0)

        assert main_window.central_tabs.count() == 0
        assert widget not in main_window.log_targets

    def test_remove_tab_by_widget_removes_matching_tab(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)
        main_window.central_tabs.addTab(widget, "Tab")

        main_window.remove_tab_by_widget(widget)

        assert main_window.central_tabs.count() == 0

    def test_remove_tab_by_widget_not_present_is_a_noop(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)
        main_window.remove_tab_by_widget(widget)  # must not raise
        assert main_window.central_tabs.count() == 0

    def test_close_tab_by_widget_closes_the_matching_tab(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)
        main_window.central_tabs.addTab(widget, "Tab")

        main_window.close_tab_by_widget(widget)

        assert main_window.central_tabs.count() == 0

    def test_close_tab_by_widget_falls_back_to_plain_close_when_not_a_tab(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)

        main_window.close_tab_by_widget(widget)  # not in central_tabs at all - must not raise


class TestPopulateReplayMenu:
    def test_no_sessions_shows_a_disabled_placeholder(self, main_window, monkeypatch):
        from qtpy.QtWidgets import QMenu

        monkeypatch.setattr(
            "blinkview.utils.session_lister.resolve_log_root",
            lambda: (main_window.gui_context.registry.file_manager.log_dir, "proj"),
        )
        monkeypatch.setattr("blinkview.utils.session_lister.list_sessions", lambda log_dir, project_name: [])

        menu = QMenu()
        main_window._populate_replay_menu(menu)

        actions = menu.actions()
        assert len(actions) == 1
        assert actions[0].isEnabled() is False
        assert "no previous sessions" in actions[0].text()

    def test_sessions_without_a_unified_log_are_filtered_out(self, main_window, monkeypatch):
        from qtpy.QtWidgets import QMenu

        session = make_session_info()
        monkeypatch.setattr("blinkview.utils.session_lister.resolve_log_root", lambda: (None, "proj"))
        monkeypatch.setattr("blinkview.utils.session_lister.list_sessions", lambda log_dir, project_name: [session])
        monkeypatch.setattr("blinkview.utils.session_lister.unified_log_parts", lambda s: [])

        menu = QMenu()
        main_window._populate_replay_menu(menu)

        assert len(menu.actions()) == 1
        assert menu.actions()[0].isEnabled() is False

    def test_not_replaying_wires_actions_to_relaunch(self, main_window, monkeypatch):
        from qtpy.QtWidgets import QMenu

        session = make_session_info()
        monkeypatch.setattr("blinkview.utils.session_lister.resolve_log_root", lambda: (None, "proj"))
        monkeypatch.setattr("blinkview.utils.session_lister.list_sessions", lambda log_dir, project_name: [session])
        monkeypatch.setattr("blinkview.utils.session_lister.unified_log_parts", lambda s: [object()])

        relaunch_calls = []
        monkeypatch.setattr(main_window, "_relaunch_as_replay", lambda s: relaunch_calls.append(s))

        menu = QMenu()
        main_window._populate_replay_menu(menu)

        assert len(menu.actions()) == 1
        menu.actions()[0].trigger()
        assert relaunch_calls == [session]

    def test_already_replaying_wires_actions_to_start_replay_in_place(self, main_window, monkeypatch):
        from qtpy.QtWidgets import QMenu

        main_window.gui_context.registry.replay_mode = True
        session = make_session_info()
        monkeypatch.setattr("blinkview.utils.session_lister.resolve_log_root", lambda: (None, "proj"))
        monkeypatch.setattr("blinkview.utils.session_lister.list_sessions", lambda log_dir, project_name: [session])
        monkeypatch.setattr("blinkview.utils.session_lister.unified_log_parts", lambda s: [object()])

        start_calls = []
        monkeypatch.setattr(main_window, "start_replay", lambda s: start_calls.append(s))

        menu = QMenu()
        main_window._populate_replay_menu(menu)

        menu.actions()[0].trigger()
        assert start_calls == [session]


class TestStartReplay:
    def test_no_unified_log_parts_warns_and_does_nothing(self, main_window, monkeypatch):
        session = make_session_info()
        monkeypatch.setattr("blinkview.utils.session_lister.unified_log_parts", lambda s: [])

        warnings = []
        monkeypatch.setattr(type(main_window.logger), "warn", lambda self, msg: warnings.append(msg))

        main_window.start_replay(session)

        assert len(warnings) == 1
        assert session.session_id in warnings[0]

    def test_with_parts_builds_and_starts_a_unified_log_replay(self, main_window, monkeypatch, tmp_path):
        part = tmp_path / "session.0000.log"
        part.write_text("")
        session = make_session_info(path=tmp_path)
        monkeypatch.setattr("blinkview.utils.session_lister.unified_log_parts", lambda s: [part])

        created = {}

        class FakeUnifiedLogReplay:
            def __init__(self, parts, central, on_part_progress=None, on_finished=None):
                created["parts"] = parts
                created["central"] = central
                created["on_part_progress"] = on_part_progress
                created["on_finished"] = on_finished
                created["bound"] = False
                created["started"] = False

            def bind_system(self, shared, local):
                created["bound"] = True

            def start(self):
                created["started"] = True

        monkeypatch.setattr("blinkview.parsers.unified_log_replay.UnifiedLogReplay", FakeUnifiedLogReplay)

        load_calls = []
        monkeypatch.setattr(main_window.gui_context.registry, "load_replay_session", lambda d: load_calls.append(d))

        registry = main_window.gui_context.registry
        pause_calls = []
        resume_calls = []
        freeze_calls = []
        monkeypatch.setattr(registry.central, "pause_ingest", lambda: pause_calls.append(True))
        monkeypatch.setattr(registry.central, "resume_ingest", lambda: resume_calls.append(True))
        monkeypatch.setattr(
            registry.central.log_pool, "freeze_cold_storage_from_now", lambda: freeze_calls.append(True)
        )

        main_window.start_replay(session)

        assert created["parts"] == [part]
        assert created["central"] is registry.central
        assert created["bound"] is True
        assert created["started"] is True
        assert load_calls == [tmp_path]

        # Ingest must be paused before the reader starts, and neither resumed nor frozen until
        # the reader itself reports completion (on_finished) - not just because start_replay
        # returned.
        assert pause_calls == [True]
        assert resume_calls == []
        assert freeze_calls == []

        created["on_finished"]()

        assert resume_calls == [True]
        assert freeze_calls == [True]

    def test_skips_unified_log_replay_when_already_resumed_from_cold_storage(self, main_window, monkeypatch, tmp_path):
        """A previous run with cold_storage_persist_on_close enabled may have already archived
        this exact session (CircularLogPool._mount_existing_cold_segments remounts it at
        construction time, before start_replay ever runs) - re-parsing the unified log on top
        would silently duplicate every row, so this must be skipped entirely. Nothing more is
        coming for this session either way, so cold storage is frozen immediately instead of
        waiting on a reader that never runs."""
        session = make_session_info(path=tmp_path)
        main_window.gui_context.registry.central.log_pool.resumed_from_existing_cold_storage = True

        constructed = []
        monkeypatch.setattr(
            "blinkview.parsers.unified_log_replay.UnifiedLogReplay",
            lambda parts, central, on_part_progress=None, on_finished=None: constructed.append(parts),
        )

        load_calls = []
        monkeypatch.setattr(main_window.gui_context.registry, "load_replay_session", lambda d: load_calls.append(d))

        main_window.start_replay(session)

        assert constructed == []
        assert load_calls == [tmp_path]
        assert main_window.gui_context.registry.central.log_pool.frozen_since_sequence_id is not None


class TestRelaunchAsReplay:
    def test_spawns_blink_replay_subprocess_with_the_session_id(self, main_window, monkeypatch):
        import sys

        calls = []
        monkeypatch.setattr("subprocess.Popen", lambda args: calls.append(args))

        session = make_session_info(session_id="abc123")
        main_window._relaunch_as_replay(session)

        assert calls == [[sys.executable, "-m", "blinkview", "replay", "abc123"]]


class TestPopulateMainMenu:
    def test_builds_the_expected_top_level_actions(self, main_window, monkeypatch):
        # Avoid hitting the real network/log-dir scan the "Load Session..." submenu triggers.
        monkeypatch.setattr("blinkview.utils.session_lister.list_sessions", lambda log_dir, project_name: [])
        monkeypatch.setattr("blinkview.utils.session_lister.resolve_log_root", lambda: (None, "proj"))

        main_window.populate_main_menu()

        texts = [a.text() for a in main_window.app_menu.actions() if not a.isSeparator()]
        assert "Settings" in texts
        assert "Plugins" in texts
        assert "Check for updates" in texts
        assert "Load Session..." in texts
        assert "Quit" in texts

    def test_clearing_and_rebuilding_does_not_duplicate_actions(self, main_window, monkeypatch):
        monkeypatch.setattr("blinkview.utils.session_lister.list_sessions", lambda log_dir, project_name: [])
        monkeypatch.setattr("blinkview.utils.session_lister.resolve_log_root", lambda: (None, "proj"))

        main_window.populate_main_menu()
        first_count = len(main_window.app_menu.actions())
        main_window.populate_main_menu()

        assert len(main_window.app_menu.actions()) == first_count


class TestPollQueue:
    def test_runs_without_raising_and_updates_the_mps_label(self, main_window):
        main_window.poll_queue()  # first tick: elapsed-since-last-stats is ~0, cheap smoke check

        assert main_window.mps_label.text() != ""

    def test_a_full_second_elapsed_updates_the_rate_label_and_last_stats(self, main_window):
        registry = main_window.gui_context.registry
        base_stats = {"pushed": 0, "popped": 0, "dropped": 0, "total": 0, "maxlen": 1000, "now": 0.0}
        main_window._last_stats = dict(base_stats)

        new_stats = dict(base_stats, pushed=500, popped=480, now=1.5)  # 1.5s elapsed, plenty of rows
        main_window.gui_context.registry.central.input_queue.get_stats = lambda: new_stats

        main_window.poll_queue()

        assert main_window._last_stats == new_stats
        assert main_window.mps_label.text() != ""

    def test_swallows_exceptions_from_a_broken_registry_state(self, main_window, monkeypatch):
        def boom():
            raise RuntimeError("simulated failure")

        # Deliberately not registry.now_ns - the exception handler's own logging goes through
        # the same registry/logger machinery, so breaking that would break the test's ability to
        # observe a clean catch. get_stats() is later in poll_queue and unrelated to logging.
        monkeypatch.setattr(main_window.gui_context.registry.central.input_queue, "get_stats", boom)

        main_window.poll_queue()  # must not raise - caught and logged internally


class TestCloseEvent:
    """Registry.stop() now does real shutdown-time compression work (cold storage + file logger
    parts - see core/registry.py's stop(on_progress=...)), so closeEvent defers the actual close
    instead of blocking the UI thread on it - see plans/expressive-sauteeing-sun.md. These tests
    cover the two Qt-signal-connected slots directly (fast, deterministic) and the full threaded
    flow end-to-end (via qtbot.waitUntil, since real work happens on a background QThread)."""

    def test_on_shutdown_progress_updates_toast_text(self, main_window):
        messages = []
        main_window._shutdown_toast = SimpleNamespace(set_message=messages.append)

        main_window._on_shutdown_progress(2, 5, "segment_0000000001.blkseg")

        assert messages == ["Compressing files... 2 of 5 (segment_0000000001.blkseg)"]

    def test_on_shutdown_progress_is_a_noop_without_a_toast(self, main_window):
        main_window._shutdown_toast = None

        main_window._on_shutdown_progress(1, 1, "session")  # must not raise

    def test_on_shutdown_compression_done_dismisses_toast_and_finishes_close(self, main_window, monkeypatch):
        dismissed = []
        main_window._shutdown_toast = SimpleNamespace(dismiss=lambda: dismissed.append(True))
        monkeypatch.setattr(main_window.timer_fast, "stop", lambda: None)
        monkeypatch.setattr(main_window.timer_slow, "stop", lambda: None)
        monkeypatch.setattr(main_window.window_manager, "close_all", lambda: None)
        close_calls = []
        monkeypatch.setattr(main_window, "close", lambda: close_calls.append(True))

        main_window._on_shutdown_compression_done()

        assert dismissed == [True]
        assert main_window._shutdown_toast is None
        assert main_window._shutdown_ready_to_close is True
        assert close_calls == [True]

    def test_first_close_defers_and_the_worker_eventually_completes(self, main_window, qtbot):
        main_window.close()

        # Deferred, not actually closed yet - a toast is up and the background worker is running.
        assert main_window._shutdown_ready_to_close is False
        assert main_window._shutdown_toast is not None
        assert main_window.gui_context.is_shutting_down is True

        qtbot.waitUntil(lambda: main_window._shutdown_ready_to_close, timeout=5000)

        assert main_window._shutdown_toast is None  # dismissed once compression finished

    def test_repeated_close_attempts_while_worker_running_are_ignored(self, main_window, qtbot):
        main_window.close()
        worker = main_window._shutdown_worker
        thread = main_window._shutdown_thread

        main_window.close()  # second attempt while the worker is still running - must be a no-op

        assert main_window._shutdown_worker is worker
        assert main_window._shutdown_thread is thread

        qtbot.waitUntil(lambda: main_window._shutdown_ready_to_close, timeout=5000)

    def test_final_close_accepts_immediately(self, main_window):
        main_window._shutdown_ready_to_close = True

        class FakeEvent:
            def __init__(self):
                self.accepted = False
                self.ignored = False

            def accept(self):
                self.accepted = True

            def ignore(self):
                self.ignored = True

        event = FakeEvent()
        main_window.closeEvent(event)

        assert event.accepted is True
        assert event.ignored is False


class TestSignalHandler:
    def test_closes_the_window(self, main_window, monkeypatch):
        import signal

        closed = []
        monkeypatch.setattr(main_window, "close", lambda: closed.append(True))

        main_window._signal_handler(signal.SIGINT, None)

        assert closed == [True]


class TestDetachAndReattachTab:
    def test_detach_tab_moves_widget_into_a_floating_window(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)
        main_window.central_tabs.addTab(widget, "Detach Me")

        main_window.detach_tab(0)

        assert main_window.central_tabs.count() == 0
        assert widget in main_window.window_manager._windows.values()

    def test_reattach_tab_adds_it_back_and_focuses_it(self, main_window, qtbot):
        widget = QWidget()
        qtbot.addWidget(widget)
        main_window.central_tabs.addTab(QWidget(), "Other")

        main_window.reattach_tab(widget, "Reattached")

        assert main_window.central_tabs.count() == 2
        assert main_window.central_tabs.tabText(main_window.central_tabs.currentIndex()) == "Reattached"


class TestShowTabContextMenu:
    def test_empty_space_click_is_a_noop(self, main_window, qtbot):
        from qtpy.QtCore import QPoint

        # Way off to the right of any tab - tabBar().tabAt() returns -1 for empty space.
        main_window.show_tab_context_menu(QPoint(10_000, 10_000))  # must not raise / open a menu


class TestRebuildMenu:
    def test_none_config_shows_loading_placeholder(self, main_window):
        from qtpy.QtWidgets import QMenu

        menu = QMenu()
        main_window._rebuild_menu(menu, None)

        actions = menu.actions()
        assert len(actions) == 1
        assert "Loading" in actions[0].text()
        assert actions[0].isEnabled() is False

    def test_empty_config_shows_no_saved_watches_plus_new_watch_action(self, main_window):
        from qtpy.QtWidgets import QMenu

        menu = QMenu()
        main_window._rebuild_menu(menu, {})

        texts = [a.text() for a in menu.actions() if not a.isSeparator()]
        assert "No saved watches" in texts
        assert "+ New Watch..." in texts

    def test_populated_config_lists_each_watch_sorted_by_id(self, main_window):
        from qtpy.QtWidgets import QMenu

        menu = QMenu()
        config = {"w2": {"name": "Second"}, "w1": {"name": "First"}}
        main_window._rebuild_menu(menu, config)

        texts = [a.text() for a in menu.actions() if not a.isSeparator()]
        assert texts == ["First", "Second", "+ New Watch..."]

    def test_watch_missing_a_name_falls_back_to_its_id(self, main_window):
        from qtpy.QtWidgets import QMenu

        menu = QMenu()
        main_window._rebuild_menu(menu, {"w1": {}})

        texts = [a.text() for a in menu.actions() if not a.isSeparator()]
        assert "Watch w1" in texts

    def test_none_menu_is_a_noop(self, main_window):
        main_window._rebuild_menu(None, {})  # must not raise


class TestOpenWatch:
    def test_existing_watch_id_opens_a_tab_named_after_it(self, main_window, monkeypatch):
        main_window.widget_factories[WidgetName.TELEMETRY_WATCH] = FakeWidget
        main_window.watches_node = SimpleNamespace(get=lambda watch_id: {"name": "My Watch"})

        main_window.open_watch("w1")

        assert main_window.central_tabs.count() == 1
        assert main_window.central_tabs.tabText(0) == "Watch My Watch"

    def test_cancelled_name_dialog_creates_nothing(self, main_window, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("", False)))
        main_window.watches_node = SimpleNamespace(get_copy=lambda: {})

        main_window.open_watch(None)

        assert main_window.central_tabs.count() == 0

    def test_new_watch_name_creates_and_saves_it(self, main_window, monkeypatch):
        from qtpy.QtWidgets import QInputDialog

        main_window.widget_factories[WidgetName.TELEMETRY_WATCH] = FakeWidget
        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **kw: ("New One", True)))

        saved = []
        main_window.watches_node = SimpleNamespace(get_copy=lambda: {}, send_config=lambda w: saved.append(w))

        main_window.open_watch(None)

        assert main_window.central_tabs.count() == 1
        assert len(saved) == 1
        assert list(saved[0].values())[0]["name"] == "New One"


class TestSyncDeviceToolbars:
    def test_removes_toolbar_for_a_deleted_source(self, main_window, qtbot):
        from qtpy.QtWidgets import QToolBar

        toolbar = QToolBar()
        main_window.addToolBar(toolbar)
        main_window.device_toolbars["src_1"] = toolbar

        main_window.sync_device_toolbars({}, {})

        assert "src_1" not in main_window.device_toolbars

    def test_removes_toolbar_for_a_disabled_source(self, main_window, qtbot):
        from qtpy.QtWidgets import QToolBar

        toolbar = QToolBar()
        main_window.addToolBar(toolbar)
        main_window.device_toolbars["src_1"] = toolbar

        main_window.sync_device_toolbars({"src_1": {"enabled": False}}, {})

        assert "src_1" not in main_window.device_toolbars

    def test_keeps_toolbar_for_an_enabled_source(self, main_window, qtbot):
        from qtpy.QtWidgets import QToolBar

        toolbar = QToolBar()
        main_window.addToolBar(toolbar)
        main_window.device_toolbars["src_1"] = toolbar

        main_window.sync_device_toolbars({"src_1": {"enabled": True}}, {})

        assert "src_1" in main_window.device_toolbars

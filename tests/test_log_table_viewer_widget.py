# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Unit tests for LogTableViewerWidget's control-flow methods that aren't already covered by
test_log_table_viewer_scrub.py (playback wiring) or test_log_table_viewer.py (LogTableStore/
LogTableCanvas). Follows the same convention as test_log_table_viewer_scrub.py: drives unbound
methods against minimal SimpleNamespace stubs rather than constructing a full widget, which needs
a real Registry/GUIContext plus a whole sidebar/canvas tree - out of proportion for pure
control-flow logic."""

from types import SimpleNamespace

from blinkview.core.playback_clock import PlaybackMode
from blinkview.core.playback_follow import ClockSnapshot, FollowState, PlaybackFollowMachine
from blinkview.ui.widgets.log_table_viewer import LogTableCol, LogTableViewerWidget
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.utils.log_level import LogLevel


class TestRestoreAndGetState:
    def _make_widget_for_restore(self):
        return SimpleNamespace(
            tab_name="old",
            show_hidden=False,
            allowed_device=None,
            filtered_module=None,
            filtered_module_children=False,
            log_level=LogLevel.ALL.name_conf,
            show_module_filter=False,
            show_rx_ts=False,
            show_process_thread=False,
            ts_precision=3,
            kv_filter_text="",
            search_text="",
            force_live=False,
            filter_sidebar_state=None,
            gui_context=SimpleNamespace(
                id_registry=SimpleNamespace(
                    resolve_device=lambda name: f"device:{name}" if name else None,
                    resolve_module=lambda name: f"module:{name}" if name else None,
                )
            ),
        )

    def test_restore_applies_top_level_and_view_state_fields(self):
        stub = self._make_widget_for_restore()
        state = {
            "tab_name": "MyTab",
            "show_hidden": True,
            "allowed_device": "dev1",
            "filtered_module": "mod1",
            "filtered_module_children": True,
            "log_level": "WARN",
            "view_state": {
                "show_module_filter": True,
                "show_rx_ts": True,
                "show_process_thread": True,
                "ts_precision": 9,
                "kv_filter_text": "k=v",
                "search_text": "needle",
            },
            "filter_sidebar": {"saved": True},
        }

        LogTableViewerWidget.restore(stub, state)

        assert stub.tab_name == "MyTab"
        assert stub.show_hidden is True
        assert stub.allowed_device == "device:dev1"
        assert stub.filtered_module == "module:mod1"
        assert stub.filtered_module_children is True
        assert stub.log_level == "WARN"
        assert stub.show_module_filter is True
        assert stub.show_rx_ts is True
        assert stub.show_process_thread is True
        assert stub.ts_precision == 9
        assert stub.kv_filter_text == "k=v"
        assert stub.search_text == "needle"
        assert stub.filter_sidebar_state == {"saved": True}

    def test_restore_with_empty_state_keeps_defaults(self):
        stub = self._make_widget_for_restore()
        LogTableViewerWidget.restore(stub, {})
        assert stub.tab_name == "old"
        assert stub.ts_precision == 3

    def test_get_state_round_trips_fields(self):
        stub = SimpleNamespace(
            tab_name="MyTab",
            allowed_device=SimpleNamespace(name="dev1"),
            filtered_module=None,
            filtered_module_children=False,
            show_module_filter=True,
            show_rx_ts=False,
            show_process_thread=True,
            ts_precision=6,
            force_live=False,
            log_filter=SimpleNamespace(kv_filter_text="k=v", text_filter_text="needle", log_level=LogLevel.WARN),
            filter_sidebar=SimpleNamespace(
                get_state=lambda: {"x": 1}, action_show_non_essential=SimpleNamespace(isChecked=lambda: True)
            ),
        )

        state = LogTableViewerWidget.get_state(stub)

        assert state["tab_name"] == "MyTab"
        assert state["allowed_device"] == "dev1"
        assert state["filtered_module"] is None
        assert state["view_state"]["ts_precision"] == 6
        assert state["view_state"]["kv_filter_text"] == "k=v"
        assert state["view_state"]["search_text"] == "needle"
        assert state["log_level"] == LogLevel.WARN.name_conf
        assert state["filter_sidebar"] == {"x": 1}
        assert state["show_hidden"] is True


class TestFilterAndLevelHandlers:
    def _redraw_stub(self, calls, with_reload_stub=False):
        stub = SimpleNamespace(
            model=SimpleNamespace(reload_and_redraw=lambda: calls.append("reload")),
            view=SimpleNamespace(
                request_repaint=lambda: calls.append("repaint"), autosize_columns=lambda: calls.append("autosize")
            ),
        )
        if with_reload_stub:
            stub._reload_and_redraw = lambda: calls.append("reload")
        return stub

    def test_reload_and_redraw_calls_model_and_view(self):
        calls = []
        stub = self._redraw_stub(calls)
        LogTableViewerWidget._reload_and_redraw(stub)
        assert calls == ["reload", "repaint", "autosize"]

    def test_handle_level_change_sets_level_and_redraws(self):
        calls = []
        stub = self._redraw_stub(calls, with_reload_stub=True)
        stub.log_filter = SimpleNamespace(set_level=lambda name: calls.append(f"level:{name}"))
        stub.level_combo = SimpleNamespace(itemData=lambda index: LogLevel.WARN)

        LogTableViewerWidget._handle_level_change(stub, 2)

        assert f"level:{LogLevel.WARN.name_conf}" in calls
        assert "reload" in calls

    def test_apply_kv_filter_text_sets_filter_and_redraws(self):
        calls = []
        stub = self._redraw_stub(calls, with_reload_stub=True)
        stub.log_filter = SimpleNamespace(set_kv_filter=lambda text: calls.append(f"kv:{text}"))

        LogTableViewerWidget._apply_kv_filter_text(stub, "a=b")

        assert "kv:a=b" in calls
        assert "reload" in calls

    def test_apply_search_text_uses_search_box_text(self):
        calls = []
        stub = self._redraw_stub(calls, with_reload_stub=True)
        stub.log_filter = SimpleNamespace(set_text_filter=lambda text: calls.append(f"search:{text}"))
        stub.search_box = SimpleNamespace(text=lambda: "hello")

        LogTableViewerWidget._apply_search_text(stub)

        assert "search:hello" in calls
        assert "reload" in calls


class TestToggleHandlers:
    def test_toggle_module_filter_shows_and_hides_sidebar(self):
        calls = []
        stub = SimpleNamespace(filter_sidebar=SimpleNamespace(setVisible=lambda v: calls.append(v)))
        LogTableViewerWidget._toggle_module_filter(stub, True)
        assert stub.show_module_filter is True
        assert calls == [True]

    def test_toggle_rx_ts_sets_column_visibility_and_autosizes_when_enabled(self):
        calls = []
        stub = SimpleNamespace(
            view=SimpleNamespace(
                set_column_visible=lambda col, vis: calls.append((col, vis)),
                autosize_columns=lambda: calls.append("autosize"),
            )
        )
        LogTableViewerWidget._toggle_rx_ts(stub, True)
        assert stub.show_rx_ts is True
        assert (LogTableCol.RX_TIMESTAMP, True) in calls
        assert "autosize" in calls

    def test_toggle_rx_ts_does_not_autosize_when_disabled(self):
        calls = []
        stub = SimpleNamespace(
            view=SimpleNamespace(
                set_column_visible=lambda col, vis: calls.append((col, vis)),
                autosize_columns=lambda: calls.append("autosize"),
            )
        )
        LogTableViewerWidget._toggle_rx_ts(stub, False)
        assert "autosize" not in calls

    def test_toggle_process_thread_sets_both_columns(self):
        calls = []
        stub = SimpleNamespace(
            view=SimpleNamespace(
                set_column_visible=lambda col, vis: calls.append((col, vis)),
                autosize_columns=lambda: calls.append("autosize"),
            )
        )
        LogTableViewerWidget._toggle_process_thread(stub, True)
        assert (LogTableCol.PROCESS, True) in calls
        assert (LogTableCol.THREAD, True) in calls

    def test_set_ts_precision_noop_when_unchanged(self):
        calls = []
        stub = SimpleNamespace(ts_precision=3, model=SimpleNamespace(set_ts_precision=lambda p: calls.append(p)))
        LogTableViewerWidget._set_ts_precision(stub, 3)
        assert calls == []

    def test_set_ts_precision_updates_model_and_view(self):
        calls = []
        stub = SimpleNamespace(
            ts_precision=3,
            model=SimpleNamespace(set_ts_precision=lambda p: calls.append(("model", p))),
            view=SimpleNamespace(
                reset_column_autosize=lambda col: calls.append(("reset", col)),
                autosize_columns=lambda: calls.append("autosize"),
                request_repaint=lambda: calls.append("repaint"),
            ),
        )

        LogTableViewerWidget._set_ts_precision(stub, 9)

        assert stub.ts_precision == 9
        assert ("model", 9) in calls
        assert ("reset", LogTableCol.TIMESTAMP) in calls
        assert ("reset", LogTableCol.RX_TIMESTAMP) in calls
        assert "autosize" in calls
        assert "repaint" in calls


class TestClearLogs:
    def test_clear_logs_resets_model_and_selection(self):
        calls = []
        stub = SimpleNamespace(
            model=SimpleNamespace(clear_logs=lambda: calls.append("clear")),
            view=SimpleNamespace(selected_seq="prev", request_repaint=lambda: calls.append("repaint")),
            _set_live_ui_state=lambda: calls.append("live_ui"),
        )

        LogTableViewerWidget.clear_logs(stub)

        assert calls == ["clear", "live_ui", "repaint"]
        assert stub.view.selected_seq is None


class TestHistoryHelpers:
    def test_topmost_row_seq_delegates_to_model(self):
        stub = SimpleNamespace(
            model=SimpleNamespace(seq_for_row=lambda row: f"seq-{row}"),
            view=SimpleNamespace(first_visible_row=lambda: 5),
        )
        assert LogTableViewerWidget._topmost_row_seq(stub) == "seq-5"

    def test_enter_history_at_top_row_noop_when_no_anchor(self):
        calls = []
        stub = SimpleNamespace(
            _topmost_row_seq=lambda: None,
            _reanchor_history=lambda *a, **kw: calls.append((a, kw)),
        )
        LogTableViewerWidget._enter_history_at_top_row(stub)
        assert calls == []

    def test_enter_history_at_top_row_reanchors_when_anchor_found(self):
        calls = []
        stub = SimpleNamespace(
            _topmost_row_seq=lambda: 42,
            _reanchor_history=lambda anchor_seq, auto=False: calls.append((anchor_seq, auto)),
        )
        LogTableViewerWidget._enter_history_at_top_row(stub, auto=True)
        assert calls == [(42, True)]

    def test_history_newest_ref_seq_uses_last_row_when_present(self):
        stub = SimpleNamespace(model=SimpleNamespace(row_count=3, seq_for_row=lambda row: f"row{row}", anchor_seq=99))
        assert LogTableViewerWidget._history_newest_ref_seq(stub) == "row2"

    def test_history_newest_ref_seq_falls_back_to_anchor_seq_when_empty(self):
        stub = SimpleNamespace(model=SimpleNamespace(row_count=0, anchor_seq=99))
        assert LogTableViewerWidget._history_newest_ref_seq(stub) == 99

    def test_at_live_edge_false_when_no_reference_seq(self):
        stub = SimpleNamespace(_history_newest_ref_seq=lambda: None)
        assert LogTableViewerWidget._at_live_edge(stub) is False

    def test_at_live_edge_true_when_caught_up(self):
        stub = SimpleNamespace(
            _history_newest_ref_seq=lambda: 100,
            gui_context=SimpleNamespace(
                registry=SimpleNamespace(central=SimpleNamespace(log_pool=SimpleNamespace(latest_sequence=lambda: 100)))
            ),
        )
        assert LogTableViewerWidget._at_live_edge(stub) is True

    def test_at_live_edge_false_when_behind(self):
        stub = SimpleNamespace(
            _history_newest_ref_seq=lambda: 50,
            gui_context=SimpleNamespace(
                registry=SimpleNamespace(central=SimpleNamespace(log_pool=SimpleNamespace(latest_sequence=lambda: 100)))
            ),
        )
        assert LogTableViewerWidget._at_live_edge(stub) is False

    def test_scroll_to_row_ignores_none_and_negative(self):
        calls = []
        stub = SimpleNamespace(
            view=SimpleNamespace(verticalScrollBar=lambda: SimpleNamespace(setValue=lambda v: calls.append(v)))
        )
        LogTableViewerWidget._scroll_to_row(stub, None)
        LogTableViewerWidget._scroll_to_row(stub, -1)
        assert calls == []

    def test_scroll_to_row_sets_value_for_valid_row(self):
        calls = []
        stub = SimpleNamespace(
            view=SimpleNamespace(verticalScrollBar=lambda: SimpleNamespace(setValue=lambda v: calls.append(v)))
        )
        LogTableViewerWidget._scroll_to_row(stub, 7)
        assert calls == [7]


class TestGoLive:
    def test_go_live_resets_state_and_updates_view(self):
        calls = []
        machine = PlaybackFollowMachine()
        machine.state = FollowState.FROZEN
        stub = SimpleNamespace(
            model=SimpleNamespace(enter_live_mode=lambda: calls.append("enter_live")),
            view=SimpleNamespace(
                request_repaint=lambda: calls.append("repaint"), autosize_columns=lambda: calls.append("autosize")
            ),
            _set_live_ui_state=lambda: calls.append("live_ui"),
            _set_pause_ui=lambda paused, auto=False: calls.append(("pause", paused)),
            _is_catching_up=False,
            _playback=machine,
            _programmatic_scroll=False,
        )

        LogTableViewerWidget._go_live(stub)

        assert calls == ["enter_live", "repaint", "autosize", "live_ui", ("pause", False)]
        assert stub._is_catching_up is True
        assert machine.state is FollowState.LIVE
        assert stub._programmatic_scroll is False  # restored after the try/finally


class TestSetPauseUi:
    def _make_stub(self):
        button = SimpleNamespace(
            properties={},
            setProperty=lambda self_ignored=None, **kw: None,
        )

        class FakeButton:
            def __init__(self):
                self.props = {}
                self.style_calls = []

            def setProperty(self, key, value):
                self.props[key] = value

            def style(self):
                return SimpleNamespace(
                    unpolish=lambda b: self.style_calls.append("unpolish"),
                    polish=lambda b: self.style_calls.append("polish"),
                )

            def update(self):
                self.style_calls.append("update")

        fake_button = FakeButton()

        action_state = {"checked": None, "text": None, "blocked": []}

        stub = SimpleNamespace(
            action_pause=SimpleNamespace(
                blockSignals=lambda v: action_state["blocked"].append(v),
                setChecked=lambda v: action_state.__setitem__("checked", v),
                setText=lambda t: action_state.__setitem__("text", t),
            ),
            toolbar=SimpleNamespace(widgetForAction=lambda action: fake_button),
            velocity_tracker=SimpleNamespace(reset=lambda: action_state.setdefault("reset_calls", 0)),
        )
        return stub, action_state, fake_button

    def test_manual_pause_sets_paused_text(self):
        stub, action_state, fake_button = self._make_stub()

        LogTableViewerWidget._set_pause_ui(stub, True, auto=False)

        # is_paused is a read-only property derived from self._playback.state now - _set_pause_ui
        # only syncs the button's own text/checked/style, tested via action_state/fake_button below.
        assert stub.auto_paused is False
        assert action_state["text"] == "▶ Resume"
        assert fake_button.props["manualPaused"] is True
        assert fake_button.props["autoPaused"] is False

    def test_auto_pause_sets_auto_paused_text(self):
        stub, action_state, fake_button = self._make_stub()

        LogTableViewerWidget._set_pause_ui(stub, True, auto=True)

        assert stub.auto_paused is True
        assert action_state["text"] == "▶ Resume (AUTO)"
        assert fake_button.props["autoPaused"] is True

    def test_unpausing_resets_velocity_tracker(self):
        stub, action_state, fake_button = self._make_stub()
        LogTableViewerWidget._set_pause_ui(stub, False, auto=False)
        assert action_state["text"] == "⏸ Pause"
        assert "reset_calls" in action_state


class TestReanchorHistory:
    def _base_stub(self, calls, was_live=True, at_live_edge=False):
        return SimpleNamespace(
            model=SimpleNamespace(
                mode=LogViewMode.LIVE if was_live else LogViewMode.HISTORY,
                enter_history_mode=lambda anchor_seq=None, anchor_ts=None: calls.append(
                    ("enter_history_mode", anchor_seq, anchor_ts)
                ),
                anchor_scroll_row=lambda: 3,
            ),
            view=SimpleNamespace(
                request_repaint=lambda: calls.append("repaint"),
                autosize_columns=lambda: calls.append("autosize"),
            ),
            _programmatic_scroll=False,
            _set_history_ui_state=lambda: calls.append("history_ui"),
            _set_pause_ui=lambda paused, auto=False: calls.append(("pause", paused, auto)),
            _at_live_edge=lambda: at_live_edge,
            _go_live=lambda: calls.append(("go_live",)),
            _scroll_to_row=lambda row: calls.append(("scroll_to", row)),
        )

    def test_live_to_history_transition_sets_history_ui_and_pauses(self):
        calls = []
        stub = self._base_stub(calls, was_live=True)

        LogTableViewerWidget._reanchor_history(stub, anchor_seq=10)

        assert ("enter_history_mode", 10, None) in calls
        assert "history_ui" in calls
        assert ("pause", True, False) in calls
        assert ("scroll_to", 3) in calls
        assert stub._programmatic_scroll is False  # restored after try/finally

    def test_live_to_history_ts_anchored_transition_does_not_pause(self):
        """A playback-follow (ts-anchored) transition must not freeze is_paused, or the next
        apply_updates() follow-guard would immediately block further following."""
        calls = []
        stub = self._base_stub(calls, was_live=True)

        LogTableViewerWidget._reanchor_history(stub, anchor_ts=555)

        assert ("enter_history_mode", None, 555) in calls
        assert "history_ui" in calls
        assert not any(c[0] == "pause" for c in calls if isinstance(c, tuple))

    def test_paging_forward_within_history_goes_live_if_caught_up(self):
        calls = []
        stub = self._base_stub(calls, was_live=False, at_live_edge=True)

        LogTableViewerWidget._reanchor_history(stub, anchor_seq=99)

        assert ("go_live",) in calls
        assert not any(c == "repaint" and False for c in calls)  # sanity - no crash
        assert not any(isinstance(c, tuple) and c[0] == "scroll_to" for c in calls)

    def test_paging_forward_within_history_scrolls_when_not_caught_up(self):
        calls = []
        stub = self._base_stub(calls, was_live=False, at_live_edge=False)

        LogTableViewerWidget._reanchor_history(stub, anchor_seq=99)

        assert ("go_live",) not in calls
        assert ("scroll_to", 3) in calls

    def test_ts_anchored_paging_never_checks_live_edge(self):
        """A ts-anchored (playback-following) reanchor's LIVE/REPLAY transitions are solely
        driven by the global clock, not this at-live-edge heuristic."""
        calls = []
        stub = self._base_stub(calls, was_live=False, at_live_edge=True)

        LogTableViewerWidget._reanchor_history(stub, anchor_ts=777)

        assert ("go_live",) not in calls
        assert ("scroll_to", 3) in calls


class TestOnScrollValueChangedOrdinaryPaging:
    def _base_stub(self, calls, value, maximum=100, minimum=0, row_count=5, at_live_edge=False):
        machine = PlaybackFollowMachine()
        machine.state = FollowState.FROZEN  # already browsing history, mirrors mode=HISTORY below
        return SimpleNamespace(
            _programmatic_scroll=False,
            model=SimpleNamespace(
                mode=LogViewMode.HISTORY,
                row_count=row_count,
                anchor_seq=50,
                seq_for_row=lambda row: 10 if row == 0 else None,
            ),
            view=SimpleNamespace(
                verticalScrollBar=lambda: SimpleNamespace(
                    value=lambda: value, maximum=lambda: maximum, minimum=lambda: minimum
                )
            ),
            _playback=machine,
            _playback_anchored=False,
            _clock=lambda: None,
            _clock_snapshot=lambda clock: ClockSnapshot(mode=PlaybackMode.LIVE, current_ts_ns=0),
            _at_live_edge=lambda: at_live_edge,
            _go_live=lambda: calls.append(("go_live",)),
            _history_newest_ref_seq=lambda: 88,
            _reanchor_history=lambda seq: calls.append(("reanchor", seq)),
        )

    def test_bottom_edge_goes_live_when_caught_up(self):
        calls = []
        stub = self._base_stub(calls, value=99, maximum=100, at_live_edge=True)

        LogTableViewerWidget._on_scroll_value_changed(stub, 99)

        assert ("go_live",) in calls

    def test_bottom_edge_reanchors_when_not_caught_up(self):
        calls = []
        stub = self._base_stub(calls, value=99, maximum=100, at_live_edge=False)

        LogTableViewerWidget._on_scroll_value_changed(stub, 99)

        assert ("reanchor", 88) in calls

    def test_top_edge_reanchors_further_back(self):
        calls = []
        stub = self._base_stub(calls, value=0, minimum=0, maximum=100, row_count=5)

        LogTableViewerWidget._on_scroll_value_changed(stub, 0)

        assert ("reanchor", 10) in calls

    def test_middle_of_the_window_does_nothing(self):
        calls = []
        stub = self._base_stub(calls, value=50, minimum=0, maximum=100)

        LogTableViewerWidget._on_scroll_value_changed(stub, 50)

        assert calls == []

    def test_ignored_while_programmatic_scroll_flag_is_set(self):
        calls = []
        stub = self._base_stub(calls, value=99, maximum=100, at_live_edge=True)
        stub._programmatic_scroll = True

        LogTableViewerWidget._on_scroll_value_changed(stub, 99)

        assert calls == []

    def test_ignored_when_not_in_history_mode(self):
        calls = []
        stub = self._base_stub(calls, value=99, maximum=100, at_live_edge=True)
        stub.model.mode = LogViewMode.LIVE

        LogTableViewerWidget._on_scroll_value_changed(stub, 99)

        assert calls == []


class TestPollHistoryTail:
    def _base_stub(self, calls, **overrides):
        defaults = dict(
            prev_history_poll=0,
            model=SimpleNamespace(anchor_seq=10),
            view=SimpleNamespace(verticalScrollBar=lambda: SimpleNamespace(value=lambda: 100, maximum=lambda: 100)),
            gui_context=SimpleNamespace(
                registry=SimpleNamespace(central=SimpleNamespace(log_pool=SimpleNamespace(latest_sequence=lambda: 500)))
            ),
            _last_polled_backend_seq=None,
            _at_live_edge=lambda: False,
            _go_live=lambda: calls.append(("go_live",)),
            _history_newest_ref_seq=lambda: 42,
            _reanchor_history=lambda seq: calls.append(("reanchor", seq)),
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_throttled_within_100ms(self, monkeypatch):
        import blinkview.ui.widgets.log_table_viewer as module

        monkeypatch.setattr(module, "time_ns", lambda: 50_000_000)
        calls = []
        stub = self._base_stub(calls, prev_history_poll=0)

        LogTableViewerWidget._poll_history_tail(stub)

        assert stub.prev_history_poll == 0  # never updated - bailed out on the throttle check
        assert calls == []

    def test_noop_when_anchor_seq_is_none(self, monkeypatch):
        import blinkview.ui.widgets.log_table_viewer as module

        monkeypatch.setattr(module, "time_ns", lambda: 1_000_000_000)
        calls = []
        stub = self._base_stub(calls, prev_history_poll=0)
        stub.model.anchor_seq = None

        LogTableViewerWidget._poll_history_tail(stub)

        assert stub.prev_history_poll == 1_000_000_000  # throttle timestamp still updates
        assert calls == []

    def test_noop_when_not_at_bottom(self, monkeypatch):
        import blinkview.ui.widgets.log_table_viewer as module

        monkeypatch.setattr(module, "time_ns", lambda: 1_000_000_000)
        calls = []
        stub = self._base_stub(calls)
        stub.view = SimpleNamespace(verticalScrollBar=lambda: SimpleNamespace(value=lambda: 10, maximum=lambda: 100))

        LogTableViewerWidget._poll_history_tail(stub)

        assert stub._last_polled_backend_seq is None  # never got far enough to check the backend
        assert calls == []

    def test_noop_when_backend_has_not_advanced(self, monkeypatch):
        import blinkview.ui.widgets.log_table_viewer as module

        monkeypatch.setattr(module, "time_ns", lambda: 1_000_000_000)
        calls = []
        stub = self._base_stub(calls, _last_polled_backend_seq=500)

        LogTableViewerWidget._poll_history_tail(stub)

        # latest_sequence() == 500 == _last_polled_backend_seq - should bail without reanchoring
        assert calls == []

    def test_goes_live_when_at_edge(self, monkeypatch):
        import blinkview.ui.widgets.log_table_viewer as module

        monkeypatch.setattr(module, "time_ns", lambda: 1_000_000_000)
        calls = []
        stub = self._base_stub(calls, _at_live_edge=lambda: True)

        LogTableViewerWidget._poll_history_tail(stub)

        assert calls == [("go_live",)]

    def test_reanchors_when_more_history_is_available(self, monkeypatch):
        import blinkview.ui.widgets.log_table_viewer as module

        monkeypatch.setattr(module, "time_ns", lambda: 1_000_000_000)
        calls = []
        stub = self._base_stub(calls)

        LogTableViewerWidget._poll_history_tail(stub)

        assert calls == [("reanchor", 42)]

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for PlaybackControlWidget's named-ranges + jog-wheel wiring, driving the
real widget against a real Registry/GUIContext/CircularLogPool - not fakes - per this project's
habit (see blinkview-playback-wiring skill) of catching cross-cutting bugs that per-piece unit
tests miss: real row-stepping through CircularLogPool.find_ts_n_rows_away, a real range saved to
and reloaded from an actual session folder on disk, and the jog wheel's stepRequested signal
driving a real PlaybackClock.step_rows() call against real ingested rows."""

import json

import pytest
from qtpy.QtWidgets import QInputDialog

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.playback_clock import PlaybackMode
from blinkview.ui.widgets.playback_control import PlaybackControlWidget
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


def push_rows(registry, count, ts_start):
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    device = registry.id_registry.get_device("playback_e2e")
    module = device.get_module("log")

    batch = array_pool.create(
        PooledLogBatch, count, count * 16, has_levels=True, has_modules=True, has_devices=True, has_sequences=True
    )
    with batch:
        for i in range(count):
            batch.insert(ts_start + i, ts_start + i, f"row{i}".encode(), LogLevel.INFO.value, module.id, device.id, 0)
        log_pool.batch_append(batch)


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "playback_control_e2e")
    yield reg
    reg.stop()


@pytest.fixture
def widget(qapp, qtbot, registry):
    push_rows(registry, 10, ts_start=1_000_000_000)
    # PlaybackClock.bounds_* only refresh inside tick() (normally driven every heartbeat by
    # PlaybackControlWidget.apply_updates()) - without this, seek()/step_rows() below would clamp
    # everything to the clock's construction-time (empty-pool) bounds of (0, 0).
    registry.playback_clock.tick(registry.now_ns())
    gui_context = make_real_gui_context(registry)
    w = PlaybackControlWidget(gui_context)
    qtbot.addWidget(w)
    yield w


class TestJogWheelStepsRealRows:
    def test_step_moves_to_the_exact_next_real_row(self, widget, registry):
        clock = registry.playback_clock
        clock.seek(1_000_000_004)  # row index 4 (ts_start=1_000_000_000, 1ns/row)

        widget._on_jog_step(1)

        assert clock.current_ts_ns == 1_000_000_005  # the real next row, from the real log_pool
        assert clock.mode is PlaybackMode.REPLAY
        assert widget.seek_bar.current_ts_ns == 1_000_000_005

    def test_step_backward_moves_to_the_exact_previous_real_row(self, widget, registry):
        clock = registry.playback_clock
        clock.seek(1_000_000_004)

        widget._on_jog_step(-2)

        assert clock.current_ts_ns == 1_000_000_002

    def test_jog_wheel_signal_end_to_end(self, widget, registry, qtbot):
        """Goes through the actual JogWheelButton signal, not a direct method call - proving the
        Qt wiring (stepRequested -> _on_jog_step -> clock.step_rows) is connected for real."""
        clock = registry.playback_clock
        clock.seek(1_000_000_004)

        widget.jog_wheel.stepRequested.emit(3)

        assert clock.current_ts_ns == 1_000_000_007


class TestNamedRangesRoundTripThroughRealSession:
    def test_mark_in_out_creates_a_range_and_persists_it_to_the_session_folder(self, widget, registry, monkeypatch):
        clock = registry.playback_clock
        clock.seek(1_000_000_002)
        widget._on_mark_in_clicked()

        clock.seek(1_000_000_007)
        monkeypatch.setattr(QInputDialog, "getText", staticmethod(lambda *a, **k: ("interesting span", True)))
        widget._on_mark_out_clicked()

        ranges = registry.playback_ranges.ranges
        assert len(ranges) == 1
        assert ranges[0].name == "interesting span"
        assert (ranges[0].start_ts_ns, ranges[0].end_ts_ns) == (1_000_000_002, 1_000_000_007)

        # Actually persisted to disk in this session's own folder, not just held in memory.
        ranges_path = registry.file_manager.get_playback_ranges_path()
        assert ranges_path.exists()
        data = json.loads(ranges_path.read_text())
        assert data["ranges"][0]["name"] == "interesting span"

    def test_selecting_a_range_from_the_combo_seeks_the_real_clock(self, widget, registry):
        registry.playback_ranges.add("crash window", 1_000_000_003, 1_000_000_006)
        widget._sync_ranges()

        widget._on_range_selected(0)

        clock = registry.playback_clock
        assert clock.current_ts_ns == 1_000_000_003
        assert clock.mode is PlaybackMode.REPLAY

    def test_entering_replay_auto_selects_the_full_recording_range(self, widget, registry):
        registry.playback_ranges.add(registry.DEFAULT_REPLAY_RANGE_NAME, 1_000_000_000, 1_000_000_009)

        ts_before_select = registry.playback_clock.current_ts_ns
        widget._on_status_clicked(True)

        rng = registry.playback_ranges.ranges[0]
        assert widget._active_range_id == rng.id
        assert widget.ranges_combo.currentData() == rng.id
        # Selecting the range must not move the playhead - the auto-select only changes which
        # range is shown zoomed, it never itself calls clock.seek().
        assert registry.playback_clock.current_ts_ns == ts_before_select

    def test_widget_constructed_while_replay_already_active_auto_selects_immediately(self, qapp, qtbot, registry):
        registry.playback_ranges.add(registry.DEFAULT_REPLAY_RANGE_NAME, 1_000_000_000, 1_000_000_009)
        registry.playback_clock.enter_replay()

        gui_context = make_real_gui_context(registry)
        late_widget = PlaybackControlWidget(gui_context)
        qtbot.addWidget(late_widget)

        rng = registry.playback_ranges.ranges[0]
        assert late_widget._active_range_id == rng.id

    def test_no_full_recording_range_is_a_noop(self, widget, registry):
        widget._on_status_clicked(True)

        assert widget._active_range_id is None

    def test_manual_range_pick_after_auto_select_is_not_overridden_by_a_later_tick(self, widget, registry):
        registry.playback_ranges.add(registry.DEFAULT_REPLAY_RANGE_NAME, 1_000_000_000, 1_000_000_009)
        registry.playback_ranges.add("crash window", 1_000_000_003, 1_000_000_006)
        widget._sync_ranges()

        widget._on_status_clicked(True)
        full_rng_id = widget._active_range_id

        crash_rng = next(r for r in registry.playback_ranges.ranges if r.name == "crash window")
        widget._on_range_selected(widget.ranges_combo.findData(crash_rng.id))
        assert widget._active_range_id == crash_rng.id
        assert widget._active_range_id != full_rng_id

        widget.apply_updates()  # another tick while still REPLAY must not re-fire the auto-select
        assert widget._active_range_id == crash_rng.id

    def test_seek_bar_paints_the_range_band(self, widget, registry):
        registry.playback_ranges.add("crash window", 1_000_000_003, 1_000_000_006)
        widget._sync_ranges()

        widget.resize(400, 30)
        widget.seek_bar.resize(400, 30)
        # Must not raise - exercises the real paintEvent range-band code path against a real
        # (non-empty) range list and real bounds.
        widget.seek_bar.repaint()

        assert len(widget.seek_bar._ranges) == 1

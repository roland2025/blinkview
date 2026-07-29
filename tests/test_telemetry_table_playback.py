# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for TelemetryTableModel's playback-clock wiring, driving the real widget
(not just the module_snapshot kernels in isolation) through a real Registry/GUIContext. Exists
to catch the class of bug the blinkview-playback-wiring skill warns about: every piece can pass
in isolation (build_snapshot_as_of returning a correct one-shot snapshot, nb_update_visible_state
handling `!=` in a microbenchmark) while still being wired together wrong."""

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.action_button_delegate import TelemetryCol
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "telemetry_table_playback_test", with_value_tracker=True)
    yield reg
    reg.stop()


@pytest.fixture
def table(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)

    device = registry.id_registry.get_device("tabletest")
    early = device.get_module("early")
    late = device.get_module("late")  # only starts emitting mid-timeline

    base = registry.now_ns()
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    src = array_pool.create(PooledLogBatch, 8, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(4):
            ts = base + i * 1_000_000_000  # 0s, 1s, 2s, 3s
            src.insert_any(ts, ts, f"state-{i}".encode("ascii"), level=0, module=early.id, device=device.id)
            if i == 3:
                late_ts = ts + 500_000_000  # 3.5s - after 'early's 3rd row, before its 4th
                src.insert_any(late_ts, late_ts, b"late-msg", level=0, module=late.id, device=device.id)
        final_ts = base + 4_000_000_000  # 4s
        src.insert_any(final_ts, final_ts, b"state-4", level=0, module=early.id, device=device.id)
        log_pool.batch_append(src)

    registry.module_value_tracker.update()

    # PlaybackClock only refreshes its cached bounds_min_ns/bounds_max_ns inside tick() (called
    # continuously by PlaybackControlWidget in the real app) - without at least one tick here,
    # bounds would still reflect the empty pool from clock construction time, before this
    # fixture's data was inserted, and enter_replay()/seek() below would clamp into that stale
    # range.
    registry.playback_clock.tick(registry.now_ns())

    w = TelemetryTable(gui_context)
    qtbot.addWidget(w)
    w.early = early
    w.late = late
    yield w


def _msg_for_module(table, module):
    for row, mod_id in enumerate(table.model.visible_mod_ids):
        if table.model.modules[mod_id] == module:
            idx = table.model.index(row, TelemetryCol.VALUE)
            return idx.data()
    return None


def test_live_apply_updates_shows_the_latest_message(table):
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-4"
    assert _msg_for_module(table, table.late) == "late-msg"


def test_replay_scrub_backward_shows_the_message_as_of_the_playhead(table):
    table.apply_updates(force=True)  # establish LIVE baseline

    clock = table.gui_context.registry.playback_clock

    clock.enter_replay(clock.bounds_min_ns)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-0"

    clock.seek(clock.bounds_max_ns)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-4"

    clock.seek(clock.bounds_min_ns)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-0"


def test_replay_before_a_late_arriving_modules_first_message_shows_empty_placeholder(table):
    clock = table.gui_context.registry.playback_clock

    # 1s in: 'early' has a message, 'late' (first message at 3.5s) does not yet.
    clock.enter_replay(clock.bounds_min_ns + 1_000_000_000)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-1"
    assert _msg_for_module(table, table.late) == "---"

    # Scrub forward past 'late's first message.
    clock.seek(clock.bounds_min_ns + 3_600_000_000)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.late) == "late-msg"

    # Scrub back again - 'late' must drop back to empty, not stay stuck at "late-msg".
    clock.seek(clock.bounds_min_ns + 1_000_000_000)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.late) == "---"


def test_returning_to_live_shows_the_latest_message_again(table):
    clock = table.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-0"

    clock.go_live()
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-4"


def test_force_live_pins_this_table_to_live_while_clock_is_in_replay(table):
    clock = table.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-0"  # still following REPLAY

    table.action_force_live.setChecked(True)
    table._toggle_force_live(True)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-4"

    # Scrubbing further must not pull this table back into REPLAY.
    clock.seek(clock.bounds_min_ns + 1_000_000_000)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-4"

    # Un-toggling resumes following the clock's current position.
    table.action_force_live.setChecked(False)
    table._toggle_force_live(False)
    table.apply_updates(force=True)
    assert _msg_for_module(table, table.early) == "state-1"

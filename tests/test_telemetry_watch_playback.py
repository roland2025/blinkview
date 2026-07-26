# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for TelemetryWatch's playback-clock wiring, driving the real widget
through a real Registry/GUIContext rather than just the module_snapshot kernels in isolation -
see tests/test_telemetry_table_playback.py's docstring for why that distinction matters here."""

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.widgets.TelemetryWatch import RowEntry, TelemetryWatch
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "telemetry_watch_playback_test", with_value_tracker=True)
    yield reg
    reg.stop()


@pytest.fixture
def watch(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)
    gui_context.set_gui_config_manager(ConfigNodeManager(gui_context))
    gui_context.logger = registry.logger_creator("gui")()

    device = registry.id_registry.get_device("watchtest")
    module = device.get_module("status")

    base = registry.now_ns()
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    src = array_pool.create(PooledLogBatch, 5, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        for i in range(5):
            ts = base + i * 1_000_000_000  # 1s apart
            src.insert_any(ts, ts, f"state-{i}".encode("ascii"), level=0, module=module.id, device=device.id)
        log_pool.batch_append(src)

    registry.module_value_tracker.update()

    # PlaybackClock only refreshes its cached bounds_min_ns/bounds_max_ns inside tick() (called
    # continuously by PlaybackControlWidget in the real app) - without at least one tick here,
    # bounds would still reflect the empty pool from clock construction time, before this
    # fixture's data was inserted, and enter_replay()/seek() below would clamp into that stale
    # range.
    registry.playback_clock.tick(registry.now_ns())

    w = TelemetryWatch(gui_context)
    qtbot.addWidget(w)
    entry = RowEntry(label="Test Row", modules=[module])
    w.entries.append(entry)
    w.rebuild_ui()
    w.module = module  # stash for tests
    w.entry = entry
    yield w


def test_live_apply_updates_shows_the_latest_message(watch):
    watch.apply_updates(force=True)
    assert watch.entry.value_label.text() == "state-4"


def test_replay_scrub_backward_and_forward_updates_the_row(watch):
    watch.apply_updates(force=True)  # establish LIVE baseline

    clock = watch.gui_context.registry.playback_clock

    clock.enter_replay(clock.bounds_min_ns)
    watch.apply_updates(force=True)
    assert watch.entry.value_label.text() == "state-0"
    assert watch.entry.last_painted_msg == "state-0"

    clock.seek(clock.bounds_max_ns)
    watch.apply_updates(force=True)
    assert watch.entry.value_label.text() == "state-4"

    clock.seek(clock.bounds_min_ns)
    watch.apply_updates(force=True)
    assert watch.entry.value_label.text() == "state-0"


def test_returning_to_live_shows_the_latest_message_again(watch):
    clock = watch.gui_context.registry.playback_clock
    clock.enter_replay(clock.bounds_min_ns)
    watch.apply_updates(force=True)
    assert watch.entry.value_label.text() == "state-0"

    clock.go_live()
    watch.apply_updates(force=True)
    assert watch.entry.value_label.text() == "state-4"

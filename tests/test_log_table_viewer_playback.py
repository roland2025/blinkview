# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end coverage for LogTableViewerWidget's playback-clock following, driving the real
widget (not just LogTableStore in isolation) through a real Registry/GUIContext - see the
blinkview-playback-wiring skill for why the real-widget level is needed to catch wiring bugs
per-piece unit tests miss. Mirrors tests/test_log_viewer_playback.py's approach."""

import pytest

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.log_table_viewer import LogTableViewerWidget
from blinkview.ui.widgets.log_view_mode import LogViewMode
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "log_table_viewer_playback_test")
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
            src.insert_any(
                ts, ts, f"{text}{i}".encode("ascii"), level=LogLevel.INFO.value, module=module.id, device=device.id
            )
        log_pool.batch_append(src)
    return base


class TestPlaybackFollow:
    def test_opening_while_replay_already_active_follows_without_manual_pause(self, qapp, qtbot, registry):
        """A tab opened *after* the clock is already in REPLAY (e.g. the user scrubs first, then
        opens a new Log Table tab to look at that point in time) must start following
        immediately, not land in a manually-paused state that requires a Resume click before the
        view reacts to further scrubbing - mirrors the LogViewerWidget regression test of the
        same name (blinkview-playback-wiring skill, Trap A)."""
        device = registry.id_registry.get_device("late_table_viewer_test")
        module = device.get_module("mod1")
        _push_messages(registry, device, module, 20)

        clock = registry.playback_clock
        clock.tick(registry.now_ns())  # refresh bounds against the rows just pushed
        mid_ts = (clock.bounds_min_ns + clock.bounds_max_ns) // 2
        clock.enter_replay(at_ts_ns=mid_ts)
        clock.tick(registry.now_ns())

        gui_context = make_real_gui_context(registry)
        late_viewer = LogTableViewerWidget(gui_context)
        qtbot.addWidget(late_viewer)
        late_viewer.resize(800, 600)

        late_viewer.prev_apply = 0
        late_viewer.apply_updates()

        assert late_viewer.is_paused is False
        assert late_viewer.model.mode == LogViewMode.HISTORY
        assert late_viewer._playback_anchored is True
        assert late_viewer.follow_playback is True

        # Scrubbing further after the tab opened must keep following without any Resume click.
        new_ts = mid_ts + 200_000_000
        clock.seek(new_ts)
        clock.tick(registry.now_ns())
        late_viewer.prev_apply = 0
        late_viewer.apply_updates()

        assert late_viewer.model.anchor_ts == new_ts
        assert late_viewer.is_paused is False

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Coverage for Registry._tick_replay_snapshot (core/registry.py) - the background-task
counterpart to module_value_tracker.update_and_print, keeping LatestModuleValueTracker's REPLAY
scrub cache fresh off the UI thread (see plans/expressive-sauteeing-sun.md). In production this
is driven by tasks.run_periodic(0.1, ...) inside Registry.start(); make_real_registry(...,
start=False) (the default, used here) never calls Registry.start(), so it's called directly
instead - the same "manual drive" convention already used for tracker.update() throughout the
playback test suite."""

from blinkview.core.numpy_batch_manager import PooledLogBatch
from tests.fakes.real_registry import make_real_registry


def _insert(registry, device_name, module_path, ts, level, message):
    device = registry.id_registry.get_device(device_name)
    module = device.get_module(module_path)
    array_pool = registry.system_ctx.array_pool
    batch = array_pool.create(PooledLogBatch, 1, 512, has_levels=True, has_modules=True, has_devices=True)
    with batch:
        batch.insert_any(ts, ts, message.encode("utf-8"), level=level, module=module.id, device=device.id)
        registry.central.log_pool.batch_append(batch)
    return module


def test_tick_replay_snapshot_is_a_noop_in_live_mode(tmp_path):
    registry = make_real_registry(tmp_path, "tick_replay_noop_test", with_value_tracker=True)
    try:
        module = _insert(registry, "dev", "temp", 1000, 1, "hello")
        registry.module_value_tracker.update()

        assert registry.playback_clock.mode.value == "live"

        # No-op: must not touch the scrub cache while the clock is LIVE.
        registry._tick_replay_snapshot()

        with registry.module_value_tracker.get_replay_snapshot() as snap:
            # Falls back to the LIVE "latest ever" state (get_snapshot()) since update_replay
            # was never called - not an empty/uninitialized snapshot.
            assert snap.get_message(module.id) == "hello"
    finally:
        registry.stop()


def test_tick_replay_snapshot_updates_the_scrub_cache_in_replay_mode(tmp_path):
    registry = make_real_registry(tmp_path, "tick_replay_updates_test", with_value_tracker=True)
    try:
        module = _insert(registry, "dev", "temp", 1000, 1, "first")
        _insert(registry, "dev", "temp", 5000, 2, "second")
        registry.module_value_tracker.update()

        # PlaybackClock only refreshes its cached bounds inside tick() - see the identical
        # comment in tests/test_module_snapshot.py's sibling playback test fixtures.
        registry.playback_clock.tick(registry.now_ns())

        registry.playback_clock.enter_replay(registry.playback_clock.bounds_min_ns + 2000)
        registry._tick_replay_snapshot()

        with registry.module_value_tracker.get_replay_snapshot() as snap:
            assert snap.get_message(module.id) == "first"

        registry.playback_clock.seek(registry.playback_clock.bounds_max_ns)
        registry._tick_replay_snapshot()

        with registry.module_value_tracker.get_replay_snapshot() as snap:
            assert snap.get_message(module.id) == "second"
    finally:
        registry.stop()

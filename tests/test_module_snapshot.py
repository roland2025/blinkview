# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Direct unit coverage for LatestModuleValueTracker/ModuleSnapshot's plain-Python surface
(ref-counting, message/level/sequence accessors, iteration, playback-scrub rebuild) - the
njit kernels (nb_copy_snapshot_state/nb_update_master_arrays_reverse/nb_build_snapshot_as_of)
are already exercised indirectly through these same code paths but aren't the target here,
since coverage.py can't see inside compiled Numba code anyway. This is narrower than
test_telemetry_table_playback.py/test_telemetry_watch_playback.py, which drive the tracker
only incidentally through a real widget."""

import pytest

from blinkview.core.module_snapshot import LatestModuleValueTracker
from blinkview.core.numpy_batch_manager import PooledLogBatch
from tests.fakes.real_log_pool import make_real_log_pool
from tests.fakes.real_registry import make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "module_snapshot_test", with_value_tracker=True)
    yield reg
    reg.stop()


def _insert(registry, device_name, module_path, ts, level, message):
    device = registry.id_registry.get_device(device_name)
    module = device.get_module(module_path)
    array_pool = registry.system_ctx.array_pool
    batch = array_pool.create(PooledLogBatch, 1, 512, has_levels=True, has_modules=True, has_devices=True)
    with batch:
        batch.insert_any(ts, ts, message.encode("utf-8"), level=level, module=module.id, device=device.id)
        registry.central.log_pool.batch_append(batch)
    return module


def test_update_reflects_latest_message_level_and_sequence(registry):
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "hello")

    tracker.update()

    with tracker.get_snapshot() as snap:
        assert snap.get_message(module.id) == "hello"
        assert snap.get_level(module.id) == 1
        assert snap.get_sequence(module.id) > 0


def test_update_keeps_only_the_newest_row_per_module(registry):
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "first")
    tracker.update()
    _insert(registry, "dev", "temp", 2000, 2, "second")
    tracker.update()

    with tracker.get_snapshot() as snap:
        assert snap.get_message(module.id) == "second"
        assert snap.get_level(module.id) == 2


def test_get_message_for_module_with_no_data_returns_empty(registry):
    tracker = registry.module_value_tracker
    device = registry.id_registry.get_device("dev")
    untouched = device.get_module("untouched")

    tracker.update()

    with tracker.get_snapshot() as snap:
        assert snap.get_message(untouched.id) == ""
        assert snap.get_sequence(untouched.id) == 0


def test_accessors_out_of_bounds_module_id_return_safe_defaults(registry):
    tracker = registry.module_value_tracker
    tracker.update()

    with tracker.get_snapshot() as snap:
        assert snap.get_message(999_999) == ""
        assert snap.get_level(999_999) == 0
        assert snap.get_sequence(999_999) == 0


def test_iteration_yields_ts_seq_message_per_module_in_id_order(registry):
    tracker = registry.module_value_tracker
    a = _insert(registry, "dev", "a", 1000, 0, "msg-a")
    b = _insert(registry, "dev", "b", 2000, 0, "msg-b")
    tracker.update()

    with tracker.get_snapshot() as snap:
        results = list(snap)

    assert results[a.id] == (1000, results[a.id][1], "msg-a")
    assert results[b.id] == (2000, results[b.id][1], "msg-b")
    assert results[a.id][1] > 0
    assert results[b.id][1] > 0


def test_iteration_yields_empty_message_and_zero_sequence_for_untouched_modules(registry):
    tracker = registry.module_value_tracker
    device = registry.id_registry.get_device("dev")
    untouched = device.get_module("untouched")
    tracker.update()

    with tracker.get_snapshot() as snap:
        results = list(snap)

    ts, seq, msg = results[untouched.id]
    assert seq == 0
    assert msg == ""


def test_retain_after_full_release_raises_runtime_error(registry):
    tracker = registry.module_value_tracker
    snap = tracker._allocate_snapshot(capacity=4, count=0, last_known_seq=0)

    snap.release()  # ref_count 1 -> 0: returns arrays to the pool

    with pytest.raises(RuntimeError):
        snap.retain()


def test_get_snapshot_retain_keeps_the_snapshot_alive_after_one_release(registry):
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "hello")
    tracker.update()

    snap = tracker.get_snapshot()  # ref_count now 2 (tracker's own + this one)
    snap.release()  # back to 1 - still held by the tracker, must not be freed yet

    assert snap.get_message(module.id) == "hello"


def test_build_snapshot_as_of_rebuilds_state_at_a_past_timestamp(registry):
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "old")
    tracker.update()
    _insert(registry, "dev", "temp", 5000, 2, "new")
    tracker.update()

    with tracker.build_snapshot_as_of(2000) as snap:
        assert snap.get_message(module.id) == "old"

    with tracker.build_snapshot_as_of(6000) as snap:
        assert snap.get_message(module.id) == "new"


def test_build_snapshot_as_of_before_any_data_existed_is_empty(registry):
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 5000, 1, "hello")
    tracker.update()

    with tracker.build_snapshot_as_of(1000) as snap:
        assert snap.get_message(module.id) == ""
        assert snap.get_sequence(module.id) == 0


def test_build_snapshot_as_of_with_zero_registered_modules_returns_empty(id_registry, array_pool):
    """Uses a bare IDRegistry (no devices/modules registered at all) rather than the `registry`
    fixture above, since Registry.configure_system() always registers a handful of internal
    "system" modules - there's no way to observe a true zero-module count through it."""
    _, log_pool = make_real_log_pool()
    tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)

    with tracker.build_snapshot_as_of(0) as snap:
        assert list(snap) == []


def test_module_that_never_logs_resolves_instantly_with_no_prior_scrub_cache(registry):
    """update() populates the persistent first-occurrence table as a byproduct - a module that
    never receives any data should be resolvable by build_snapshot_as_of's very first-ever call
    on this tracker (no _scrub_cache yet at all), not just on a later call that benefits from a
    previously-built cache."""
    tracker = registry.module_value_tracker
    logging_module = _insert(registry, "dev", "chatty", 1000, 1, "hello")
    silent_device = registry.id_registry.get_device("dev")
    silent_module = silent_device.get_module("silent")  # registered, but never logs anything
    tracker.update()

    assert tracker._scrub_cache is None  # this is genuinely the first-ever call below

    with tracker.build_snapshot_as_of(5000) as snap:
        assert snap.get_message(logging_module.id) == "hello"
        assert snap.get_message(silent_module.id) == ""
        assert snap.get_sequence(silent_module.id) == 0


def test_module_registered_after_first_seen_coverage_but_never_logs_still_resolves_correctly(registry):
    """A module registered after update() already ran isn't yet represented in the
    first-occurrence table (array not grown, coverage doesn't vouch for it) - must still resolve
    correctly (falls through to an actual scan, finds nothing), not crash or return stale data."""
    tracker = registry.module_value_tracker
    tracker.update()  # establishes first-occurrence coverage before the module below exists

    device = registry.id_registry.get_device("dev")
    late_silent_module = device.get_module("late_silent")  # registered after the update() above

    with tracker.build_snapshot_as_of(5000) as snap:
        assert snap.get_message(late_silent_module.id) == ""
        assert snap.get_sequence(late_silent_module.id) == 0


def test_module_resolves_via_live_snapshot_with_no_prior_cache_or_first_seen_confirmation(registry):
    """A module WITH data: build_snapshot_as_of's very first-ever call, anchored at/after that
    module's live latest-ever message, should resolve correctly using _current_snapshot directly
    - no _scrub_cache exists yet, and the module obviously isn't in the confirmed-absent table
    either (it has data)."""
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "hello")
    tracker.update()

    assert tracker._scrub_cache is None

    with tracker.build_snapshot_as_of(2000) as snap:  # after the message, before any scrub cache
        assert snap.get_message(module.id) == "hello"
        assert snap.get_sequence(module.id) > 0


def test_get_replay_snapshot_falls_back_to_get_snapshot_before_any_update_replay_call(registry):
    """Before update_replay() has ever run (e.g. REPLAY just entered, the background task -
    Registry._tick_replay_snapshot in production - hasn't ticked yet), get_replay_snapshot()
    should return the LIVE "latest ever" state as a reasonable placeholder rather than blocking
    or computing inline."""
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "hello")
    tracker.update()

    with tracker.get_replay_snapshot() as snap:
        assert snap.get_message(module.id) == "hello"


def test_update_replay_then_get_replay_snapshot_matches_build_snapshot_as_of(registry):
    """update_replay()/get_replay_snapshot() are thin wrappers around the same build_snapshot_as_of
    state (_scrub_cache) - after update_replay(ts_ns), get_replay_snapshot() should read back
    exactly what a direct build_snapshot_as_of(ts_ns) call would have returned."""
    tracker = registry.module_value_tracker
    module = _insert(registry, "dev", "temp", 1000, 1, "old")
    _insert(registry, "dev", "temp", 5000, 2, "new")

    tracker.update_replay(2000)
    with tracker.get_replay_snapshot() as snap:
        assert snap.get_message(module.id) == "old"

    tracker.update_replay(6000)
    with tracker.get_replay_snapshot() as snap:
        assert snap.get_message(module.id) == "new"

    # Interchangeable with build_snapshot_as_of - both read/write the same _scrub_cache, so a
    # direct call after update_replay (or vice versa) still follows the forward/backward cache
    # rules rather than being some separate, disconnected piece of state.
    with tracker.build_snapshot_as_of(2000) as snap:
        assert snap.get_message(module.id) == "old"

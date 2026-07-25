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

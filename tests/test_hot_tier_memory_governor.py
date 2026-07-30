# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.hot_tier_memory_governor import HotTierMemoryGovernor, compute_target_pieces
from blinkview.core.limits import MAX_SEGMENTS_EVICTED_PER_TICK
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool
from blinkview.utils.log_level import LogLevel


def wait_for(predicate, timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return predicate()


def push_rows(pool, global_pool, count, ts_start):
    """One-row-per-segment pushes, paced like tests/test_numpy_log_cold_tier.py's push_rows so the
    archiver's single background writer thread can keep up without hitting its bounded-queue
    drop path."""
    for i in range(count):
        batch = global_pool.create(
            PooledLogBatch,
            req_capacity=1,
            buffer_bytes=1,
            has_levels=True,
            has_modules=True,
            has_devices=True,
            has_sequences=True,
        )
        batch.insert(ts_start + i, ts_start + i, f"row{i}".encode(), LogLevel.INFO.value, 0, 0, 0)
        pool.batch_append(batch)
        batch.release()
        time.sleep(0.01)


class TestComputeTargetPieces:
    def test_no_op_within_hysteresis_band(self):
        # slack = available - target_free = 0, strictly below the grow threshold (segment_bytes)
        # and not negative -> no-op.
        target = compute_target_pieces(
            available_bytes=1000,
            current_pieces=4,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=1000,
        )
        assert target == 4

    def test_no_op_just_under_grow_threshold(self):
        target = compute_target_pieces(
            available_bytes=1099,
            current_pieces=4,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=1000,
        )
        assert target == 4

    def test_grows_by_one_segment_once_hysteresis_band_cleared(self):
        # available >= target_free + segment_bytes triggers exactly a +1 grow step.
        target = compute_target_pieces(
            available_bytes=1100,
            current_pieces=4,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=1000,
        )
        assert target == 5

    def test_shrinks_by_ceil_of_deficit_segments(self):
        # slack = -150 -> ceil(150/100) = 2 segments.
        target = compute_target_pieces(
            available_bytes=850,
            current_pieces=6,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=1000,
        )
        assert target == 4

    def test_shrink_step_capped_at_max_segments_evicted_per_tick(self):
        # A huge deficit would ask for far more than MAX_SEGMENTS_EVICTED_PER_TICK in one tick -
        # capped so a single update_max_pieces() call can't hold log_pool._lock for an unbounded
        # eviction loop.
        target = compute_target_pieces(
            available_bytes=0,
            current_pieces=10,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=10_000,
        )
        assert target == 10 - MAX_SEGMENTS_EVICTED_PER_TICK

    def test_shrink_clamped_at_min_hot_pieces_floor(self):
        target = compute_target_pieces(
            available_bytes=0,
            current_pieces=5,
            segment_bytes=100,
            min_hot_pieces=3,
            max_hot_pieces=None,
            target_free_bytes=10_000,
        )
        assert target == 3

    def test_grow_clamped_at_max_hot_pieces_ceiling(self):
        target = compute_target_pieces(
            available_bytes=1_000_000,
            current_pieces=5,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=5,
            target_free_bytes=1000,
        )
        assert target == 5

    def test_unbounded_when_max_hot_pieces_is_none(self):
        target = compute_target_pieces(
            available_bytes=1_000_000,
            current_pieces=5,
            segment_bytes=100,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=1000,
        )
        assert target == 6


class TestHotTierMemoryGovernorRealPool:
    """Drives a real CircularLogPool (with cold storage enabled) through HotTierMemoryGovernor.tick()
    calls with a scripted fake get_available_bytes reading, mirroring the plan doc's "Real-pool
    integration test" strategy - calling tick() directly (rather than start()/stop() via a real
    TaskManager) keeps this deterministic instead of timing-flaky."""

    @pytest.fixture
    def global_pool(self):
        return NumpyArrayPool(min_bytes=8, max_bytes=1024 * 1024)

    @pytest.fixture
    def pool(self, global_pool, tmp_path):
        p = CircularLogPool(
            global_pool, max_pieces=6, cold_max_pieces=10, cold_storage_dir=str(tmp_path), final_buffer_bytes=1024
        )
        p.segment_capacity = 1
        p._optimized = True
        p.clear()
        yield p
        p.release_all()

    def test_shrinks_hot_tier_under_memory_pressure_and_archives_to_cold(self, pool, global_pool):
        push_rows(pool, global_pool, 6, ts_start=100)
        assert wait_for(lambda: len(pool.segments) == 6)

        readings = iter([0])  # far below target_free -> shrink
        governor = HotTierMemoryGovernor(
            log_pool=pool,
            task_manager=None,  # not exercised - these tests call tick() directly, not start()
            get_available_bytes=lambda: next(readings),
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=10_000,
        )

        governor.tick()

        assert pool.max_pieces == 6 - MAX_SEGMENTS_EVICTED_PER_TICK
        assert wait_for(lambda: len(pool.cold_segments) >= MAX_SEGMENTS_EVICTED_PER_TICK)

    def test_shrink_never_crosses_min_hot_pieces_floor_across_repeated_ticks(self, pool, global_pool):
        push_rows(pool, global_pool, 6, ts_start=100)
        assert wait_for(lambda: len(pool.segments) == 6)

        governor = HotTierMemoryGovernor(
            log_pool=pool,
            task_manager=None,  # not exercised - these tests call tick() directly, not start()
            get_available_bytes=lambda: 0,  # persistently tight
            min_hot_pieces=2,
            max_hot_pieces=None,
            target_free_bytes=10_000,
        )

        for _ in range(5):
            governor.tick()

        assert pool.max_pieces == 2

    def test_grows_ceiling_when_memory_is_abundant(self, pool, global_pool):
        push_rows(pool, global_pool, 2, ts_start=100)
        assert wait_for(lambda: len(pool.segments) == 2)

        governor = HotTierMemoryGovernor(
            log_pool=pool,
            task_manager=None,  # not exercised - these tests call tick() directly, not start()
            get_available_bytes=lambda: 1_000_000_000,  # plenty free
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=10_000,
        )

        starting_max_pieces = pool.max_pieces
        governor.tick()

        assert pool.max_pieces == starting_max_pieces + 1
        # Growing the ceiling doesn't backfill already-evicted-or-never-created segments.
        assert len(pool.segments) == 2

    def test_grow_respects_max_hot_pieces_ceiling(self, pool, global_pool):
        push_rows(pool, global_pool, 2, ts_start=100)
        assert wait_for(lambda: len(pool.segments) == 2)

        governor = HotTierMemoryGovernor(
            log_pool=pool,
            task_manager=None,  # not exercised - these tests call tick() directly, not start()
            get_available_bytes=lambda: 1_000_000_000,
            min_hot_pieces=1,
            max_hot_pieces=pool.max_pieces,
            target_free_bytes=10_000,
        )

        governor.tick()

        assert pool.max_pieces == 6  # unchanged - already at the configured ceiling


class FakeTasks:
    """Duck-typed stand-in for TaskManager's run_periodic/stop_periodic - see
    tests/test_uart_reader.py's FakeTasks for the same pattern used elsewhere."""

    def __init__(self):
        self.started = []
        self.stopped = []
        self._next_id = 0

    def run_periodic(self, interval, func, *args, **kwargs):
        self._next_id += 1
        task_id = f"task-{self._next_id}"
        self.started.append((task_id, interval, func))
        return task_id

    def stop_periodic(self, task_id):
        self.stopped.append(task_id)


class TestHotTierMemoryGovernorScheduling:
    """The governor rides the shared TaskManager's scheduler (see io/adb_reader.py, io/uart.py,
    io/source_handshake.py for the same run_periodic/stop_periodic pattern) instead of owning a
    dedicated OS thread."""

    def make_governor(self, tasks, **overrides):
        kwargs = dict(
            log_pool=object(),
            task_manager=tasks,
            get_available_bytes=lambda: 0,
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=10_000,
            poll_interval_sec=3.0,
        )
        kwargs.update(overrides)
        return HotTierMemoryGovernor(**kwargs)

    def test_start_registers_a_periodic_task_at_the_configured_interval(self):
        tasks = FakeTasks()
        governor = self.make_governor(tasks, poll_interval_sec=5.0)

        governor.start()

        assert len(tasks.started) == 1
        task_id, interval, func = tasks.started[0]
        assert interval == 5.0
        assert func == governor._tick_safe
        assert governor._task_id == task_id

    def test_start_is_idempotent(self):
        tasks = FakeTasks()
        governor = self.make_governor(tasks)

        governor.start()
        governor.start()

        assert len(tasks.started) == 1

    def test_stop_deregisters_the_task(self):
        tasks = FakeTasks()
        governor = self.make_governor(tasks)
        governor.start()
        task_id = governor._task_id

        governor.stop()

        assert tasks.stopped == [task_id]
        assert governor._task_id is None

    def test_update_policy_does_not_restart_task_when_interval_unchanged(self):
        tasks = FakeTasks()
        governor = self.make_governor(tasks, poll_interval_sec=3.0)
        governor.start()

        governor.update_policy(min_hot_pieces=2, max_hot_pieces=5, target_free_bytes=20_000, poll_interval_sec=3.0)

        assert governor.min_hot_pieces == 2
        assert governor.max_hot_pieces == 5
        assert governor.target_free_bytes == 20_000
        assert tasks.stopped == []
        assert len(tasks.started) == 1

    def test_update_policy_restarts_task_when_interval_changes(self):
        tasks = FakeTasks()
        governor = self.make_governor(tasks, poll_interval_sec=3.0)
        governor.start()
        old_task_id = governor._task_id

        governor.update_policy(min_hot_pieces=1, max_hot_pieces=None, target_free_bytes=10_000, poll_interval_sec=1.0)

        assert tasks.stopped == [old_task_id]
        assert len(tasks.started) == 2
        assert governor._task_id != old_task_id
        assert governor.poll_interval_sec == 1.0

    def test_tick_safe_swallows_and_logs_exceptions(self):
        class ExplodingLogPool:
            max_pieces = 4
            final_buffer_bytes = 100

            def update_max_pieces(self, target):
                raise RuntimeError("boom")

        logged = []
        governor = HotTierMemoryGovernor(
            log_pool=ExplodingLogPool(),
            task_manager=None,
            get_available_bytes=lambda: 0,  # forces a shrink -> update_max_pieces -> raises
            min_hot_pieces=1,
            max_hot_pieces=None,
            target_free_bytes=10_000,
            logger=SimpleNamespace(exception=lambda *a, **k: logged.append(a)),
        )

        governor._tick_safe()  # must not raise

        assert len(logged) == 1

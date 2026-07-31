# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Coverage for LatestModuleValueTracker.build_snapshot_as_of's bidirectional scrub cache
(core/module_snapshot.py) - added after real-data benchmarking (against a 50M-row replay) found
every call costing a flat ~30ms regardless of anchor position, because the old implementation
always rescanned from scratch. The fix caches the previous call's full per-module state
(_scrub_cache/_scrub_cache_ts_ns) so:
- a forward-advancing anchor (the common REPLAY-follow-tick case) only scans the new segments/
  rows between the old and new anchor, and
- a backward-jumping anchor pre-seeds any module whose cached occurrence remains correct (its
  cached ts is already <= the new, smaller anchor, or it's a confirmed "no data" entry) straight
  into found_mask, skipping it without touching a single row - only modules whose cached
  occurrence is now excluded need a fresh (unbounded) backward search.
See plans/expressive-sauteeing-sun.md.

Correctness tests use tests.fakes.real_registry.make_real_registry (hot-tier-only, few rows) -
same pattern as test_module_snapshot.py. The perf tests build real cold-storage segments (same
pattern as tests/test_log_pool_fetch_perf.py) with a module that only appears in the very oldest
segment - mirroring the real-world "one-shot boot/handshake message" shape that made the old
implementation's cost independent of anchor position (it always had to scan back to that one
segment to resolve every module)."""

import time

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_segment import ColdSegmentMeta, write_cold_segment_file
from blinkview.core.module_snapshot import LatestModuleValueTracker
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import CircularLogPool
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


def _fresh_snapshot_dict(registry, ts_ns):
    """Builds a from-scratch LatestModuleValueTracker (guaranteed no scrub cache) and returns
    {module_id: (timestamp, sequence, message)} as of ts_ns - the "ground truth" a cached/
    incremental call must match exactly."""
    fresh_tracker = LatestModuleValueTracker(
        registry.central.log_pool, registry.id_registry.modules_table, registry.system_ctx.array_pool, registry.now_ns
    )
    with fresh_tracker.build_snapshot_as_of(ts_ns) as snap:
        return _snapshot_dict(snap)


def _snapshot_dict(snap):
    """(ts, seq, msg) per module, normalizing "no data yet" (seq==0) entries to a fixed ts
    placeholder - the timestamp slot for an unresolved module is never zeroed (only sequence_ids
    is), so it holds whatever stale value a previous, unrelated pool allocation left there
    (array_pool doesn't zero-fill on acquire). __iter__ already gates on seq==0 before a caller
    would trust ts; two independently-allocated snapshots (e.g. this tracker's vs a fresh
    reference tracker's) can legitimately differ there without it being a real mismatch."""
    return {i: (ts if seq != 0 else None, seq, msg) for i, (ts, seq, msg) in enumerate(snap)}


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "scrub_cache_test", with_value_tracker=True)
    yield reg
    reg.stop()


class TestForwardIncrementalMatchesFullScan:
    def test_sequence_of_increasing_ts_matches_independent_full_scans(self, registry):
        tracker = registry.module_value_tracker
        mod_a = _insert(registry, "dev", "boot_only", 1000, 1, "boot")  # only ever logs once, early
        mod_b = _insert(registry, "dev", "chatty", 2000, 1, "b1")
        _insert(registry, "dev", "chatty", 4000, 1, "b2")
        _insert(registry, "dev", "chatty", 6000, 1, "b3")
        _insert(registry, "dev", "chatty", 8000, 1, "b4")

        for ts_ns in (1500, 3000, 5000, 7000, 9000):
            with tracker.build_snapshot_as_of(ts_ns) as snap:
                got = _snapshot_dict(snap)
            expected = _fresh_snapshot_dict(registry, ts_ns)
            assert got[mod_a.id] == expected[mod_a.id], f"mismatch for boot-only module at ts={ts_ns}"
            assert got[mod_b.id] == expected[mod_b.id], f"mismatch for chatty module at ts={ts_ns}"

    def test_repeated_call_at_same_ts_returns_identical_result_without_error(self, registry):
        tracker = registry.module_value_tracker
        mod = _insert(registry, "dev", "temp", 1000, 1, "hello")

        with tracker.build_snapshot_as_of(2000) as snap:
            first = _snapshot_dict(snap)
        with tracker.build_snapshot_as_of(2000) as snap:
            second = _snapshot_dict(snap)

        assert first[mod.id] == second[mod.id]


class TestBackwardJumpAfterForwardRun:
    def test_backward_jump_matches_full_scan_and_forward_resumes_correctly(self, registry):
        tracker = registry.module_value_tracker
        mod = _insert(registry, "dev", "temp", 1000, 1, "first")
        _insert(registry, "dev", "temp", 5000, 2, "second")
        _insert(registry, "dev", "temp", 9000, 3, "third")

        with tracker.build_snapshot_as_of(6000) as snap:
            assert _snapshot_dict(snap)[mod.id][2] == "second"

        # Backward jump: cached occurrence (ts=5000) is now newer than the new anchor (2000), so
        # it's invalidated and must be re-resolved by scanning for the module's next-older row.
        with tracker.build_snapshot_as_of(2000) as snap:
            got = _snapshot_dict(snap)[mod.id]
        assert got == _fresh_snapshot_dict(registry, 2000)[mod.id]
        assert got[2] == "first"

        # Forward again from the rewound position should still be correct.
        with tracker.build_snapshot_as_of(10000) as snap:
            got = _snapshot_dict(snap)[mod.id]
        assert got == _fresh_snapshot_dict(registry, 10000)[mod.id]
        assert got[2] == "third"


class TestBackwardJumpOnlyInvalidatesSupersededModules:
    def test_valid_module_untouched_while_superseded_module_is_reresolved(self, registry):
        """Two modules: `stable` only ever logs once, early - its cached occurrence stays valid
        across the backward jump below and should be pre-seeded (skipped) rather than rescanned.
        `moving` logs twice - its later occurrence gets invalidated by the jump and must resolve
        to its earlier one."""
        tracker = registry.module_value_tracker
        mod_stable = _insert(registry, "dev", "stable", 1000, 1, "only_ever")
        mod_moving = _insert(registry, "dev", "moving", 2000, 1, "early")
        _insert(registry, "dev", "moving", 8000, 2, "late")

        with tracker.build_snapshot_as_of(9000) as snap:
            got = _snapshot_dict(snap)
        assert got[mod_stable.id][2] == "only_ever"
        assert got[mod_moving.id][2] == "late"

        # Jump back below "late" (8000) but above "early" (2000) and "only_ever" (1000).
        with tracker.build_snapshot_as_of(5000) as snap:
            got = _snapshot_dict(snap)
        expected = _fresh_snapshot_dict(registry, 5000)
        assert got[mod_stable.id] == expected[mod_stable.id]
        assert got[mod_stable.id][2] == "only_ever"
        assert got[mod_moving.id] == expected[mod_moving.id]
        assert got[mod_moving.id][2] == "early"


class TestModuleCountGrowsBetweenCalls:
    def test_new_module_registered_after_cache_primed_is_handled(self, registry):
        tracker = registry.module_value_tracker
        mod_old = _insert(registry, "dev", "temp", 1000, 1, "old_module")

        with tracker.build_snapshot_as_of(2000) as snap:
            assert _snapshot_dict(snap)[mod_old.id][2] == "old_module"

        # New module registered (grows modules_table.bundle().count) after the cache was primed.
        mod_new = _insert(registry, "dev", "brand_new", 3000, 1, "hi")

        with tracker.build_snapshot_as_of(4000) as snap:
            got = _snapshot_dict(snap)
        expected = _fresh_snapshot_dict(registry, 4000)
        assert got[mod_old.id] == expected[mod_old.id]
        assert got[mod_new.id] == expected[mod_new.id]
        assert got[mod_new.id][2] == "hi"


# ---------------------------------------------------------------------------
# Perf: second (small forward step) call should not scale with total segment count.
# ---------------------------------------------------------------------------

BASE_TS = 1_000_000_000_000
SEGMENT_SPACING_NS = 10_000_000_000  # 10s apart


def _make_cold_segment(array_pool, path, index, ts_ns, module):
    batch = array_pool.create(
        PooledLogBatch,
        req_capacity=1,
        buffer_bytes=32,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=True,
        has_tids=True,
    )
    batch.insert_any(ts_ns, ts_ns, str(float(index)).encode("ascii"), level=0, module=module, device=0, seq=index + 1)
    header = write_cold_segment_file(path, batch.bundle)
    batch.release()
    meta = ColdSegmentMeta(str(path), header.earliest_ts, header.latest_ts, header.first_seq, header.last_seq)
    return PooledLogBatch.from_memmap(str(path), metadata=meta)


def _build_pool_with_boot_only_module(tmp_path, n, boot_module, chatty_module):
    """n cold segments, SEGMENT_SPACING_NS apart. Segment 0 (oldest) carries `boot_module`'s
    only-ever row; every other segment carries `chatty_module`. Resolving "latest as of the
    newest anchor" for both modules requires scanning all the way back to segment 0 - the same
    shape as a real one-shot boot/handshake message that made the pre-cache implementation's
    cost independent of anchor position."""
    array_pool = NumpyArrayPool()
    log_pool = CircularLogPool(array_pool, max_pieces=1, final_buffer_bytes=64 * 1024)

    tmp_path.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        ts_ns = BASE_TS + i * SEGMENT_SPACING_NS
        module = boot_module if i == 0 else chatty_module
        path = tmp_path / f"segment_{i:010d}.blkseg"
        cold = _make_cold_segment(array_pool, path, i, ts_ns, module)
        log_pool.cold_segments.append(cold)

    return array_pool, log_pool


def _build_pool_all_chatty(tmp_path, n, chatty_module):
    """n cold segments, SEGMENT_SPACING_NS apart, all carrying `chatty_module`'s rows only - used
    alongside a separately-registered, never-appearing module to test the persistent
    first-occurrence "confirmed absent" pre-seed."""
    array_pool = NumpyArrayPool()
    log_pool = CircularLogPool(array_pool, max_pieces=1, final_buffer_bytes=64 * 1024)

    tmp_path.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        ts_ns = BASE_TS + i * SEGMENT_SPACING_NS
        path = tmp_path / f"segment_{i:010d}.blkseg"
        cold = _make_cold_segment(array_pool, path, i, ts_ns, chatty_module)
        log_pool.cold_segments.append(cold)

    return array_pool, log_pool


def _build_pool_with_one_module_per_segment(tmp_path, n, module_ids):
    """n cold segments, SEGMENT_SPACING_NS apart, each carrying exactly one row for a distinct
    module (module_ids[i] for segment i) - resolving "latest as of the newest anchor" for every
    module requires visiting every segment, since none of them repeat. Used to test the live-valid
    (_current_snapshot) pre-seed: after a single update() call has scanned everything once, every
    module's live-latest is already known, so a fresh build_snapshot_as_of anchored at/after the
    newest row should resolve everything without touching a single segment."""
    array_pool = NumpyArrayPool()
    log_pool = CircularLogPool(array_pool, max_pieces=1, final_buffer_bytes=64 * 1024)

    tmp_path.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        ts_ns = BASE_TS + i * SEGMENT_SPACING_NS
        path = tmp_path / f"segment_{i:010d}.blkseg"
        cold = _make_cold_segment(array_pool, path, i, ts_ns, module_ids[i])
        log_pool.cold_segments.append(cold)

    return array_pool, log_pool


def _time_calls(fn, repeats=8):
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    samples.sort()
    return samples[len(samples) // 2]


class TestSecondForwardCallDoesNotScaleWithSegmentCount:
    def test_going_from_4_to_128_cold_segments(self, tmp_path, id_registry):
        device = id_registry.get_device("esp32")
        boot_module = device.get_module("boot_handshake").id
        chatty_module = device.get_module("telemetry").id

        array_pool_small, log_pool_small = _build_pool_with_boot_only_module(
            tmp_path / "small", 4, boot_module, chatty_module
        )
        array_pool_large, log_pool_large = _build_pool_with_boot_only_module(
            tmp_path / "large", 128, boot_module, chatty_module
        )

        def make_calls(array_pool, log_pool, n):
            tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
            newest_ts = BASE_TS + (n - 1) * SEGMENT_SPACING_NS

            def first_call():
                with tracker.build_snapshot_as_of(newest_ts):
                    pass

            def second_call_small_forward_step():
                with tracker.build_snapshot_as_of(newest_ts + 1):
                    pass

            return tracker, first_call, second_call_small_forward_step

        tracker_small, first_small, second_small = make_calls(array_pool_small, log_pool_small, 4)
        tracker_large, first_large, second_large = make_calls(array_pool_large, log_pool_large, 128)

        # First (cold, priming) call is expected to scale with segment count - not the thing
        # being fixed here, just establishing this scenario actually reproduces the old cost
        # shape before the fix kicks in on the *next* call.
        first_small_t = _time_calls(first_small, repeats=1)
        first_large_t = _time_calls(first_large, repeats=1)

        # The first_small/first_large calls above already primed each tracker's cache at
        # newest_ts - these second calls exercise the forward-incremental (cache-hit) path, using
        # the median of several repeats to absorb any one-time Numba compile cost on the first
        # sample (nb_copy_snapshot_state's first-ever call in this process happens here).
        second_small_t = _time_calls(second_small, repeats=8)
        second_large_t = _time_calls(second_large, repeats=8)

        ratio = second_large_t / second_small_t if second_small_t else float("inf")
        print(
            f"\nfirst (cold) call: 4 segments = {first_small_t * 1000:.3f} ms, "
            f"128 segments = {first_large_t * 1000:.3f} ms"
        )
        print(
            f"second (forward-incremental) call: 4 segments = {second_small_t * 1000:.4f} ms, "
            f"128 segments = {second_large_t * 1000:.4f} ms, ratio = {ratio:.1f}x"
        )

        assert ratio < 8, (
            f"forward-incremental build_snapshot_as_of scaled {ratio:.1f}x going from 4 to 128 "
            f"cold segments on a small forward step - it should only touch segments in the new "
            f"window, not rescan the whole pool via the cache."
        )


class TestBackwardStepWithinProvenEmptyRangeIsFree:
    """A backward step that doesn't cross any module's actual cached occurrence should touch
    zero segments (found_mask pre-seeded all-True, remaining=0, the scan loop never runs) -
    regardless of how many total segments exist below it."""

    def test_going_from_4_to_128_cold_segments(self, tmp_path, id_registry):
        device = id_registry.get_device("esp32")
        boot_module = device.get_module("boot_handshake").id
        chatty_module = device.get_module("telemetry").id

        array_pool_small, log_pool_small = _build_pool_with_boot_only_module(
            tmp_path / "small", 4, boot_module, chatty_module
        )
        array_pool_large, log_pool_large = _build_pool_with_boot_only_module(
            tmp_path / "large", 128, boot_module, chatty_module
        )

        def make_calls(array_pool, log_pool, n):
            tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
            # Anchor comfortably past the newest real row so the priming call still resolves
            # everything (same cost shape as TestSecondForwardCallDoesNotScaleWithSegmentCount),
            # leaving margin to step backward without crossing chatty's actual row.
            newest_row_ts = BASE_TS + (n - 1) * SEGMENT_SPACING_NS
            margin_anchor = newest_row_ts + SEGMENT_SPACING_NS

            def prime():
                with tracker.build_snapshot_as_of(margin_anchor):
                    pass

            def backward_step_within_margin():
                with tracker.build_snapshot_as_of(margin_anchor - 1):
                    pass

            return prime, backward_step_within_margin

        prime_small, step_small = make_calls(array_pool_small, log_pool_small, 4)
        prime_large, step_large = make_calls(array_pool_large, log_pool_large, 128)

        prime_small()
        prime_large()

        step_small_t = _time_calls(step_small, repeats=8)
        step_large_t = _time_calls(step_large, repeats=8)

        ratio = step_large_t / step_small_t if step_small_t else float("inf")
        print(
            f"\nbackward step within proven-empty range: 4 segments = {step_small_t * 1000:.4f} ms, "
            f"128 segments = {step_large_t * 1000:.4f} ms, ratio = {ratio:.1f}x"
        )
        assert ratio < 8, (
            f"backward step within an already-proven-empty range scaled {ratio:.1f}x going from "
            f"4 to 128 cold segments - it should be pre-seeded found and skip scanning entirely."
        )


class TestBackwardStepOnlyReresolvesInvalidatedModule:
    """A backward step that invalidates only the frequently-logging module (crosses its cached
    occurrence) should re-resolve just that module by looking at the next-older segment or two -
    not rescan all the way back to the sparse boot-only module's segment 0."""

    def test_going_from_4_to_128_cold_segments(self, tmp_path, id_registry):
        device = id_registry.get_device("esp32")
        boot_module = device.get_module("boot_handshake").id
        chatty_module = device.get_module("telemetry").id

        array_pool_small, log_pool_small = _build_pool_with_boot_only_module(
            tmp_path / "small", 4, boot_module, chatty_module
        )
        array_pool_large, log_pool_large = _build_pool_with_boot_only_module(
            tmp_path / "large", 128, boot_module, chatty_module
        )

        def make_calls(array_pool, log_pool, n):
            tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
            newest_row_ts = BASE_TS + (n - 1) * SEGMENT_SPACING_NS

            def prime():
                # Touches all n segments (chatty found immediately, boot only found at segment 0).
                with tracker.build_snapshot_as_of(newest_row_ts):
                    pass

            def backward_step_invalidating_chatty_only():
                # Between the newest and second-newest chatty rows - invalidates chatty's cached
                # occurrence (must find the previous segment's row) but boot's cached occurrence
                # (segment 0, far below) stays valid and pre-seeded.
                with tracker.build_snapshot_as_of(newest_row_ts - SEGMENT_SPACING_NS // 2):
                    pass

            return prime, backward_step_invalidating_chatty_only

        prime_small, step_small = make_calls(array_pool_small, log_pool_small, 4)
        prime_large, step_large = make_calls(array_pool_large, log_pool_large, 128)

        prime_small()
        prime_large()

        step_small_t = _time_calls(step_small, repeats=8)
        step_large_t = _time_calls(step_large, repeats=8)

        ratio = step_large_t / step_small_t if step_small_t else float("inf")
        print(
            f"\nbackward step re-resolving only the chatty module: 4 segments = "
            f"{step_small_t * 1000:.4f} ms, 128 segments = {step_large_t * 1000:.4f} ms, "
            f"ratio = {ratio:.1f}x"
        )
        assert ratio < 8, (
            f"backward step that only invalidates the frequently-logging module scaled "
            f"{ratio:.1f}x going from 4 to 128 cold segments - it should re-resolve that module "
            f"from the next-older segment(s), not rescan down to the sparse module's segment."
        )


class TestFirstEverCallStaysFlatWhenAModuleNeverLogs:
    """The persistent first-occurrence table (populated by update(), the always-running LIVE
    tracker) should let a module that never receives any data resolve instantly on
    build_snapshot_as_of's very first-ever call for a tracker - no _scrub_cache required at all,
    unlike the cache-based pre-seeding tested elsewhere in this file."""

    def test_going_from_4_to_128_cold_segments(self, tmp_path, id_registry):
        device = id_registry.get_device("esp32")
        chatty_module = device.get_module("telemetry").id
        never_logs_module = device.get_module("never_logs").id  # registered, but never appears

        def make_tracker(tmp_subdir, n):
            array_pool, log_pool = _build_pool_all_chatty(tmp_subdir, n, chatty_module)
            tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
            tracker.update()  # establishes first-occurrence coverage, confirming never_logs absent
            newest_ts = BASE_TS + (n - 1) * SEGMENT_SPACING_NS
            return tracker, newest_ts

        tracker_small, newest_small = make_tracker(tmp_path / "small", 4)
        tracker_large, newest_large = make_tracker(tmp_path / "large", 128)

        def first_call(tracker, newest_ts):
            def call():
                with tracker.build_snapshot_as_of(newest_ts):
                    pass

            return call

        # Each tracker's build_snapshot_as_of has never been called before - genuinely the first
        # ever call, no _scrub_cache in play.
        assert tracker_small._scrub_cache is None
        assert tracker_large._scrub_cache is None

        small_t = _time_calls(first_call(tracker_small, newest_small), repeats=8)
        large_t = _time_calls(first_call(tracker_large, newest_large), repeats=8)

        ratio = large_t / small_t if small_t else float("inf")
        print(
            f"\nfirst-ever call, never-logging module: 4 segments = {small_t * 1000:.4f} ms, "
            f"128 segments = {large_t * 1000:.4f} ms, ratio = {ratio:.1f}x"
        )
        assert ratio < 8, (
            f"first-ever build_snapshot_as_of call scaled {ratio:.1f}x going from 4 to 128 cold "
            f"segments even though the only unresolved module never logs anything - the "
            f"first-occurrence table should confirm it absent for free via update(), without "
            f"scanning to the end of the pool every time."
        )


class TestFirstEverCallStaysFlatNearLiveEdge:
    """_current_snapshot (update()'s continuously-maintained "latest ever per module") should let
    build_snapshot_as_of's very first-ever call resolve every module for free when anchored at or
    after the live edge - no _scrub_cache required."""

    def test_going_from_4_to_128_cold_segments(self, tmp_path, id_registry):
        device = id_registry.get_device("esp32")

        def make_tracker(tmp_subdir, n):
            module_ids = [device.get_module(f"m{i}").id for i in range(n)]
            array_pool, log_pool = _build_pool_with_one_module_per_segment(tmp_subdir, n, module_ids)
            tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
            tracker.update()  # populates _current_snapshot with every module's live-latest value
            newest_ts = BASE_TS + (n - 1) * SEGMENT_SPACING_NS
            return tracker, newest_ts

        tracker_small, newest_small = make_tracker(tmp_path / "small", 4)
        tracker_large, newest_large = make_tracker(tmp_path / "large", 128)

        def first_call(tracker, newest_ts):
            def call():
                with tracker.build_snapshot_as_of(newest_ts):
                    pass

            return call

        assert tracker_small._scrub_cache is None
        assert tracker_large._scrub_cache is None

        small_t = _time_calls(first_call(tracker_small, newest_small), repeats=8)
        large_t = _time_calls(first_call(tracker_large, newest_large), repeats=8)

        ratio = large_t / small_t if small_t else float("inf")
        print(
            f"\nfirst-ever call, anchored at live edge: 4 segments = {small_t * 1000:.4f} ms, "
            f"128 segments = {large_t * 1000:.4f} ms, ratio = {ratio:.1f}x"
        )
        assert ratio < 8, (
            f"first-ever build_snapshot_as_of call scaled {ratio:.1f}x going from 4 to 128 cold "
            f"segments even though every module's live-latest occurrence already satisfies the "
            f"query - _current_snapshot should resolve them all for free via update(), without "
            f"scanning every segment to find each one's own row."
        )

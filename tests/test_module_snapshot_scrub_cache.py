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
same pattern as test_module_snapshot.py. The scaling tests build real cold-storage segments (same
pattern as tests/test_log_pool_fetch_perf.py), mostly with a module that only appears in the very
oldest segment - mirroring the real-world "one-shot boot/handshake message" shape that made the old
implementation's cost independent of anchor position (it always had to scan back to that one
segment to resolve every module). They count the segments a call scans rather than timing it."""

import pytest

from blinkview.core import module_snapshot
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
# Scaling: how many segments a call scans must not grow with total segment count.
# ---------------------------------------------------------------------------

BASE_TS = 1_000_000_000_000
SEGMENT_SPACING_NS = 10_000_000_000  # 10s apart


@pytest.fixture
def count_scans(monkeypatch):
    """count_scans(fn) runs fn once and returns how many segments build_snapshot_as_of handed to
    its per-segment kernel (nb_build_snapshot_as_of). Counted instead of timed: a timed call
    repeated at the same anchor only measures the cache-hit early return after the first sample,
    and sub-millisecond timing ratios are noisy anyway."""
    scanned = []
    real_kernel = module_snapshot.nb_build_snapshot_as_of

    def counting_kernel(segment_bundle, *args):
        scanned.append(segment_bundle)
        return real_kernel(segment_bundle, *args)

    monkeypatch.setattr(module_snapshot, "nb_build_snapshot_as_of", counting_kernel)

    def count(fn):
        scanned.clear()
        fn()
        return len(scanned)

    return count


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


def _build_pool(tmp_path, module_ids):
    """One cold segment per entry of module_ids, SEGMENT_SPACING_NS apart, oldest first; segment
    i holds a single row for module_ids[i]."""
    array_pool = NumpyArrayPool()
    log_pool = CircularLogPool(array_pool, max_pieces=1, final_buffer_bytes=64 * 1024)

    tmp_path.mkdir(parents=True, exist_ok=True)
    for i, module in enumerate(module_ids):
        ts_ns = BASE_TS + i * SEGMENT_SPACING_NS
        path = tmp_path / f"segment_{i:010d}.blkseg"
        log_pool.cold_segments.append(_make_cold_segment(array_pool, path, i, ts_ns, module))

    return array_pool, log_pool


def _build_pool_with_boot_only_module(tmp_path, n, boot_module, chatty_module):
    """n cold segments. Segment 0 (oldest) carries `boot_module`'s only-ever row; every other
    segment carries `chatty_module`. Resolving "latest as of the newest anchor" for both modules
    requires scanning all the way back to segment 0 - the same shape as a real one-shot
    boot/handshake message that made the pre-cache implementation's cost independent of anchor
    position."""
    return _build_pool(tmp_path, [boot_module] + [chatty_module] * (n - 1))


def _newest_ts(n):
    return BASE_TS + (n - 1) * SEGMENT_SPACING_NS


SEGMENT_COUNTS = (4, 128)


class TestSecondForwardCallDoesNotScaleWithSegmentCount:
    @pytest.mark.parametrize("n", SEGMENT_COUNTS)
    def test_small_forward_step_scans_no_segment(self, tmp_path, id_registry, count_scans, n):
        device = id_registry.get_device("esp32")
        boot_module = device.get_module("boot_handshake").id
        chatty_module = device.get_module("telemetry").id
        array_pool, log_pool = _build_pool_with_boot_only_module(tmp_path, n, boot_module, chatty_module)
        tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
        newest_ts = _newest_ts(n)

        def call_at(ts_ns):
            def call():
                with tracker.build_snapshot_as_of(ts_ns):
                    pass

            return call

        # The priming call has to reach segment 0 for the boot-only module - the cost the cache
        # exists to avoid repeating.
        assert count_scans(call_at(newest_ts)) == n

        # Nothing lies in (newest_ts, newest_ts + 1], so the forward-incremental path has no
        # segment to look at.
        assert count_scans(call_at(newest_ts + 1)) == 0


class TestBackwardStepWithinProvenEmptyRangeIsFree:
    """A backward step that doesn't cross any module's actual cached occurrence should touch
    zero segments (found_mask pre-seeded all-True, remaining=0, the scan loop never runs) -
    regardless of how many total segments exist below it."""

    @pytest.mark.parametrize("n", SEGMENT_COUNTS)
    def test_backward_step_scans_no_segment(self, tmp_path, id_registry, count_scans, n):
        device = id_registry.get_device("esp32")
        boot_module = device.get_module("boot_handshake").id
        chatty_module = device.get_module("telemetry").id
        array_pool, log_pool = _build_pool_with_boot_only_module(tmp_path, n, boot_module, chatty_module)
        tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
        # Anchor past the newest real row, leaving margin to step backward without crossing
        # chatty's actual row.
        margin_anchor = _newest_ts(n) + SEGMENT_SPACING_NS

        def call_at(ts_ns):
            def call():
                with tracker.build_snapshot_as_of(ts_ns):
                    pass

            return call

        assert count_scans(call_at(margin_anchor)) == n
        assert count_scans(call_at(margin_anchor - 1)) == 0


class TestBackwardStepOnlyReresolvesInvalidatedModule:
    """A backward step that invalidates only the frequently-logging module (crosses its cached
    occurrence) should re-resolve just that module by looking at the next-older segment - not
    rescan all the way back to the sparse boot-only module's segment 0."""

    @pytest.mark.parametrize("n", SEGMENT_COUNTS)
    def test_backward_step_scans_only_the_next_older_segment(self, tmp_path, id_registry, count_scans, n):
        device = id_registry.get_device("esp32")
        boot_module = device.get_module("boot_handshake").id
        chatty_module = device.get_module("telemetry").id
        array_pool, log_pool = _build_pool_with_boot_only_module(tmp_path, n, boot_module, chatty_module)
        tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
        newest_row_ts = _newest_ts(n)

        def call_at(ts_ns):
            def call():
                with tracker.build_snapshot_as_of(ts_ns):
                    pass

            return call

        assert count_scans(call_at(newest_row_ts)) == n

        # Between the newest and second-newest chatty rows: invalidates chatty's cached
        # occurrence (the previous segment's row is the answer), while boot's cached occurrence
        # in segment 0 stays valid and pre-seeded.
        assert count_scans(call_at(newest_row_ts - SEGMENT_SPACING_NS // 2)) == 1


class TestFirstEverCallStaysFlatWhenAModuleNeverLogs:
    """The persistent first-occurrence table (populated by update(), the always-running LIVE
    tracker) should let a module that never receives any data resolve instantly on
    build_snapshot_as_of's very first-ever call for a tracker - no _scrub_cache required at all,
    unlike the cache-based pre-seeding tested elsewhere in this file."""

    @pytest.mark.parametrize("n", SEGMENT_COUNTS)
    @pytest.mark.parametrize("run_update", [True, False], ids=["after_update", "control_without_update"])
    def test_first_call_scans_no_segment(self, tmp_path, id_registry, count_scans, n, run_update):
        device = id_registry.get_device("esp32")
        chatty_module = device.get_module("telemetry").id
        device.get_module("never_logs")  # registered, but never appears in any segment

        array_pool, log_pool = _build_pool(tmp_path, [chatty_module] * n)
        tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
        if run_update:
            tracker.update()  # establishes first-occurrence coverage, confirming never_logs absent

        def call():
            with tracker.build_snapshot_as_of(_newest_ts(n)):
                pass

        assert tracker._scrub_cache is None  # genuinely the first-ever call
        # Without update() nothing vouches for never_logs being absent, so the scan runs to the
        # oldest segment looking for it.
        assert count_scans(call) == (0 if run_update else n)


class TestFirstEverCallStaysFlatNearLiveEdge:
    """_current_snapshot (update()'s continuously-maintained "latest ever per module") should let
    build_snapshot_as_of's very first-ever call resolve every module for free when anchored at or
    after the live edge - no _scrub_cache required."""

    @pytest.mark.parametrize("n", SEGMENT_COUNTS)
    @pytest.mark.parametrize("run_update", [True, False], ids=["after_update", "control_without_update"])
    def test_first_call_scans_no_segment(self, tmp_path, id_registry, count_scans, n, run_update):
        device = id_registry.get_device("esp32")
        # One distinct module per segment: without _current_snapshot, resolving all of them means
        # visiting every segment.
        module_ids = [device.get_module(f"m{i}").id for i in range(n)]
        array_pool, log_pool = _build_pool(tmp_path, module_ids)
        tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)
        if run_update:
            tracker.update()  # populates _current_snapshot with every module's live-latest value

        def call():
            with tracker.build_snapshot_as_of(_newest_ts(n)):
                pass

        assert tracker._scrub_cache is None
        assert count_scans(call) == (0 if run_update else n)

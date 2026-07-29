# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Performance regression tests: does a CircularLogPool "fetch" function's per-call cost scale
with the *number* of cold (mmap-backed, disk-archived) segments, or only with the number of rows
actually relevant to the query? See plans/mmap-coldstore.md for the cold-tier design - each cold
segment's PooledLogBatch.metadata is a ColdSegmentMeta(path, earliest_ts, latest_ts, first_seq,
last_seq) carrying a cached timestamp/sequence range specifically so a caller can answer "is this
segment in range" in O(1) without touching (page-faulting in) any of its mmap'd column data - see
also PooledLogBatch.start_ts/first_sequence_id/last_sequence_id, which now serve straight from
this cache for cold segments instead of indexing into the bundle arrays.

Investigated all the plausible culprits the user suspected (plotter / log window / watch):

- fetch_telemetry_window() (core/numpy_log.py) - used by TelemetryPlotter's REPLAY-follow/scrub
  path (plotter.py's apply_updates, ticked up to 10 Hz per visible module) - THE BUG. Its
  before_segments/after_segments loops call nb_extract_telemetry_segment_window_backward/forward
  unconditionally on every segment in the snapshot, never consulting the cached earliest_ts/
  latest_ts the design exists to provide. Each call does two binary searches on the segment's
  (possibly mmap'd) timestamps array before its own internal early-return kicks in. Cost is
  therefore O(total hot+cold segment count), not O(rows actually in the requested window) -
  confirmed below: it gets ~30x slower going from 4 to 128 cold segments even though only one of
  them ever contains a relevant row.
- fetch_telemetry_arrays() (core/numpy_log.py) - used by TelemetryPlotter's live-forward path.
  Fine: its loop breaks as soon as a segment's cheap `last_sequence_id` (O(1) read) proves every
  remaining older segment predates the watermark.
- CircularLogPool.get_time_bounds() - fine: true O(1), reads straight from the cached
  ColdSegmentMeta fields on the oldest/newest segment, no iteration at all.
- LogSegmentScanner.scan_tail (core/log_fetch.py, backs the log viewer/table's LIVE fetch) - fine,
  by inspection: breaks out of its segment loop as soon as a segment's `last_sequence_id` (O(1))
  proves it's already-seen, the same cheap pattern fetch_telemetry_arrays uses.
- LatestModuleValueTracker.update() (core/module_snapshot.py, the live "latest value per module"
  path TelemetryWatch/TelemetryTableModel/console read from) - fine, same
  `last_sequence_id`-watermark early-break pattern as scan_tail.
- get_telemetry_anchor() (core/numpy_log.py) - fine, breaks via the same cheap
  `last_sequence_id`/`first_sequence_id` checks.

A second audit pass (prompted by "check ALL log_pool access locations for the same slow
behaviour") found two more real instances of the exact same bug shape - both HISTORY-direction
(playback-scrub/scroll-back) fetches, mirroring fetch_telemetry_window exactly:

- **LogSegmentScanner.scan_history_window()** (core/log_fetch.py) - backs the log viewer/table's
  HISTORY window (scrolling back through old rows, or a REPLAY playback-clock anchor). Its
  before/after loops call segment_filter_reversed/segment_filter (each doing 1-2 binary searches
  via nb_fast_find_first_ge/gt) unconditionally on every segment until the requested row quota is
  filled - never skipping a segment that's provably outside the anchor's ts/seq range first. THE
  SECOND BUG.
- **LatestModuleValueTracker.build_snapshot_as_of()** (core/module_snapshot.py) - the actual
  "watch" path: TelemetryWatch/TelemetryTableModel's REPLAY-follow tick rebuilds "latest value per
  module as of a past ts" from scratch every call (see its own docstring: "expected to call this
  once per follow tick"). Its segment loop is even more expensive per irrelevant segment than the
  other two: nb_build_snapshot_as_of does a plain **linear scan of every row** in a segment
  (no binary search at all), and the outer loop never skips a segment whose data entirely
  postdates the query ts before calling it. THE THIRD BUG - this is the one actually reachable
  from "watch" in the user's original guess.

Both are fixed the same way fetch_telemetry_window was: check the segment's cached
ColdSegmentMeta.earliest_ts/latest_ts (ts-anchored case) or already-cheap first_sequence_id/
last_sequence_id properties (seq-anchored case) before calling into the kernel, skipping segments
that can't possibly contribute a match.
"""

import time

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_segment import ColdSegmentMeta, write_cold_segment_file
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.numpy_log import (
    CircularLogPool,
    allocate_telemetry_workspace,
    fetch_telemetry_arrays,
    fetch_telemetry_window,
)

MODULE_A = 3
BASE_TS = 1_000_000_000_000
SEGMENT_SPACING_NS = 10_000_000_000  # 10s apart - far wider than any window used below


def _make_cold_segment(array_pool, path, index, ts_ns, module=MODULE_A):
    """Builds one real one-row segment, writes it to a real .blkseg file, and reopens it as a
    memmap-backed PooledLogBatch - the exact on-disk shape ColdStorageArchiver produces (see
    core/cold_segment.py), without going through the archiver's background thread/pacing, so many
    segments can be built quickly and deterministically."""
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


def _build_pool_with_cold_segments(tmp_path, n, module=MODULE_A):
    """A pool with `n` cold segments and nothing in the hot tier - each segment holds exactly one
    row, SEGMENT_SPACING_NS apart, so only the newest segment's row ever falls inside the narrow
    query windows used below regardless of how many total segments exist. `module` defaults to
    MODULE_A (used by tests with no real IDRegistry) but can be overridden to a real registered
    module id for tests (scan_history_window/build_snapshot_as_of below) that need one."""
    array_pool = NumpyArrayPool()
    log_pool = CircularLogPool(array_pool, max_pieces=1, final_buffer_bytes=64 * 1024)

    tmp_path.mkdir(parents=True, exist_ok=True)
    for i in range(n):
        ts_ns = BASE_TS + i * SEGMENT_SPACING_NS
        path = tmp_path / f"segment_{i:010d}.blkseg"
        cold = _make_cold_segment(array_pool, path, i, ts_ns, module=module)
        log_pool.cold_segments.append(cold)

    newest_ts = BASE_TS + (n - 1) * SEGMENT_SPACING_NS
    return array_pool, log_pool, newest_ts, BASE_TS


def _time_calls(fn, repeats=15):
    """Runs fn() once unmeasured (lets Numba finish JIT-compiling any kernel it calls - warmup
    cost would otherwise dominate and mask the per-call scaling this is trying to measure), then
    returns the median wall time of `repeats` further calls."""
    fn()
    samples = []
    for _ in range(repeats):
        start = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - start)
    samples.sort()
    return samples[len(samples) // 2]


class TestFetchTelemetryWindowScalesWithSegmentCount:
    """fetch_telemetry_window's before/after loops call the extraction kernel on every segment in
    the snapshot, whether or not it's anywhere near the requested window - so its cost should (on
    the current, unoptimized code) grow roughly with total segment count. A fixed version would
    consult each segment's cached ColdSegmentMeta.earliest_ts/latest_ts and skip the kernel call
    entirely for segments outside [anchor - before_span, anchor + after_span], making the cost
    track the number of *relevant* segments (always 1 here) instead."""

    @pytest.mark.parametrize("n", [4, 128])
    def test_reports_the_wall_time_for_this_segment_count(self, tmp_path, n, record_property):
        array_pool, log_pool, anchor_ts, _oldest_ts = _build_pool_with_cold_segments(tmp_path / f"n{n}", n)
        temp_floats = allocate_telemetry_workspace(1)

        def call():
            with fetch_telemetry_window(
                array_pool,
                log_pool,
                MODULE_A,
                num_channels=1,
                temp_floats=temp_floats,
                anchor_ts_ns=anchor_ts,
                before_span_ns=SEGMENT_SPACING_NS // 2,
                after_span_ns=SEGMENT_SPACING_NS // 2,
                before_cap=10,
                after_cap=10,
            ) as batch:
                return batch.times.shape[0]

        median_time = _time_calls(call)
        record_property("median_seconds", median_time)
        print(f"\nfetch_telemetry_window median call time with {n} cold segments: {median_time * 1000:.3f} ms")

    def test_going_from_4_to_128_cold_segments_should_not_scale_the_call_time_by_32x(self, tmp_path):
        """The regression assertion: only ONE row is ever relevant to the query in either case -
        the rest is pure decoy segments outside the window. A properly-optimized fetch should cost
        about the same regardless of how many of those decoys exist. Generous 8x threshold (vs.
        the ~32x segment-count growth) leaves headroom for noise while still failing hard against
        the current O(segment count) behavior."""
        array_pool_small, log_pool_small, anchor_small, _ = _build_pool_with_cold_segments(tmp_path / "small", 4)
        array_pool_large, log_pool_large, anchor_large, _ = _build_pool_with_cold_segments(tmp_path / "large", 128)
        temp_floats = allocate_telemetry_workspace(1)

        def make_call(array_pool, log_pool, anchor_ts):
            def call():
                with fetch_telemetry_window(
                    array_pool,
                    log_pool,
                    MODULE_A,
                    num_channels=1,
                    temp_floats=temp_floats,
                    anchor_ts_ns=anchor_ts,
                    before_span_ns=SEGMENT_SPACING_NS // 2,
                    after_span_ns=SEGMENT_SPACING_NS // 2,
                    before_cap=10,
                    after_cap=10,
                ) as batch:
                    return batch.times.shape[0]

            return call

        small_time = _time_calls(make_call(array_pool_small, log_pool_small, anchor_small))
        large_time = _time_calls(make_call(array_pool_large, log_pool_large, anchor_large))

        ratio = large_time / small_time if small_time > 0 else float("inf")
        print(
            f"\nfetch_telemetry_window: 4 segments = {small_time * 1000:.3f} ms, "
            f"128 segments = {large_time * 1000:.3f} ms, ratio = {ratio:.1f}x"
        )

        assert ratio < 8, (
            f"fetch_telemetry_window scaled {ratio:.1f}x going from 4 to 128 cold segments "
            f"(32x more segments, but the same single relevant row in both cases) - it isn't "
            f"using each cold segment's cached ColdSegmentMeta.earliest_ts/latest_ts to skip "
            f"irrelevant segments in O(1), so its cost scales with total segment count instead "
            f"of relevant row count."
        )


class TestFetchTelemetryArraysDoesNotScaleWithSegmentCount:
    """Control case: fetch_telemetry_arrays' loop breaks as soon as a segment's O(1)
    `last_sequence_id` proves every older segment already predates the watermark - expected to
    stay flat regardless of cold segment count."""

    def test_going_from_4_to_128_cold_segments_stays_flat(self, tmp_path):
        array_pool_small, log_pool_small, _, _oldest_small = _build_pool_with_cold_segments(tmp_path / "small", 4)
        array_pool_large, log_pool_large, _, _oldest_large = _build_pool_with_cold_segments(tmp_path / "large", 128)
        temp_floats = allocate_telemetry_workspace(1)

        def make_call(array_pool, log_pool):
            def call():
                with fetch_telemetry_arrays(
                    array_pool,
                    log_pool,
                    MODULE_A,
                    start_seq=SEQ_NONE,
                    num_channels=1,
                    temp_floats=temp_floats,
                    max_points=10,
                ) as batch:
                    return batch.times.shape[0]

            return call

        small_time = _time_calls(make_call(array_pool_small, log_pool_small))
        large_time = _time_calls(make_call(array_pool_large, log_pool_large))

        ratio = large_time / small_time if small_time > 0 else float("inf")
        print(
            f"\nfetch_telemetry_arrays: 4 segments = {small_time * 1000:.3f} ms, "
            f"128 segments = {large_time * 1000:.3f} ms, ratio = {ratio:.1f}x"
        )

        assert ratio < 8, (
            f"fetch_telemetry_arrays unexpectedly scaled {ratio:.1f}x with cold segment count - "
            f"expected it to stay flat via its early-break on last_sequence_id."
        )


class TestGetTimeBoundsDoesNotScaleWithSegmentCount:
    """Control case: get_time_bounds() reads straight from the oldest/newest segment's cached
    ColdSegmentMeta fields - no iteration over the deque at all, so this should be the flattest
    of all of them."""

    def test_going_from_4_to_128_cold_segments_stays_flat(self, tmp_path):
        _, log_pool_small, _, _ = _build_pool_with_cold_segments(tmp_path / "small", 4)
        _, log_pool_large, _, _ = _build_pool_with_cold_segments(tmp_path / "large", 128)

        small_time = _time_calls(log_pool_small.get_time_bounds)
        large_time = _time_calls(log_pool_large.get_time_bounds)

        ratio = large_time / small_time if small_time > 0 else float("inf")
        print(
            f"\nget_time_bounds: 4 segments = {small_time * 1000:.4f} ms, "
            f"128 segments = {large_time * 1000:.4f} ms, ratio = {ratio:.1f}x"
        )

        assert ratio < 8, f"get_time_bounds unexpectedly scaled {ratio:.1f}x with cold segment count."


class TestGetReversedSnapshotSinceDoesNotScaleWithSegmentCount:
    """CircularLogPool.get_reversed_snapshot_since (the fix for the 60Hz
    LatestModuleValueTracker.update()/10Hz LogSegmentScanner.scan_tail retain/release cost
    scaling with total segment count, not just relevant-row count - see
    plans/lazy-retain-skip-for-fetch-scans.md) should skip retain()/release() for every cold
    segment already-known-stale, unlike get_reversed_snapshot which retains every segment in
    both tiers unconditionally regardless of last_known_seq."""

    def test_get_reversed_snapshot_since_stays_flat_going_from_4_to_128_cold_segments(self, tmp_path):
        _, log_pool_small, _, _ = _build_pool_with_cold_segments(tmp_path / "small", 4)
        _, log_pool_large, _, _ = _build_pool_with_cold_segments(tmp_path / "large", 128)

        def call_only_newest_relevant(log_pool, n):
            def call():
                # last_known_seq = n - 1 excludes every segment except the newest (seq=n) - see
                # _build_pool_with_cold_segments/_make_cold_segment's seq=index+1 assignment.
                with log_pool.get_reversed_snapshot_since(n - 1) as segments:
                    return len(segments)

            return call

        small_time = _time_calls(call_only_newest_relevant(log_pool_small, 4))
        large_time = _time_calls(call_only_newest_relevant(log_pool_large, 128))

        ratio = large_time / small_time if small_time > 0 else float("inf")
        print(
            f"\nget_reversed_snapshot_since (only newest relevant): 4 segments = {small_time * 1000:.4f} ms, "
            f"128 segments = {large_time * 1000:.4f} ms, ratio = {ratio:.1f}x"
        )
        assert ratio < 8, (
            f"get_reversed_snapshot_since unexpectedly scaled {ratio:.1f}x with cold segment "
            f"count even though only the newest segment was ever relevant."
        )

    def test_skips_retain_for_stale_cold_segments_but_not_the_relevant_one(self, tmp_path):
        """Correctness, not just perf: confirms the returned snapshot actually contains only the
        segments that couldn't be proven stale, not merely that it's fast."""
        _, log_pool, _, _ = _build_pool_with_cold_segments(tmp_path, 5)

        with log_pool.get_reversed_snapshot_since(3) as segments:
            # seq 1..5 across 5 cold segments, plus the pool's always-included (empty, hot)
            # active segment - last_known_seq=3 should leave the cold segments with last_seq
            # 4 and 5 (indices 3, 4), newest-first.
            assert [int(s.last_sequence_id) for s in segments if s.size > 0] == [5, 4]

        with log_pool.get_reversed_snapshot_since(0) as segments:
            # SEQ_NONE-like watermark - nothing is stale, full rescan behavior.
            assert [int(s.last_sequence_id) for s in segments if s.size > 0] == [5, 4, 3, 2, 1]

    def test_hot_segments_are_always_included_regardless_of_last_known_seq(self, tmp_path):
        """Hot segments have no immutable last_seq to check cheaply (see
        get_reversed_snapshot_since's docstring) - they're always retained/included, matching
        get_reversed_snapshot's behavior for the hot tier."""
        array_pool = NumpyArrayPool()
        log_pool = CircularLogPool(array_pool, max_pieces=2, final_buffer_bytes=64 * 1024)
        log_pool.active_segment.insert_any(BASE_TS, BASE_TS, b"hot row", level=0, module=MODULE_A, device=0, seq=1)

        with log_pool.get_reversed_snapshot_since(999_999) as segments:
            assert len(segments) == 1
            assert segments[0] is log_pool.segments[-1]


class TestScanHistoryWindowScalesWithSegmentCount:
    """LogSegmentScanner.scan_history_window (core/log_fetch.py) backs the log viewer/table's
    HISTORY window - scrolling back through old rows, or a REPLAY playback-clock anchor. Its
    before/after loops call segment_filter_reversed/segment_filter unconditionally on every
    segment until the row quota is filled, never skipping a segment that's provably outside the
    anchor's ts range first - the same bug shape as fetch_telemetry_window, just with a binary
    search instead of a full extraction kernel per irrelevant segment."""

    def _make_scanner(self, id_registry, log_pool):
        from blinkview.core.log_fetch import LogSegmentScanner
        from blinkview.utils.log_filter import LogFilter
        from blinkview.utils.log_level import LogLevel

        log_filter = LogFilter(id_registry, log_level=LogLevel.ALL.name_conf)
        return LogSegmentScanner(
            id_registry,
            lambda: log_pool,
            log_filter,
            get_sidebar_filter=lambda: (False, np.zeros(0, dtype=np.uint8)),
            get_show_hidden=lambda: True,
        )

    def test_going_from_4_to_128_cold_segments_should_not_scale_the_call_time_by_32x(self, tmp_path, id_registry):
        """Anchored at the NEWEST segment's ts (mirroring fetch_telemetry_window's test setup):
        the "before" scan must walk every older decoy segment before quota logic ever kicks in,
        since none of them produce a match to count against the quota."""
        device = id_registry.get_device("esp32")
        module = device.get_module("wifi")

        array_pool_small, log_pool_small, anchor_small, _ = _build_pool_with_cold_segments(
            tmp_path / "small", 4, module=module.id
        )
        array_pool_large, log_pool_large, anchor_large, _ = _build_pool_with_cold_segments(
            tmp_path / "large", 128, module=module.id
        )

        def make_call(log_pool, anchor_ts):
            scanner = self._make_scanner(id_registry, log_pool)

            def call():
                return scanner.scan_history_window(
                    anchor_ts=anchor_ts + 1,
                    before_cap=10,
                    after_cap=10,
                    consume_before=lambda *a: None,
                    consume_after=lambda *a: None,
                )

            return call

        small_time = _time_calls(make_call(log_pool_small, anchor_small))
        large_time = _time_calls(make_call(log_pool_large, anchor_large))

        ratio = large_time / small_time if small_time > 0 else float("inf")
        print(
            f"\nscan_history_window: 4 segments = {small_time * 1000:.3f} ms, "
            f"128 segments = {large_time * 1000:.3f} ms, ratio = {ratio:.1f}x"
        )

        assert ratio < 8, (
            f"scan_history_window scaled {ratio:.1f}x going from 4 to 128 cold segments - it "
            f"isn't using each cold segment's cached ColdSegmentMeta.earliest_ts/latest_ts to "
            f"skip irrelevant segments in O(1) before calling segment_filter_reversed/"
            f"segment_filter, so its cost scales with total segment count instead of relevant "
            f"row count."
        )


class TestBuildSnapshotAsOfScalesWithSegmentCount:
    """LatestModuleValueTracker.build_snapshot_as_of (core/module_snapshot.py) is the actual
    "watch" path: TelemetryWatch/TelemetryTableModel's REPLAY-follow tick calls this once per
    tick to rebuild "latest value per module as of a past ts" from scratch. Its segment loop
    (scanning newest-to-oldest) never skips a segment whose data entirely postdates the query ts
    before calling nb_build_snapshot_as_of - which does a plain linear scan of every row in the
    segment (no binary search at all), making this the most expensive per-irrelevant-segment cost
    of the three bugs found in this audit."""

    def test_going_from_4_to_128_cold_segments_should_not_scale_the_call_time_by_32x(self, tmp_path, id_registry):
        """Anchored at the OLDEST segment's ts: build_snapshot_as_of scans newest-to-oldest
        looking for the latest row at-or-before the anchor, so here the decoys are every segment
        NEWER than the anchor (the opposite arrangement from the fetch_telemetry_window/
        scan_history_window tests above, which anchor at the newest segment instead)."""
        from blinkview.core.module_snapshot import LatestModuleValueTracker

        device = id_registry.get_device("esp32")
        module = device.get_module("wifi")

        array_pool_small, log_pool_small, _, oldest_small = _build_pool_with_cold_segments(
            tmp_path / "small", 4, module=module.id
        )
        array_pool_large, log_pool_large, _, oldest_large = _build_pool_with_cold_segments(
            tmp_path / "large", 128, module=module.id
        )

        def make_call(array_pool, log_pool, oldest_ts):
            tracker = LatestModuleValueTracker(log_pool, id_registry.modules_table, array_pool, lambda: 0)

            def call():
                with tracker.build_snapshot_as_of(oldest_ts):
                    pass

            return call

        small_time = _time_calls(make_call(array_pool_small, log_pool_small, oldest_small))
        large_time = _time_calls(make_call(array_pool_large, log_pool_large, oldest_large))

        ratio = large_time / small_time if small_time > 0 else float("inf")
        print(
            f"\nbuild_snapshot_as_of: 4 segments = {small_time * 1000:.3f} ms, "
            f"128 segments = {large_time * 1000:.3f} ms, ratio = {ratio:.1f}x"
        )

        assert ratio < 8, (
            f"build_snapshot_as_of scaled {ratio:.1f}x going from 4 to 128 cold segments - it "
            f"isn't using each cold segment's cached ColdSegmentMeta.earliest_ts/latest_ts to "
            f"skip segments that entirely postdate the query ts, so its cost scales with total "
            f"segment count instead of relevant row count."
        )

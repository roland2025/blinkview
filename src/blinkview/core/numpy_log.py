# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from contextlib import ExitStack, contextmanager
from itertools import chain
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Iterable, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.log_batch import TelemetryBatch
from blinkview.core.types.telemetry import TsWindowBundle
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.segments import nb_copy_batch_to_segment
from blinkview.ops.telemetry import (
    nb_count_module_occurrences_backwards,
    nb_extract_telemetry_segment_to_end,
    nb_extract_telemetry_segment_window_backward,
    nb_extract_telemetry_segment_window_forward,
    nb_peek_segment_channels_backwards,
)
from blinkview.utils.log_level import LogLevel

if TYPE_CHECKING:
    from blinkview.core.cold_storage_archiver import ColdStorageArchiver


class SegmentSnapshot:
    """
    A context-managed snapshot of segments.
    Ensures memory isn't recycled while a query is running.
    """

    def __init__(self, segments_iter: Iterable[PooledLogBatch]):
        # Single pass: consumes the iterator, retains segments, and builds the final list
        self.segments: list[PooledLogBatch] = [seg.retain() for seg in segments_iter]

    def __enter__(self):
        return self.segments

    def __exit__(self, exc_type, exc_val, exc_tb):
        for seg in self.segments:
            seg.release()


def insert_truncated_error(
    batch: "PooledLogBatch", ts_ns: int, module: int, device: int, seq: int, msg_bytes: bytes, limit: int = 512
) -> bool:
    """
    Standalone utility to force a log into a batch with truncation.
    Decoupled from the PooledLogBatch class to keep the primitive lean.
    """
    suffix = b" ... [TRUNCATED]"

    if len(msg_bytes) > limit:
        # Precision slice to ensure total length is exactly 'limit'
        msg_bytes = msg_bytes[: limit - len(suffix)] + suffix

    # Note: LogLevel.ERROR.value should be imported from your constants
    return batch.insert(
        ts_ns=ts_ns,
        rx_ts_ns=ts_ns,
        level=LogLevel.ERROR.value,
        module=module,
        device=device,
        seq=seq,
        msg_bytes=msg_bytes,
    )


class CircularLogPool:
    def __init__(
        self,
        global_pool: NumpyArrayPool,
        max_pieces: int = 16,
        final_buffer_bytes: int = 32 * 1024 * 1024,
        cold_max_pieces: int = 0,
        cold_storage_dir: Optional[str] = None,
        logger=None,
    ):
        self._global_pool = global_pool
        self.max_pieces = max_pieces
        self.final_buffer_bytes = final_buffer_bytes

        # Initial "probe" settings (1MB)
        self.current_buffer_bytes = 1024 * 1024
        initial_chars_per_log = 32
        self.segment_capacity = self.current_buffer_bytes // initial_chars_per_log

        self.segments: deque[PooledLogBatch] = deque()
        self.segment_counter = 0
        self.active_segment: Optional[PooledLogBatch] = None

        self.sequence: dtypes.SEQ_TYPE = SEQ_NONE
        self._lock = Lock()
        self._optimized = False

        # --- Cold (disk-backed) tier - see plans/mmap-coldstore.md ---
        self.cold_max_pieces = cold_max_pieces
        self.cold_segments: deque[PooledLogBatch] = deque()
        self._logger = logger
        self._archiver: Optional["ColdStorageArchiver"] = None

        if cold_max_pieces > 0 and cold_storage_dir:
            from blinkview.core.cold_storage_archiver import ColdStorageArchiver

            self._archiver = ColdStorageArchiver(cold_storage_dir, on_archived=self._handle_archived, logger=logger)

        self._rotate_segment()

    def latest_sequence(self):
        return self.sequence

    def _handle_archived(self, cold_segment: PooledLogBatch):
        """Called from the archiver's background thread once a segment has been written to disk
        and reopened as a memmap-backed PooledLogBatch. Appends it to the cold tier and, if that
        pushes the cold tier past cold_max_pieces, evicts (releases + deletes the file for) the
        oldest one - same eviction shape as the hot tier's max_pieces handling."""
        to_evict = None
        with self._lock:
            self.cold_segments.append(cold_segment)
            if len(self.cold_segments) > self.cold_max_pieces:
                to_evict = self.cold_segments.popleft()

        if to_evict is not None:
            self._evict_cold_segment(to_evict)

    def _evict_hot_segment(self, hot_segment: PooledLogBatch):
        """Hands a segment falling out of the hot (RAM) tier off to the cold-storage archiver if
        one is configured, otherwise releases it outright - the pre-existing, cold-storage-off
        behavior. Shared by _rotate_segment's normal path and update_max_pieces' immediate-shrink
        path so both go through the same archive-or-drop decision."""
        if self._archiver is not None:
            self._archiver.archive(hot_segment)
        else:
            hot_segment.release()

    def _evict_cold_segment(self, cold_segment: PooledLogBatch):
        meta = cold_segment.metadata
        path = meta.path if meta is not None else None
        cold_segment.release()
        if path:
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                if self._logger:
                    self._logger.warning(f"Failed to delete evicted cold segment file: {path}")

    def _rotate_segment(self):
        if not self._optimized and self.active_segment is not None:
            self._apply_real_world_heuristics()

        if len(self.segments) >= self.max_pieces:
            self._evict_hot_segment(self.segments.popleft())

        # Create using standard PooledLogBatch (Unified class)
        new_segment = self._global_pool.create(
            PooledLogBatch,
            req_capacity=self.segment_capacity,
            buffer_bytes=self.current_buffer_bytes,
            metadata=self.segment_counter,
            has_levels=True,
            has_modules=True,
            has_devices=True,
            has_sequences=True,
            has_pids=True,
            has_tids=True,
        )

        self.segments.append(new_segment)
        self.active_segment = new_segment
        self.segment_counter += 1

    def _apply_real_world_heuristics(self):
        seg = self.active_segment
        if seg and seg.size > 0:
            avg_bytes_per_msg = seg.msg_cursor / seg.size
            self.current_buffer_bytes = self.final_buffer_bytes

            # Calculate capacity based on the target byte size
            potential_capacity = int(self.current_buffer_bytes / avg_bytes_per_msg)
            self.segment_capacity = max(1000, min(potential_capacity, 500_000))
            self._optimized = True

    # def insert(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes):
    #     with self._lock:
    #         # Type safety check for the IDE
    #         if not self.active_segment:
    #             return
    #
    #         success = self.active_segment.insert(ts_ns, msg_bytes, level, module, device, seq)
    #         if not success:
    #             self._rotate_segment()
    #             self.active_segment.insert(ts_ns, msg_bytes, level, module, device, seq)

    def get_reversed_snapshot(self) -> SegmentSnapshot:
        """Newest-to-oldest. Hot (RAM) segments are always newer than cold (disk) ones, so hot is
        yielded first, then cold - each half individually newest-first."""
        with self._lock:
            return SegmentSnapshot(chain(reversed(self.segments), reversed(self.cold_segments)))

    def get_snapshot(self) -> SegmentSnapshot:
        """Oldest-to-newest: cold segments (already oldest-to-newest) then hot segments."""
        with self._lock:
            return SegmentSnapshot(chain(self.cold_segments, self.segments))

    def get_counts(self) -> tuple[int, int, int]:
        with self._lock:
            current_total = sum(seg.size for seg in self.segments) + sum(seg.size for seg in self.cold_segments)
            active_cap = self.active_segment.capacity if self.active_segment else self.segment_capacity
            max_total = self.max_pieces * active_cap
            return current_total, max_total, int(self.sequence)

    def get_time_bounds(self) -> tuple[int, int]:
        """Returns (earliest_ts_ns, latest_ts_ns) of currently retained rows (hot + cold), or
        (0, 0) if empty. The cold-side earliest read comes straight from the cached header field
        on the oldest cold segment's metadata (see ColdSegmentMeta) rather than
        `bundle.timestamps[0]`, so this never has to fault in a cold segment's mmap'd pages just
        to answer a bounds query."""
        with self._lock:
            if self.cold_segments:
                oldest_cold = self.cold_segments[0]
                earliest = oldest_cold.metadata.earliest_ts
            elif self.segments:
                oldest = self.segments[0]
                earliest = oldest.bundle.timestamps[0] if oldest.size else 0
            else:
                return 0, 0

            newest = self.active_segment
            if newest is not None and newest.size:
                latest = newest.bundle.timestamps[newest.size - 1]
            elif self.segments:
                newest_hot = self.segments[-1]
                latest = newest_hot.bundle.timestamps[newest_hot.size - 1] if newest_hot.size else 0
            elif self.cold_segments:
                latest = self.cold_segments[-1].metadata.latest_ts
            else:
                latest = 0

            return int(earliest), int(latest)

    def find_ts_n_rows_away(self, current_ts_ns: int, delta_rows: int) -> int:
        """Returns the timestamp of the row `delta_rows` positions away from `current_ts_ns` in
        the combined hot+cold chronological row sequence - positive steps forward (newer),
        negative steps backward (older). Clamps to the nearest available row if `delta_rows`
        overshoots either end (returns the oldest/newest retained row's timestamp rather than
        raising). Unfiltered: walks every retained row regardless of level/module filter state -
        PlaybackClock (the only caller) has no concept of a UI filter, that's a per-widget
        concern layered on top by whatever's actually rendering the scrubbed position.

        Rows sharing `current_ts_ns` exactly are treated as "at" the current position (not past
        it) on both sides, so stepping by 1 from a tie always lands on a genuinely different row
        rather than re-selecting one of several rows at the same instant."""
        if delta_rows == 0:
            return current_ts_ns

        if delta_rows > 0:
            with self.get_snapshot() as segments:
                remaining = delta_rows
                last_ts = current_ts_ns
                for seg in segments:
                    if seg.size == 0:
                        continue
                    ts = seg.bundle.timestamps[: seg.size]
                    start_idx = int(np.searchsorted(ts, current_ts_ns, side="right"))
                    available = seg.size - start_idx
                    if available <= 0:
                        continue
                    if remaining <= available:
                        return int(ts[start_idx + remaining - 1])
                    remaining -= available
                    last_ts = int(ts[seg.size - 1])
                return last_ts
        else:
            with self.get_reversed_snapshot() as segments:
                remaining = -delta_rows
                first_ts = current_ts_ns
                for seg in segments:
                    if seg.size == 0:
                        continue
                    ts = seg.bundle.timestamps[: seg.size]
                    end_idx = int(np.searchsorted(ts, current_ts_ns, side="left"))  # rows strictly before
                    available = end_idx
                    if available <= 0:
                        continue
                    if remaining <= available:
                        return int(ts[end_idx - remaining])
                    remaining -= available
                    first_ts = int(ts[0])
                return first_ts

    def release_all(self):
        # Stop the archiver (and let it drain whatever's already in its queue) *before* draining
        # segments/cold_segments - otherwise a segment handed off just before shutdown could still
        # land in cold_segments via _handle_archived() after we've already emptied it, orphaning
        # both the PooledLogBatch reference and its on-disk file.
        if self._archiver is not None:
            self._archiver.stop()

        with self._lock:
            while self.segments:
                self.segments.popleft().release()
            while self.cold_segments:
                self.cold_segments.popleft().release()
            self.active_segment = None

        if self._archiver is not None:
            self._archiver.cleanup()
            self._archiver = None

    def batch_append(self, batch: PooledLogBatch):
        if (size := batch.size) == 0:
            return

        with self._lock:
            rows_written = 0
            b_src = batch.bundle
            if not b_src:
                return

            while rows_written < size:
                # Fast Path: Symmetrical Copy (Bundle to Bundle)
                copied = nb_copy_batch_to_segment(self.active_segment.bundle, b_src, rows_written, self.sequence)

                rows_written += copied
                self.sequence += copied

                if rows_written < size:
                    # Check for toxic logs (exceeds current segment buffer)
                    next_msg_len = b_src.lengths[rows_written]
                    toxic_threshold = min(self.current_buffer_bytes, 1024 * 1024)

                    if next_msg_len > toxic_threshold:
                        self._rotate_segment()

                        # Use the unified insert_truncated_error method
                        ts, raw_msg, _lvl, mod, dev, _seq, _e1, _e2, _e3 = batch[rows_written]
                        insert_truncated_error(self.active_segment, ts, mod, dev, self.sequence, raw_msg)

                        rows_written += 1
                        self.sequence += 1
                    else:
                        self._rotate_segment()

    def clear(self):
        with self._lock:
            while self.segments:
                self.segments.popleft().release()

            evicted_cold = list(self.cold_segments)
            self.cold_segments.clear()

            self.segment_counter = 0
            self.sequence = SEQ_NONE
            self.active_segment = None
            self._rotate_segment()

        # Release + delete cold segment files outside the lock (matches release_all's ordering -
        # avoids holding self._lock across filesystem I/O).
        for cold_segment in evicted_cold:
            self._evict_cold_segment(cold_segment)

    def acquire_indices_buffer(self, capacity: Optional[int] = None):
        """
        Acquires a preallocated indices array from the global memory pool.
        Sized by default to accommodate the maximum segment capacity.

        Returns:
            A memory handle. The caller must call .release() on it when finished.
        """
        # Default to the pool's current segment capacity, which adapts
        # after the _apply_real_world_heuristics probe phase.
        req_cap = capacity if capacity is not None else self.segment_capacity
        return self._global_pool.acquire(req_cap, dtype=np.int64)

    def update_max_pieces(self, new_max_pieces: int):
        """
        Dynamically updates the lookback window ceiling.
        Trims older segments immediately if the new ceiling is smaller than the current pool size.
        """
        if new_max_pieces <= 0:
            raise ValueError("max_pieces must be greater than 0")

        with self._lock:
            if self.max_pieces == new_max_pieces:
                return

            self.max_pieces = new_max_pieces

            # Immediately evict excess historical chunks if the window was shrunk
            while len(self.segments) > self.max_pieces:
                self._evict_hot_segment(self.segments.popleft())

    def update_final_buffer_bytes(self, new_buffer_bytes: int):
        """
        Dynamically updates the target buffer byte size for future segments.
        Resets optimization state to recalculate structural row capacities based on heuristics.
        """
        if new_buffer_bytes <= 0:
            raise ValueError("final_buffer_bytes must be greater than 0")

        with self._lock:
            if self.final_buffer_bytes == new_buffer_bytes:
                return

            self.final_buffer_bytes = new_buffer_bytes

            # Reset optimization flag so the next _rotate_segment recalculates
            # the optimal row capacity (`segment_capacity`) using the new byte budget.
            self._optimized = False

    def update_cold_max_pieces(self, new_cold_max_pieces: int):
        """Dynamically updates the cold-tier lookback ceiling. Immediately evicts (releases +
        deletes the file for) the oldest cold segments if the new ceiling is smaller than the
        current cold tier size. A no-op if cold storage isn't configured (self._archiver is None)
        - there's nothing to shrink."""
        if new_cold_max_pieces < 0:
            raise ValueError("cold_max_pieces must be >= 0")

        if self._archiver is None:
            self.cold_max_pieces = new_cold_max_pieces
            return

        with self._lock:
            if self.cold_max_pieces == new_cold_max_pieces:
                return

            self.cold_max_pieces = new_cold_max_pieces

            evicted = []
            while len(self.cold_segments) > self.cold_max_pieces:
                evicted.append(self.cold_segments.popleft())

        for cold_segment in evicted:
            self._evict_cold_segment(cold_segment)

    @staticmethod
    @register_warmup(priority=100)
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for Batch Append and Log Filtering/Formatting. Runs first among
        the registered warmup callbacks (explicit high priority, not import-order luck) since
        every other callback's log/telemetry kernels need rows already present in
        helper.log_pool."""

        print("[Warmup] CircularLogPool ...")

        log_level = LogLevel.INFO.value

        with helper.array_pool.create(
            PooledLogBatch,
            1024,
            1024 * 64,
            has_levels=True,
            has_modules=True,
            has_devices=True,
        ) as batch:
            # Trigger string/float parsing kernels
            for i in range(1000):
                time_now = helper.time_ns()
                batch.insert(
                    time_now + i,
                    time_now + i,
                    b"ADC: -1.234, 5.678 ; 100 -0.001",
                    log_level,
                    helper.floats_mod.id,
                    helper.floats_mod.device.id,
                )
            batch.insert(
                helper.time_ns(),
                helper.time_ns(),
                b"System Hot",
                log_level,
                helper.warmup_mod.id,
                helper.warmup_mod.device.id,
            )
            # Trigger: Batch Append Logic
            helper.log_pool.batch_append(batch)

        print("[Warmup] CircularLogPool ... done")


def allocate_telemetry_workspace(num_channels: int) -> np.ndarray:
    """
    Allocates a persistent scratchpad for the telemetry extractor.
    This should be stored in the ModuleBuffer to avoid mid-loop allocations.
    """
    return np.empty(num_channels, dtype=dtypes.PLOT_VAL_TYPE)


# A safe upper bound for probing unknown telemetry modules
MAX_PROBE_CHANNELS = 512


def allocate_discovery_workspace() -> np.ndarray:
    """
    Allocates a shared scratchpad for schema discovery (the anchor logic).
    Since only one module is 'peaked' at a time, this can be shared.
    """
    return np.empty(MAX_PROBE_CHANNELS, dtype=dtypes.PLOT_VAL_TYPE)


@contextmanager
def fetch_telemetry_arrays(
    array_pool: "NumpyArrayPool",
    log_pool: "CircularLogPool",
    target_module_int: int,
    start_seq: int,
    num_channels: int,
    temp_floats: np.ndarray,
    max_points: int = 5000,
    effective_mask: Optional[np.ndarray] = None,
):
    """effective_mask - per-module level threshold (see ops/segments.py's
    segment_filter/segment_filter_reversed and ops/telemetry.py's
    nb_extract_telemetry_segment_to_end) applied on top of the existing exact target_module
    match, so a caller can make plotted telemetry respect the same level filter a log view
    would. Defaults to a permissive single-entry mask covering just target_module_int (the only
    index the kernel ever reads, since rows are already filtered to that module) - equivalent to
    "no filtering", preserving this function's original module-only behavior for callers that
    don't pass one. No caller wires a real mask in yet - see TelemetryPlotter's call sites."""
    if effective_mask is None:
        effective_mask = np.zeros(target_module_int + 1, dtype=dtypes.LEVEL_TYPE)

    # CRITICAL: Ensure this is from contextlib, not typing
    with ExitStack() as stack:
        # 1. Acquire Snapshot
        segments = stack.enter_context(log_pool.get_reversed_snapshot())

        # 2. Acquire Pool Memory
        # These handles are context managers; ExitStack will call __exit__ (release)
        times_handle = stack.enter_context(array_pool.get(max_points, dtype=dtypes.PLOT_TS_TYPE))
        times_int64_handle = stack.enter_context(array_pool.get(max_points, dtype=np.int64))  # NEW
        values_handle = stack.enter_context(array_pool.get(max_points * num_channels, dtype=dtypes.PLOT_VAL_TYPE))

        # Setup extraction views
        out_times = times_handle.array[:max_points]
        out_times_int64 = times_int64_handle.array[:max_points]
        out_values = values_handle.array[: max_points * num_channels].reshape((max_points, num_channels))

        curr_write_idx = max_points
        new_watermark = start_seq

        # 3. Extraction (Reverse-to-End)
        if segments:
            for segment in segments:
                if curr_write_idx <= 0:
                    break
                segment_last_sequence_id = segment.last_sequence_id
                if segment.size == 0 or segment_last_sequence_id <= start_seq:
                    break

                if new_watermark == start_seq:
                    new_watermark = segment_last_sequence_id

                curr_write_idx = nb_extract_telemetry_segment_to_end(
                    segment.bundle,
                    target_module_int,
                    dtypes.SEQ_TYPE(start_seq),
                    num_channels,
                    out_times,
                    out_times_int64,
                    out_values,
                    temp_floats,
                    curr_write_idx,
                    effective_mask,
                )

        # 4. Yield result (even if empty)
        yield TelemetryBatch(
            times=out_times[curr_write_idx:],
            times_int64=out_times_int64[curr_write_idx:],
            values=out_values[curr_write_idx:],
            watermark=new_watermark,
        )

        # When the caller's 'with' block ends, ExitStack finishes and releases all handles.


@contextmanager
def fetch_telemetry_window(
    array_pool: "NumpyArrayPool",
    log_pool: "CircularLogPool",
    target_module_int: int,
    num_channels: int,
    temp_floats: np.ndarray,
    anchor_ts_ns: int,
    before_span_ns: int,
    after_span_ns: int,
    before_cap: int,
    after_cap: int,
    effective_mask: Optional[np.ndarray] = None,
    plus_one: bool = False,
):
    """Playback-scrub counterpart to fetch_telemetry_arrays: extracts a bounded time window of
    telemetry samples for one module around anchor_ts_ns, independent of any forward-fetch
    sequence watermark. Used to populate a ReplayWindowBuffer (core/buffers.py) while following
    registry.playback_clock, rather than a ModuleBuffer ring's forward-only accumulation.

    before_span_ns/after_span_ns bound the ts range considered on each side of the anchor;
    before_cap/after_cap additionally cap the sample count per side - kept small while actively
    following (see TelemetryPlotter.apply_updates), upgraded to a larger cap once the user pans
    away from following, mirroring log_viewer.py's FOLLOW-window-vs-HISTORY-window split.

    plus_one - if True, also fetches the single nearest sample immediately outside each edge of
    the [anchor - before_span_ns, anchor + after_span_ns] window (unbounded on the far side, so
    it's found regardless of how sparse the data is - a fixed-time padding on before_span_ns/
    after_span_ns alone doesn't guarantee this: a gap between samples wider than the padding
    still leaves nothing there). Needed by TelemetryPlotter, whose renderer
    (nb_slice_and_downsample_linear, ops/telemetry.py) has its own "+1 point" edge-snap logic
    that draws the line exactly to the viewport boundary *if* a real sample exists just past it
    - without one, the line stops short of the edge instead of filling the view. Each segment's
    windowed scan and its edge-neighbor search happen in a single
    nb_extract_telemetry_segment_window_backward/forward call (its capture_edge/edge_remaining
    params), writing the edge row contiguous with the window rows already written - not a
    separate pass over the segments or a separate output slot needing its own concatenation.

    effective_mask - see fetch_telemetry_arrays' docstring; same permissive-default-when-None
    behavior.

    The returned TelemetryBatch's `watermark` field is meaningless here (no forward-fetch
    watermark concept applies to an arbitrary-time-anchored window) and is always SEQ_NONE -
    ReplayWindowBuffer.update() does not read it, unlike ModuleBuffer.update()'s use of
    fetch_telemetry_arrays' watermark.
    """
    if effective_mask is None:
        effective_mask = np.zeros(target_module_int + 1, dtype=dtypes.LEVEL_TYPE)

    edge = 1 if plus_one else 0
    before_slot_count = before_cap + edge
    after_slot_count = after_cap + edge

    with ExitStack() as stack:
        before_segments = stack.enter_context(log_pool.get_reversed_snapshot())
        after_segments = stack.enter_context(log_pool.get_snapshot())

        # A single shared buffer per field, not two separate allocations - before_out_* and
        # after_out_* below are adjacent views into it (before ends exactly where after starts),
        # so the two halves' *used* ranges end up contiguous too (see the final slice below).
        max_points = before_slot_count + after_slot_count
        times_handle = stack.enter_context(array_pool.get(max_points, dtype=dtypes.PLOT_TS_TYPE))
        times_int64_handle = stack.enter_context(array_pool.get(max_points, dtype=np.int64))
        values_handle = stack.enter_context(array_pool.get(max_points * num_channels, dtype=dtypes.PLOT_VAL_TYPE))

        out_times = times_handle.array[:max_points]
        out_times_int64 = times_int64_handle.array[:max_points]
        out_values = values_handle.array[: max_points * num_channels].reshape((max_points, num_channels))

        before_out_times = out_times[:before_slot_count]
        before_out_times_int64 = out_times_int64[:before_slot_count]
        before_out_values = out_values[:before_slot_count]
        after_out_times = out_times[before_slot_count:max_points]
        after_out_times_int64 = out_times_int64[before_slot_count:max_points]
        after_out_values = out_values[before_slot_count:max_points]

        # --- Before half: [anchor - before_span, anchor - 1], newest-to-oldest, writing
        # backward - end_ts excludes the anchor row itself so the after half (which includes it)
        # doesn't double-extract it, mirroring log_viewer.py's anchor_ts - 1 / anchor_ts split
        # between its before/after log-row scans. plus_one's edge row (if any) is captured by
        # the same per-segment call and lands at write_idx - 1, i.e. always contiguous with
        # whatever window rows were already written - see capture_edge's docstring.
        before_write_idx = before_slot_count
        before_edge_remaining = edge
        before_window = TsWindowBundle(
            start_ts=dtypes.TS_TYPE(anchor_ts_ns - before_span_ns),
            end_ts=dtypes.TS_TYPE(anchor_ts_ns - 1),
        )
        for segment in before_segments:
            if before_write_idx <= 0:
                break
            if segment.size == 0:
                continue
            before_write_idx, before_edge_remaining = nb_extract_telemetry_segment_window_backward(
                segment.bundle,
                target_module_int,
                before_window,
                num_channels,
                before_out_times,
                before_out_times_int64,
                before_out_values,
                temp_floats,
                before_write_idx,
                effective_mask,
                plus_one,
                before_edge_remaining,
            )

        # --- After half: [anchor, anchor + after_span], oldest-to-newest, writing forward ---
        after_write_idx = 0
        after_edge_remaining = edge
        after_window = TsWindowBundle(
            start_ts=dtypes.TS_TYPE(anchor_ts_ns),
            end_ts=dtypes.TS_TYPE(anchor_ts_ns + after_span_ns),
        )
        for segment in after_segments:
            if after_write_idx >= after_slot_count:
                break
            if segment.size == 0:
                continue
            after_write_idx, after_edge_remaining = nb_extract_telemetry_segment_window_forward(
                segment.bundle,
                target_module_int,
                after_window,
                num_channels,
                after_out_times,
                after_out_times_int64,
                after_out_values,
                temp_floats,
                after_write_idx,
                effective_mask,
                plus_one,
                after_edge_remaining,
            )

        # before_out_*/after_out_* are adjacent views into the same out_times/out_times_int64/
        # out_values buffer (before ends at exactly before_slot_count, after starts there) - the
        # before half's used range [before_write_idx, before_slot_count) therefore sits right up
        # against the after half's used range [before_slot_count, before_slot_count +
        # after_write_idx) with no gap between them. A single slice of the shared buffer across
        # both is already the fully-ascending combined result (before scanned newest-to-oldest
        # but written backward; after scanned oldest-to-newest and written forward) - no
        # concatenation (i.e. no copy) needed.
        combined = slice(before_write_idx, before_slot_count + after_write_idx)

        yield TelemetryBatch(
            times=out_times[combined],
            times_int64=out_times_int64[combined],
            values=out_values[combined],
            watermark=SEQ_NONE,
        )


def get_telemetry_anchor(
    pool: "CircularLogPool",
    target_module_int: int,
    last_known_seq: dtypes.SEQ_TYPE,
    temp_floats: np.ndarray,
    view_capacity: int = 5000,
) -> tuple[dtypes.SEQ_TYPE, int]:
    lks = dtypes.SEQ_TYPE(last_known_seq)
    remaining = int(view_capacity)
    detected_channels = 0
    final_anchor = lks

    with pool.get_reversed_snapshot() as segments:
        for segment in segments:
            # Skip segment if it's strictly older than our high-water mark
            if segment.size == 0 or (segment.last_sequence_id <= lks and lks != SEQ_NONE):
                break

            bundle = segment.bundle
            # --- PHASE 1: DISCOVERY ---
            if detected_channels == 0:
                head_seq, channels = nb_peek_segment_channels_backwards(bundle, target_module_int, lks, temp_floats)

                if head_seq != SEQ_NONE:
                    detected_channels = channels
                    # Start counting backwards from the head_seq we just found
                    found_in_seg, earliest = nb_count_module_occurrences_backwards(
                        bundle, target_module_int, head_seq, remaining
                    )
                    remaining -= found_in_seg
                    final_anchor = earliest
                else:
                    # Nothing in this segment for this module, try the next (older) one
                    continue

            # --- PHASE 2: ANCHORING ---
            else:
                found_in_seg, earliest = nb_count_module_occurrences_backwards(
                    bundle, target_module_int, segment.last_sequence_id, remaining
                )

                # CRITICAL FIX: Only update the anchor if we actually found points
                if found_in_seg > 0:
                    remaining -= found_in_seg
                    final_anchor = earliest

            # --- EXIT CONDITIONS ---
            # 1. We found enough points to fill the view
            if remaining <= 0:
                break

            # 2. We've reached data we already have in the UI
            if segment.first_sequence_id <= lks and lks != SEQ_NONE:
                break

    # Return the earliest sequence ID found that satisfies the capacity
    return max(lks, final_anchor), detected_channels

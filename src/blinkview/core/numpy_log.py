# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from contextlib import ExitStack, contextmanager
from threading import Lock
from typing import Iterable, Optional

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
    def __init__(self, global_pool: NumpyArrayPool, max_pieces: int = 16, final_buffer_bytes: int = 32 * 1024 * 1024):
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

        self._rotate_segment()

    def latest_sequence(self):
        return self.sequence

    def _rotate_segment(self):
        if not self._optimized and self.active_segment is not None:
            self._apply_real_world_heuristics()

        if len(self.segments) >= self.max_pieces:
            oldest = self.segments.popleft()
            oldest.release()

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
        with self._lock:
            return SegmentSnapshot(reversed(self.segments))

    def get_snapshot(self) -> SegmentSnapshot:
        with self._lock:
            return SegmentSnapshot(self.segments)

    def get_counts(self) -> tuple[int, int, int]:
        with self._lock:
            current_total = sum(seg.size for seg in self.segments)
            active_cap = self.active_segment.capacity if self.active_segment else self.segment_capacity
            max_total = self.max_pieces * active_cap
            return current_total, max_total, int(self.sequence)

    def get_time_bounds(self) -> tuple[int, int]:
        """Returns (earliest_ts_ns, latest_ts_ns) of currently retained rows, or (0, 0) if empty."""
        with self._lock:
            if not self.segments:
                return 0, 0
            oldest = self.segments[0]
            newest = self.active_segment
            earliest = oldest.bundle.timestamps[0] if oldest.size else 0
            latest = newest.bundle.timestamps[newest.size - 1] if newest and newest.size else 0
            return int(earliest), int(latest)

    def release_all(self):
        with self._lock:
            while self.segments:
                self.segments.popleft().release()
            self.active_segment = None

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

            self.segment_counter = 0
            self.sequence = SEQ_NONE
            self.active_segment = None
            self._rotate_segment()

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
                oldest = self.segments.popleft()
                oldest.release()

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
):
    """Playback-scrub counterpart to fetch_telemetry_arrays: extracts a bounded time window of
    telemetry samples for one module around anchor_ts_ns, independent of any forward-fetch
    sequence watermark. Used to populate a ReplayWindowBuffer (core/buffers.py) while following
    registry.playback_clock, rather than a ModuleBuffer ring's forward-only accumulation.

    before_span_ns/after_span_ns bound the ts range considered on each side of the anchor;
    before_cap/after_cap additionally cap the sample count per side - kept small while actively
    following (see TelemetryPlotter.apply_updates), upgraded to a larger cap once the user pans
    away from following, mirroring log_viewer.py's FOLLOW-window-vs-HISTORY-window split.

    effective_mask - see fetch_telemetry_arrays' docstring; same permissive-default-when-None
    behavior.

    The returned TelemetryBatch's `watermark` field is meaningless here (no forward-fetch
    watermark concept applies to an arbitrary-time-anchored window) and is always SEQ_NONE -
    ReplayWindowBuffer.update() does not read it, unlike ModuleBuffer.update()'s use of
    fetch_telemetry_arrays' watermark.
    """
    if effective_mask is None:
        effective_mask = np.zeros(target_module_int + 1, dtype=dtypes.LEVEL_TYPE)

    with ExitStack() as stack:
        before_segments = stack.enter_context(log_pool.get_reversed_snapshot())
        after_segments = stack.enter_context(log_pool.get_snapshot())

        max_points = before_cap + after_cap
        times_handle = stack.enter_context(array_pool.get(max_points, dtype=dtypes.PLOT_TS_TYPE))
        times_int64_handle = stack.enter_context(array_pool.get(max_points, dtype=np.int64))
        values_handle = stack.enter_context(array_pool.get(max_points * num_channels, dtype=dtypes.PLOT_VAL_TYPE))

        out_times = times_handle.array[:max_points]
        out_times_int64 = times_int64_handle.array[:max_points]
        out_values = values_handle.array[: max_points * num_channels].reshape((max_points, num_channels))

        # --- Before half: [anchor - before_span, anchor - 1], newest-to-oldest, writing
        # backward into out_*[:before_cap] - end_ts excludes the anchor row itself so the after
        # half (which includes it) doesn't double-extract it, mirroring log_viewer.py's
        # anchor_ts - 1 / anchor_ts split between its before/after log-row scans.
        before_write_idx = before_cap
        before_window = TsWindowBundle(
            start_ts=dtypes.TS_TYPE(anchor_ts_ns - before_span_ns),
            end_ts=dtypes.TS_TYPE(anchor_ts_ns - 1),
        )
        for segment in before_segments:
            if before_write_idx <= 0:
                break
            if segment.size == 0:
                continue
            before_write_idx = nb_extract_telemetry_segment_window_backward(
                segment.bundle,
                target_module_int,
                before_window,
                num_channels,
                out_times[:before_cap],
                out_times_int64[:before_cap],
                out_values[:before_cap],
                temp_floats,
                before_write_idx,
                effective_mask,
            )

        # --- After half: [anchor, anchor + after_span], oldest-to-newest, writing forward into
        # out_*[before_cap:before_cap+after_cap] ---
        after_write_idx = 0
        after_window = TsWindowBundle(
            start_ts=dtypes.TS_TYPE(anchor_ts_ns),
            end_ts=dtypes.TS_TYPE(anchor_ts_ns + after_span_ns),
        )
        after_out_times = out_times[before_cap:]
        after_out_times_int64 = out_times_int64[before_cap:]
        after_out_values = out_values[before_cap:]
        for segment in after_segments:
            if after_write_idx >= after_cap:
                break
            if segment.size == 0:
                continue
            after_write_idx = nb_extract_telemetry_segment_window_forward(
                segment.bundle,
                target_module_int,
                after_window,
                num_channels,
                after_out_times[:after_cap],
                after_out_times_int64[:after_cap],
                after_out_values[:after_cap],
                temp_floats,
                after_write_idx,
                effective_mask,
            )

        # Both halves already land ascending (before scanned newest-to-oldest but written
        # backward; after scanned oldest-to-newest and written forward) - straight concatenation
        # needs no merge/sort step.
        before_slice = slice(before_write_idx, before_cap)
        after_slice = slice(before_cap, before_cap + after_write_idx)

        yield TelemetryBatch(
            times=np.concatenate([out_times[before_slice], out_times[after_slice]]),
            times_int64=np.concatenate([out_times_int64[before_slice], out_times_int64[after_slice]]),
            values=np.concatenate([out_values[before_slice], out_values[after_slice]]),
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

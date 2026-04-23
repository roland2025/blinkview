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
from blinkview.ops.segments import copy_batch_to_segment
from blinkview.ops.telemetry import (
    count_module_occurrences_backwards,
    extract_telemetry_segment_to_end,
    peek_segment_channels_backwards,
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
        ts_ns=ts_ns, level=LogLevel.ERROR.value, module=module, device=device, seq=seq, msg_bytes=msg_bytes
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
                copied = copy_batch_to_segment(self.active_segment.bundle, b_src, rows_written, self.sequence)

                rows_written += copied
                self.sequence += copied

                if rows_written < size:
                    # Check for toxic logs (exceeds current segment buffer)
                    next_msg_len = b_src.lengths[rows_written]
                    toxic_threshold = min(self.current_buffer_bytes, 1024 * 1024)

                    if next_msg_len > toxic_threshold:
                        self._rotate_segment()

                        # Use the unified insert_truncated_error method
                        ts, raw_msg, _, mod, dev, _ = batch[rows_written]
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
):
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

                curr_write_idx = extract_telemetry_segment_to_end(
                    segment.bundle,
                    target_module_int,
                    dtypes.SEQ_TYPE(start_seq),
                    num_channels,
                    out_times,
                    out_times_int64,
                    out_values,
                    temp_floats,
                    curr_write_idx,
                )

        # 4. Yield result (even if empty)
        yield TelemetryBatch(
            times=out_times[curr_write_idx:],
            times_int64=out_times_int64[curr_write_idx:],
            values=out_values[curr_write_idx:],
            watermark=new_watermark,
        )

        # When the caller's 'with' block ends, ExitStack finishes and releases all handles.


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
                head_seq, channels = peek_segment_channels_backwards(bundle, target_module_int, lks, temp_floats)

                if head_seq != SEQ_NONE:
                    detected_channels = channels
                    # Start counting backwards from the head_seq we just found
                    found_in_seg, earliest = count_module_occurrences_backwards(
                        bundle, target_module_int, head_seq, remaining
                    )
                    remaining -= found_in_seg
                    final_anchor = earliest
                else:
                    # Nothing in this segment for this module, try the next (older) one
                    continue

            # --- PHASE 2: ANCHORING ---
            else:
                found_in_seg, earliest = count_module_occurrences_backwards(
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

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from threading import Lock
from typing import Iterable, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.id_registry import IDRegistry
from blinkview.core.log_row import LogRow
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.log_batch import TelemetryBatch
from blinkview.core.types.segments import LogSegmentParams
from blinkview.ops.segments import copy_batch_to_segment, filter_segment
from blinkview.ops.telemetry import (
    extract_telemetry_segment_to_end,
    peek_segment_channels_backwards,
)
from blinkview.utils.log_level import LogLevel


class SegmentSnapshot:
    """
    A context-managed snapshot of segments.
    Ensures memory isn't recycled while a query is running.
    """

    def __init__(self, segments_iter: Iterable["LogSegment"]):
        # Single pass: consumes the iterator, retains segments, and builds the final list
        self.segments: list["LogSegment"] = [seg.retain() for seg in segments_iter]

    def __enter__(self):
        return self.segments

    def __exit__(self, exc_type, exc_val, exc_tb):
        for seg in self.segments:
            seg.release()


class LogSegment:
    __slots__ = (
        "segment_seq",
        "capacity",
        "_count_arr",
        "_cursor_arr",
        "_pool",
        "_ref_count",
        "_lock",
        "_bundle",  # The only thing holding the array references
        "_ts_h",
        "_lvl_h",
        "_mod_h",
        "_dev_h",
        "_seq_h",
        "_off_h",
        "_len_h",
        "_buf_h",
    )

    def __init__(self, pool, segment_seq: int, req_capacity: int, req_buffer_mb: int):
        self._pool = pool
        self.segment_seq = segment_seq
        self._count_arr = np.zeros(1, dtype=np.int64)
        self._cursor_arr = np.zeros(1, dtype=np.int64)
        self._ref_count = 1  # Created with 1 ref (held by the CircularLogPool)
        self._lock = Lock()

        # Acquire arrays natively from the global pool using element count
        self._ts_h = self._pool.acquire(req_capacity, dtype=dtypes.TS_TYPE)

        self.capacity = len(self._ts_h.array)

        self._lvl_h = self._pool.acquire(self.capacity, dtype=dtypes.LEVEL_TYPE)

        self._mod_h = self._pool.acquire(self.capacity, dtype=dtypes.ID_TYPE)

        self._dev_h = self._pool.acquire(self.capacity, dtype=dtypes.ID_TYPE)

        self._seq_h = self._pool.acquire(self.capacity, dtype=dtypes.SEQ_TYPE)

        self._off_h = self._pool.acquire(self.capacity, dtype=dtypes.OFFSET_TYPE)

        self._len_h = self._pool.acquire(self.capacity, dtype=dtypes.LEN_TYPE)

        # 1 MB = 1024 * 1024 bytes (elements for uint8)
        self._buf_h = self._pool.acquire(req_buffer_mb * 1024 * 1024, dtype=dtypes.BYTE)
        self._bundle = LogSegmentParams(
            self._ts_h.array,
            self._lvl_h.array,
            self._mod_h.array,
            self._dev_h.array,
            self._seq_h.array,
            self._off_h.array,
            self._len_h.array,
            self._buf_h.array,
            self._count_arr,
            self._cursor_arr,
            self.capacity,
        )

    def bundle(self) -> LogSegmentParams:
        """Returns a baked snapshot. Re-baked only if the row count has changed."""
        return self._bundle

    @property
    def count(self):
        return self._count_arr[0]

    @property
    def msg_cursor(self):
        return self._cursor_arr[0]

    def clear_and_recycle(self, new_segment_seq: int):
        """O(1) reset to reuse the existing memory slabs for the next rotation."""
        self.segment_seq = new_segment_seq
        self._count_arr[0] = 0
        self._cursor_arr[0] = 0

    def insert(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes) -> bool:
        # 1. Grab a local reference to the bundle
        # This avoids repeated 'self._bundle' attribute lookups in the hot path.
        b = self._bundle

        # 2. Extract current state from the shared arrays
        count = b.count[0]
        cursor = b.msg_cursor[0]

        # 3. Capacity Checks
        if count >= b.capacity:
            return False

        msg_len = len(msg_bytes)
        if cursor + msg_len > len(b.buffer):
            return False

        # 4. Direct Write to Arrays
        # We use 'count' as our index
        b.timestamps[count] = ts_ns
        b.levels[count] = level
        b.modules[count] = module
        b.devices[count] = device
        b.sequence_ids[count] = seq
        b.offsets[count] = cursor
        b.lengths[count] = msg_len

        # 5. Buffer Copy
        b.buffer[cursor : cursor + msg_len] = np.frombuffer(msg_bytes, dtype=dtypes.BYTE)

        # 6. Update shared counters
        # These updates are immediately visible to the Numba kernels
        # because they share the same memory reference.
        b.msg_cursor[0] += msg_len
        b.count[0] += 1

        return True

    @property
    def last_sequence_id(self) -> dtypes.SEQ_TYPE:
        """Returns the sequence ID of the last log, or SEQ_NONE if empty."""
        cnt = self.count
        # Using SEQ_NONE (0) ensures uint64 consistency for Numba
        return self._bundle.sequence_ids[cnt - 1] if cnt > 0 else SEQ_NONE

    def retain(self):
        """Increments reference count for query/telemetry processing."""
        with self._lock:
            if self._ref_count <= 0:
                raise RuntimeError("Cannot retain a segment already released to pool.")
            self._ref_count += 1
        return self

    def release(self):
        """Decrements reference count. If 0, returns all arrays to the global pool."""
        with self._lock:
            self._ref_count -= 1
            if self._ref_count > 0:
                return

        # This drops the "baked" references to the NumPy arrays.
        self._bundle = None

        for h in (
            self._ts_h,
            self._lvl_h,
            self._mod_h,
            self._dev_h,
            self._seq_h,
            self._off_h,
            self._len_h,
            self._buf_h,
        ):
            if h is not None:
                h.release()

        # 3. Batch-clear the slots to prevent double-release/leaks
        self._ts_h = self._lvl_h = self._mod_h = self._dev_h = None
        self._seq_h = self._off_h = self._len_h = self._buf_h = None

    def insert_batch_chunk(self, batch: "PooledLogBatch", start_idx: int, start_seq_id: int) -> int:
        """
        Appends a chunk of the batch starting from `start_idx`.
        Assigns sequentially increasing IDs starting from `start_seq_id`.
        Returns the number of rows successfully appended.
        """

        # We pass self.msg_cursor explicitly as it is the only dynamic write-head
        rows_copied = copy_batch_to_segment(self._bundle, batch.bundle(), start_idx, start_seq_id)

        return rows_copied

    def insert_truncated_error(self, ts_ns: int, module: int, device: int, seq: int, msg_bytes: bytes):
        """Forces a message into the buffer by truncating it to 512 chars."""
        limit = 512
        suffix = b" ... [TRUNCATED]"

        # Ensure the total length is exactly 'limit'
        if len(msg_bytes) > limit:
            truncated_msg = msg_bytes[: limit - len(suffix)] + suffix
        else:
            truncated_msg = msg_bytes

        print(
            f"WARNING append_truncated_error: Original length {len(msg_bytes)} exceeds limit. Truncated to {len(truncated_msg)} bytes. msg={truncated_msg}"
        )

        return self.insert(ts_ns, LogLevel.ERROR.value, module, device, seq, truncated_msg)

    def debug_print(self):
        """Prints the log segment rows in a compact, scannable format."""
        b = self._bundle
        count = b.count[0]
        cursor = b.msg_cursor[0]

        print(f"--- LogSegment Dump (seq={self.segment_seq} count={count}/{b.capacity} cursor={cursor}) ---")

        for i in range(count):
            # Access everything via the bundle local reference
            ts = b.timestamps[i]
            lvl = b.levels[i]
            mod = b.modules[i]
            dev = b.devices[i]
            seq = b.sequence_ids[i]
            off = b.offsets[i]
            n_len = b.lengths[i]

            # Extract message from the byte buffer
            raw_bytes = b.buffer[off : off + n_len].tobytes()
            try:
                msg = raw_bytes.decode("utf-8")
            except UnicodeDecodeError:
                msg = f"ERR:{raw_bytes!r}"

            print(
                f"idx={i:<4} ts={ts:<15} lvl={lvl:<2} mod={mod:<3} "
                f"dev={dev:<3} seq={seq:<6} off={off:<8} len={n_len:<4} "
                f'msg="{msg}"'
            )

        print("--- End Dump ---")


class CircularLogPool:
    def __init__(self, global_pool, max_pieces: int = 16, final_buffer_mb: int = 32):
        self._global_pool = global_pool
        self.max_pieces = max_pieces
        self.final_buffer_mb = final_buffer_mb

        self.buffer_mb = 1
        initial_chars_per_log = 32
        self.segment_capacity = (1 * 1024 * 1024) // initial_chars_per_log

        self.segments: deque[LogSegment] = deque()
        self.segment_counter = 0
        self.active_segment: Optional[LogSegment] = None

        # Start at 0 (Sentinel). The first log will be 0 + 1 = 1.
        self.sequence: dtypes.SEQ_TYPE = SEQ_NONE

        self._lock = Lock()
        self._optimized = False

        self._rotate_segment()

    def latest_sequence(self):
        return self.sequence

    def _rotate_segment(self):
        # No lock here because _rotate_segment is called from
        # append/batch_append which already hold the lock.

        # 1. HEURISTIC FIX: Use msg_cursor
        if not self._optimized and self.active_segment is not None:
            self._apply_real_world_heuristics()

        if len(self.segments) >= self.max_pieces:
            oldest = self.segments.popleft()
            oldest.release()

        new_segment = self._global_pool.create(LogSegment, self.segment_counter, self.segment_capacity, self.buffer_mb)

        self.segments.append(new_segment)
        self.active_segment = new_segment
        self.segment_counter += 1

    def _apply_real_world_heuristics(self):
        seg = self.active_segment
        if seg.count > 0:
            # FIX: Changed bytes_used to msg_cursor
            avg_bytes_per_msg = seg.msg_cursor / seg.count

            self.buffer_mb = self.final_buffer_mb

            # Calculate capacity based on the 32MB target
            potential_capacity = int((self.buffer_mb * 1024 * 1024) / avg_bytes_per_msg)

            # Sanity check: cap the capacity so we don't allocate
            # millions of rows of metadata if logs are 1 byte each.
            self.segment_capacity = max(1000, min(potential_capacity, 500_000))

            self._optimized = True

    def insert(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes):
        with self._lock:
            success = self.active_segment.insert(ts_ns, level, module, device, seq, msg_bytes)
            if not success:
                self._rotate_segment()
                self.active_segment.insert(ts_ns, level, module, device, seq, msg_bytes)

    def get_reversed_snapshot(self) -> SegmentSnapshot:
        """Returns a snapshot ordered from newest to oldest."""
        with self._lock:
            # reversed() is an O(1) operation that returns an iterator.
            # SegmentSnapshot consumes it instantly.
            return SegmentSnapshot(reversed(self.segments))

    def get_snapshot(self) -> SegmentSnapshot:
        """Returns a snapshot ordered from oldest to newest."""
        with self._lock:
            # Drop the list() cast. Just pass the raw iterable.
            return SegmentSnapshot(self.segments)

    def get_counts(self) -> tuple[int, int, int]:
        with self._lock:
            current_total = sum(seg.count for seg in self.segments)

            # FIX: Use the LATEST segment's capacity for the max estimate,
            # otherwise the 1MB probe segment will make your UI look
            # like the buffer is 100% full immediately.
            active_cap = self.active_segment.capacity if self.active_segment else self.segment_capacity
            max_total = self.max_pieces * active_cap
            return current_total, max_total, int(self.sequence)

    def release_all(self):
        """Gracefully shutdown and return all memory to the global pool."""
        with self._lock:
            while self.segments:
                seg = self.segments.popleft()
                seg.release()

    def batch_append(self, batch: "PooledLogBatch"):
        if batch.size == 0:
            return

        with self._lock:
            rows_written = 0

            # print(f"[log_pool] batch={batch}")
            # for ts, msg, lvl, mod, dev, seq in batch:
            #     print(f"ts={ts} lvl={lvl} mod={mod} dev={dev} seq={seq} msg={msg}")

            while rows_written < batch.size:
                # Fast Path: Numba handles the bulk
                copied = self.active_segment.insert_batch_chunk(batch, rows_written, self.sequence)

                # self.active_segment.debug_print()

                rows_written += copied
                self.sequence += copied

                # Slow Path: Rotation or Truncation
                if rows_written < batch.size:
                    next_msg_len = batch.lengths[rows_written]

                    # Logic: If it can't fit in a segment OR it's just objectively
                    # huge (e.g., > 1MB), we treat it as toxic and truncate to 512.
                    toxic_threshold = self.buffer_mb * 1024 * 1024

                    if next_msg_len > toxic_threshold or next_msg_len > 1024 * 1024:
                        self._rotate_segment()

                        ts, raw_msg, _, mod, dev, _ = batch[rows_written]

                        self.sequence += 1
                        self.active_segment.insert_truncated_error(ts, mod, dev, self.sequence, raw_msg)

                        rows_written += 1
                        self.sequence += 1

                        # if self.logger:
                        #     self.logger.error(f"Toxic log detected ({next_msg_len} bytes). Truncated to 512 chars.")
                    else:
                        # Normal rotation for a normal-sized log
                        self._rotate_segment()

    def clear(self):
        """
        Wipes all log data, resets sequence counters, and prepares
        the pool for fresh data. Useful for removing 'warm-up' dummy data.
        """
        with self._lock:
            # 1. Release all currently held segments back to the global array pool
            # This is safer than just zeroing indices because it ensures
            # no "residue" remains in the memory slabs.
            while self.segments:
                seg = self.segments.popleft()
                seg.release()

            # 2. Reset global counters
            self.segment_counter = 0
            self.sequence = SEQ_NONE
            self.active_segment = None

            # 3. Re-initialize with a single fresh segment
            self._rotate_segment()


#
# def query_pool(id_registry: IDRegistry, pool: CircularLogPool, target_modules: list[int] = None, **filters):
#     """Generator that yields populated LogRow objects from all segments."""
#
#     get_level = LogLevel.from_value
#     module_from_int = id_registry.module_from_int
#
#     # List to numpy for Numba compatibility
#     if target_modules:
#         tm_arr = np.array(target_modules, dtype=dtypes.ID_TYPE)
#     else:
#         tm_arr = np.empty(0, dtype=dtypes.ID_TYPE)
#
#     # Grab the sequence filter from kwargs
#     start_seq = filters.get("start_seq", SEQ_NONE)
#
#     with pool.get_snapshot() as segments:
#         for segment in segments:
#             if segment.count == 0:
#                 continue
#
#             b = segment.bundle()
#
#             matched_idx = filter_segment(
#                 b,
#                 target_modules_arr=tm_arr,
#                 start_seq=start_seq,  # Wire this in!
#                 start_ts=filters.get("start_ts", -1),
#                 end_ts=filters.get("end_ts", -1),
#                 target_level=filters.get("target_level", 0xFF),
#                 target_module=filters.get("target_module", 0xFFFF),
#                 target_device=filters.get("target_device", 0xFFFF),
#             )
#
#             for idx in matched_idx:
#                 # Use names from LogSegmentParams (the bundle)
#                 offset = b.offsets[idx]
#                 length = b.lengths[idx]
#                 msg = b.buffer[offset : offset + length].tobytes().decode("utf-8")
#
#                 yield LogRow(
#                     timestamp_ns=b.timestamps[idx],
#                     level=get_level(b.levels[idx]),
#                     module=module_from_int(b.modules[idx]),
#                     message=msg,
#                     seq=b.sequence_ids[idx],
#                 )


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
                if segment.count == 0 or segment_last_sequence_id <= start_seq:
                    break

                if new_watermark == start_seq:
                    new_watermark = segment_last_sequence_id

                curr_write_idx = extract_telemetry_segment_to_end(
                    segment.bundle(),
                    target_module_int,
                    start_seq,
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
    """
    Finds the latest sequence ID and calculates where the fetcher should start
    to fill exactly 'view_capacity' points.
    """
    # Ensure all inputs are strictly typed as uint64 to lock Numba signatures
    lks = dtypes.SEQ_TYPE(last_known_seq)
    cap = dtypes.SEQ_TYPE(view_capacity)

    with pool.get_reversed_snapshot() as segments:
        for segment in segments:
            if segment.count == 0:
                continue

            # Skip segment if it's strictly older than what we already have
            # (unless we're starting from scratch where lks == SEQ_NONE)
            if segment.last_sequence_id <= lks and lks != SEQ_NONE:
                break

            # peek_segment_channels_backwards now returns SEQ_NONE (0) instead of -1
            found_seq, channels = peek_segment_channels_backwards(segment.bundle(), target_module_int, lks, temp_floats)

            if channels > 0:
                # --- UNSIGNED UNDERFLOW PROTECTION ---
                # In uint64: 3000 - 5000 = 18 quintillion.
                # We must gate the subtraction.
                if found_seq > cap:
                    requested_start = found_seq - cap
                else:
                    requested_start = SEQ_NONE

                # Apply the high-water mark: never go back further than
                # the data we've already committed to the UI.
                optimized_start = max(lks, requested_start)

                return optimized_start, channels

    # If no new telemetry found, stay exactly where we are
    return lks, 0

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from threading import Lock

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry import IDRegistry
from blinkview.core.log_row import LogRow
from blinkview.core.numba_config import app_njit
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.segments import LogSegmentParams
from blinkview.ops.segments import copy_batch_to_segment, filter_segment
from blinkview.ops.telemetry import extract_floats_from_bytes, extract_telemetry_segment, peek_segment_channels
from blinkview.utils.log_level import LogLevel


class LogSegment:
    __slots__ = (
        "segment_seq",
        "capacity",
        "count",
        "_pool",
        # Sliced views (or full slabs)
        "timestamps",
        "levels",
        "modules",
        "devices",
        "sequence_ids",
        "msg_offsets",
        "msg_lens",
        "msg_buffer",
        "msg_cursor",
        # Pool Handles
        "_ts_h",
        "_lvl_h",
        "_mod_h",
        "_dev_h",
        "_seq_h",
        "_off_h",
        "_len_h",
        "_buf_h",
        "_bundle",
    )

    def __init__(self, pool, segment_seq: int, req_capacity: int, req_buffer_mb: int):
        self._pool = pool
        self.segment_seq = segment_seq
        self.count = 0
        self.msg_cursor = 0

        # Acquire arrays natively from the global pool using element count
        self._ts_h = self._pool.acquire(req_capacity, dtype=dtypes.TS_TYPE)
        self.timestamps = self._ts_h.array

        # Read true power-of-two capacity
        self.capacity = len(self.timestamps)

        self._lvl_h = self._pool.acquire(self.capacity, dtype=dtypes.LEVEL_TYPE)
        self.levels = self._lvl_h.array

        self._mod_h = self._pool.acquire(self.capacity, dtype=dtypes.ID_TYPE)
        self.modules = self._mod_h.array

        self._dev_h = self._pool.acquire(self.capacity, dtype=dtypes.ID_TYPE)
        self.devices = self._dev_h.array

        self._seq_h = self._pool.acquire(self.capacity, dtype=dtypes.SEQ_TYPE)
        self.sequence_ids = self._seq_h.array

        self._off_h = self._pool.acquire(self.capacity, dtype=dtypes.OFFSET_TYPE)
        self.msg_offsets = self._off_h.array

        self._len_h = self._pool.acquire(self.capacity, dtype=dtypes.LEN_TYPE)
        self.msg_lens = self._len_h.array

        # 1 MB = 1024 * 1024 bytes (elements for uint8)
        self._buf_h = self._pool.acquire(req_buffer_mb * 1024 * 1024, dtype=dtypes.BYTE)
        self.msg_buffer = self._buf_h.array

        self._bundle = None

    def bundle(self) -> LogSegmentParams:
        """Returns a baked snapshot. Re-baked only if the row count has changed."""
        if self._bundle is None:
            self._bundle = LogSegmentParams(
                self.timestamps,
                self.levels,
                self.modules,
                self.devices,
                self.sequence_ids,
                self.msg_offsets,
                self.msg_lens,
                self.msg_buffer,
                self.count,
                self.capacity,
            )
        return self._bundle

    def invalidate(self):
        """Forces a re-bake of the bundle on the next access."""
        self._bundle = None

    def clear_and_recycle(self, new_segment_seq: int):
        """O(1) reset to reuse the existing memory slabs for the next rotation."""
        self.segment_seq = new_segment_seq
        self.count = 0
        self.msg_cursor = 0
        self.invalidate()

    def append(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes) -> bool:
        if self.count >= self.capacity:
            return False

        msg_len = len(msg_bytes)
        if self.msg_cursor + msg_len > len(self.msg_buffer):
            return False

        idx = self.count

        self.timestamps[idx] = ts_ns
        self.levels[idx] = level
        self.modules[idx] = module
        self.devices[idx] = device
        self.sequence_ids[idx] = seq

        self.msg_offsets[idx] = self.msg_cursor
        self.msg_lens[idx] = msg_len

        self.msg_buffer[self.msg_cursor : self.msg_cursor + msg_len] = np.frombuffer(msg_bytes, dtype=dtypes.BYTE)

        self.msg_cursor += msg_len
        self.count += 1

        self.invalidate()
        return True

    def release(self):
        """Returns arrays to the global pool if this Segment is permanently destroyed."""
        for handle_name in ("_ts_h", "_lvl_h", "_mod_h", "_dev_h", "_seq_h", "_off_h", "_len_h", "_buf_h"):
            h = getattr(self, handle_name)
            if h is not None:
                h.release()
                setattr(self, handle_name, None)

    def append_batch_chunk(self, batch: "PooledLogBatch", start_idx: int, start_seq_id: int) -> int:
        """
        Appends a chunk of the batch starting from `start_idx`.
        Assigns sequentially increasing IDs starting from `start_seq_id`.
        Returns the number of rows successfully appended.
        """
        seg_view = self.bundle()
        batch_view = batch.bundle()

        # 2. Call the "De-Souped" JIT Kernel
        # We pass self.msg_cursor explicitly as it is the only dynamic write-head
        rows_copied, bytes_copied = copy_batch_to_segment(
            seg_view, self.msg_cursor, batch_view, start_idx, start_seq_id
        )

        self.count += rows_copied
        self.msg_cursor += bytes_copied
        self.invalidate()
        return rows_copied

    def append_truncated_error(self, ts_ns: int, module: int, device: int, seq: int, msg_bytes: bytes):
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

        return self.append(ts_ns, LogLevel.ERROR.value, module, device, seq, truncated_msg)


class CircularLogPool:
    def __init__(self, global_pool, max_pieces: int, segment_capacity: int = 100_000, buffer_mb: int = 32):
        self._global_pool = global_pool
        self.max_pieces = max_pieces
        self.segment_capacity = segment_capacity
        self.buffer_mb = buffer_mb

        self.segments: deque[LogSegment] = deque()
        self.segment_counter = 0
        self.active_segment = None

        self.sequence = 0  # log item sequence number

        self._lock = Lock()

        self._rotate_segment()

    def _rotate_segment(self):
        """Creates a new segment OR recycles the oldest one."""
        if len(self.segments) == self.max_pieces:
            # 1. Pop the oldest segment
            recycled_segment = self.segments.popleft()

            # 2. Reset its counters and give it the new sequence ID
            recycled_segment.clear_and_recycle(self.segment_counter)

            # 3. Make it the active segment
            self.active_segment = recycled_segment
            self.segments.append(recycled_segment)
        else:
            # Startup phase: Allocate from the global pool until max_pieces is reached
            new_segment = self._global_pool.create(
                LogSegment, self.segment_counter, self.segment_capacity, self.buffer_mb
            )
            self.segments.append(new_segment)
            self.active_segment = new_segment

        self.segment_counter += 1

    def append(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes):
        with self._lock:
            success = self.active_segment.append(ts_ns, level, module, device, seq, msg_bytes)
            if not success:
                self._rotate_segment()
                self.active_segment.append(ts_ns, level, module, device, seq, msg_bytes)

    def get_ordered_segments(self) -> list[LogSegment]:
        with self._lock:
            return list(self.segments)

    def get_counts(self) -> tuple[int, int]:
        with self._lock:
            current_total = sum(seg.count for seg in self.segments)
            # Use actual power-of-two capacity from the segments
            actual_capacity = self.segments[0].capacity if self.segments else self.segment_capacity
            max_total = self.max_pieces * actual_capacity
            return current_total, max_total

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

            while rows_written < batch.size:
                # Fast Path: Numba handles the bulk
                copied = self.active_segment.append_batch_chunk(batch, rows_written, self.sequence)

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

                        # Grab the metadata using your shiny new __getitem__
                        ts, raw_msg, _, mod, dev, _ = batch[rows_written]

                        # This now enforces your 512 limit
                        self.active_segment.append_truncated_error(ts, mod, dev, self.sequence, raw_msg)

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
            self.sequence = 0
            self.active_segment = None

            # 3. Re-initialize with a single fresh segment
            self._rotate_segment()


def query_pool(id_registry: IDRegistry, pool: CircularLogPool, target_modules: list[int] = None, **filters):
    """Generator that yields populated LogRow objects from all segments."""

    get_level = LogLevel.from_value
    module_from_int = id_registry.module_from_int

    # List to numpy for Numba compatibility
    if target_modules:
        tm_arr = np.array(target_modules, dtype=dtypes.ID_TYPE)
    else:
        tm_arr = np.empty(0, dtype=dtypes.ID_TYPE)

    # Grab the sequence filter from kwargs
    start_seq = filters.get("start_seq", -1)

    for segment in pool.get_ordered_segments():
        if segment.count == 0:
            continue

        matched_idx = filter_segment(
            segment.bundle(),
            target_modules_arr=tm_arr,
            start_seq=start_seq,  # Wire this in!
            start_ts=filters.get("start_ts", -1),
            end_ts=filters.get("end_ts", -1),
            target_level=filters.get("target_level", 0xFF),
            target_module=filters.get("target_module", 0xFFFF),
            target_device=filters.get("target_device", 0xFFFF),
        )

        for idx in matched_idx:
            offset = segment.msg_offsets[idx]
            length = segment.msg_lens[idx]
            msg = segment.msg_buffer[offset : offset + length].tobytes().decode("utf-8")

            yield LogRow(
                timestamp_ns=segment.timestamps[idx],
                level=get_level(segment.levels[idx]),
                module=module_from_int(segment.modules[idx]),
                message=msg,
                seq=segment.sequence_ids[idx],
            )


def fetch_telemetry_arrays(pool: "CircularLogPool", target_module_int: int, start_seq: int, num_channels: int):
    """
    Generator used by the plotter to grab batches of natively formatted NumPy data.
    """
    for segment in pool.get_ordered_segments():
        if segment.count == 0:
            continue

        # Fast exit: Skip entire segment if we've already processed its newest log
        if segment.sequence_ids[segment.count - 1] <= start_seq:
            continue

        # Execute the C-speed extraction
        new_times, new_values, max_seq = extract_telemetry_segment(
            segment.bundle(),
            target_module_int,
            start_seq,
            num_channels,
        )

        if len(new_times) > 0:
            yield new_times, new_values, max_seq


def peek_channel_count(pool: "CircularLogPool", target_module_int: int, start_seq: int) -> int:
    """Returns the channel count for the first numeric log after start_seq."""
    for segment in pool.get_ordered_segments():
        if segment.count == 0:
            continue

        if segment.sequence_ids[segment.count - 1] <= start_seq:
            continue

        channels = peek_segment_channels(
            segment.bundle(),
            target_module_int,
            start_seq,
        )

        if channels > 0:
            return channels

    return 0

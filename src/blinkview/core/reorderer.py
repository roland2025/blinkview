# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import defaultdict, deque
from typing import NamedTuple

import numpy as np
from numba.typed import List as NumbaList

from blinkview.core import dtypes
from blinkview.core.base_reorder import BaseReorder, ReorderFactory
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.log_row import LogRow
from blinkview.core.numba_config import app_njit
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.log_batch import LogBundle
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

# =========================================================================
# NUMBA-COMPATIBLE DATA STRUCTURES
# =========================================================================


class QueuedBatch(NamedTuple):
    """Lightweight wrapper with an array-boxed cursor for mutability."""

    batch: object
    cursor: np.ndarray  # np.zeros(1, dtype=np.uint32)


class MergeChunk(NamedTuple):
    """Strictly typed instruction for the Numba merge kernel."""

    bundle: LogBundle
    start: int
    end: int


# =========================================================================
# PURE JIT KERNELS (Bypassing NumPy Python API)
# =========================================================================
@app_njit()
def _hybrid_merge_and_copy(chunks, ts_scr, b_idx_scr, r_idx_scr, sort_order, out_bundle):
    """
    1. Flattens data to bypass Numba object refcount overhead.
    2. Performs an O(N * K) k-way merge directly on the flat arrays.
    3. Copies chronologically.
    """
    k = len(chunks)
    cursor = 0

    cursors = np.zeros(k, dtype=np.uint32)
    ends = np.zeros(k, dtype=np.uint32)

    # 1. FILL SCRATCHPADS (Flattens the data into primitive 1D arrays)
    for i in range(k):
        chunk = chunks[i]
        bundle = chunk.bundle
        s = chunk.start
        e = chunk.end

        cursors[i] = cursor

        for j in range(s, e):
            ts_scr[cursor] = bundle.timestamps[j]
            b_idx_scr[cursor] = i
            r_idx_scr[cursor] = j
            cursor += 1

        ends[i] = cursor

    num_rows = cursor

    # 2. TINY-K MERGE INTO SORT_ORDER
    # (Replaces np.argsort with a fast linear scan over block heads)
    for out_i in range(num_rows):
        best_k = -1
        min_ts = ts_scr[0]  # Dummy init for Numba static type inference

        for i in range(k):
            c = cursors[i]
            if c < ends[i]:
                ts = ts_scr[c]
                if best_k == -1 or ts < min_ts:
                    min_ts = ts
                    best_k = i

        sort_order[out_i] = cursors[best_k]
        cursors[best_k] += 1

    # 3. COPY TO OUTPUT BUNDLE (Pure sequential memory copy)
    out_idx = out_bundle.size[0]
    out_msg_cursor = out_bundle.msg_cursor[0]

    for i in range(num_rows):
        idx = sort_order[i]
        b_id = b_idx_scr[idx]
        r_id = r_idx_scr[idx]

        src_bundle = chunks[b_id].bundle

        # Copy mandatory columns
        out_bundle.timestamps[out_idx] = src_bundle.timestamps[r_id]
        src_off = src_bundle.offsets[r_id]
        src_len = src_bundle.lengths[r_id]

        out_bundle.offsets[out_idx] = out_msg_cursor
        out_bundle.lengths[out_idx] = src_len

        # Raw Memory Copy for Bytes
        for b in range(src_len):
            out_bundle.buffer[out_msg_cursor + b] = src_bundle.buffer[src_off + b]
        out_msg_cursor += src_len

        # Copy optional columns
        if out_bundle.has_levels and src_bundle.has_levels:
            out_bundle.levels[out_idx] = src_bundle.levels[r_id]
        if out_bundle.has_modules and src_bundle.has_modules:
            out_bundle.modules[out_idx] = src_bundle.modules[r_id]
        if out_bundle.has_devices and src_bundle.has_devices:
            out_bundle.devices[out_idx] = src_bundle.devices[r_id]
        if out_bundle.has_sequences and src_bundle.has_sequences:
            out_bundle.sequences[out_idx] = src_bundle.sequences[r_id]

        out_idx += 1

    # Write back the new sizes to the 1D arrays
    out_bundle.size[0] = out_idx
    out_bundle.msg_cursor[0] = out_msg_cursor


@app_njit()
def _linear_merge_and_copy(chunks, out_bundle):
    """
    MEGA-KERNEL: O(N * K) direct linear scan merge.
    For K <= 10, this brutally outperforms heaps and argsort by maintaining
    perfect memory cache locality and avoiding all array allocations.
    """
    k = len(chunks)

    # Pre-extract cursors and limits into local Numba arrays
    # to avoid repeated object overhead in the hot loop
    cursors = np.zeros(k, dtype=np.uint32)
    ends = np.zeros(k, dtype=np.uint32)
    for i in range(k):
        cursors[i] = chunks[i].start
        ends[i] = chunks[i].end

    out_idx = out_bundle.size[0]
    out_msg_cursor = out_bundle.msg_cursor[0]

    # Dummy initialization so Numba can statically infer the type of min_ts
    min_ts = chunks[0].bundle.timestamps[0]

    while True:
        best_b = -1

        # The Tiny-K Loop: Numba unrolls this beautifully.
        for i in range(k):
            c = cursors[i]
            if c < ends[i]:
                ts = chunks[i].bundle.timestamps[c]
                if best_b == -1 or ts < min_ts:
                    min_ts = ts
                    best_b = i

        # If no chunk has data left, we are done
        if best_b == -1:
            break

        # --- COPY DATA ---
        src_bundle = chunks[best_b].bundle
        r_id = cursors[best_b]

        # Mandatory columns
        out_bundle.timestamps[out_idx] = src_bundle.timestamps[r_id]
        src_off = src_bundle.offsets[r_id]
        src_len = src_bundle.lengths[r_id]

        out_bundle.offsets[out_idx] = out_msg_cursor
        out_bundle.lengths[out_idx] = src_len

        # Raw Memory Copy for Bytes
        for b in range(src_len):
            out_bundle.buffer[out_msg_cursor + b] = src_bundle.buffer[src_off + b]

        out_msg_cursor += src_len

        # Optional columns
        if out_bundle.has_levels and src_bundle.has_levels:
            out_bundle.levels[out_idx] = src_bundle.levels[r_id]
        if out_bundle.has_modules and src_bundle.has_modules:
            out_bundle.modules[out_idx] = src_bundle.modules[r_id]
        if out_bundle.has_devices and src_bundle.has_devices:
            out_bundle.devices[out_idx] = src_bundle.devices[r_id]
        if out_bundle.has_sequences and src_bundle.has_sequences:
            out_bundle.sequences[out_idx] = src_bundle.sequences[r_id]

        # Advance cursors
        out_idx += 1
        cursors[best_b] += 1

    # Write back the new sizes to the 1D arrays
    out_bundle.size[0] = out_idx
    out_bundle.msg_cursor[0] = out_msg_cursor


@app_njit()
def _find_split_idx(timestamps, cursor, size, safe_ts):
    """Zero-allocation binary search replacing np.searchsorted."""
    left = cursor
    right = size
    while left < right:
        mid = (left + right) // 2
        if timestamps[mid] <= safe_ts:
            left = mid + 1
        else:
            right = mid
    return left - cursor


@app_njit()
def _sum_lengths(lengths, start, end):
    """Zero-allocation sum replacing np.sum(slice)."""
    total = 0
    for i in range(start, end):
        total += lengths[i]
    return total


@app_njit()
def _merge_and_copy(chunks, ts_scr, b_idx_scr, r_idx_scr, out_bundle):
    """
    MEGA-KERNEL: Fills scratchpads, executes argsort internally,
    and copies raw columnar data into the final output bundle.
    """
    # 1. FILL SCRATCHPADS
    cursor = 0
    for i in range(len(chunks)):
        chunk = chunks[i]
        bundle = chunk.bundle
        s = chunk.start
        e = chunk.end

        for j in range(s, e):
            ts_scr[cursor] = bundle.timestamps[j]
            b_idx_scr[cursor] = i
            r_idx_scr[cursor] = j
            cursor += 1

    # 2. SORT CHRONOLOGICALLY
    # Numba natively supports np.argsort and compiles it down to C
    sort_order = np.argsort(ts_scr, kind="mergesort")

    # 3. COPY TO OUTPUT BUNDLE
    num_rows = len(sort_order)
    out_idx = out_bundle.size[0]
    out_msg_cursor = out_bundle.msg_cursor[0]

    for i in range(num_rows):
        idx = sort_order[i]
        b_id = b_idx_scr[idx]
        r_id = r_idx_scr[idx]

        src_bundle = chunks[b_id].bundle

        # Copy mandatory columns
        out_bundle.timestamps[out_idx] = src_bundle.timestamps[r_id]
        src_off = src_bundle.offsets[r_id]
        src_len = src_bundle.lengths[r_id]

        out_bundle.offsets[out_idx] = out_msg_cursor
        out_bundle.lengths[out_idx] = src_len

        # Raw Memory Copy for Bytes
        for b in range(src_len):
            out_bundle.buffer[out_msg_cursor + b] = src_bundle.buffer[src_off + b]
        # out_bundle.buffer[out_msg_cursor : out_msg_cursor + src_len] = src_bundle.buffer[src_off : src_off + src_len]

        out_msg_cursor += src_len

        # Copy optional columns
        if out_bundle.has_levels and src_bundle.has_levels:
            out_bundle.levels[out_idx] = src_bundle.levels[r_id]
        if out_bundle.has_modules and src_bundle.has_modules:
            out_bundle.modules[out_idx] = src_bundle.modules[r_id]
        if out_bundle.has_devices and src_bundle.has_devices:
            out_bundle.devices[out_idx] = src_bundle.devices[r_id]
        if out_bundle.has_sequences and src_bundle.has_sequences:
            out_bundle.sequences[out_idx] = src_bundle.sequences[r_id]

        out_idx += 1

    # Write back the new sizes to the 1D arrays
    out_bundle.size[0] = out_idx
    out_bundle.msg_cursor[0] = out_msg_cursor


@ReorderFactory.register("default")
class Reorder(BaseReorder):
    def __init__(self):
        super().__init__()
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put
        self.numba_needs_compile = True

    def run_(self):
        pool = self.shared.array_pool
        pool_create = pool.create

        time_ns = self.shared.time_ns
        delay_ns = self.delay * 1_000_000

        distribute = self.distribute
        get = self.input_queue.get
        get_nowait = self.input_queue.get_nowait

        batch_out = None
        speed_out = Speedometer(logger=self.logger.child("stats_out"))
        tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

        device_queues = defaultdict(deque)

        def flush():
            nonlocal batch_out
            if batch_out is not None and batch_out.size > 0:
                with batch_out:
                    tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=0.1)
                    distribute(batch_out)
            batch_out = None

        stop_is_set = self._stop_event.is_set

        # [FIX 1] Hoist the NumbaList outside the loop to prevent GC churn
        ready_chunks = NumbaList()

        try:
            # =================================================================
            # --- [START] WARM UP THE REORDER NUMBA KERNEL ---
            # =================================================================
            if self.numba_needs_compile:
                try:
                    self.logger.info("Warming up Reorder kernel...")
                    with (
                        pool_create(
                            PooledLogBatch, 10, 1, has_levels=True, has_modules=True, has_devices=True
                        ) as dummy_in,
                        pool_create(
                            PooledLogBatch, 10, 1, has_levels=True, has_modules=True, has_devices=True
                        ) as dummy_out,
                    ):
                        dummy_in.insert(time_ns(), b"warmup", level=0, module=0, device=0)

                        warmup_chunks = NumbaList()
                        warmup_chunks.append(MergeChunk(dummy_in.bundle(), 0, 1))

                        w_ts_scr = np.zeros(1, dtype=dtypes.TS_TYPE)
                        w_b_idx_scr = np.zeros(1, dtype=np.uint32)
                        w_r_idx_scr = np.zeros(1, dtype=np.uint32)

                        # --- Explicitly warm up the helper functions ---
                        _find_split_idx(dummy_in.timestamps, 0, dummy_in.size, time_ns())
                        _sum_lengths(dummy_in.lengths, 0, 1)

                        _merge_and_copy(warmup_chunks, w_ts_scr, w_b_idx_scr, w_r_idx_scr, dummy_out.bundle())

                    self.logger.info("Reorder kernel warmed up and cached.")
                except Exception as e:
                    self.logger.exception("Failed to warm up Reorder kernel", e)

                self.numba_needs_compile = False
            # =================================================================
            # --- [END] WARM UP ---
            # =================================================================

            while not stop_is_set():
                now = time_ns()

                # 1. Drain input queue
                first_batch = get(timeout=0.015)
                if first_batch is None:
                    # Optional: Uncomment below to flush stale data during idle periods
                    # flush()
                    continue

                batches_to_ingest = [first_batch]
                while True:
                    b = get_nowait()
                    if b is None:
                        break
                    batches_to_ingest.append(b)

                for b in batches_to_ingest:
                    dev_id = b.devices[0] if b.has_devices and b.size > 0 else 0
                    device_queues[dev_id].append(QueuedBatch(b, np.zeros(1, dtype=np.uint32)))

                # 2. Determine "Ready" Chunks
                safe_ts = now - delay_ns
                if len(ready_chunks) > 0:
                    ready_chunks.clear()
                total_ready_rows = 0
                total_ready_bytes = 0
                batches_to_release = []

                try:
                    for dev_id, queue in list(device_queues.items()):
                        while queue:
                            qb = queue[0]
                            batch = qb.batch
                            cursor = int(qb.cursor[0])

                            if cursor >= batch.size:
                                batches_to_release.append(queue.popleft().batch)
                                continue

                            idx = int(_find_split_idx(batch.timestamps, cursor, batch.size, safe_ts))

                            if idx > 0:
                                s = cursor
                                e = cursor + idx

                                ready_chunks.append(MergeChunk(batch.bundle(), s, e))
                                total_ready_rows += idx
                                total_ready_bytes += int(_sum_lengths(batch.lengths, s, e))

                                qb.cursor[0] = e
                                if qb.cursor[0] == batch.size:
                                    batches_to_release.append(queue.popleft().batch)
                            else:
                                break

                    # 3. K-Way Merge & Flush
                    if total_ready_rows > 0:
                        if (
                            batch_out is None
                            or batch_out.size + total_ready_rows > batch_out.capacity
                            or batch_out.msg_cursor + total_ready_bytes > len(batch_out.buffer)
                        ):
                            flush()
                            cap = max(tuner_out.estimated_capacity, total_ready_rows)
                            buf_kb = max(tuner_out.estimated_buffer_kb, (total_ready_bytes // 1024) + 1)
                            batch_out = pool_create(
                                PooledLogBatch, cap, buf_kb, has_levels=True, has_modules=True, has_devices=True
                            )

                        h_ts = None
                        h_b_idx = None
                        h_r_idx = None

                        try:
                            h_ts = pool.acquire(total_ready_rows, dtype=dtypes.TS_TYPE)
                            h_b_idx = pool.acquire(total_ready_rows, dtype=np.uint32)
                            h_r_idx = pool.acquire(total_ready_rows, dtype=np.uint32)

                            ts_scr = h_ts.array[:total_ready_rows]
                            b_idx_scr = h_b_idx.array[:total_ready_rows]
                            r_idx_scr = h_r_idx.array[:total_ready_rows]

                            _merge_and_copy(ready_chunks, ts_scr, b_idx_scr, r_idx_scr, batch_out.bundle())

                            # [FIX 4 - Logic Note] If you want true batching, remove the flush() line below
                            # and rely on the capacity check above and the idle flush in the get() timeout block.
                            flush()

                        finally:
                            if h_ts is not None:
                                h_ts.release()
                            if h_b_idx is not None:
                                h_b_idx.release()
                            if h_r_idx is not None:
                                h_r_idx.release()

                except Exception as e:
                    self.logger.exception("Error during reorder merge", e)

                finally:
                    # Guaranteed memory return for incoming batches
                    for b in batches_to_release:
                        b.release()

        except Exception as e:
            self.logger.exception("run failure", e)
        finally:
            self.numba_needs_compile = False

            # [FIX 3] Prevent active batch_out from leaking on shutdown/thread crash
            if batch_out is not None:
                try:
                    batch_out.release()
                except Exception:
                    pass

            for queue in device_queues.values():
                while queue:
                    queue.popleft().batch.release()

    def run(self):
        pool = self.shared.array_pool
        pool_create = pool.create

        time_ns = self.shared.time_ns
        delay_ns = self.delay * 1_000_000

        distribute = self.distribute
        get = self.input_queue.get
        get_nowait = self.input_queue.get_nowait

        batch_out = None
        speed_out = Speedometer(logger=self.logger.child("stats_out"))
        tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

        device_queues = defaultdict(deque)

        def flush():
            nonlocal batch_out
            if batch_out is not None and batch_out.size > 0:
                with batch_out:
                    tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=0.1)
                    distribute(batch_out)
            batch_out = None

        stop_is_set = self._stop_event.is_set

        # Hoist the NumbaList outside the loop to prevent GC churn
        ready_chunks = NumbaList()

        try:
            # =================================================================
            # --- [START] WARM UP THE REORDER NUMBA KERNEL ---
            # =================================================================
            if self.numba_needs_compile:
                try:
                    self.logger.info("Warming up Reorder kernel...")
                    with (
                        pool_create(
                            PooledLogBatch, 10, 1, has_levels=True, has_modules=True, has_devices=True
                        ) as dummy_in,
                        pool_create(
                            PooledLogBatch, 10, 1, has_levels=True, has_modules=True, has_devices=True
                        ) as dummy_out,
                    ):
                        dummy_in.insert(time_ns(), b"warmup", level=0, module=0, device=0)

                        warmup_chunks = NumbaList()
                        warmup_chunks.append(MergeChunk(dummy_in.bundle(), 0, 1))

                        # --- Explicitly warm up the helper functions ---
                        _find_split_idx(dummy_in.timestamps, 0, dummy_in.size, time_ns())
                        _sum_lengths(dummy_in.lengths, 0, 1)

                        # Warm up the new linear scan kernel
                        _linear_merge_and_copy(warmup_chunks, dummy_out.bundle())

                    self.logger.info("Reorder kernel warmed up and cached.")
                except Exception as e:
                    self.logger.exception("Failed to warm up Reorder kernel", e)

                self.numba_needs_compile = False
            # =================================================================
            # --- [END] WARM UP ---
            # =================================================================

            while not stop_is_set():
                now = time_ns()

                # 1. Drain input queue
                first_batch = get(timeout=0.015)
                if first_batch is None:
                    # Optional: Uncomment below to flush stale data during idle periods
                    # flush()
                    continue

                batches_to_ingest = [first_batch]
                while True:
                    b = get_nowait()
                    if b is None:
                        break
                    batches_to_ingest.append(b)

                for b in batches_to_ingest:
                    dev_id = b.devices[0] if b.has_devices and b.size > 0 else 0
                    device_queues[dev_id].append(QueuedBatch(b, np.zeros(1, dtype=np.uint32)))

                # 2. Determine "Ready" Chunks
                safe_ts = now - delay_ns
                if len(ready_chunks) > 0:
                    ready_chunks.clear()
                total_ready_rows = 0
                total_ready_bytes = 0
                batches_to_release = []

                try:
                    for dev_id, queue in list(device_queues.items()):
                        while queue:
                            qb = queue[0]
                            batch = qb.batch
                            cursor = int(qb.cursor[0])

                            if cursor >= batch.size:
                                batches_to_release.append(queue.popleft().batch)
                                continue

                            idx = int(_find_split_idx(batch.timestamps, cursor, batch.size, safe_ts))

                            if idx > 0:
                                s = cursor
                                e = cursor + idx

                                ready_chunks.append(MergeChunk(batch.bundle(), s, e))
                                total_ready_rows += idx
                                total_ready_bytes += int(_sum_lengths(batch.lengths, s, e))

                                qb.cursor[0] = e
                                if qb.cursor[0] == batch.size:
                                    batches_to_release.append(queue.popleft().batch)
                            else:
                                break

                    # 3. K-Way Merge & Flush
                    if total_ready_rows > 0:
                        if (
                            batch_out is None
                            or batch_out.size + total_ready_rows > batch_out.capacity
                            or batch_out.msg_cursor + total_ready_bytes > len(batch_out.buffer)
                        ):
                            flush()
                            cap = max(tuner_out.estimated_capacity, total_ready_rows)
                            buf_kb = max(tuner_out.estimated_buffer_kb, (total_ready_bytes // 1024) + 1)
                            batch_out = pool_create(
                                PooledLogBatch, cap, buf_kb, has_levels=True, has_modules=True, has_devices=True
                            )

                        h_ts = None
                        h_b_idx = None
                        h_r_idx = None
                        h_sort = None

                        try:
                            # Reinstating the O(N) scratchpads to bypass Numba unboxing overhead
                            h_ts = pool.acquire(total_ready_rows, dtype=dtypes.TS_TYPE)
                            h_b_idx = pool.acquire(total_ready_rows, dtype=np.uint32)
                            h_r_idx = pool.acquire(total_ready_rows, dtype=np.uint32)
                            h_sort = pool.acquire(total_ready_rows, dtype=np.uint32)

                            ts_scr = h_ts.array[:total_ready_rows]
                            b_idx_scr = h_b_idx.array[:total_ready_rows]
                            r_idx_scr = h_r_idx.array[:total_ready_rows]
                            sort_order = h_sort.array[:total_ready_rows]

                            _hybrid_merge_and_copy(
                                ready_chunks, ts_scr, b_idx_scr, r_idx_scr, sort_order, batch_out.bundle()
                            )

                            flush()

                        finally:
                            if h_ts is not None:
                                h_ts.release()
                            if h_b_idx is not None:
                                h_b_idx.release()
                            if h_r_idx is not None:
                                h_r_idx.release()
                            if h_sort is not None:
                                h_sort.release()

                except Exception as e:
                    self.logger.exception("Error during reorder merge", e)

                finally:
                    # Guaranteed memory return for incoming batches
                    for b in batches_to_release:
                        b.release()

        except Exception as e:
            self.logger.exception("run failure", e)
        finally:
            self.numba_needs_compile = False

            # Prevent active batch_out from leaking on shutdown/thread crash
            if batch_out is not None:
                try:
                    batch_out.release()
                except Exception:
                    pass

            for queue in device_queues.values():
                while queue:
                    queue.popleft().batch.release()

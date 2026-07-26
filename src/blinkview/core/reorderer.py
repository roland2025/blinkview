# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import defaultdict, deque
from typing import NamedTuple, Optional

import numpy as np
from numba.typed import List as NumbaList

from blinkview.core import dtypes
from blinkview.core.base_reorder import BaseReorder, ReorderFactory
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.reorder import MergeChunk, nb_find_split_idx, nb_hybrid_merge_and_copy, nb_sum_lengths
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

# =========================================================================
# NUMBA-COMPATIBLE DATA STRUCTURES
# =========================================================================


class QueuedBatch(NamedTuple):
    """Lightweight wrapper with an array-boxed cursor for mutability."""

    batch: object
    cursor: np.ndarray  # np.zeros(1, dtype=np.uint32)


@ReorderFactory.register("default")
class Reorder(BaseReorder):
    def __init__(self):
        super().__init__()
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put

    def run(self):
        pool = self.shared.array_pool
        pool_create = pool.create

        time_ns = self.shared.time_ns
        delay_ns = self.delay * 1_000_000
        delay_sec = self.delay / 1000 / 2

        distribute = self.distribute
        get = self.input_queue.get
        get_nowait = self.input_queue.get_nowait

        batch_out: Optional[PooledLogBatch] = None
        speed_out = Speedometer(logger=self.logger.child("stats_out"))
        tuner_out = ThroughputAutoTuner(speed_out, logger=self.logger.child("tuner_out"))

        # logger_backlog = self.logger.child("backlog")

        device_queues = defaultdict(deque)

        def flush():
            nonlocal batch_out
            if batch_out is not None and batch_out.size > 0:
                with batch_out:
                    tuner_out.update(batch_out.msg_cursor, batch_out.size, target_window_sec=0.1)
                    distribute(batch_out)
            batch_out = None

            held_batches = 0
            held_messages = 0
            for _q in device_queues.values():
                held_batches += len(_q)
                for _qb in _q:
                    # Calculate how many messages in this batch haven't been merged yet
                    held_messages += _qb.batch.size - int(_qb.cursor[0])

            # Use debug or info depending on how verbose you want your logs
            # logger_backlog.debug(f"batches={held_batches} msgs={held_messages}")

        stop_is_set = self._stop_event.is_set

        # Hoist the NumbaList outside the loop to prevent GC churn
        ready_chunks = NumbaList()

        try:
            while not stop_is_set():
                # 1. Drain input queue
                now_pre_get = time_ns()

                # --- Calculate Dynamic Timeout ---
                dynamic_timeout_sec = delay_sec
                if device_queues:
                    oldest_ts = -1
                    for q in device_queues.values():
                        if q:
                            qb = q[0]
                            cursor = int(qb.cursor[0])
                            # Ensure we don't read past the batch size
                            if cursor < qb.batch.size:
                                ts = qb.batch.bundle.timestamps[cursor]
                                if oldest_ts == -1 or ts < oldest_ts:
                                    oldest_ts = ts

                    if oldest_ts != -1:
                        # How much time until the oldest message is ready to be released?
                        time_to_ready_ns = (oldest_ts + delay_ns) - now_pre_get
                        time_to_ready_sec = time_to_ready_ns / 1_000_000_000.0

                        # Clamp between 0 (already overdue) and our max delay_sec
                        dynamic_timeout_sec = max(0.0, min(delay_sec, time_to_ready_sec))

                # prevent massive thread context switching
                if dynamic_timeout_sec < 0.02:
                    dynamic_timeout_sec = 0.02

                # 1. Drain input queue (Blocking precisely until the next item is due)
                first_batch = get(timeout=dynamic_timeout_sec)

                batches_to_ingest = []
                if first_batch is not None:
                    batches_to_ingest.append(first_batch)
                    while True:
                        b = get_nowait()
                        if b is None:
                            break
                        batches_to_ingest.append(b)

                for b in batches_to_ingest:
                    device_queues[b.get_device()].append(QueuedBatch(b, np.zeros(1, dtype=np.uint32)))

                # 2. Determine "Ready" Chunks

                now = time_ns()
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
                            batch_bundle = batch.bundle
                            idx = int(nb_find_split_idx(batch_bundle.timestamps, cursor, batch.size, safe_ts))

                            if idx > 0:
                                s = cursor
                                e = cursor + idx

                                ready_chunks.append(MergeChunk(batch_bundle, s, e))
                                total_ready_rows += idx
                                total_ready_bytes += int(nb_sum_lengths(batch_bundle.lengths, s, e))

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
                            buf_bytes = max(tuner_out.estimated_buffer_bytes, total_ready_bytes)
                            batch_out = pool_create(
                                PooledLogBatch,
                                cap,
                                buf_bytes,
                                has_levels=True,
                                has_modules=True,
                                has_devices=True,
                                has_pids=True,
                                has_tids=True,
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

                            nb_hybrid_merge_and_copy(
                                ready_chunks, ts_scr, b_idx_scr, r_idx_scr, sort_order, batch_out.bundle
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
            # Prevent active batch_out from leaking on shutdown/thread crash
            if batch_out is not None:
                try:
                    batch_out.release()
                except Exception:
                    pass

            for queue in device_queues.values():
                while queue:
                    queue.popleft().batch.release()

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for the Reorder kernel (Hybrid Merge & Copy, plus the
        nb_find_split_idx/nb_sum_lengths helpers) against small dummy scratchpads."""
        print("[Warmup] Reorder ...")

        pool_create = helper.array_pool.create
        time_ns = helper.time_ns

        with (
            pool_create(
                PooledLogBatch,
                10,
                1024,
                has_levels=True,
                has_modules=True,
                has_devices=True,
                has_pids=True,
                has_tids=True,
            ) as dummy_in,
            pool_create(
                PooledLogBatch,
                10,
                1024,
                has_levels=True,
                has_modules=True,
                has_devices=True,
                has_pids=True,
                has_tids=True,
            ) as dummy_out,
        ):
            ts = time_ns()
            dummy_in.insert(ts, ts, b"warmup", level=0, module=0, device=0)

            dummy_in_b = dummy_in.bundle

            warmup_chunks = NumbaList()
            warmup_chunks.append(MergeChunk(dummy_in_b, 0, 1))

            # Create small dummy scratchpads for the hybrid kernel warmup
            w_ts_scr = np.zeros(1, dtype=dtypes.TS_TYPE)
            w_b_idx_scr = np.zeros(1, dtype=np.uint32)
            w_r_idx_scr = np.zeros(1, dtype=np.uint32)
            w_sort_scr = np.zeros(1, dtype=np.uint32)

            # --- Explicitly warm up the helper functions ---
            nb_find_split_idx(dummy_in_b.timestamps, 0, dummy_in.size, time_ns())
            nb_sum_lengths(dummy_in_b.lengths, 0, 1)

            # Warm up the Hybrid Merge & Copy kernel
            nb_hybrid_merge_and_copy(warmup_chunks, w_ts_scr, w_b_idx_scr, w_r_idx_scr, w_sort_scr, dummy_out.bundle)

        print("[Warmup] Reorder ... done")

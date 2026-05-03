# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep

import numpy as np

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import configuration_property
from blinkview.core.constants import SysCat
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.log_row import LogRow
from blinkview.core.numba_config import app_njit
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.reusable_batch_pool import BatchPool, TimeDataEntry
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.log_filter import LogFilter


@app_njit()
def _blast_benchmark_cache(
    bundle,  # LogBundle (NamedTuple of Numpy arrays)
    start_ts,
    chunks,  # Generation params
    c_buf,
    c_offs,
    c_lens,
    c_items,  # Compiled cache arrays
):
    """Numba kernel to bulk-insert cached messages using LogBundle."""
    # Read current cursors from the 1-element arrays
    row_cursor = bundle.size[0]
    byte_cursor = bundle.msg_cursor[0]

    written_bytes = 0

    for i in range(chunks):
        idx = i % c_items

        # 1. Write Metadata
        bundle.timestamps[row_cursor] = start_ts + i
        bundle.offsets[row_cursor] = byte_cursor

        c_len = c_lens[idx]
        bundle.lengths[row_cursor] = c_len

        # 2. Fast byte copy into the contiguous buffer
        c_off = c_offs[idx]
        bundle.buffer[byte_cursor : byte_cursor + c_len] = c_buf[c_off : c_off + c_len]

        byte_cursor += c_len
        row_cursor += 1
        written_bytes += c_len

    # Update the batch's internal trackers directly via the array references!
    bundle.size[0] = row_cursor
    bundle.msg_cursor[0] = byte_cursor

    # We only need to return written_bytes for the global benchmark counters
    return written_bytes


@DeviceFactory.register("benchmark")
@configuration_property(
    "batch_size", type="integer", default=10000, description="Number of messages to generate in each batch"
)
@configuration_property(
    "max_backlog",
    type="integer",
    default=600000,
    description="Maximum allowed backlog (difference between sent and received messages) before pausing generation",
)
@configuration_property(
    "max_msg_per_sec",
    type="integer",
    default=0,
    description="Maximum messages to generate per second (0 means unlimited)",
)
@configuration_property("sources_", type="string", required=True, _reference="/targets", default="")
@configuration_property(
    "targets_",  # Consider renaming this to "source" (singular) if your backend allows it
    title="Target",
    type="string",
    required=True,
    _reference="/pipelines",
    default="",
)
# @configuration_property("device", description="Source device name", _reference="/targets", required=True)
class Benchmark(BaseReader):
    batch_size: int
    max_backlog: int
    max_msg_per_sec: int  # <-- New property

    __doc__ = """A synthetic log generator that produces random messages at a configurable rate.\nUseful for benchmarking and testing the system's throughput and backpressure handling."""

    def __init__(self):
        super().__init__()

        self.sources = [SysCat.STORAGE]

        self.input_queue = BatchQueue()
        self.put = self.input_queue.put

        self.numba_needs_compile = True

    def run(self):
        try:
            stop_is_set = self._stop_event.is_set
            time_ns = self.shared.time_ns
            logger = self.logger
            stats_logger = logger.child("stats")

            queue_get = self.input_queue.get
            pool_create = self.shared.array_pool.create

            device = self.shared.registry.get_reference_target(self.targets_).local.device_id
            target_device_id = device.id

            INTERVAL_NS = 30 * 1_000_000
            TICKS_PER_SEC = 1_000_000_000 / INTERVAL_NS

            # --- [ADAPTIVE PARAMETERS] ---
            current_msg_per_sec = float(self.batch_size * TICKS_PER_SEC)
            current_step = 0.20
            dampening = 0.75
            min_step = 0.001
            last_state = "PROBING"
            ema_backlog = 0.0
            alpha = 0.2
            adjustment_interval_ns = 3_000_000_000
            session_start_time = time_ns()
            last_adjustment_time = session_start_time
            effective_max_backlog = self.max_backlog or (self.batch_size * 20)

            # --- [PRE-COMPILED CACHE SETUP] ---
            rows_per_bytes = 50
            cache_base = [b"Random: %d\n" % i for i in range(1000)]
            grouped_cache = [
                b"".join(cache_base[i : i + rows_per_bytes]) for i in range(0, len(cache_base), rows_per_bytes)
            ]
            flat_bytes = b"".join(grouped_cache)
            c_buf = np.frombuffer(flat_bytes, dtype=np.uint8)
            c_lens = np.array([len(c) for c in grouped_cache], dtype=np.uint32)
            c_offs = np.zeros(len(c_lens), dtype=np.uint32)
            current_offset = 0
            for i in range(len(c_lens)):
                c_offs[i] = current_offset
                current_offset += c_lens[i]
            c_items = len(c_lens)

            # --- [START] NUMBA WARMUP ---
            if self.numba_needs_compile:
                try:
                    logger.info("Warming up benchmark JIT kernels...")
                    # Create a tiny dummy batch (1 chunk, 1KB buffer)
                    with pool_create(PooledLogBatch, 1, 1) as dummy_batch:
                        _ = _blast_benchmark_cache(dummy_batch.bundle, time_ns(), 1, c_buf, c_offs, c_lens, c_items)
                    logger.info("Benchmark kernels warmed up.")
                except Exception as e:
                    logger.exception("Failed to warm up benchmark kernels", e)
                self.numba_needs_compile = False
            # --- [END] NUMBA WARMUP ---

            total_sent_msgs = total_recv_msgs = 0
            interval_msgs = interval_bytes = session_total_bytes = 0
            next_send_tick = time_ns()

            logger.warn("Starting Damped Reactive Benchmarker | Target: 30ms Interval")

            while not stop_is_set():
                current_time = time_ns()

                # --- 1. Productive Wait & Drain ---
                while current_time < next_send_tick:
                    timeout = (next_send_tick - current_time) / 1_000_000_000.0
                    if timeout <= 0:
                        break
                    try:
                        batch = queue_get(timeout=timeout)
                        if batch is not None:
                            batch: PooledLogBatch
                            with batch:
                                b = batch.bundle
                                if batch.size > 0:
                                    if not b.has_devices:
                                        total_recv_msgs += batch.size
                                    else:
                                        total_recv_msgs += np.count_nonzero(b.devices[: batch.size] == target_device_id)
                    except Exception:
                        break
                    current_time = time_ns()

                # --- 2. Damped Equilibrium Logic ---
                in_flight = total_sent_msgs - total_recv_msgs
                ema_backlog = (alpha * in_flight) + ((1 - alpha) * ema_backlog)

                elapsed_ns = current_time - last_adjustment_time
                if elapsed_ns >= adjustment_interval_ns:
                    # Determine current state
                    if ema_backlog > (effective_max_backlog * 0.5):
                        state = "CONSOLIDATING"
                    else:
                        state = "PROBING"

                    # [REVERSAL LOGIC] If we switched direction, dampen the step size
                    if state != last_state:
                        current_step = max(min_step, current_step * dampening)
                        last_state = state

                    # Apply the adjustment
                    if state == "CONSOLIDATING":
                        # Be slightly more aggressive on downswings (step * 1.5)
                        # to clear the actual backlog faster.
                        current_msg_per_sec *= 1.0 - (current_step * 1.5)
                        log_fn = stats_logger.warn
                    else:
                        current_msg_per_sec *= 1.0 + current_step
                        log_fn = stats_logger.info

                    # Reporting
                    elapsed_sec = elapsed_ns / 1_000_000_000.0
                    interval_msg_rate = interval_msgs / elapsed_sec
                    interval_mb_s = (interval_bytes / 1_048_576) / elapsed_sec
                    session_elapsed_sec = (current_time - session_start_time) / 1_000_000_000.0
                    session_avg_mb_s = (session_total_bytes / 1_048_576) / session_elapsed_sec

                    log_fn(
                        f"rate={interval_msg_rate:.0f} "
                        f"throughput_mb_s={interval_mb_s:.2f} "
                        f"step_pct={current_step * 100:.1f} "
                        f"backlog={int(ema_backlog)} "
                        f"state={state}"
                    )

                    last_adjustment_time = current_time
                    interval_msgs = interval_bytes = 0

                # --- 3. Message Burst ---
                msgs_per_tick = int(current_msg_per_sec / TICKS_PER_SEC)
                if in_flight < effective_max_backlog:
                    chunks = max(1, msgs_per_tick // rows_per_bytes)
                    estimated_bytes = sum(c_lens[i % c_items] for i in range(chunks))

                    batch = pool_create(PooledLogBatch, chunks, estimated_bytes)
                    written_bytes = _blast_benchmark_cache(
                        batch.bundle, current_time, chunks, c_buf, c_offs, c_lens, c_items
                    )

                    interval_bytes += written_bytes
                    session_total_bytes += written_bytes
                    sent_count = chunks * rows_per_bytes
                    total_sent_msgs += sent_count
                    interval_msgs += sent_count

                    if batch.size > 0:
                        with batch:
                            self.distribute(batch)

                # --- 4. Advance Tick ---
                next_send_tick += INTERVAL_NS
                if (time_ns() - next_send_tick) > INTERVAL_NS:
                    next_send_tick = time_ns()

        except Exception as e:
            self.logger.exception("Exception in benchmark reader", e)

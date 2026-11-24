# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import configuration_property
from blinkview.core.constants import SysCat
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.io.benchmark_templates import TEMPLATES, build_cache
from blinkview.ops.benchmark import nb_blast_benchmark_cache


def warm_up_blast(pool_create, now_ns, c_buf, c_offs, c_lens) -> int:
    """Compiles the blast kernel by writing the cache's first chunk into a batch that just fits it (a template's
    chunks are ~3 KB, more than a minimal batch holds). Returns the bytes written."""
    with pool_create(PooledLogBatch, 1, int(c_lens[0])) as dummy_batch:
        return nb_blast_benchmark_cache(dummy_batch.bundle, now_ns, 1, c_buf, c_offs, c_lens, len(c_lens))


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
@configuration_property(
    "template",
    type="string",
    default="random",
    enum=list(TEMPLATES),
    description="What the generated lines look like. 'random': short 'Random: N' lines (any pipeline). "
    "'zephyr_deferred': Zephyr deferred log lines (~67 B), for a serial_zephyr_deferred pipeline. "
    "'adb_long': 'adb logcat -v long' frames (~138 B), for a pipeline with the decode_adb_long_frame decoder "
    "and the adb_long_frame steps. The text is synthetic; lengths follow real logs. A pipeline that parses the "
    "device timestamp tops out at ~1 M lines/s: the time projection keeps timestamps at least 1 us apart, so "
    "faster rows run ahead of the clock and the reorder holds them. Drop the timestamp step to load the parser "
    "harder.",
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
    template: str

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
            # max_msg_per_sec caps the controller: it can still slow down on backlog, never go above the cap
            rate_cap = float(getattr(self, "max_msg_per_sec", 0) or 0)
            current_msg_per_sec = float(self.batch_size * TICKS_PER_SEC)
            if rate_cap > 0:
                current_msg_per_sec = min(current_msg_per_sec, rate_cap)
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
            # Chunks of whole frames; one chunk counts as rows_per_bytes messages in the backlog
            c_buf, c_offs, c_lens, rows_per_bytes = build_cache(getattr(self, "template", None) or "random")
            c_items = len(c_lens)
            # Bytes of the first k cache entries, so a tick's size is two lookups, not a Python loop
            # over its chunks (that loop held the GIL for ~1 M iterations/s across 4 sources).
            c_cum = [0]
            for n in c_lens:
                c_cum.append(c_cum[-1] + int(n))
            c_total = c_cum[-1]

            # --- [START] NUMBA WARMUP ---
            if self.numba_needs_compile:
                try:
                    logger.info("Warming up benchmark JIT kernels...")
                    warm_up_blast(pool_create, time_ns(), c_buf, c_offs, c_lens)
                    logger.info("Benchmark kernels warmed up.")
                except Exception as e:
                    logger.exception("Failed to warm up benchmark kernels", exc=e)
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
                        if rate_cap > 0:
                            current_msg_per_sec = min(current_msg_per_sec, rate_cap)
                        log_fn = stats_logger.info

                    # Reporting
                    elapsed_sec = elapsed_ns / 1_000_000_000.0
                    interval_msg_rate = interval_msgs / elapsed_sec
                    interval_mb_s = (interval_bytes / 1_048_576) / elapsed_sec
                    session_elapsed_sec = (current_time - session_start_time) / 1_000_000_000.0
                    session_avg_mb_s = (session_total_bytes / 1_048_576) / session_elapsed_sec

                    log_fn(
                        "rate=%.0f throughput_mb_s=%.2f step_pct=%.1f backlog=%s state=%s",
                        interval_msg_rate,
                        interval_mb_s,
                        current_step * 100,
                        int(ema_backlog),
                        state,
                    )

                    last_adjustment_time = current_time
                    interval_msgs = interval_bytes = 0

                # --- 3. Message Burst ---
                msgs_per_tick = int(current_msg_per_sec / TICKS_PER_SEC)
                if in_flight < effective_max_backlog:
                    chunks = max(1, msgs_per_tick // rows_per_bytes)
                    full, rest = divmod(chunks, c_items)
                    estimated_bytes = full * c_total + c_cum[rest]  # sum of c_lens[i % c_items], i < chunks

                    batch = pool_create(PooledLogBatch, chunks, estimated_bytes)
                    written_bytes = nb_blast_benchmark_cache(
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
            self.logger.exception("Exception in benchmark reader", exc=e)

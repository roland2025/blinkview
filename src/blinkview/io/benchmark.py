# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import configuration_property
from blinkview.core.constants import SysCat
from blinkview.core.device_identity import DeviceIdentity
from blinkview.core.log_row import LogRow
from blinkview.core.reusable_batch_pool import BatchPool, TimeDataEntry
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.log_filter import LogFilter


@DeviceFactory.register("benchmark")
@configuration_property(
    "batch_size", type="integer", default=10000, description="Number of messages to generate in each batch"
)
@configuration_property(
    "max_backlog",
    type="integer",
    default=300000,
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

    def _run(self):
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger
        _len = len

        logger.warn("Starting RandomGenerator Benchmark...")
        batch_size = self.batch_size
        max_backlog = self.max_backlog
        max_msg_per_sec = self.max_msg_per_sec

        device: DeviceIdentity = self.shared.registry.get_reference_target(self.targets_).local.device_id
        log_filter = LogFilter(self.shared.id_registry, allowed_device=device)

        # If rate limited, allow a max backlog of 3 seconds worth of data.
        # Otherwise, fall back to the raw configured max_backlog.
        if max_msg_per_sec > 0:
            effective_max_backlog = max(max_msg_per_sec * 3, batch_size * 2)
        else:
            effective_max_backlog = max(self.max_backlog, batch_size * 2)

        logger.warn(
            f"Batch size: {batch_size} | Max backlog: {max_backlog} | Rate limit: {max_msg_per_sec or 'Unlimited'}"
        )

        # create synthetic batches
        cache = []
        cache_size = 0
        rows_per_bytes = 50
        bytes_objects = self.batch_size // rows_per_bytes
        # put x rows in one bytes buffer
        for i in range(bytes_objects):
            row_data = []
            for j in range(rows_per_bytes):
                row = b"Random: [value]: %d\n" % (i * rows_per_bytes + j)
                row_data.append(row)
            cache_size += len(row_data)
            cache.append(b"".join(row_data))

        total_sent_msgs = 0
        total_recv_msgs = 0
        interval_msgs = 0
        interval_bytes = 0

        last_report_time = time_ns()

        pool_acquire = self.shared.pool.get(TimeDataEntry, self.__class__.__name__).acquire

        while not stop_is_set():
            # Always evaluate time and print reports first!
            current_time = time_ns()
            elapsed_ns = current_time - last_report_time
            in_flight = total_sent_msgs - total_recv_msgs

            if elapsed_ns >= 1_000_000_000:
                elapsed_sec = elapsed_ns / 1_000_000_000.0
                # Prevent ZeroDivisionError just in case
                if elapsed_sec > 0:
                    msg_per_sec = interval_msgs / elapsed_sec
                    mb_per_sec = (interval_bytes / 1024 / 1024) / elapsed_sec
                else:
                    msg_per_sec = mb_per_sec = 0

                msg_per_sec = (
                    f"TX: {msg_per_sec:,.0f} msg/s ({mb_per_sec:.2f} MB/s) | "
                    f"Backlog: {in_flight:,} / {effective_max_backlog:,} | Total RX: {total_recv_msgs:,}"
                )
                logger.warn(msg_per_sec)
                # print(f"{device.name}: {msg_per_sec}")

                interval_msgs = 0
                interval_bytes = 0
                last_report_time = current_time

                # Backpressure Check (Now uses effective_max_backlog)
            if in_flight > effective_max_backlog:
                total_recv_msgs += self._drain_queue(device, log_filter, timeout=0.01)
                continue

            # Rate Limit Check
            current_batch_size = batch_size
            if max_msg_per_sec > 0:
                remaining_in_sec = max_msg_per_sec - interval_msgs
                if remaining_in_sec <= 0:
                    # We hit the limit. Wait until the next second starts.
                    total_recv_msgs += self._drain_queue(device, log_filter, timeout=0.05)
                    continue
                current_batch_size = min(batch_size, remaining_in_sec)

            # Generate batch

            with pool_acquire() as batch:
                _append = batch.append
                t = time_ns()

                items_from_cache = current_batch_size // rows_per_bytes
                for i in range(items_from_cache):
                    tmp = cache[i]
                    _append((t + i), tmp)
                    interval_bytes += _len(tmp)

                # variant 2
                # batch = []
                # _append = batch.append
                # t = time_ns()
                #
                # items_from_cache = current_batch_size // rows_per_bytes
                # for i in range(items_from_cache):
                #     tmp = cache[i]
                #     _append(((t + i), tmp))
                #     interval_bytes += _len(tmp)
                #
                #
                # variant 3
                # for i in range(t, current_batch_size + t, 1):
                # msg = b"Random value: %d\n" % i
                #
                # _append((i, msg))
                # interval_bytes += _len(msg)

                interval_msgs += current_batch_size
                total_sent_msgs += current_batch_size

                # 5. Blast it
                self.distribute(batch)

            # 6. Drain the queue (update total normally)
            total_recv_msgs += self._drain_queue(device, log_filter)

    def run(self):
        # Localizing for micro-optimization
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger
        stats_logger = logger.child("stats")
        _len = len

        # Input Queue handles
        queue_get = self.input_queue.get
        device = self.shared.registry.get_reference_target(self.targets_).local.device_id
        filter_batch = LogFilter(self.shared.id_registry, allowed_device=device).filter_batch

        # 30ms Interval Config
        INTERVAL_NS = 30 * 1_000_000
        TICKS_PER_SEC = 1_000_000_000 / INTERVAL_NS

        # --- Adaptive Parameters ---
        current_msg_per_sec = float(self.batch_size * TICKS_PER_SEC)
        increase_factor = 1.02
        decrease_factor = 0.95
        ema_backlog = 0.0
        alpha = 0.2

        # Timing Windows
        adjustment_interval_ns = 3_000_000_000
        session_start_time = time_ns()  # Used for session average
        last_adjustment_time = session_start_time

        effective_max_backlog = self.max_backlog or (self.batch_size * 20)

        # Pre-calculated Cache & Lengths
        rows_per_bytes = 50
        cache_base = [b"Random: %d\n" % i for i in range(1000)]
        cache = [b"".join(cache_base[i : i + rows_per_bytes]) for i in range(0, len(cache_base), rows_per_bytes)]
        cache_lengths = [len(c) for c in cache]
        num_cache_items = len(cache)

        # Counters
        total_sent_msgs = total_recv_msgs = 0
        interval_msgs = 0
        interval_bytes = 0  # Bytes sent in the last 3 seconds
        session_total_bytes = 0  # Total bytes sent since start

        next_send_tick = time_ns()
        pool_acquire = self.shared.pool.get(TimeDataEntry, self.__class__.__name__).acquire

        logger.warn("Starting Reactive Benchmarker | Target: 30ms Interval")

        while not stop_is_set():
            current_time = time_ns()

            # --- 1. Productive Wait & Drain ---
            while current_time < next_send_tick:
                timeout = (next_send_tick - current_time) / 1_000_000_000.0
                try:
                    batch = queue_get(timeout=timeout)
                    if batch is not None:
                        total_recv_msgs += _len(filter_batch(batch))
                except Exception:
                    break
                current_time = time_ns()

            # --- 2. Equilibrium & Throughput Reporting (Every 3s) ---
            in_flight = total_sent_msgs - total_recv_msgs
            ema_backlog = (alpha * in_flight) + ((1 - alpha) * ema_backlog)

            elapsed_ns = current_time - last_adjustment_time
            if elapsed_ns >= adjustment_interval_ns:
                # Interval Calculations
                elapsed_sec = elapsed_ns / 1_000_000_000.0
                interval_msg_rate = interval_msgs / elapsed_sec
                interval_mb_s = (interval_bytes / 1_048_576) / elapsed_sec

                # Session Average Calculations
                session_elapsed_sec = (current_time - session_start_time) / 1_000_000_000.0
                session_avg_mb_s = (session_total_bytes / 1_048_576) / session_elapsed_sec

                # Rate Adjustment logic
                if ema_backlog > (effective_max_backlog * 0.5):
                    current_msg_per_sec *= decrease_factor
                    state = "CONSOLIDATING"
                    log_fn = stats_logger.warn
                else:
                    current_msg_per_sec *= increase_factor
                    state = "PROBING"
                    log_fn = stats_logger.info

                log_fn(
                    f"{interval_msg_rate:.0f} msg/s ({interval_mb_s:.2f} MB/s) | "
                    f"Session Avg: {session_avg_mb_s:.2f} MB/s | "
                    f"Backlog: {int(ema_backlog):} / {effective_max_backlog:} | {state}"
                )

                # Reset interval-only counters
                last_adjustment_time = current_time
                interval_msgs = 0
                interval_bytes = 0

            # --- 3. Message Burst ---
            msgs_per_tick = int(current_msg_per_sec / TICKS_PER_SEC)

            if in_flight < effective_max_backlog:
                with pool_acquire() as batch:
                    _append = batch.append
                    chunks = max(1, msgs_per_tick // rows_per_bytes)

                    tick_bytes = 0
                    for i in range(chunks):
                        idx = i % num_cache_items
                        data = cache[idx]
                        _append((current_time + i), data)
                        tick_bytes += cache_lengths[idx]

                    # Update all byte counters
                    interval_bytes += tick_bytes
                    session_total_bytes += tick_bytes

                    sent_count = chunks * rows_per_bytes
                    total_sent_msgs += sent_count
                    interval_msgs += sent_count
                    self.distribute(batch)

            # --- 4. Advance Tick ---
            next_send_tick += INTERVAL_NS
            if (time_ns() - next_send_tick) > INTERVAL_NS:
                next_send_tick = time_ns()

    def _drain_queue(self, device, log_filter, timeout=None, _len=len) -> int:
        """Pulls batches from the queue and counts the individual messages."""
        get = self.input_queue.get
        filter_batch = log_filter.filter_batch
        # timeout = timeout or 0.001
        drained_count = 0

        while True:
            batch = get(timeout)
            if batch is None:
                break

            # drained_count += sum(item.module.device is device for item in batch)
            # for item in batch:
            #     item: LogRow
            #     if item.module.device is device:
            #         drained_count += 1

            drained_count += _len(filter_batch(batch))

        return drained_count

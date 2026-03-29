# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import configuration_property
from blinkview.core.constants import SysCat
from blinkview.io.BaseReader import BaseReader, DeviceFactory


@DeviceFactory.register("benchmark")
@configuration_property(
    "batch_size", type="integer", default=5000, description="Number of messages to generate in each batch"
)
@configuration_property(
    "max_backlog",
    type="integer",
    default=150000,
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
    type="string",
    required=True,
    _reference="/targets",
    default="",
)
class Benchmark(BaseReader):
    batch_size: int
    max_backlog: int
    max_msg_per_sec: int  # <-- New property

    __doc__ = """A synthetic log generator that produces random messages at a configurable rate.\nUseful for benchmarking and testing the system's throughput and backpressure handling."""

    def __init__(self):
        super().__init__()

        self.sources = [SysCat.STORAGE]

        self.queue = BatchQueue()
        self.put = self.queue.put

    def run(self):
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        logger.warn("Starting RandomGenerator Benchmark...")
        batch_size = self.batch_size
        max_backlog = self.max_backlog
        max_msg_per_sec = self.max_msg_per_sec

        # If rate limited, allow a max backlog of 3 seconds worth of data.
        # Otherwise, fall back to the raw configured max_backlog.
        if max_msg_per_sec > 0:
            effective_max_backlog = max_msg_per_sec * 3
        else:
            effective_max_backlog = self.max_backlog

        logger.warn(
            f"Batch size: {batch_size} | Max backlog: {max_backlog} | Rate limit: {max_msg_per_sec or 'Unlimited'}"
        )

        total_sent_msgs = 0
        total_recv_msgs = 0
        interval_msgs = 0
        interval_bytes = 0

        last_report_time = time_ns()

        while not stop_is_set():
            # 1. Always evaluate time and print reports first!
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

                logger.warn(
                    f"TX: {msg_per_sec:,.0f} msg/s ({mb_per_sec:.2f} MB/s) | "
                    f"Backlog: {in_flight:,} / {effective_max_backlog:,} | Total RX: {total_recv_msgs:,}"
                )

                interval_msgs = 0
                interval_bytes = 0
                last_report_time = current_time

                # 2. Backpressure Check (Now uses effective_max_backlog)
            if in_flight > effective_max_backlog:
                total_recv_msgs += self._drain_queue()
                continue

            # 3. Rate Limit Check
            current_batch_size = batch_size
            if max_msg_per_sec > 0:
                remaining_in_sec = max_msg_per_sec - interval_msgs
                if remaining_in_sec <= 0:
                    # We hit the rate limit for this second. Drain queue and wait.
                    total_recv_msgs += self._drain_queue()
                    continue

                # Prevent overshooting the limit if the remaining allowance is less than a full batch
                current_batch_size = min(batch_size, remaining_in_sec)

            # 4. Generate batch
            batch = []
            for _ in range(current_batch_size):
                timestamp = time_ns()
                msg = f"Random value: {timestamp}\n".encode()
                batch.append((timestamp, msg))
                interval_bytes += len(msg)

            interval_msgs += current_batch_size
            total_sent_msgs += current_batch_size

            # 5. Blast it
            self.distribute(batch)

            # 6. Drain the queue (update total normally)
            total_recv_msgs += self._drain_queue()

    def _drain_queue(self) -> int:
        """Pulls batches from the queue and counts the individual messages."""
        drained_count = 0
        while True:
            batch = self.queue.get(0.001)
            if batch is None:
                break
            drained_count += len(batch)

        return drained_count

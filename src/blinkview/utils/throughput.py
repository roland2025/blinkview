# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time


class Speedometer:
    """
    Tracks bytes and message counts to calculate real-time throughput
    and lifetime totals. Optionally logs stats every update interval.
    """

    def __init__(self, update_interval_ns: int = 1_000_000_000, logger=None):
        self.update_interval_ns = update_interval_ns
        self.logger = logger

        # Lifetime Totals
        self.total_bytes = 0
        self.total_msgs = 0

        # Windowed Accumulators
        self._bytes_acc = 0
        self._msgs_acc = 0
        self._last_time_ns = time.time_ns()

        # Calculated Rates
        self.bytes_per_sec = 0.0
        self.msgs_per_sec = 0.0

    def update(self, bytes_in: int, msgs_in: int) -> bool:
        """
        Records data and updates rates.
        If a logger is provided, it outputs the current speed automatically.
        """
        self.total_bytes += bytes_in
        self.total_msgs += msgs_in
        self._bytes_acc += bytes_in
        self._msgs_acc += msgs_in

        now = time.time_ns()
        elapsed_ns = now - self._last_time_ns

        if elapsed_ns >= self.update_interval_ns:
            elapsed_sec = elapsed_ns / 1e9
            self.bytes_per_sec = self._bytes_acc / elapsed_sec
            self.msgs_per_sec = self._msgs_acc / elapsed_sec

            # Reset window
            self._bytes_acc = 0
            self._msgs_acc = 0
            self._last_time_ns = now

            if self.logger:
                self.logger.info(
                    f"mb_s={self.bytes_per_sec / 1024 / 1024:.0f} bytes_s={self.bytes_per_sec:.0f} msg_s={self.msgs_per_sec:.0f}"  # total_bytes={self.total_bytes} total_msgs={self.total_msgs}"
                )
            return True
        return False

    def batch(self, batch):
        self.update(batch.msg_cursor, batch.size)

    def __str__(self) -> str:
        mb_total = self.total_bytes / (1024 * 1024)
        mb_s = self.bytes_per_sec / (1024 * 1024)
        return f"<Speedometer mb_s={mb_s:.2f} MB/s msg_s={self.msgs_per_sec:.0f} msg/s total={mb_total:.1f} MB>"


class ThroughputAutoTuner:
    """
    Uses a Speedometer to project required buffer sizes in bytes.
    Logs specifically when it recalculates resource projections.
    """

    def __init__(
        self,
        speedometer: Speedometer,
        default_buffer_bytes: int = 4096,
        msg_size_bytes: int = 32,
        safety_factor: float = 1.5,
        logger=None,
    ):
        self.speedo = speedometer
        self.default_buffer_bytes = default_buffer_bytes
        self.msg_size_bytes = msg_size_bytes
        self.safety_factor = safety_factor
        self.logger = logger

        # Capacity is derived from bytes
        self.default_capacity = int(default_buffer_bytes / msg_size_bytes)

        self.estimated_buffer_bytes = self.default_buffer_bytes
        self.estimated_capacity = self.default_capacity

    def update(self, bytes_in: int, msgs_in: int, target_window_sec: float) -> bool:
        # Speedometer logs its own stats internally
        if self.speedo.update(bytes_in, msgs_in):
            bytes_needed = self.speedo.bytes_per_sec * target_window_sec * self.safety_factor
            msgs_needed = self.speedo.msgs_per_sec * target_window_sec * self.safety_factor

            self.estimated_buffer_bytes = max(self.default_buffer_bytes, int(bytes_needed))
            self.estimated_capacity = max(self.default_capacity, int(msgs_needed))

            if self.logger:
                # Log state specifically; we can format to KB here for readability
                kb = self.estimated_buffer_bytes / 1024
                self.logger.debug(f"Tuner Projection: {kb:.1f} KB, {self.estimated_capacity} rows")
            return True
        return False

    def ensure_burst_capacity(self, current_batch_bytes: int):
        required_bytes = int(current_batch_bytes * self.safety_factor)

        if required_bytes > self.estimated_buffer_bytes:
            self.estimated_buffer_bytes = required_bytes
            self.estimated_capacity = int(self.estimated_buffer_bytes / self.msg_size_bytes)

            if self.logger:
                kb = self.estimated_buffer_bytes / 1024
                self.logger.warning(f"Burst detected! Force-resized buffer projection to {kb:.1f} KB")

    def __str__(self) -> str:
        kb = self.estimated_buffer_bytes / 1024
        return f"<ThroughputAutoTuner rows={self.estimated_capacity} buffer={kb:.1f} KB safety={self.safety_factor}x>"

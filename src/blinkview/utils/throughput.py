# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time

from blinkview.core.limits import TUNER_MAX_BUFFER_BYTES, TUNER_MAX_CAPACITY


class Speedometer:
    """
    Tracks bytes and message counts to calculate real-time throughput
    and lifetime totals. Optionally logs stats every update interval.
    """

    __slots__ = (
        "update_interval_ns",
        "logger",
        "total_bytes",
        "total_msgs",
        "_bytes_acc",
        "_msgs_acc",
        "_last_time_ns",
        "bytes_per_sec",
        "msgs_per_sec",
    )

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
            bytes_per_sec = self.bytes_per_sec = self._bytes_acc / elapsed_sec
            msgs_per_sec = self.msgs_per_sec = self._msgs_acc / elapsed_sec

            # Reset window
            self._bytes_acc = 0
            self._msgs_acc = 0
            self._last_time_ns = now

            if logger := self.logger:
                logger.debug(
                    "mb_s=%.0f bytes_s=%.0f msg_s=%.0f",  # total_bytes={self.total_bytes} total_msgs={self.total_msgs}"
                    bytes_per_sec / 1024 / 1024,
                    bytes_per_sec,
                    msgs_per_sec,
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

    Its estimates never exceed max_buffer_bytes / max_capacity, with one exception: a single burst
    passed to ensure_burst_capacity() bigger than max_buffer_bytes gets a buffer that just fits it,
    since the caller has to put it somewhere. The next update() brings the estimate back under.
    """

    __slots__ = (
        "speedo",
        "default_buffer_bytes",
        "msg_size_bytes",
        "safety_factor",
        "logger",
        "estimated_buffer_bytes",
        "estimated_capacity",
        "default_capacity",
        "max_buffer_bytes",
        "max_capacity",
    )

    def __init__(
        self,
        speedometer: Speedometer,
        default_buffer_bytes: int = 4096,
        msg_size_bytes: int = 32,
        safety_factor: float = 1.5,
        logger=None,
        max_buffer_bytes: int = TUNER_MAX_BUFFER_BYTES,
        max_capacity: int = TUNER_MAX_CAPACITY,
    ):
        self.speedo = speedometer
        self.default_buffer_bytes = default_buffer_bytes
        self.msg_size_bytes = msg_size_bytes
        self.safety_factor = safety_factor
        self.logger = logger
        # A caller's default above the cap is kept as asked: the cap bounds what the tuner grows
        # to, not the floor a caller picked.
        self.max_buffer_bytes = max(max_buffer_bytes, default_buffer_bytes)

        # Capacity is derived from bytes
        self.default_capacity = int(default_buffer_bytes / msg_size_bytes)
        self.max_capacity = max(max_capacity, self.default_capacity)

        self.estimated_buffer_bytes = self.default_buffer_bytes
        self.estimated_capacity = self.default_capacity

    def update(self, bytes_in: int, msgs_in: int, target_window_sec: float) -> bool:
        # Speedometer logs its own stats internally
        speedo = self.speedo
        if speedo.update(bytes_in, msgs_in):
            bytes_needed = int(speedo.bytes_per_sec * target_window_sec * self.safety_factor)
            msgs_needed = int(speedo.msgs_per_sec * target_window_sec * self.safety_factor)

            estimated_buffer_bytes = self.estimated_buffer_bytes = min(
                self.max_buffer_bytes, max(self.default_buffer_bytes, bytes_needed)
            )
            estimated_capacity = self.estimated_capacity = min(
                self.max_capacity, max(self.default_capacity, msgs_needed)
            )

            if logger := self.logger:
                # Log state specifically; we can format to KB here for readability
                kb = estimated_buffer_bytes / 1024
                logger.debug("size_kb=%.1f rows=%s", kb, estimated_capacity)
            return True
        return False

    def ensure_burst_capacity(self, burst_bytes: int, burst_rows: int = 1, splittable: bool = False):
        """Grows the estimates so a batch fits a burst of `burst_bytes` in `burst_rows` rows.

        The buffer gets the safety factor on top, up to max_buffer_bytes; past that only the burst
        itself is guaranteed to fit, unless `splittable` says the caller spreads a burst over as
        many batches as it takes (a parser's output), in which case the cap holds. It never asks
        for more than burst_bytes * safety_factor, so a caller may skip the call when that already
        fits. Rows grow only to `burst_rows`: a burst's byte size says nothing about how many rows
        it is, and deriving rows from bytes is what once made a parser's output batches ~500x too
        large."""
        max_buffer_bytes = self.max_buffer_bytes
        if splittable and burst_bytes > max_buffer_bytes:
            burst_bytes = max_buffer_bytes
        required_bytes = max(burst_bytes, min(max_buffer_bytes, int(burst_bytes * self.safety_factor)))

        if required_bytes > self.estimated_buffer_bytes:
            self.estimated_buffer_bytes = required_bytes

            if self.logger:
                kb = required_bytes / 1024
                self.logger.warning("Burst detected! Force-resized buffer projection to %.1f KB", kb)

        if burst_rows > self.estimated_capacity:
            self.estimated_capacity = min(self.max_capacity, burst_rows)

    def __str__(self) -> str:
        kb = self.estimated_buffer_bytes / 1024
        return f"<ThroughputAutoTuner rows={self.estimated_capacity} buffer={kb:.1f} KB safety={self.safety_factor}x>"

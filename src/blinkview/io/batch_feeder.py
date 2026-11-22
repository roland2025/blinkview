# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner


class BatchFeeder:
    """The batching half of a stream reader's run() loop: owns the current batch and the
    ThroughputAutoTuner that sizes the next one.

    A reader builds one at the top of run(), pushes each chunk it reads, flushes when the
    batch's delay window has passed and closes it in its `finally`:

        feeder = BatchFeeder(self, msg_size_bytes=20, default_buffer_bytes=read_size)
        try:
            while ...:
                feeder.push_view(now, view, n)
                feeder.maybe_flush(now)
        finally:
            feeder.close()

    There is always a batch between __init__ and close().

    Keep it a local of run(): nothing here is locked, so other threads must not reach it.
    """

    __slots__ = ("batch", "delay_ns", "_delay_s", "_distribute", "_tuner", "_create", "_batch_cls", "_logger")

    def __init__(
        self,
        reader,
        *,
        msg_size_bytes: int,
        default_buffer_bytes: int = 4096,
        batch_cls: type[PooledLogBatch] = PooledLogBatch,
    ):
        self.delay_ns = int(reader.delay * 1_000_000)
        self._delay_s = reader.delay / 1000.0
        self._distribute = reader.distribute
        self._logger = reader.logger_link or reader.logger

        logger = reader.logger
        self._tuner = ThroughputAutoTuner(
            speedometer=Speedometer(logger=logger.stats_child("stats")),
            default_buffer_bytes=default_buffer_bytes,
            msg_size_bytes=msg_size_bytes,
            logger=logger.stats_child("tuner"),
        )
        self._create = reader.shared.array_pool.create
        self._batch_cls = batch_cls

        self.batch: PooledLogBatch = self._acquire()

    def _acquire(self) -> PooledLogBatch:
        tuner = self._tuner
        return self._create(self._batch_cls, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

    def _hand_on(self, batch: PooledLogBatch):
        """Distributes a non-empty batch and feeds its size to the tuner; releases an empty one."""
        if len(batch):
            with batch:
                self._distribute(batch)
                self._tuner.update(batch.msg_cursor, batch.size, self._delay_s)
        else:
            batch.release()

    def push_view(self, now: int, view, n: int):
        """Inserts view[:n] as one row stamped `now`. If it does not fit, the batch is handed on,
        the tuner grows its estimate to fit the chunk if needed, and the chunk goes into a new
        batch."""
        if self.batch.insert_view(now, now, view, n):
            return
        batch = self._replace(n)
        if not batch.insert_view(now, now, view, n):
            self._logger.error("Dropped a %d-byte read: it does not fit a new batch", n)

    def push(self, now: int, data: bytes):
        """Inserts `data` as one row stamped `now`; a chunk that does not fit is handled as in
        push_view."""
        if self.batch.insert(now, now, data):
            return
        batch = self._replace(len(data))
        if not batch.insert(now, now, data):
            self._logger.error("Dropped a %d-byte read: it does not fit a new batch", len(data))

    def append(self, now: int, data: bytes):
        """Appends `data` to the last row (uart: the rest of a burst after its first byte). If it
        does not fit, the batch is handed on and `data` becomes a new row stamped `now` in a batch
        sized for it."""
        if self.batch.append(data):
            return
        batch = self._replace(len(data))
        if not batch.insert(now, now, data):
            self._logger.error("Dropped a %d-byte read: it does not fit a new batch", len(data))

    def _replace(self, burst_bytes: int) -> PooledLogBatch:
        batch, self.batch = self.batch, None
        self._hand_on(batch)
        # A chunk can be bigger than the tuner's estimate (it starts at default_buffer_bytes and
        # learns from what it delivers): size the new batch for it, or the retry fails. A chunk is
        # one row, ensure_burst_capacity's default. It never asks for more than safety_factor x the
        # chunk, so when that already fits, a batch that is simply full skips the call.
        tuner = self._tuner
        if int(burst_bytes * tuner.safety_factor) > tuner.estimated_buffer_bytes:
            tuner.ensure_burst_capacity(burst_bytes)
        self.batch = new = self._acquire()
        return new

    def maybe_flush(self, now: int):
        """The timed flush: flushes once the batch's first row is delay_ns old. Never fires on
        an empty batch, whose start_ts is max int64."""
        if now - self.batch.start_ts >= self.delay_ns:
            self._replace(0)

    def flush(self) -> PooledLogBatch:
        """Distributes the batch and starts a new one; does nothing to an empty batch (handing on
        an empty one would still reset the stall watchdog's ingest clock). Returns the current
        batch."""
        if not len(self.batch):
            return self.batch
        return self._replace(0)

    def close(self):
        """Distributes what is left, or releases an empty batch. The feeder holds no batch after
        this. No tuner update: nothing reads it again."""
        batch, self.batch = self.batch, None
        if batch is None:
            return
        if len(batch):
            with batch:
                self._distribute(batch)
        else:
            batch.release()

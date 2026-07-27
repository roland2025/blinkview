# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
from pathlib import Path
from time import sleep

from blinkview.core.configurable import configuration_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.paths import resolve_config_path
from blinkview.utils.throughput import Speedometer


@DeviceFactory.register("file_tail")
@configuration_property(
    "file_path",
    type="string",
    required=True,
    ui_type="file",
    ui_file_filter="Log Files (*.log *.txt);;All Files (*)",
    description="Path to the log file to tail. Supports relative paths via resolve_config_path.",
)
@configuration_property(
    "from_start",
    type="boolean",
    default=False,
    description="Read the file's existing content before tailing new appended data. If false, only "
    "data written after the reader starts is read (like 'tail -f').",
)
@configuration_property(
    "poll_interval",
    type="integer",
    default=200,
    description="How often (in milliseconds) to check the file for new data when idle.",
)
@configuration_property(
    "chunk_size",
    type="integer",
    default=65536,
    description="Maximum number of bytes to read from the file per poll.",
)
@configuration_property(
    "delay",
    type="integer",
    default=100,
    description="The maximum time (in milliseconds) to hold newly read bytes before flushing a batch downstream.",
)
class FileTailReader(BaseReader):
    __doc__ = """Live-tails a growing text/log file from disk (desktop/console application logs).

* Polls the file for appended bytes at a configurable interval, streaming new content downstream
  as it's written - the read side of a `tail -f`.
* Detects truncation or file replacement (log rotation) by tracking inode identity and file size,
  reopening from the start when either changes.
* Does not assemble multi-line log entries (e.g. stack traces) - each newline-delimited chunk is
  forwarded independently as its own row.
"""

    file_path: str
    from_start: bool
    poll_interval: int
    chunk_size: int
    delay: int

    def __init__(self):
        super().__init__()

    def run(self):
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        path = Path(resolve_config_path(self.file_path))
        poll_interval_s = self.poll_interval / 1000.0
        delay_ns = self.delay * 1_000_000
        chunk_size = self.chunk_size

        logger.info(f"Starting File Tail Reader: {path} (poll {self.poll_interval}ms, from_start={self.from_start})")

        while not stop_is_set() and not path.exists():
            logger.warning(f"Tail target does not exist yet, waiting: {path}")
            sleep(poll_interval_s)

        if stop_is_set():
            return

        f = path.open("rb")
        current_ino = os.fstat(f.fileno()).st_ino

        if not self.from_start:
            f.seek(0, os.SEEK_END)

        pool_create = self.shared.array_pool.create
        buffer_bytes = max(chunk_size * 4, 65536)
        buffer_chunks = max(buffer_bytes // 256, 64)

        def batch_acquire():
            return pool_create(PooledLogBatch, buffer_chunks, buffer_bytes)

        batch = None
        stats = Speedometer(logger=self.logger.child("stats"))

        try:
            while not stop_is_set():
                # Detect rotation/truncation: inode change or size shrink -> reopen from start
                try:
                    st = path.stat()
                except FileNotFoundError:
                    sleep(poll_interval_s)
                    continue

                if st.st_ino != current_ino or st.st_size < f.tell():
                    logger.info(f"Detected log rotation, reopening: {path}")
                    f.close()
                    f = path.open("rb")
                    current_ino = os.fstat(f.fileno()).st_ino

                if batch is None:
                    batch = batch_acquire()

                ts_data = time_ns()
                data = f.read(chunk_size)

                if not data:
                    # No new data - flush anything pending once the batching window elapses, then wait
                    if len(batch) > 0 and (time_ns() - batch.start_ts) >= delay_ns:
                        with batch:
                            self.distribute(batch)
                            stats.batch(batch)
                        batch = None
                    sleep(poll_interval_s)
                    continue

                if not batch.insert(ts_data, ts_data, data):
                    with batch:
                        self.distribute(batch)
                        stats.batch(batch)
                    batch = batch_acquire()
                    batch.insert(ts_data, ts_data, data)

                if (time_ns() - batch.start_ts) >= delay_ns:
                    with batch:
                        self.distribute(batch)
                        stats.batch(batch)
                    batch = None

        except Exception as e:
            logger.exception(f"Error in FileTailReader for {path.name}", e)
        finally:
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()
            f.close()
            logger.info(f"File tail closed: {path.name}")

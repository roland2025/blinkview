# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import sleep

import numpy as np

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.paths import resolve_config_path
from blinkview.utils.throughput import Speedometer


@DeviceFactory.register("binary_file")
@configuration_property(
    "file_path",
    type="string",
    required=True,
    ui_type="file",
    ui_file_filter="Binary Files (*.bin *.dat *.raw);;All Files (*)",
    description="Path to the binary file to stream. Supports relative paths via resolve_config_path.",
)
@configuration_property(
    "chunk_size", type="integer", default=8, description="Number of bytes to read per injection 'tick'."
)
@configuration_property("frequency", type="integer", default=100, description="Read rate in Hz (times per second).")
@configuration_property("delay", type="integer", default=30, description="Time to collect batch")
@configuration_property(
    "loop",
    type="boolean",
    required=True,
    default=True,
    description="Restart from the beginning of the file when EOF is reached.",
)
class BinaryFileReader(BaseReader):
    __doc__ = """A development replay tool for streaming raw binary data.

* Mimics a live data source by injecting file content at a fixed frequency.
* Generates 'Now' timestamps for un-timestamped raw data.
* Memory-maps the file read-only (np.memmap); each tick is a zero-copy slice of the mapping.
* Uses pathlib for robust cross-platform path handling.
"""

    file_path: str
    chunk_size: int
    frequency: int
    delay: int
    loop: bool

    def __init__(self):
        super().__init__()

    def run(self):
        # Setup and Path Resolution
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        path = Path(resolve_config_path(self.file_path))
        interval_s = 1.0 / max(1, self.frequency)

        # Convert delay (ms) to nanoseconds for comparison with time_ns()
        delay_ns = self.delay * 1_000_000
        chunk_size = self.chunk_size

        logger.info(
            "Starting Binary Reader: %s (@%sHz, %sms batching)",
            path,
            self.frequency,
            self.delay,
        )

        if not path.exists():
            logger.error("Binary file not found: %s", path)
            return

        try:
            file_size = path.stat().st_size
            if file_size == 0:
                # np.memmap refuses to map an empty file
                logger.warning("Binary file is empty: %s", path)
                return
            # Plain read-only ndarray view over the mapping: slices are zero-copy. Push them via
            # insert_view() - its read-only-array nb_bundle_push_len variant is already warmed
            # (binary_parser); insert() would compile a new read-only-array nb_bundle_push.
            data_map = np.memmap(path, dtype=dtypes.BYTE, mode="r").view(np.ndarray)
        except Exception as e:
            logger.error("Failed to memory-map binary file %s: %s", path, e)
            return

        buffer_bytes = self.frequency * chunk_size * (self.delay + 30) // 1000
        buffer_chunks = buffer_bytes // chunk_size

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            return pool_create(PooledLogBatch, buffer_chunks, buffer_bytes)

        batch = None
        offset = 0

        stats = Speedometer(logger=self.logger.stats_child("stats"))

        try:
            while not stop_is_set():
                # 1. Initialize a new batch if we don't have one active
                if batch is None:
                    batch = batch_acquire()

                # 2. Handle End of File
                if offset >= file_size:
                    if self.loop:
                        offset = 0
                        logger.debug("Replay loop: Resetting %s", path.name)
                        continue
                    else:
                        # Flush remaining data in current batch before exiting
                        if len(batch) > 0:
                            with batch:  # This automatically calls release()
                                self.distribute(batch)
                                stats.batch(batch)
                        else:
                            batch.release()  # Manually return empty batch to pool

                        batch = None
                        logger.info("Binary replay finished: %s", path.name)
                        break

                # 3. Take the next raw chunk (zero-copy view into the mapping)
                ts_data = time_ns()
                end = offset + chunk_size
                data = data_map[offset:end]
                offset = end

                # 4. Add data to the current batch
                data_len = len(data)
                if not batch.insert_view(ts_data, ts_data, data, data_len):
                    # Batch capacity or buffer is full, flush it
                    with batch:
                        self.distribute(batch)

                        stats.batch(batch)

                    # Acquire new batch and immediately append the skipped data
                    batch = batch_acquire()
                    batch.insert_view(ts_data, ts_data, data, data_len)

                # 5. Check if the batching window has elapsed
                if (time_ns() - batch.start_ts) >= delay_ns:
                    with batch:
                        self.distribute(batch)

                        stats.batch(batch)

                    # Set to None so Step 1 pulls a fresh batch on the next loop.
                    # Do NOT call batch.release() here; the 'with' block already did!
                    batch = None

                # 6. Maintain injection frequency
                sleep(interval_s)

        except Exception as e:
            logger.exception("Error in BinaryFileReader for %s", path.name, exc=e)
        finally:
            # Guarantee we don't leak the batch on unexpected errors
            if batch is not None:
                batch.release()
            # Drop the last references so the mapping (and the Windows file lock) is released
            data = None
            data_map = None
            logger.info("Binary file unmapped: %s", path.name)

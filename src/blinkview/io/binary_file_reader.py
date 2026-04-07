# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import sleep

from ..core.configurable import configuration_property
from ..core.numpy_batch_manager import PooledLogBatch
from ..core.reusable_batch_pool import TimeDataEntry
from ..utils.paths import resolve_config_path
from .BaseReader import BaseReader, DeviceFactory


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
    "loop", type="boolean", default=True, description="Restart from the beginning of the file when EOF is reached."
)
class BinaryFileReader(BaseReader):
    __doc__ = """A development replay tool for streaming raw binary data.

* Mimics a live data source by injecting file content at a fixed frequency.
* Generates 'Now' timestamps for un-timestamped raw data.
* Uses pathlib for robust cross-platform path handling.
"""

    file_path: str
    chunk_size: int
    frequency: int
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

        logger.info(f"Starting Binary Reader: {path} (@{self.frequency}Hz, {self.delay}ms batching)")

        if not path.exists():
            logger.error(f"Binary file not found: {path}")
            return

        buffer_bytes = self.frequency * chunk_size * (self.delay + 30) // 1000
        buffer_chunks = buffer_bytes // chunk_size

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            return pool_create(PooledLogBatch, buffer_chunks, buffer_bytes // 1024)

        f = None
        batch = None

        try:
            f = path.open("rb")
            _read = f.read
            _seek = f.seek

            while not stop_is_set():
                # 1. Initialize a new batch if we don't have one active
                if batch is None:
                    batch = batch_acquire()

                # 2. Read the next raw chunk
                ts_data = time_ns()
                data = _read(chunk_size)

                # 3. Handle End of File
                if not data:
                    if self.loop:
                        _seek(0)
                        logger.debug(f"Replay loop: Resetting {path.name}")
                        continue
                    else:
                        # Flush remaining data in current batch before exiting
                        if len(batch) > 0:
                            with batch:  # This automatically calls release()
                                self.distribute(batch)
                        else:
                            batch.release()  # Manually return empty batch to pool

                        batch = None
                        logger.info(f"Binary replay finished: {path.name}")
                        break

                # 4. Add data to the current batch
                if not batch.append(ts_data, data):
                    # Batch capacity or buffer is full, flush it
                    with batch:
                        self.distribute(batch)

                    # Acquire new batch and immediately append the skipped data
                    batch = batch_acquire()
                    batch.append(ts_data, data)

                # 5. Check if the batching window has elapsed
                if (time_ns() - batch.start_ts) >= delay_ns:
                    with batch:
                        self.distribute(batch)

                    # Set to None so Step 1 pulls a fresh batch on the next loop.
                    # Do NOT call batch.release() here; the 'with' block already did!
                    batch = None

                    # 6. Maintain injection frequency
                sleep(interval_s)

        except Exception as e:
            logger.exception(f"Error in BinaryFileReader for {path.name}", e)
        finally:
            # Guarantee we don't leak the batch on unexpected errors
            if batch is not None:
                batch.release()
            if f:
                f.close()
                logger.info(f"Binary file closed: {path.name}")

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import sleep

from ..core.configurable import configuration_property
from ..core.numpy_batch_manager import PooledLogBatch
from ..utils.paths import resolve_config_path
from ..utils.throughput import Speedometer
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
    "read_mode",
    required=True,
    enum=["stream", "memory"],
    description="Mode of reading: 'stream' (read from disk continuously) or 'memory' (preload entire file to RAM).",
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
* Supports streaming directly from disk or preloading to memory.
* Uses pathlib for robust cross-platform path handling.
"""

    file_path: str
    read_mode: str
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
        delay_s = self.delay / 1000.0

        logger.info(
            f"Starting Binary Reader: {path} (@{self.frequency}Hz, {self.delay}ms batching, mode: {self.read_mode})"
        )

        if not path.exists():
            logger.error(f"Binary file not found: {path}")
            return

        buffer_bytes = self.frequency * chunk_size * (self.delay + 30) // 1000
        buffer_chunks = buffer_bytes // chunk_size

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            return pool_create(PooledLogBatch, buffer_chunks, buffer_bytes)

        batch = None

        # Setup reader abstractions based on read_mode
        # Setup reader abstractions based on read_mode
        if self.read_mode.lower() == "memory":
            try:
                logger.debug(f"Preloading entire file to memory: {path.name}")
                with path.open("rb") as mem_f:
                    # Wrap the loaded bytes in a memoryview for zero-copy slicing
                    in_memory_data = memoryview(mem_f.read())
                memory_length = len(in_memory_data)
                memory_offset = 0
            except Exception as e:
                logger.error(f"Failed to load file to memory: {e}")
                return

            def _read_chunk(size: int):
                nonlocal memory_offset
                if memory_offset >= memory_length:
                    return b""  # The main loop's `if not data:` handles this perfectly

                end = memory_offset + size
                # Because in_memory_data is a memoryview, this slice is zero-copy
                # and returns another memoryview
                chunk = in_memory_data[memory_offset:end]
                memory_offset = end
                return chunk

            def _reset_source():
                nonlocal memory_offset
                memory_offset = 0

            def _cleanup():
                nonlocal in_memory_data
                # Explicitly release the memoryview buffer
                in_memory_data.release()
                logger.debug(f"Released memory buffer for: {path.name}")

        else:  # stream mode
            f = path.open("rb")
            _f_read = f.read
            _f_seek = f.seek

            def _read_chunk(size: int) -> bytes:
                return _f_read(size)

            def _reset_source():
                _f_seek(0)

            def _cleanup():
                if f:
                    f.close()
                    logger.info(f"Binary file closed: {path.name}")

        stats = Speedometer(logger=self.logger.child("stats"))

        try:
            while not stop_is_set():
                # 1. Initialize a new batch if we don't have one active
                if batch is None:
                    batch = batch_acquire()

                # 2. Read the next raw chunk
                ts_data = time_ns()
                data = _read_chunk(chunk_size)

                # 3. Handle End of File
                if not data:
                    if self.loop:
                        _reset_source()
                        logger.debug(f"Replay loop: Resetting {path.name}")
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
                        logger.info(f"Binary replay finished: {path.name}")
                        break

                # 4. Add data to the current batch
                if not batch.insert(ts_data, ts_data, data):
                    # Batch capacity or buffer is full, flush it
                    with batch:
                        self.distribute(batch)

                        stats.batch(batch)

                    # Acquire new batch and immediately append the skipped data
                    batch = batch_acquire()
                    batch.insert(ts_data, ts_data, data)

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
            logger.exception(f"Error in BinaryFileReader for {path.name}", e)
        finally:
            # Guarantee we don't leak the batch on unexpected errors
            if batch is not None:
                batch.release()
            _cleanup()

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import perf_counter
from typing import Callable

import numpy as np

from ..core import dtypes
from ..core.bindable import bindable
from ..core.configurable import configuration_property
from ..core.factory import BaseFactory
from ..core.numpy_batch_manager import PooledLogBatch
from ..core.system_context import SystemContext
from ..ops.formatting import (
    estimate_batch_capacity,
    format_binary_batch,
    format_log_row_batch,
)
from ..subscribers.subscriber import BaseSubscriber


class BaseFileLogger(BaseSubscriber):
    def __init__(self):
        super().__init__()


class FileLoggerFactory(BaseFactory[BaseFileLogger]):
    pass


@FileLoggerFactory.register("default")
@configuration_property(
    "processor",
    required=True,
    type="object",
    _factory="logging_processor",
    _factory_default="log_row",
    _factory_dropdown_hidden=True,
)
# @override_property("enabled")
@configuration_property("name", hidden=True, type="string")
@configuration_property(
    "flush_interval",
    type="number",
    default=10.0,
    description="Maximum time (in seconds) to wait before flushing the batch to disk, even if the batch size is not reached.",
    title="Flush Interval (s)",
)
@configuration_property(
    "max_file_size",
    type="integer",
    default=100,
    description="Maximum file size in MiB before rotating to a new file. Set to 0 for unlimited.",
    title="Max File Size (MiB)",
)
class FileLogger(BaseFileLogger):
    __doc__ = "Logs data to a file in batches. Configurable with different batch processors for formatting."
    name: str
    flush_interval: float
    max_file_size: int  # MiB

    def __init__(self):
        super().__init__()

        self.file_path: Path = None
        self.file_handle = None
        self.max_batch: int = 1000

        self.batch_processor = None
        self.process_batch = None

        self.part_index = 0  # Track current chunk

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        self.batch_processor = self.shared.factories.build("logging_processor", config.get("processor"), self.shared)
        self.process_batch = self.batch_processor.process

        self.shared.registry.file_manager.add_file_logger(self)
        return changed

    def open_file(self, increment_part_index=False):
        if increment_part_index:
            self.part_index += 1
            # Sync the increment back to metadata immediately during rotation
            self.shared.registry.file_manager.metadata["loggers"][self.local.logging_id]["last_part"] = self.part_index
            self.shared.registry.file_manager.write_metadata()

        self.file_path = self.shared.registry.file_manager.get_path_for_log(self, self.part_index)
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

        self.file_handle = self.file_path.open("ab")

        current_file_size = self.file_path.stat().st_size
        self.shared.registry.file_manager.update_logger_stats(self, current_file_size, absolute=True)
        self.logger.info(f"FileLogger '{self.name}' will log to: {self.file_path}")
        return current_file_size

    def set_batch_processor(self, batch_processor):
        self.batch_processor = batch_processor
        self.process_batch = self.batch_processor.process

    def _flush(self) -> int:
        if not self.file_handle:
            return 0

        # get_data() now returns a zero-copy memoryview
        data_view = self.batch_processor.get_data()
        len_data = len(data_view)

        if len_data == 0:
            return 0

        # write() accepts memoryviews natively
        self.file_handle.write(data_view)
        self.file_handle.flush()

        self.shared.registry.file_manager.update_logger_stats(self, len_data)
        return len_data

    def run(self):
        print(
            f"[{self.name}] FileLogger thread started with batch processor: {self.batch_processor.__class__.__name__}"
        )
        bytes_total = self.open_file()

        # Localize for performance
        queue_get = self.input_queue.get
        stop_is_set = self._stop_event.is_set
        process_batch = self.process_batch

        # Constants/Configuration
        max_batch = self.max_batch
        flush_interval = self.flush_interval
        max_file_size = self.max_file_size * 1024 * 1024 if self.max_file_size > 0 else float("inf")

        # State tracking
        last_flush_ts = perf_counter()

        # Instead of calling an external current_size func, we will track rows across batches
        # until they hit max_batch, or flush_interval hits.
        buffered_rows = 0

        try:
            while not stop_is_set():
                batch = queue_get(timeout=120)
                now = perf_counter()

                if batch is not None:
                    # using 'with' auto-releases the batch back to the NumpyArrayPool
                    with batch:
                        process_batch(batch)
                        buffered_rows += batch.size

                # Flush condition
                if buffered_rows > 0:
                    if buffered_rows >= max_batch or (now - last_flush_ts) >= flush_interval:
                        bytes_written = self._flush()
                        bytes_total += bytes_written

                        last_flush_ts = now
                        buffered_rows = 0  # reset tracking

                        # Check for file rotation
                        if bytes_total >= max_file_size:
                            print(f"[{self.name}] Rotating log: {bytes_total} bytes reached.")
                            bytes_total = self.open_file(increment_part_index=True)
        finally:
            self._flush()
            if self.file_handle:
                self.file_handle.close()

    process_batch: Callable[[list], bytearray | str]
    put: Callable[[list], None]


@bindable
class BaseBatchProcessor:
    is_binary: bool
    extension: str
    shared: SystemContext

    def __init__(self):
        self._buffer_size = 0
        self._buffer = None
        self._out_buffer = None
        self._written_bytes = 0

    def clear(self):
        """Full reset: releases the pooled array and clears tracking."""
        self._written_bytes = 0
        self._buffer_size = 0
        self._out_buffer = None
        if self._buffer is not None:
            self._buffer.release()
        self._buffer = None

    def __del__(self):
        self.clear()

    def _ensure_capacity(self, required_bytes: int):
        """Checks if the current buffer can hold the data; grows if necessary."""
        if self._buffer is None or required_bytes > self._buffer_size:
            # We preserve the tracking variable to calculate growth,
            # even though clear() nullifies the handle.
            old_size = self._buffer_size
            self.clear()

            # Grow by 1.5x or exactly required
            new_size = max(required_bytes, int(old_size * 1.5))

            # Since 0 results in the minimum block (1 KiB), simple floor division works.
            self._buffer = self.shared.array_pool.get(new_size, dtypes.BYTE)
            self._out_buffer = self._buffer.array
            self._buffer_size = self._buffer.capacity

    def process(self, batch: PooledLogBatch):
        """Implemented by subclasses."""
        pass

    def get_data(self) -> memoryview:
        """Returns a zero-copy view of the processed bytes and resets written counter."""
        if self._written_bytes == 0:
            return memoryview(b"")

        view = memoryview(self._out_buffer)[: self._written_bytes]
        self._written_bytes = 0
        return view


class BatchProcessorFactory(BaseFactory[BaseBatchProcessor]):
    pass


@BatchProcessorFactory.register("binary")
class BinaryBatchProcessor(BaseBatchProcessor):
    is_binary = True
    extension = "bin"

    def process(self, batch: "PooledLogBatch"):
        if batch.size == 0:
            return

        bundle = batch.bundle

        # 1. Binary Overhead (Strict 16 bytes for the protocol header)
        required = estimate_batch_capacity(bundle, 16)
        self._ensure_capacity(required)

        # 2. Binary Serialization Kernel
        self._written_bytes = format_binary_batch(self._out_buffer, bundle)


@BatchProcessorFactory.register("log_row")
class LogRowBatchProcessor(BaseBatchProcessor):
    is_binary = True
    extension = "log"

    def __init__(self):
        super().__init__()

        # State arrays allocated ONCE and reused across all batches
        self._sec_state = np.full(1, -1, dtype=np.int64)
        self._ts_cache = np.zeros(19, dtype=dtypes.BYTE)  # YYYY-MM-DDTHH:MM:SS

    def process(self, batch: "PooledLogBatch"):
        if batch.size == 0:
            return

        bundle = batch.bundle

        # 1. Text Overhead (approx 120 bytes for TS, IDs, and delimiters)
        required = estimate_batch_capacity(bundle, 120)
        self._ensure_capacity(required)

        # 2. Registry state (SoA bundle)
        registry = self.shared.id_registry.bundle()

        # 3. Text Serialization Kernel
        self._written_bytes = format_log_row_batch(self._out_buffer, bundle, registry, self._sec_state, self._ts_cache)

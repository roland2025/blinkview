# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path
from time import perf_counter
from typing import Callable

import numpy as np

from blinkview.core import dtypes
from blinkview.core.bindable import bindable
from blinkview.core.configurable import configuration_property
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.system_context import SystemContext
from blinkview.ops.formatting import (
    nb_estimate_batch_capacity,
    nb_format_binary_batch,
    nb_format_log_row_batch,
)
from blinkview.storage.log_file_archive import compress_log_part_file
from blinkview.subscribers.subscriber import BaseSubscriber


def _compress_and_delete_log_part(path: Path, logger=None) -> bool:
    """Compresses an already-closed log part file and deletes the original - best-effort: a
    failure is logged and skipped (returns False) rather than raised, since a part left
    uncompressed is still fully valid (just not space-saved), never lost. Shared by FileLogger's
    rotation (background task) and shutdown (synchronous) compression call sites. Returns True on
    success - callers that reuse `path`'s part index afterward (see run()'s finally block) must
    only do so once the original is confirmed gone, or a later reopen-and-rewrite at the same
    path would silently clobber this compressed archive on its next compression pass."""
    try:
        compress_log_part_file(path)
        path.unlink()
        return True
    except OSError as e:
        if logger:
            logger.warning("Failed to compress log part %s: %s", path, e)
        return False


class BaseFileLogger(BaseSubscriber):
    def __init__(self):
        super().__init__()


@register_factory_category(FactoryCategory.FILE_LOGGING)
class FileLoggerFactory(BaseFactory[BaseFileLogger]):
    pass


@FileLoggerFactory.register("default")
@configuration_property(
    "processor",
    required=True,
    type="object",
    _factory=FactoryCategory.LOGGING_PROCESSOR,
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
        self.batch_processor = self.shared.factories.build(
            FactoryCategory.LOGGING_PROCESSOR, config.get("processor"), self.shared
        )
        self.process_batch = self.batch_processor.process

        self.shared.registry.file_manager.add_file_logger(self)
        return changed

    def open_file(self, increment_part_index=False):
        # The part being rotated away from is now immutable (closed, never appended to again) -
        # compress it off-thread so a large completed part doesn't block this logger thread from
        # writing the new one. Captured before self.file_path gets reassigned below.
        rotated_away_path = self.file_path if increment_part_index else None

        if increment_part_index:
            self.part_index += 1
            # Sync the increment back to metadata immediately during rotation
            self.shared.registry.file_manager.metadata["loggers"][self.local.logging_id]["last_part"] = self.part_index
            self.shared.registry.file_manager.write_metadata()

        self.file_path = self.shared.registry.file_manager.get_path_for_log(self, self.part_index)
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

        if rotated_away_path is not None:
            self.shared.tasks.run_task(_compress_and_delete_log_part, rotated_away_path, self.logger)

        self.file_handle = self.file_path.open("ab")

        current_file_size = self.file_path.stat().st_size
        self.shared.registry.file_manager.update_logger_stats(self, current_file_size, absolute=True)
        self.logger.info("FileLogger '%s' will log to: %s", self.name, self.file_path)
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
            self._close_and_compress_final_part()

    def _close_and_compress_final_part(self):
        """Closes the current file handle and compresses the final part synchronously (not via
        run_task, unlike rotation) - called from run()'s finally block, i.e. on this logger
        thread itself right before it exits. BaseDaemon.stop() joins this thread before
        returning, so a background task here could easily lose the race against process/Registry
        shutdown. This is also what guarantees every session ends up compressed even one that
        never rotates, not just ones that happen to."""
        if self.file_handle:
            self.file_handle.close()
            self.file_handle = None

        if self.file_path is not None and self.file_path.exists() and self.file_path.stat().st_size > 0:
            if _compress_and_delete_log_part(self.file_path, self.logger):
                # A restart() (config change -> BaseDaemon.restart, or any future start()) must
                # never reopen this now-deleted, now-archived path - open_file() would just
                # recreate an empty file there and a later compression pass would silently
                # overwrite this archive with only the new stint's content, losing what's already
                # safely compressed. Bumping part_index (only on confirmed success - a failed
                # compression leaves the original file intact and safely reappendable)
                # guarantees the next open_file() always starts a fresh, never-before-used part,
                # same as a real rotation would.
                self.part_index += 1
                self.shared.registry.file_manager.metadata["loggers"][self.local.logging_id]["last_part"] = (
                    self.part_index
                )
                self.shared.registry.file_manager.write_metadata()

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


@register_factory_category(FactoryCategory.LOGGING_PROCESSOR)
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
        required = nb_estimate_batch_capacity(bundle, 16)
        self._ensure_capacity(required)

        # 2. Binary Serialization Kernel
        self._written_bytes = nb_format_binary_batch(self._out_buffer, bundle)


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
        required = nb_estimate_batch_capacity(bundle, 120)
        self._ensure_capacity(required)

        # 2. Registry state (SoA bundle)
        registry = self.shared.id_registry.bundle()

        # 3. Text Serialization Kernel
        self._written_bytes = nb_format_log_row_batch(
            self._out_buffer, bundle, registry, self._sec_state, self._ts_cache
        )

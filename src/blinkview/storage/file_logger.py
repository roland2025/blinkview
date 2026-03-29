# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from abc import ABC, abstractmethod
from datetime import datetime
from io import BytesIO, StringIO
from pathlib import Path
from struct import Struct
from threading import Thread
from time import perf_counter, sleep
from typing import Callable

from ..core.batch_queue import BatchQueue
from ..core.configurable import configuration_property, override_property
from ..core.factory import BaseFactory
from ..core.log_row import LogRow
from ..subscribers.subscriber import BaseSubscriber
from ..utils.time_utils import ISO8601TimestampFormatter, TimeUtils


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
        super().apply_config(config)
        self.batch_processor = self.shared.factories.build("logging_processor", config.get("processor"), self.shared)
        self.process_batch = self.batch_processor.process

        self.shared.registry.file_manager.add_file_logger(self)

    def open_file(self, increment_part_index=False):
        if increment_part_index:
            self.part_index += 1

        self.file_path = self.shared.registry.file_manager.get_path_for_log(self, self.part_index)
        if self.file_handle is not None:
            self.file_handle.close()
            self.file_handle = None

        is_binary = self.batch_processor.is_binary
        mode = "ab" if is_binary else "a"

        # Text mode needs newline control, Binary mode does not.
        self.file_handle = self.file_path.open(
            mode,
            encoding="utf-8" if not is_binary else None,
            newline="\n" if not is_binary else None,  # <--- THIS FIXES IT
        )

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

        data = self.batch_processor.get_data()
        if not data:
            return 0

        self.file_handle.write(data)
        self.file_handle.flush()
        len_data = len(data)
        self.shared.registry.file_manager.update_logger_stats(self, len_data)
        return len_data

    def run(self):
        print(
            f"[{self.name}] FileLogger thread started with batch processor: {self.batch_processor.__class__.__name__}"
        )
        bytes_total = self.open_file()

        # Localize for performance
        queue_get = self._queue.get
        stop_is_set = self._stop_event.is_set
        process_batch = self.process_batch
        current_size = self.batch_processor.current_size

        # Constants/Configuration
        max_batch = self.max_batch
        flush_interval = self.flush_interval
        max_file_size = self.max_file_size * 1024 * 1024 if self.max_file_size > 0 else float("inf")

        # State tracking
        last_flush_ts = perf_counter()

        try:
            while not stop_is_set():
                batch = queue_get(timeout=0.1)
                if batch is not None:
                    process_batch(batch)

                now = perf_counter()
                buf_size = current_size()

                if buf_size > 0:
                    if buf_size >= max_batch or (now - last_flush_ts) >= flush_interval:
                        # _flush() should return the number of bytes written
                        bytes_written = self._flush()
                        bytes_total += bytes_written
                        last_flush_ts = now

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


class BaseBatchProcessor:
    is_binary: bool
    extension: str

    def process(self, batch):
        # We don't mark this @abstractmethod so we can
        # override it with a closure in __init__
        pass


class BatchProcessorFactory(BaseFactory[BaseBatchProcessor]):
    pass


@BatchProcessorFactory.register("binary")
class BinaryBatchProcessor(BaseBatchProcessor):
    is_binary = True
    extension = "bin"

    SYNC_WORD = 0xA5
    FORMAT_VERSION = 0x01  # Our Reserved/Version byte

    # Payload Type Mapping
    TYPE_DATA = 0x01
    TYPE_STATUS = 0x02
    TYPE_ERROR = 0x03

    def __init__(self):
        self._buffer = BytesIO()
        self.current_size = self._buffer.tell

        # Pre-compile the structure (16 bytes total)
        # < : Little-endian
        # B, B, B, B : Sync, Type, Version, Reserved (1 byte each)
        # I : Payload Length (4 bytes)
        # Q : Timestamp NS (8 bytes)
        header_struct = Struct("<BBBBIQ")

        # Localize methods/constants for speed
        pack_into_buffer = header_struct.pack
        write = self._buffer.write
        sync = self.SYNC_WORD
        msg_type = self.TYPE_DATA
        version = self.FORMAT_VERSION

        def process(batch):
            for ts_ns, data in batch:
                # Expecting (ts_ns, data)

                write(
                    pack_into_buffer(
                        sync,
                        msg_type,
                        version,
                        0,  # flags / reserved
                        len(data),
                        ts_ns,
                    )
                )
                write(data)

        self.process = process

    def get_data(self, clear: bool = True) -> bytes:
        """
        Returns the accumulated bytes.
        If clear=True, the internal buffer is reset.
        """
        data = self._buffer.getvalue()
        if clear:
            self.clear()
        return data

    def clear(self):
        """Resets the buffer and the pointer."""
        self._buffer.seek(0)
        self._buffer.truncate(0)


@BatchProcessorFactory.register("log_row")
class LogRowBatchProcessor(BaseBatchProcessor):
    is_binary = False
    extension = "log"

    def __init__(self):
        super().__init__()
        self.time_formatter = ISO8601TimestampFormatter()
        self.format_time = self.time_formatter.format
        self._buffer = StringIO()
        self.current_size = self._buffer.tell
        self.bake()

    def bake(self):
        format_time = self.format_time

        def fast_format(row: "LogRow"):
            # 2026-03-13T12:47:01.402Z INFO C3X module_name: Message text
            return (
                f"{format_time(row.timestamp_ns)} {row.level.name_log} "
                f"{row.module.device} {row.module.name}: {row.message}\n"
            )

        self.format = fast_format

    def process(self, batch: list):
        """
        Formats and appends a batch of LogRows to the internal string buffer.
        """
        for row in batch:
            self._buffer.write(self.format(row))

    def get_data(self, clear: bool = True) -> str:
        """
        Returns the accumulated log string.
        If clear=True, the internal buffer is reset.
        """
        data = self._buffer.getvalue()
        if clear:
            self.clear()
        return data

    def clear(self):
        """Resets the string buffer."""
        self._buffer.seek(0)
        self._buffer.truncate(0)

    format: Callable[["LogRow"], str]

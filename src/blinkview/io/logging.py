# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import logging
    from logging import LogRecord

from ..core.configurable import configuration_property
from .BaseReader import BaseReader, DeviceFactory


def _get_logger_handler_class():
    import logging

    class LoggerHandler(logging.Handler):
        """
        A high-performance in-memory logging handler.
        Intercepts LogRecords and places them directly into a thread-safe Queue.
        """

        def __init__(self, output_queue: queue.Queue):
            super().__init__()
            self.output_queue = output_queue

        def emit(self, record: LogRecord):
            try:
                # Pass the raw (fake_timestamp, record) tuple
                self.output_queue.put((int(record.created * 1_000_000_000), record))
            except Exception:
                self.handleError(record)

    return LoggerHandler


@DeviceFactory.register("logging")
@configuration_property(
    "level",
    type="string",
    default="INFO",
    enum=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    description="Sets the global root logger level.",
)
@configuration_property(
    "maxlen", type="integer", default=1000, description="The maximum number of LogRecords to batch before flushing."
)
@configuration_property(
    "delay",
    type="integer",
    default=100,
    description="The maximum time (in milliseconds) to hold records before flushing a batch.",
)
class LoggerReader(BaseReader):
    __doc__ = """Universal in-memory ingestion source.

Attaches a custom handler directly to the Python root logger. 
Captures all application logs natively and batches the raw LogRecord 
objects downstream based on count or time.
"""

    maxlen: int
    delay: int
    level: str

    def __init__(self):
        super().__init__()
        self._queue = queue.Queue()
        self.handler = _get_logger_handler_class(self._queue)

    def run(self):
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        delay_ns = int(self.delay * 1_000_000)
        maxlen = self.maxlen

        import logging

        # Attach to the global Root Logger
        root_logger = logging.getLogger()
        root_logger.addHandler(self.handler)

        # Convert string "INFO" to logging.INFO (20), etc.
        numeric_level = getattr(logging, self.level.upper(), logging.INFO)
        root_logger.setLevel(numeric_level)

        logger.info("Starting Global Logger Reader Thread (Attached to Root)")

        batch = []
        last_flush_time = time_ns()
        batch_size = 0

        def flush():
            nonlocal batch, batch_size, last_flush_time
            if batch:
                last_flush_time = time_ns()
                self.distribute(batch)
                batch = []
                batch_size = 0

        try:
            while not stop_is_set():
                now = time_ns()
                try:
                    item = self._queue.get(timeout=0.05)
                    batch.append(item)
                    batch_size += 1

                    if batch_size >= maxlen:
                        flush()
                        continue

                except queue.Empty:
                    pass

                if batch and (now - last_flush_time >= delay_ns):
                    flush()

        finally:
            # Crucial Cleanup: Detach from the Root Logger on exit
            flush()
            root_logger.removeHandler(self.handler)
            logger.info("Global Logger Reader Thread stopped and detached.")

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Optional

from ..utils.throughput import Speedometer
from .base_daemon import BaseDaemon
from .batch_queue import BatchQueue
from .configurable import configuration_factory, configuration_property, override_property
from .factory import BaseFactory
from .limits import CENTRAL_STORAGE_BUFFER_SIZE_MB, CENTRAL_STORAGE_MAX_PIECES, CENTRAL_STORAGE_MAXLEN
from .numpy_log import (
    CircularLogPool,
)


@configuration_factory("central")
@override_property("enabled", default=True, hidden=True)
class BaseCentralStorage(BaseDaemon):
    def __init__(self):
        super().__init__()


class CentralFactory(BaseFactory[BaseCentralStorage]):
    pass


@CentralFactory.register("default")
@configuration_property(
    "maxlen",
    type="integer",
    default=CENTRAL_STORAGE_MAXLEN,
    description="Maximum number of log entries to keep in memory",
    ui_order=10,
)
@configuration_property(
    "max_pieces",
    type="integer",
    default=CENTRAL_STORAGE_MAX_PIECES,
    description="Maximum number of pieces in the circular log pool",
    ui_order=11,
)
@configuration_property(
    "buffer_size_mb",
    type="integer",
    default=CENTRAL_STORAGE_BUFFER_SIZE_MB,
    description="Total in memory = max_pieces * buffer_size_mb",
    ui_order=12,
)
@override_property(
    "logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "log_row"}}, ui_order=20
)
class CentralStorage(BaseCentralStorage):
    maxlen: int
    max_pieces: int
    buffer_size_mb: int

    def __init__(self):
        super().__init__()
        self.name = "central"
        self.input_queue = BatchQueue()  # messages that have not yet been pushed to subscribers

        self.put = self.input_queue.put

        self.log_pool: Optional[CircularLogPool] = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        if self.log_pool is None:
            buffer_bytes = self.buffer_size_mb * 1024 * 1024
            self.log_pool = CircularLogPool(
                self.shared.array_pool, max_pieces=self.max_pieces, final_buffer_bytes=buffer_bytes
            )
        else:
            # Runtime dynamic updates
            self.log_pool.update_max_pieces(self.max_pieces)

            new_bytes = self.buffer_size_mb * 1024 * 1024
            self.log_pool.update_final_buffer_bytes(new_bytes)

        return changed

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        get = self.input_queue.get

        speedometer = Speedometer(logger=self.logger.child("stats"))

        while not stop_is_set():
            # we need to push messages to subscribers here, but for now we just keep them in the log

            try:
                batch = get(timeout=120)
                if batch is None:
                    continue

                with batch:
                    # print(f"[CENTRAL] Received batch of {len(entry)} entries.")
                    # print(f"[Central] batch={batch}")
                    # print(f"[central] batch={batch}")

                    speedometer.batch(batch)

                    self.log_pool.batch_append(batch)

                    self.distribute(batch)

            except Exception as e:
                self.logger.exception("fcked", e)

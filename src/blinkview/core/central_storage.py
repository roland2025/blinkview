# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from itertools import count
from time import sleep

from ..storage.file_logger import LogRow
from ..utils.log_filter import LogFilter
from .base_daemon import BaseDaemon
from .batch_queue import BatchQueue
from .batched_logrows import BatchedLogRows
from .configurable import configuration_factory, configuration_property, override_property
from .factory import BaseFactory
from .limits import BATCH_QUEUE_MAXLEN, CENTRAL_STORAGE_MAXLEN
from .numpy_log import CircularLogPool


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
@override_property(
    "logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "log_row"}}, ui_order=20
)
class CentralStorage(BaseCentralStorage):
    maxlen: int

    def __init__(self):
        super().__init__()
        self.name = "central"

        self.input_queue = BatchQueue()  # messages that have not yet been pushed to subscribers
        self.sequence = 1

        self.put = self.input_queue.put

        self.log_pool = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        if changed:
            buffer_size_mb = 1
            avg_msg_len = 32
            segments = buffer_size_mb * 1024 * 1024 // avg_msg_len
            if self.log_pool is None:
                self.log_pool = CircularLogPool(self.shared.array_pool, 10, segments, buffer_size_mb)

        return changed

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        get = self.input_queue.get
        sequence = self.sequence

        while not stop_is_set():
            # we need to push messages to subscribers here, but for now we just keep them in the log
            batch = get(timeout=0.2)
            if batch is None:
                continue

            with batch:
                # print(f"[CENTRAL] Received batch of {len(entry)} entries.")

                for seq_id, item in enumerate(batch, start=sequence):
                    item: LogRow
                    item.seq = seq_id
                    item.module.latest_row = item

                    # append(self, ts_ns: int, level: int, module: int, device: int, seq: int, msg_bytes: bytes):
                    self.log_pool.append(
                        item.timestamp_ns,
                        item.level.value,
                        item.module.id,
                        item.module.device.id,
                        seq_id,
                        item.message.encode(),
                    )

                # Increment the sequence tracker by the size of the batch
                sequence += len(batch)

                self.distribute(batch)

        self.sequence = sequence

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
from .configurable import configuration_factory, configuration_property, override_property
from .factory import BaseFactory
from .limits import BATCH_QUEUE_MAXLEN, CENTRAL_STORAGE_MAXLEN


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

        self._msg_log = BatchQueue(self.maxlen)

        self.input_queue = BatchQueue()  # messages that have not yet been pushed to subscribers
        self.sequence = 1

        self.put = self.input_queue.put

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
                    item.seq = seq_id
                    item.module.latest_row = item

                # Increment the sequence tracker by the size of the batch
                sequence += len(batch)
                # for item in batch:
                #     item: LogRow
                #     # assign a unique, incrementing ID to each log entry as it comes into central storage.
                #     # This can be used by subscribers to track which entries they've already seen and ensure they process each entry exactly once.
                #     item.seq = get_next_id()
                #     item.module.latest_row = item

                self._msg_log.put(batch.copy())

                self.distribute(batch)

        self.sequence = sequence

    def get_rows(self, log_filter: LogFilter, total: int, after_seq: int = -1):
        """Returns the most recent log entries that match the given filter, up to the specified total."""
        # We can optimize this by iterating backwards through the log and stopping once we've collected enough entries.
        matching_batches = []
        total_counted = 0
        with self._msg_log._lock:
            for batch in reversed(self._msg_log._deque):
                filtered_batch = log_filter.filter_batch(batch, after_seq=after_seq)
                if filtered_batch:
                    matching_batches.append(filtered_batch)
                    total_counted += len(filtered_batch)
                    if total_counted >= total:
                        break

        # Flatten the list of batches and return only the most recent 'total' entries
        # reverse the order back to chronological
        matching_entries = [entry for batch in reversed(matching_batches) for entry in batch]
        return matching_entries[-total:]

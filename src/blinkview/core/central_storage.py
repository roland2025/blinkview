# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep

from .base_configurable import configuration_property, configuration_factory, override_property
from .base_daemon import BaseDaemon
from .batch_queue import BatchQueue
from .factory import BaseFactory
from ..storage.file_logger import LogRow
from itertools import count

from ..utils.log_filter import LogFilter


@configuration_factory("central")
@override_property("enabled", default=True, hidden=True)
class BaseCentralStorage(BaseDaemon):
    def __init__(self):
        super().__init__()


class CentralFactory(BaseFactory[BaseCentralStorage]):
    pass


@CentralFactory.register("default")
@configuration_property("max_rows", type="integer", default=1_000_000, description="Maximum number of log entries to keep in memory", ui_order=10)
@override_property("logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "log_row"}}, ui_order=20)
class CentralStorage(BaseCentralStorage):

    max_rows: int

    def __init__(self):
        super().__init__()
        self.name = "central"

        self._msg_log = BatchQueue(self.max_rows)

        self._unpushed = BatchQueue()  # messages that have not yet been pushed to subscribers
        self._id_generator = count(start=1)

        self.put = self._unpushed.put

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        get = self._unpushed.get
        get_next_id = self._id_generator.__next__

        while not stop_is_set():
            # we need to push messages to subscribers here, but for now we just keep them in the log
            entry = get(timeout=0.2)
            if entry is None:
                continue
            # print(f"[CENTRAL] Received batch of {len(entry)} entries.")

            for item in entry:
                item: LogRow
                # assign a unique, incrementing ID to each log entry as it comes into central storage.
                # This can be used by subscribers to track which entries they've already seen and ensure they process each entry exactly once.
                item.seq = get_next_id()
                item.module.latest_row = item

            self._msg_log.put(entry)

            self.distribute(entry)

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


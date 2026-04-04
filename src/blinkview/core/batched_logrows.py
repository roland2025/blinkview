# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.limits import BATCH_QUEUE_MAXLEN
from blinkview.utils.log_filter import LogFilter


class BatchedLogRows(BatchQueue):
    def __init__(self, maxlen=BATCH_QUEUE_MAXLEN):
        super().__init__(maxlen)

    def get_batches(self, log_filter: LogFilter, total: int, start_seq: int = -1):
        """Returns the most recent log entries that match the given filter, up to the specified total."""
        # We can optimize this by iterating backwards through the log and stopping once we've collected enough entries.
        matching_batches = []
        total_counted = 0
        with self._lock:
            latest_seq = self._deque[-1][-1].seq if self._deque else 0
            # go through chronological order
            for batch in self._deque:
                first_seq_in_batch = batch[0].seq
                if first_seq_in_batch > start_seq:
                    filtered_batch = log_filter.filter_batch(batch, after_seq=start_seq)

                    if not filtered_batch:
                        continue

                    matching_batches.append(filtered_batch)
                    total_counted += len(filtered_batch)
                    if total_counted >= total:
                        break

        return matching_batches, latest_seq

    def get_telemetry_batch(self, pool_acquire, module, start_seq: int, target_cols: int = 0):
        """Returns a PooledTelemetryBatch context manager containing the arrays."""
        with self._lock:
            if not self._deque or self._total_objects == 0:
                return None

            latest_seq = self._deque[-1][-1].seq
            batch_container = None
            idx = 0

            for batch in self._deque:
                if batch[-1].seq <= start_seq:
                    continue

                for row in batch:
                    if row.seq > start_seq and row.module == module:
                        vals = row.get_values()
                        if not vals:
                            continue

                        if target_cols <= 0:
                            target_cols = len(vals)

                        if len(vals) == target_cols:
                            # Lazy initialization of the container
                            if batch_container is None:
                                max_possible = self._total_objects
                                batch_container = pool_acquire(max_possible, target_cols)

                            batch_container.base_times[idx] = row.timestamp
                            batch_container.base_values[idx] = vals
                            idx += 1

            if idx == 0 or batch_container is None:
                if batch_container is not None:
                    batch_container.release()
                return None

            # Prepare the zero-copy views and metadata
            batch_container.set_views(idx, latest_seq)
            return batch_container

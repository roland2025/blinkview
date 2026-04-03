# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from bisect import bisect_right
from operator import attrgetter

from blinkview.core.base_reorder import BaseReorder, ReorderFactory
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.log_row import LogRow


@ReorderFactory.register("default")
class Reorder(BaseReorder):
    def __init__(self):
        super().__init__()

        self.input_queue = BatchQueue()

        self.put = self.input_queue.put

    def run(self):
        buffer = []
        time_ns = self.shared.time_ns
        delay_ns = self.delay * 1_000_000  # Convert milliseconds to nanoseconds
        distribute = self.distribute
        get_many = self.input_queue.get_many
        get = self.input_queue.get
        bisect_right_ = bisect_right

        # Fast C-level attribute lookup for the sort and bisect
        # Replace 'timestamp_ns' with the exact name of the attribute on your LogRow
        get_ts = attrgetter("timestamp_ns")

        pool_acquire = self.shared.pool.get(tag="LogRows").acquire

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            now = time_ns()

            # DYNAMIC TIMEOUT
            if buffer:
                # buffer[0] is guaranteed to be the oldest item because we sort it
                wait_ns = (buffer[0].timestamp_ns + delay_ns) - now
                timeout_sec = wait_ns / 1_000_000_000.0 if wait_ns > 0 else 0.0
            else:
                # Idle state: sleep until data arrives or 100ms passes
                timeout_sec = 0.1

            # GREEDY FETCH
            # The OS blocks this thread perfectly based on the oldest item's needs

            # batches = []
            batch = get(timeout=timeout_sec)
            # print(f"[Reorder] got batch: {batch} with timeout: {timeout_sec:.3f}s")
            if batch is not None:
                with batch:
                    buffer.extend(batch)
                # batches.append(batch)
                # batches = get_many(timeout=timeout_sec)

                # if batches:
                #     for in_batch in batches:
                #         buffer.extend(in_batch)
                #         in_batch.release()

                # TIMSORT
                # We only sort if new data arrived.
                # Timsort is brutally fast here because the list is already 99% sorted.
                buffer.sort(key=get_ts)

            if not buffer:
                continue

            # RECALCULATE TIME
            # We must check the clock again because get_many() might have blocked
            now = time_ns()
            cutoff = now - delay_ns

            # C-LEVEL BINARY SEARCH (Python 3.10+)
            # Finds exactly where the mature items end and the delayed items begin
            split_idx = bisect_right_(buffer, cutoff, key=get_ts)

            # SLICE AND DISTRIBUTE
            if split_idx > 0:
                # Acquire a batch from the manager
                with pool_acquire() as out_batch:
                    _append = out_batch.append

                    # Move references from our buffer to the new ReusableBatch
                    # This is a very fast operation (just pointer copying)
                    for i in range(split_idx):
                        _append(buffer[i])

                    # Remove from buffer (Slice is faster than 'del' for large chunks)
                    buffer = buffer[split_idx:]

                    # 5. DISTRIBUTE AND RETAIN
                    # We distribute the batch. The pool logic handles the ref count.
                    distribute(out_batch)

        if buffer:
            with pool_acquire() as out_batch:
                for dat in buffer:
                    out_batch.append(dat)
                distribute(out_batch)

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from heapq import heappop, heappush

from blinkview.core.base_reorder import BaseReorder, ReorderFactory
from blinkview.core.batch_queue import BatchQueue


# @ReorderFactory.register("heapq")
class ReorderBuffer(BaseReorder):
    def __init__(self):
        super().__init__()

        self.input_queue = BatchQueue()
        self.heap = []

        self.put = self.input_queue.put

    def run(self):

        heap = self.heap
        time_ns = self.shared.time_ns
        delay_ns = self.delay * 1_000_000  # Convert milliseconds to nanoseconds
        out_batch = []
        append = out_batch.append
        get = self.input_queue.get
        distribute = self.distribute
        # Localize heap functions for speed
        push = heappush
        pop = heappop

        stop_is_set = self._stop_event.is_set
        while not stop_is_set():
            # Peek at the heap to see how long we CAN wait
            # We use a fresh time here just for the timeout calculation
            loop_start = time_ns()

            if heap:
                # Target: Oldest item + delay window
                wait_ns = (heap[0].timestamp_ns + delay_ns) - loop_start
                timeout_sec = max(0.0, wait_ns / 1_000_000_000.0)
            else:
                timeout_sec = 0.1

            # Block for new data
            in_batch = get(timeout=timeout_sec)

            # CRITICAL: Update 'now' AFTER the block
            # This is the real "current time" for the popping logic
            now = time_ns()

            if in_batch:
                # print(f"[REORDER] Received batch of {len(in_batch)} items for reordering")
                for item in in_batch:
                    push(heap, item)

            # Drain the "Mature" items
            # An item is mature if it has lived in our buffer for at least delay_ns
            out_batch = []
            append = out_batch.append
            while heap and (heap[0].timestamp_ns + delay_ns) <= now:
                append(pop(heap))

            if out_batch:
                distribute(out_batch)

        # On exit, flush all remaining items
        while heap:
            append(pop(heap))

        if out_batch:
            distribute(out_batch)

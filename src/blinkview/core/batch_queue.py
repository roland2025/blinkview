# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import itertools
from collections import deque
from threading import Condition, Lock
from time import time

from blinkview.core.limits import BATCH_QUEUE_MAXLEN


class BatchQueue:
    def __init__(self, maxlen=BATCH_QUEUE_MAXLEN):
        self.maxlen = maxlen
        self._deque = deque()  # Stores the batches (lists of objects)
        self._total_objects = 0  # Tracks the actual number of individual objects

        # Statistics members
        self.pushed = 0  # Total individual objects added over time
        self.popped = 0  # Total individual objects retrieved over time
        self.dropped = 0  # Added to track overflow

        # Condition is safer and faster than Event + Lock for queues
        self._lock = Lock()
        self._not_empty = Condition(self._lock)

    def put(self, batch):
        """
        Puts a batch (list of objects) into the queue.
        Drops the oldest batches if total objects exceed maxlen.
        """
        batch_size = len(batch)
        if batch_size == 0:
            return

        with self._not_empty:
            retain = getattr(batch, "retain", None)
            if retain is not None:
                retain()

            self._deque.append(batch)
            self._total_objects += batch_size

            self.pushed += batch_size

            # Drop oldest batches until we are under maxlen.
            # We enforce len(self._deque) > 1 so we don't drop the batch
            # we *just* added, even if it alone exceeds maxlen.
            while self._total_objects > self.maxlen and len(self._deque) > 1:
                oldest_batch = self._deque.popleft()
                drop_count = len(oldest_batch)
                self._total_objects -= drop_count
                self.dropped += drop_count
                release = getattr(oldest_batch, "release", None)
                if release is not None:
                    release()

                # print(
                #     f"[BatchQueue] Dropped batch of size {len(oldest_batch)} to maintain maxlen. Total objects now: {self._total_objects}"
                # )

            self._not_empty.notify()

    def get(self, timeout=None):
        """Returns the oldest batch."""
        with self._not_empty:
            if self._total_objects == 0:
                if timeout is None:
                    return None
                # Check our total objects tracker instead of the deque length
                if not self._not_empty.wait_for(lambda: self._total_objects > 0, timeout):
                    return None

            batch = self._deque.popleft()
            batch_len = len(batch)
            self._total_objects -= batch_len
            self.popped += batch_len
            return batch

    def get_many(self, timeout=None):
        """Returns ALL objects currently in the queue, flattened."""
        with self._not_empty:
            if self._total_objects == 0:
                if timeout is None:
                    return None
                # Check our total objects tracker
                if not self._not_empty.wait_for(lambda: self._total_objects > 0, timeout):
                    return None

            # Flatten the nested batches into a single list of objects instantly
            count = self._total_objects
            all_objects = list(itertools.chain.from_iterable(self._deque))

            self._deque.clear()
            self._total_objects = 0
            self.popped += count

            return all_objects

    def get_nowait(self):
        """Returns the oldest batch immediately, or None if empty."""
        with self._not_empty:
            if self._total_objects > 0:
                batch = self._deque.popleft()

                batch_len = len(batch)
                self._total_objects -= batch_len
                self.popped += batch_len

                return batch
        return None

    def get_stats(self):
        """Returns snapshot with timestamp for rate calculation."""
        with self._lock:
            # Returning a dict makes it easier to manage in the stats loop
            return {
                "total": self._total_objects,
                "maxlen": self.maxlen,
                "pushed": self.pushed,
                "popped": self.popped,
                "dropped": self.dropped,
                "now": time(),
            }

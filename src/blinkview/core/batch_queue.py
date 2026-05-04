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
        self._shutdown = False

    def put(self, batch):
        """Adds a batch to the queue with localized lookups for performance."""
        # Localize built-ins and attributes before the loop/logic
        _len = len
        _getattr = getattr

        if not (batch_size := _len(batch)):
            return

        if (retain := _getattr(batch, "retain", None)) is not None:
            retain()

        dropped_batches = None

        with self._not_empty:
            dq = self._deque
            dq.append(batch)

            self._total_objects += batch_size
            self.pushed += batch_size

            while _len(dq) > 1:
                # Reusing _len here saves a global lookup on every iteration
                oldest_batch_size = _len(dq[0])
                if (self._total_objects - oldest_batch_size) >= self.maxlen:
                    oldest_batch = dq.popleft()
                    self._total_objects -= oldest_batch_size
                    self.dropped += oldest_batch_size

                    if (_ := _getattr(oldest_batch, "release", None)) is not None:
                        if dropped_batches is None:
                            dropped_batches = []
                        dropped_batches.append(oldest_batch)
                else:
                    break

            self._not_empty.notify()

        if dropped_batches:
            for b in dropped_batches:
                b.release()

    def get(self, timeout=None):
        """Returns the oldest batch."""
        _not_empty = self._not_empty

        with _not_empty:
            if self._total_objects == 0:
                if timeout is None:
                    return None
                # Check our total objects tracker instead of the deque length
                if not _not_empty.wait_for(lambda: self._total_objects > 0 or self._shutdown, timeout):
                    return None

            # print(f"batch queue get: {self._total_objects} {self._deque}")

            # If the deque is empty here, it means we woke up due to _shutdown
            if not (dq := self._deque):
                return None

                # 3. Use the localized 'dq' for the pop
            batch = dq.popleft()

            batch_len = len(batch)
            self._total_objects -= batch_len
            self.popped += batch_len
            return batch

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

    def clear(self):
        """Clears all batches, releases memory, and resets the shutdown state."""
        with self._not_empty:
            dq = self._deque  # Localize for the loop
            while dq:
                batch = dq.pop()
                # Check and bind 'release' in one shot
                if (release := getattr(batch, "release", None)) is not None:
                    release()

            self._total_objects = 0
            self._shutdown = False  # Reset so it's ready for a fresh start

    def __getitem__(self, index):
        """
        Returns the batch at the specified index.
        Supports negative indexing (e.g., bq[-1] for the newest batch).
        Lock-free: assumes no concurrent mutations.
        """
        return self._deque[index]

    def __iter__(self):
        """
        Returns the raw deque iterator.
        Lock-free: assumes no concurrent mutations.
        """
        return iter(self._deque)

    def __len__(self):
        """Returns the number of batches currently in the queue."""
        # len() on a deque is O(1) and atomic in CPython/3.14t
        return len(self._deque)

    def trigger_shutdown(self):
        """Instantly wakes up any threads blocking on get()."""
        with self._not_empty:
            self._shutdown = True
            self._not_empty.notify_all()

    def reset_shutdown(self):
        """Resets the shutdown flag, allowing get() to resume blocking."""
        with self._not_empty:
            self._shutdown = False

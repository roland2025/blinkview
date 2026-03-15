# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from collections import deque
from threading import Lock, Event
from typing import List, Any


class BatchQueueOld:
    def __init__(self, maxlen=1000):
        self._deque = deque(maxlen=maxlen)
        self._lock = Lock()
        self._event = Event()

    def put_many(self, items):
        with self._lock:
            self._deque.extend(items)  # atomic inside lock
        self._event.set()

    def put(self, item):
        with self._lock:
            self._deque.append(item)
        self._event.set()

    def get(self, timeout=None):
        with self._lock:
            if self._deque:
                return self._deque.popleft()

        if timeout is not None:
            self._event.wait(timeout)
            self._event.clear()
            with self._lock:
                if self._deque:
                    return self._deque.popleft()
        return None

    def get_nowait(self):
        with self._lock:
            if self._deque:
                return self._deque.popleft()
        return None

    def get_many(self, timeout=None):
        # TODO: horribly slow
        with self._lock:
            if self._deque:
                items = list(self._deque)
                self._deque.clear()
                return items

        if timeout is not None:
            self._event.wait(timeout)
            self._event.clear()
            with self._lock:
                if self._deque:
                    items = list(self._deque)
                    self._deque.clear()
                    return items

        return None


from collections import deque
from threading import Lock, Condition
import itertools


class BatchQueue:
    def __init__(self, maxlen=100_000):
        self.maxlen = maxlen
        self._deque = deque()  # Stores the batches (lists of objects)
        self._total_objects = 0  # Tracks the actual number of individual objects

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
            self._deque.append(batch)
            self._total_objects += batch_size

            # Drop oldest batches until we are under maxlen.
            # We enforce len(self._deque) > 1 so we don't drop the batch
            # we *just* added, even if it alone exceeds maxlen.
            while self._total_objects > self.maxlen and len(self._deque) > 1:
                oldest_batch = self._deque.popleft()
                self._total_objects -= len(oldest_batch)

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
            self._total_objects -= len(batch)
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
            all_objects = list(itertools.chain.from_iterable(self._deque))

            self._deque.clear()
            self._total_objects = 0

            return all_objects

    def get_nowait(self):
        """Returns the oldest batch immediately, or None if empty."""
        with self._not_empty:
            if self._total_objects > 0:
                batch = self._deque.popleft()
                self._total_objects -= len(batch)
                return batch
        return None


class DrainableQueue:
    __slots__ = ('_lock', '_queue')

    def __init__(self):
        self._lock = Lock()
        self._queue: deque = deque()

    def put(self, item: Any):
        """Used by the Parser Thread to push a batch of logs."""
        with self._lock:
            self._queue.append(item)

    def put_many(self, items: List[Any]):
        """Even faster if the parser already has a list."""
        with self._lock:
            self._queue.extend(items)

    def drain(self) -> deque:
        """
        Used by the GUI/Reorder Thread.
        Locks ONCE, takes everything, and resets the queue instantly.
        """
        with self._lock:
            # 1. Grab the reference to the full queue
            full_queue = self._queue

            # 2. Replace it with a fresh, empty queue for the parsers
            self._queue = deque()

            # 3. Lock releases immediately
            return full_queue


class BoundedDrainableQueue:
    __slots__ = ('_lock', '_queue', 'maxlen', 'current_count', 'dropped_count')

    def __init__(self, config: dict = None):
        self._lock = Lock()
        self.maxlen = 100_000
        # deque automatically drops the oldest item if it exceeds maxlen!
        self._queue: deque = deque()
        self.current_count = 0
        self.dropped_count = 0

        if config:
            self.apply_config(config)

    def apply_config(self, config: dict):
        with self._lock:
            self.resize(config.get("maxlen", self.maxlen))

    def resize(self, new_maxlen: int):
        with self._lock:
            # check if maxlen has changed
            if new_maxlen == self.maxlen:
                return

            self.maxlen = new_maxlen

            # check if the new maxlen is smaller than the current count
            if new_maxlen > self.maxlen:
                return

            # Shed until we fit the new limit
            while self.current_count > self.maxlen and self._queue:
                dropped = self._queue.popleft()
                self.current_count -= len(dropped)
                self.dropped_count += len(dropped)

    def put(self, batch: list):
        with self._lock:
            batch_size = len(batch)

            # 1. Check if the incoming batch is bigger than the entire queue
            if batch_size > self.maxlen:
                # Keep only the tail of the incoming batch
                batch = batch[-self.maxlen:]
                batch_size = self.maxlen

            # 2. Shed old batches until there is room for the new one
            while self.current_count + batch_size > self.maxlen and self._queue:
                old_batch = self._queue.popleft()
                old_batch_len = len(old_batch)
                self.current_count -= old_batch_len
                self.dropped_count += old_batch_len

            # 3. Add the new batch
            self._queue.append(batch)
            self.current_count += batch_size

    def drain(self) -> deque:
        with self._lock:
            full_queue = self._queue
            self._queue = deque(maxlen=self.maxlen)
            return full_queue

    def get_dropped_count(self) -> int:
        """Read and reset the dropped counter."""
        with self._lock:
            count = self.dropped_count
            self.dropped_count = 0
            return count

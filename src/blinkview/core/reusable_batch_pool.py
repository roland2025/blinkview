# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from itertools import islice
from queue import Empty, SimpleQueue
from threading import Lock


class TimeDataEntry:
    __slots__ = ("time", "data")

    def __init__(self, t=0, d=b""):
        self.time = t
        self.data = d

    # Enables: ts, msg = entry
    def __iter__(self):
        yield self.time
        yield self.data

    # Enables: entry[0], entry[1]
    def __getitem__(self, index):
        if index == 0:
            return self.time
        if index == 1:
            return self.data
        raise IndexError

    def __len__(self):
        return 2

    def __repr__(self):
        return f"TimeDataEntry(time={self.time}, data={self.data})"

    def __str__(self):
        return f"TimeDataEntry(time={self.time}, data={self.data})"

    def set_data(self, t, d):
        """Internal setter used by ReusableBatch."""
        self.time = t
        self.data = d

    def clear(self):
        """Reset fields to allow GC of the data payload."""
        self.data = None


class ReusableBatch:
    __slots__ = "_items", "capacity", "size", "in_use", "append", "_pool", "_ref_count", "_lock", "_factory"

    def __init__(self, capacity: int, entry_factory=None, pool=None):
        capacity = capacity or 20
        self.capacity = capacity
        self.size = 0
        self.in_use = False
        self._pool: "BatchPool" = pool

        # Thread-safe ref counting
        self._ref_count = 0
        self._lock = Lock()

        self._factory = entry_factory

        # 1. Initialize items based on mode
        if entry_factory is not None:
            # PRE-ALLOCATED MODE: Mutation (Faster for custom entries)
            self._items = [entry_factory() for _ in range(capacity)]
            items = self._items
            setter_fn = entry_factory.set_data

            # Specialization Logic for Setters
            try:
                arg_count = setter_fn.__code__.co_argcount - 1
            except AttributeError:
                import inspect

                arg_count = len(inspect.signature(setter_fn).parameters) - 1

            if arg_count == 1:

                def _append(a):
                    try:
                        idx = self.size
                        setter_fn(items[idx], a)
                        self.size = idx + 1
                    except IndexError:
                        new = entry_factory()
                        setter_fn(new, a)
                        items.append(new)
                        self.size += 1
                        self.capacity += 1
            elif arg_count == 2:

                def _append(a, b):
                    try:
                        idx = self.size
                        setter_fn(items[idx], a, b)
                        self.size = idx + 1
                    except IndexError:
                        new = entry_factory()
                        setter_fn(new, a, b)
                        items.append(new)
                        self.size += 1
                        self.capacity += 1
            else:

                def _append(*args):
                    try:
                        idx = self.size
                        setter_fn(items[idx], *args)
                        self.size = idx + 1
                    except IndexError:
                        new = entry_factory()
                        setter_fn(new, *args)
                        items.append(new)
                        self.size += 1
                        self.capacity += 1
        else:
            # PASSIVE MODE: Direct Reference Storage (For ints, strings, etc.)
            self._items = [None] * capacity
            items = self._items

            def _append(val):
                try:
                    idx = self.size
                    items[idx] = val
                    self.size = idx + 1
                except IndexError:
                    items.append(val)
                    self.size += 1
                    self.capacity += 1

        self.append = _append

    def clear(self):
        """Clears all entries to prevent memory leaks while idle in the pool."""
        if self._factory is not None:
            # PRE-ALLOCATED MODE: Clear the internal objects
            items = self._items
            for i in range(self.size):
                items[i].clear()
        else:
            # PASSIVE MODE: Drop references to the objects themselves
            items = self._items
            for i in range(self.size):
                items[i] = None

        self.size = 0

    def retain(self):
        with self._lock:
            self._ref_count += 1
        return self

    def release(self):
        with self._lock:
            self._ref_count -= 1
            if self._ref_count > 0:
                return

            self.clear()
            self.in_use = False

        # If in pre-allocated mode, we keep the objects for reuse
        if self._pool:
            self._pool._return_to_pool(self)

    def __getitem__(self, index):
        # 1. Handle Slicing (e.g., batch[1:3])
        if isinstance(index, slice):
            start, stop, step = index.indices(self.size)
            return [self._items[i] for i in range(start, stop, step)]

        # 2. Handle Negative Indexing (e.g., batch[-1])
        if index < 0:
            index += self.size

        # 3. Bounds Checking
        if index < 0 or index >= self.size:
            raise IndexError(f"ReusableBatch index {index} out of range (size {self.size})")

        return self._items[index]

    def __len__(self):
        return self.size

    def __iter__(self):
        return islice(self._items, self.size)

    def __repr__(self):
        return f"{self.__class__.__name__}(capacity={self.capacity}, size={self.size})"

    __str__ = __repr__

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # This is where you trigger the return logic
        self.release()

    def copy(self):
        """Creates a shallow copy of the batch for independent use."""
        return self._items[: self.size]  # Copy only the active portion


class BatchPool:
    def __init__(self, batch_size, entry_factory, pool_size=None):
        self.batch_size = batch_size or 20
        self.entry_factory = entry_factory
        self._queue = SimpleQueue()

        for _ in range(pool_size or 20):
            # Create the initial fleet
            batch = ReusableBatch(capacity=batch_size, entry_factory=entry_factory, pool=self)
            self._queue.put(batch)

    def acquire(self) -> ReusableBatch:
        try:
            # Try to get an existing batch INSTANTLY
            batch = self._queue.get_nowait()
        except Empty:
            # If the pool is empty, don't wait!
            # Create a new batch immediately using the learned size.
            batch = ReusableBatch(self.batch_size, self.entry_factory, pool=self)

        # Always reset state for the new owner
        with batch._lock:
            batch._ref_count = 1
            batch.in_use = True

        return batch

    def _return_to_pool(self, batch: ReusableBatch):
        """Internal callback when ref_count hits 0."""

        # --- THE LEARNING STEP ---
        # If this batch grew during its lifetime, update the pool's
        # default size so future 'fallback' batches start larger.
        if batch.capacity > self.batch_size:
            self.batch_size = batch.capacity

        self._queue.put(batch)


class PoolManager:
    def __init__(self):
        self._pools = {}
        self._lock = Lock()

    def get(self, entry_factory=None, tag=None, batch_size=None, pool_size=None):
        """
        Retrieves an existing pool for the given entry class or creates a new one.
        Uses a specialized try-except fast-path for maximum throughput.
        """
        # 1. Fast Path: The "Optimistic" lookup
        # This will succeed 99.9% of the time after the system warms up.
        if tag is not None and not isinstance(tag, str):
            try:
                tag = tag.__class__.__name__
            except AttributeError:
                raise TypeError("tag must be str")

        pool_key = (entry_factory, tag)
        try:
            return self._pools[pool_key]
        except KeyError:
            # 2. Slow Path: Pool doesn't exist yet.
            # We use the lock to safely initialize the pool.
            with self._lock:
                # Double-check: another thread might have created it
                # while we were waiting for the lock.
                if pool_key not in self._pools:
                    pool = BatchPool(batch_size=batch_size, entry_factory=entry_factory, pool_size=pool_size)
                    self._pools[pool_key] = pool
                return pool

    def status(self):
        """Returns a snapshot of the current health of all managed pools."""
        # Using list comprehension for a thread-safe snapshot of items
        return {
            str(name): {"batch_size": p.batch_size, "in_queue": p._queue.qsize()}
            for name, p in list(self._pools.items())
        }

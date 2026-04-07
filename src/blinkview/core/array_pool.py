# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import threading
import time

import numpy as np


class PooledArrayHandle:
    """
    High-performance slotted handle.
    Directly holds the raw power-of-two slab.
    """

    __slots__ = ("_pool", "array", "bucket_key", "_refcount", "_lock")

    def __init__(self, pool, array, bucket_key):
        self._pool = pool
        self.array = array
        self.bucket_key = bucket_key
        self._refcount = 1
        self._lock = threading.Lock()

    def retain(self):
        with self._lock:
            if self._refcount == 0:
                raise RuntimeError("Cannot retain a released array.")
            self._refcount += 1
        return self

    def release(self):
        with self._lock:
            if self._refcount <= 0:
                return
            self._refcount -= 1
            if self._refcount == 0:
                # Return the full, un-sliced slab
                self._pool._put(self.bucket_key, self.array)
                self.array = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class NumpyArrayPool:
    __slots__ = ("min_bytes", "max_bytes", "lock", "buckets")

    def __init__(self, min_bytes=1024, max_bytes=1024 * 1024):
        self.min_bytes = min_bytes
        self.max_bytes = max_bytes
        self.lock = threading.Lock()
        self.buckets = {}

    def _calc_slab_size(self, size_in_bytes):
        """Always returns the next power of two, regardless of pool limits."""
        if size_in_bytes <= self.min_bytes:
            return self.min_bytes
        # Round up to next power of 2
        return 1 << (size_in_bytes - 1).bit_length()

    def acquire(self, element_count, dtype=np.uint8):
        """
        Acquires an array capable of holding at least `element_count` items.
        """
        dt = np.dtype(dtype)
        # Calculate the required bytes internally
        size_in_bytes = element_count * dt.itemsize

        slab_size = self._calc_slab_size(size_in_bytes)
        bucket_key = (slab_size, dt)

        base_arr = None

        # Try to fetch from pool if within limits
        if self.min_bytes <= slab_size <= self.max_bytes:
            with self.lock:
                bucket = self.buckets.get(bucket_key)
                if bucket:
                    _, base_arr = bucket.pop()  # LIFO

        # Always allocate a full power-of-two slab if no pooled array was found
        if base_arr is None:
            num_elements = slab_size // dt.itemsize
            base_arr = np.empty(num_elements, dtype=dt)

        return PooledArrayHandle(self, base_arr, bucket_key)

    def _put(self, bucket_key, array):
        """Thread-safe return to pool with aging timestamp."""
        slab_size, _ = bucket_key
        if self.min_bytes <= slab_size <= self.max_bytes:
            with self.lock:
                # Initialize bucket lazily to keep __init__ clean
                if bucket_key not in self.buckets:
                    self.buckets[bucket_key] = []
                self.buckets[bucket_key].append((time.monotonic(), array))

    def cleanup(self, max_age_seconds):
        """Evicts arrays that have been idle for too long."""
        now = time.monotonic()
        with self.lock:
            for key in list(self.buckets.keys()):
                bucket = self.buckets[key]
                # Filter bucket: keep only arrays returned within the time window
                self.buckets[key] = [(ts, arr) for ts, arr in bucket if (now - ts) < max_age_seconds]

    def get(self, element_count, dtype=np.uint8):
        """Sugar for context manager usage."""
        return self.acquire(element_count, dtype)

    def create(self, wrapper_class, *args, **kwargs):
        """
        Instantiates a high-level wrapper object (like PooledLogBatch),
        automatically injecting this pool as the first argument.
        """
        return wrapper_class(self, *args, **kwargs)

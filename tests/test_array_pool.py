# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool, PooledArrayHandle


class TestAcquire:
    def test_returns_array_of_requested_dtype_and_at_least_requested_size(self):
        pool = NumpyArrayPool()
        handle = pool.acquire(100, dtype=dtypes.TS_TYPE)
        try:
            assert handle.array.dtype == np.dtype(dtypes.TS_TYPE)
            assert len(handle.array) >= 100
        finally:
            handle.release()

    def test_defaults_to_byte_dtype(self):
        pool = NumpyArrayPool()
        handle = pool.acquire(16)
        try:
            assert handle.array.dtype == np.dtype(dtypes.BYTE)
        finally:
            handle.release()

    def test_small_request_is_padded_up_to_min_bytes(self):
        pool = NumpyArrayPool(min_bytes=1024)
        # 4 uint64 elements = 32 bytes, far under min_bytes -> slab is padded to min_bytes.
        handle = pool.acquire(4, dtype=dtypes.UINT64)
        try:
            assert len(handle.array) * handle.array.itemsize == 1024
        finally:
            handle.release()

    def test_request_rounds_up_to_next_power_of_two(self):
        pool = NumpyArrayPool(min_bytes=8)
        # 5 bytes (dtype=BYTE) is above min_bytes(8)? no - 5 <= 8, so pick a size above it.
        handle = pool.acquire(20, dtype=dtypes.BYTE)  # 20 bytes -> rounds up to 32
        try:
            assert len(handle.array) == 32
        finally:
            handle.release()

    def test_get_is_sugar_for_acquire(self):
        pool = NumpyArrayPool()
        handle = pool.get(10, dtype=dtypes.ID_TYPE)
        try:
            assert isinstance(handle, PooledArrayHandle)
            assert handle.array.dtype == np.dtype(dtypes.ID_TYPE)
        finally:
            handle.release()


class TestPoolingAndReuse:
    def test_released_array_within_limits_is_reused_lifo(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        h1 = pool.acquire(64, dtype=dtypes.BYTE)
        arr1 = h1.array
        h1.release()

        h2 = pool.acquire(64, dtype=dtypes.BYTE)
        try:
            # Same bucket -> the exact same backing array should have been handed back.
            assert h2.array is arr1
        finally:
            h2.release()

    def test_acquiring_two_at_once_gets_distinct_arrays(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        h1 = pool.acquire(64, dtype=dtypes.BYTE)
        h2 = pool.acquire(64, dtype=dtypes.BYTE)
        try:
            assert h1.array is not h2.array
        finally:
            h1.release()
            h2.release()

    def test_different_dtypes_do_not_share_a_bucket(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        h1 = pool.acquire(8, dtype=dtypes.UINT64)  # 64 bytes
        arr1 = h1.array
        h1.release()

        h2 = pool.acquire(64, dtype=dtypes.BYTE)  # also 64 bytes, different dtype
        try:
            assert h2.array is not arr1
        finally:
            h2.release()

    def test_release_is_idempotent_and_only_returns_to_pool_once(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        h1 = pool.acquire(64, dtype=dtypes.BYTE)
        arr1 = h1.array
        h1.release()
        h1.release()  # second release on an already-released handle must be a no-op

        bucket_key = (64, np.dtype(dtypes.BYTE))
        assert len(pool.buckets[bucket_key]) == 1

        h2 = pool.acquire(64, dtype=dtypes.BYTE)
        try:
            assert h2.array is arr1
        finally:
            h2.release()

    def test_slabs_outside_pool_limits_are_not_retained(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=128)
        # Requesting 256 bytes exceeds max_bytes -> allocated fresh each time, never pooled.
        h1 = pool.acquire(256, dtype=dtypes.BYTE)
        arr1 = h1.array
        h1.release()

        assert pool.buckets == {}

        h2 = pool.acquire(256, dtype=dtypes.BYTE)
        try:
            assert h2.array is not arr1
        finally:
            h2.release()


class TestPooledArrayHandle:
    def test_context_manager_releases_on_exit(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        with pool.acquire(64, dtype=dtypes.BYTE) as handle:
            arr = handle.array
            assert arr is not None

        bucket_key = (64, np.dtype(dtypes.BYTE))
        assert len(pool.buckets[bucket_key]) == 1

    def test_retain_increments_refcount_and_delays_release(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        handle = pool.acquire(64, dtype=dtypes.BYTE)
        handle.retain()

        handle.release()
        assert pool.buckets == {}  # still held: refcount only dropped to 1

        handle.release()
        bucket_key = (64, np.dtype(dtypes.BYTE))
        assert len(pool.buckets[bucket_key]) == 1  # now actually returned

    def test_retain_after_full_release_raises(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        handle = pool.acquire(64, dtype=dtypes.BYTE)
        handle.release()

        with pytest.raises(RuntimeError):
            handle.retain()

    def test_capacity_reflects_full_slab_length(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        handle = pool.acquire(64, dtype=dtypes.BYTE)
        try:
            assert handle.capacity == 64
        finally:
            handle.release()

    def test_capacity_is_zero_after_release(self):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        handle = pool.acquire(64, dtype=dtypes.BYTE)
        handle.release()
        assert handle.capacity == 0


class TestCreate:
    def test_create_injects_pool_as_first_argument(self):
        pool = NumpyArrayPool()

        class Wrapper:
            def __init__(self, injected_pool, tag):
                self.injected_pool = injected_pool
                self.tag = tag

        wrapper = pool.create(Wrapper, "hello")
        assert wrapper.injected_pool is pool
        assert wrapper.tag == "hello"


class TestCleanup:
    def test_cleanup_evicts_entries_older_than_max_age(self, monkeypatch):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])

        handle = pool.acquire(64, dtype=dtypes.BYTE)
        handle.release()  # timestamped at t=1000.0

        bucket_key = (64, np.dtype(dtypes.BYTE))
        assert len(pool.buckets[bucket_key]) == 1

        current_time[0] = 1200.0  # 200s later
        pool.cleanup(max_age_seconds=100)

        assert pool.buckets[bucket_key] == []

    def test_cleanup_keeps_entries_within_max_age(self, monkeypatch):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])

        handle = pool.acquire(64, dtype=dtypes.BYTE)
        handle.release()

        current_time[0] = 1050.0  # 50s later
        pool.cleanup(max_age_seconds=100)

        bucket_key = (64, np.dtype(dtypes.BYTE))
        assert len(pool.buckets[bucket_key]) == 1

    @staticmethod
    def _fill(pool, current_time, entries):
        """Returns one slab per (time, bytes) entry to the pool, in order."""
        handles = [(t, pool.acquire(n, dtype=dtypes.BYTE)) for t, n in entries]
        for t, h in handles:
            current_time[0] = t
            h.release()

    def test_cleanup_cuts_only_the_expired_prefix_and_reports_it(self, monkeypatch):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [0.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])
        self._fill(pool, current_time, [(1000.0, 64), (1010.0, 64), (1090.0, 64)])

        current_time[0] = 1110.0
        assert pool.cleanup(max_age_seconds=100) == (2, 128)

        key = (64, np.dtype(dtypes.BYTE))
        assert [ts for ts, _ in pool.buckets[key]] == [1090.0]

    def test_cleanup_stops_at_max_bytes_and_finishes_on_later_calls(self, monkeypatch):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [0.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])
        self._fill(pool, current_time, [(1000.0 + i, 256) for i in range(5)])

        current_time[0] = 2000.0
        assert pool.cleanup(max_age_seconds=100, max_bytes=600) == (2, 512)
        key = (256, np.dtype(dtypes.BYTE))
        assert [ts for ts, _ in pool.buckets[key]] == [1002.0, 1003.0, 1004.0]  # oldest went first
        assert pool.cleanup(max_age_seconds=100, max_bytes=600) == (2, 512)
        assert pool.cleanup(max_age_seconds=100, max_bytes=600) == (1, 256)
        assert pool.buckets[key] == []
        assert pool.cleanup(max_age_seconds=100, max_bytes=600) == (0, 0)

    def test_cleanup_evicts_one_slab_even_when_it_is_larger_than_max_bytes(self, monkeypatch):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [0.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])
        self._fill(pool, current_time, [(1000.0, 1024), (1001.0, 1024)])

        current_time[0] = 2000.0
        assert pool.cleanup(max_age_seconds=100, max_bytes=100) == (1, 1024)
        assert pool.cleanup(max_age_seconds=100, max_bytes=100) == (1, 1024)

    def test_cleanup_budget_is_shared_across_buckets(self, monkeypatch):
        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [0.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])
        self._fill(pool, current_time, [(1000.0, 512), (1000.0, 512), (1000.0, 64), (1000.0, 64)])

        current_time[0] = 2000.0
        assert pool.cleanup(max_age_seconds=100, max_bytes=1024 + 64) == (3, 1088)
        assert sum(len(b) for b in pool.buckets.values()) == 1

    def test_cleanup_frees_the_arrays_after_releasing_the_lock(self, monkeypatch):
        import weakref

        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        current_time = [1000.0]
        monkeypatch.setattr(time, "monotonic", lambda: current_time[0])
        handles = [pool.acquire(64, dtype=dtypes.BYTE) for _ in range(3)]
        refs = [weakref.ref(h.array) for h in handles]
        for h in handles:
            h.release()

        lock_held_at_free = []
        real_sleep = time.sleep

        def sleep(s):
            lock_held_at_free.append(pool.lock.locked())
            real_sleep(s)

        monkeypatch.setattr(time, "sleep", sleep)
        current_time[0] = 2000.0
        pool.cleanup(max_age_seconds=100)

        assert all(r() is None for r in refs)
        assert lock_held_at_free == [False, False]  # one hand-off between each pair of frees

    def test_buckets_stay_sorted_by_return_time_under_concurrent_use(self):
        """cleanup's prefix cut relies on this: _put stamps under the lock, acquire pops the newest."""
        import random
        import threading

        pool = NumpyArrayPool(min_bytes=64, max_bytes=1024)
        errors = []

        def worker(seed):
            rng = random.Random(seed)
            held = []
            try:
                for _ in range(3000):
                    if held and rng.random() < 0.5:
                        held.pop(rng.randrange(len(held))).release()
                    else:
                        held.append(pool.acquire(rng.choice((64, 128)), dtype=dtypes.BYTE))
                    if rng.random() < 0.05:
                        pool.cleanup(max_age_seconds=0.001, max_bytes=256)
                for h in held:
                    h.release()
            except Exception as e:  # noqa: BLE001
                errors.append(e)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        for bucket in pool.buckets.values():
            stamps = [ts for ts, _ in bucket]
            assert stamps == sorted(stamps)

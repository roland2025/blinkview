# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import threading
import time

from blinkview.core.batch_queue import BatchQueue


class FakeBatch:
    """Duck-typed stand-in for a PooledLogBatch: has a length and retain/release hooks
    BatchQueue calls opportunistically (via getattr) on real batch objects."""

    def __init__(self, size):
        self._size = size
        self.retained = 0
        self.released = 0

    def __len__(self):
        return self._size

    def retain(self):
        self.retained += 1

    def release(self):
        self.released += 1


class TestPut:
    def test_appends_batch_and_updates_stats(self):
        q = BatchQueue(maxlen=100)
        q.put(FakeBatch(5))

        assert len(q) == 1
        assert q.pushed == 5
        assert q.get_stats()["total"] == 5

    def test_ignores_empty_batch(self):
        q = BatchQueue(maxlen=100)
        empty = FakeBatch(0)
        q.put(empty)

        assert len(q) == 0
        assert q.pushed == 0
        assert empty.retained == 0  # bails out before even checking retain

    def test_calls_retain_once_per_put(self):
        q = BatchQueue(maxlen=100)
        b = FakeBatch(2)
        q.put(b)
        assert b.retained == 1

    def test_batch_without_retain_or_release_does_not_error(self):
        q = BatchQueue(maxlen=100)
        q.put([1, 2, 3])  # plain list: no retain/release attributes
        assert len(q) == 1
        assert q.pushed == 3


class TestPutEviction:
    def test_does_not_evict_while_only_one_batch_present(self):
        q = BatchQueue(maxlen=2)
        huge = FakeBatch(100)
        q.put(huge)

        assert len(q) == 1
        assert q.dropped == 0  # sole batch retained even though it alone exceeds maxlen

    def test_does_not_evict_when_remaining_would_drop_below_maxlen(self):
        q = BatchQueue(maxlen=5)
        b1, b2 = FakeBatch(3), FakeBatch(3)
        q.put(b1)
        q.put(b2)  # total=6 > maxlen(5), but removing b1 alone would leave 3 < 5

        assert len(q) == 2
        assert q.dropped == 0
        assert b1.released == 0

    def test_evicts_oldest_once_remaining_still_meets_maxlen(self):
        q = BatchQueue(maxlen=5)
        b1, b2, b3 = FakeBatch(3), FakeBatch(3), FakeBatch(3)
        q.put(b1)
        q.put(b2)
        q.put(b3)  # total=9; removing b1(3) leaves 6 >= maxlen(5) -> b1 evicted

        assert len(q) == 2
        assert list(q) == [b2, b3]
        assert q.dropped == 3
        assert b1.released == 1
        assert q.get_stats()["total"] == 6


class TestGetNowait:
    def test_returns_none_when_empty(self):
        q = BatchQueue(maxlen=100)
        assert q.get_nowait() is None

    def test_pops_oldest_and_updates_stats(self):
        q = BatchQueue(maxlen=100)
        b1, b2 = FakeBatch(2), FakeBatch(3)
        q.put(b1)
        q.put(b2)

        popped = q.get_nowait()

        assert popped is b1
        assert q.popped == 2
        assert q.get_stats()["total"] == 3


class TestGet:
    def test_returns_none_immediately_when_empty_and_timeout_none(self):
        q = BatchQueue(maxlen=100)
        assert q.get(timeout=None) is None

    def test_transfers_ownership_without_releasing(self):
        q = BatchQueue(maxlen=100)
        b = FakeBatch(1)
        q.put(b)

        popped = q.get()

        assert popped is b
        assert b.released == 0  # caller now owns it; the queue must not release on its behalf

    def test_times_out_and_returns_none(self):
        q = BatchQueue(maxlen=100)
        start = time.monotonic()
        result = q.get(timeout=0.05)
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed >= 0.05

    def test_blocks_until_put_wakes_it(self):
        q = BatchQueue(maxlen=100)
        result = []

        def getter():
            result.append(q.get(timeout=2))

        t = threading.Thread(target=getter)
        t.start()
        time.sleep(0.05)  # let the getter start blocking
        q.put(FakeBatch(3))
        t.join(timeout=2)

        assert len(result) == 1
        assert len(result[0]) == 3


class TestShutdown:
    def test_trigger_shutdown_wakes_a_blocked_get(self):
        q = BatchQueue(maxlen=100)
        result = []

        def getter():
            result.append(q.get(timeout=5))

        t = threading.Thread(target=getter)
        t.start()
        time.sleep(0.05)
        start = time.monotonic()
        q.trigger_shutdown()
        t.join(timeout=2)
        elapsed = time.monotonic() - start

        assert result == [None]
        assert elapsed < 1  # woke immediately, did not wait out the 5s timeout

    def test_reset_shutdown_allows_blocking_again(self):
        q = BatchQueue(maxlen=100)
        q.trigger_shutdown()
        assert q.get(timeout=0.5) is None  # shutdown wakes it instantly

        q.reset_shutdown()
        start = time.monotonic()
        result = q.get(timeout=0.05)
        elapsed = time.monotonic() - start

        assert result is None
        assert elapsed >= 0.05  # genuinely waited this time, no stale shutdown flag


class TestClear:
    def test_releases_all_batches_and_resets_total(self):
        q = BatchQueue(maxlen=100)
        b1, b2 = FakeBatch(2), FakeBatch(3)
        q.put(b1)
        q.put(b2)

        q.clear()

        assert b1.released == 1
        assert b2.released == 1
        assert len(q) == 0
        assert q.get_stats()["total"] == 0

    def test_resets_shutdown_flag(self):
        q = BatchQueue(maxlen=100)
        q.trigger_shutdown()

        q.clear()

        assert q._shutdown is False


class TestDunderAccess:
    def test_len_reflects_batch_count_not_object_count(self):
        q = BatchQueue(maxlen=100)
        q.put(FakeBatch(10))
        q.put(FakeBatch(20))
        assert len(q) == 2  # 2 batches, not 30 objects

    def test_getitem_supports_negative_index(self):
        q = BatchQueue(maxlen=100)
        b1, b2 = FakeBatch(1), FakeBatch(2)
        q.put(b1)
        q.put(b2)

        assert q[0] is b1
        assert q[-1] is b2

    def test_iter_yields_batches_in_insertion_order(self):
        q = BatchQueue(maxlen=100)
        b1, b2 = FakeBatch(1), FakeBatch(2)
        q.put(b1)
        q.put(b2)

        assert list(q) == [b1, b2]


class TestStats:
    def test_get_stats_snapshot_contains_expected_fields(self):
        q = BatchQueue(maxlen=50)
        q.put(FakeBatch(4))

        stats = q.get_stats()

        assert stats["total"] == 4
        assert stats["maxlen"] == 50
        assert stats["pushed"] == 4
        assert stats["popped"] == 0
        assert stats["dropped"] == 0
        assert "now" in stats

    def test_default_maxlen_matches_limits_constant(self):
        from blinkview.core.limits import BATCH_QUEUE_MAXLEN

        assert BatchQueue().maxlen == BATCH_QUEUE_MAXLEN


class SizedFakeBatch(FakeBatch):
    """A FakeBatch that reports the memory it holds, as PooledLogBatch.allocated_bytes does."""

    def __init__(self, size, nbytes):
        super().__init__(size)
        self.nbytes = nbytes

    def allocated_bytes(self):
        return self.nbytes


class TestByteBound:
    def test_evicts_oldest_once_remaining_bytes_still_meet_max_bytes(self):
        """The finding's case: few rows, huge batches. The row bound alone never trips."""
        q = BatchQueue(maxlen=1_000_000, max_bytes=100)
        b1, b2, b3 = SizedFakeBatch(1, 60), SizedFakeBatch(1, 60), SizedFakeBatch(1, 60)
        q.put(b1)
        q.put(b2)  # 120 bytes, but dropping b1 would leave 60 < 100: kept
        assert list(q) == [b1, b2]

        q.put(b3)  # 180 bytes; dropping b1 leaves 120 >= 100: b1 goes

        assert list(q) == [b2, b3]
        assert q.dropped == 1
        assert b1.released == 1
        assert q.get_stats()["bytes"] == 120

    def test_never_evicts_the_newest_batch_however_big(self):
        q = BatchQueue(maxlen=1_000_000, max_bytes=100)
        b1, huge = SizedFakeBatch(1, 10), SizedFakeBatch(1, 10_000)
        q.put(b1)
        q.put(huge)

        assert list(q) == [huge]
        assert q.get_stats()["bytes"] == 10_000

    def test_get_get_nowait_and_clear_give_the_bytes_back(self):
        q = BatchQueue(maxlen=1_000_000, max_bytes=1_000)
        for _ in range(3):
            q.put(SizedFakeBatch(1, 100))

        q.get(timeout=0)
        assert q.get_stats()["bytes"] == 200
        q.get_nowait()
        assert q.get_stats()["bytes"] == 100
        q.clear()
        assert q.get_stats()["bytes"] == 0

    def test_a_batch_without_allocated_bytes_counts_as_zero(self):
        q = BatchQueue(maxlen=1_000_000, max_bytes=1)
        b1, b2 = FakeBatch(1), FakeBatch(1)
        q.put(b1)
        q.put(b2)

        assert list(q) == [b1, b2]
        assert q.get_stats()["bytes"] == 0

    def test_real_batches_count_their_whole_slabs(self):
        """A nearly empty PooledLogBatch costs its full allocation, not the bytes it uses."""
        from blinkview.core.array_pool import NumpyArrayPool
        from blinkview.core.numpy_batch_manager import PooledLogBatch

        pool = NumpyArrayPool(max_bytes=64 * 1024 * 1024)
        batch = pool.create(PooledLogBatch, 1024, 1024 * 1024, has_levels=True)
        batch.insert(1, 1, b"one row")
        slabs = (
            batch._ts_h.array.nbytes
            + batch._rx_ts_h.array.nbytes
            + batch._off_h.array.nbytes
            + batch._len_h.array.nbytes
            + batch._buf_h.array.nbytes
            + batch._lvl_h.array.nbytes
        )
        assert batch.allocated_bytes() == slabs >= 1024 * 1024

        q = BatchQueue(maxlen=1_000_000, max_bytes=slabs + 1)
        second = pool.create(PooledLogBatch, 1024, 1024 * 1024, has_levels=True)
        second.insert(1, 1, b"another row")
        q.put(batch)
        q.put(second)  # 2x slabs; dropping the first leaves slabs < max_bytes: kept
        assert q.get_stats()["bytes"] == 2 * slabs and q.dropped == 0

        third = pool.create(PooledLogBatch, 1024, 1024 * 1024, has_levels=True)
        third.insert(1, 1, b"third row")
        q.put(third)  # dropping the first leaves 2x slabs >= max_bytes: dropped

        assert list(q) == [second, third]
        assert batch._ref_count == 1  # the queue gave its reference back; the test holds the other
        q.clear()
        for b in (batch, second, third):
            b.release()
        assert batch.allocated_bytes() == 0


class TestByteBoundDefaults:
    def test_default_max_bytes_matches_limits_constant(self):
        from blinkview.core.limits import BATCH_QUEUE_MAX_BYTES

        assert BatchQueue().max_bytes == BATCH_QUEUE_MAX_BYTES

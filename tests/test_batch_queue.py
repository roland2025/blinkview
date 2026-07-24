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

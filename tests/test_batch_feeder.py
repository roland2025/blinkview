# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.io.batch_feeder import BatchFeeder
from blinkview.utils.throughput import ThroughputAutoTuner


class RecordingLogger(PrintLogger):
    def __init__(self, name):
        super().__init__(name)
        self.errors = []

    def error(self, msg, *args, **kwargs):
        self.errors.append(msg % args)


class FakeReader:
    """What BatchFeeder reads off a reader: delay, distribute, the loggers and the array pool.
    distribute() copies each batch's rows out, since the batch is released right after."""

    def __init__(self, delay=50):
        self.delay = delay
        self.logger = PrintLogger("test.feeder")
        self.logger_link = RecordingLogger("test.feeder.link")
        self.shared = SimpleNamespace(array_pool=NumpyArrayPool(max_bytes=64 * 1024 * 1024))
        self.events = []
        self.batches = []

    def distribute(self, batch):
        rows = [bytes(msg) for _ts, msg, *_rest in batch]
        self.batches.append(rows)
        self.events.append(("distribute", sum(len(r) for r in rows), len(rows)))


@pytest.fixture
def tuner_calls(monkeypatch):
    """Records every ThroughputAutoTuner.update call into the reader's event list, so tests can
    check what the tuner was told and in which order relative to distribute()."""
    calls = []
    real_update = ThroughputAutoTuner.update

    def update(self, bytes_in, msgs_in, target_window_sec):
        calls.append(("update", bytes_in, msgs_in))
        return real_update(self, bytes_in, msgs_in, target_window_sec)

    monkeypatch.setattr(ThroughputAutoTuner, "update", update)
    return calls


def view_of(data: bytes):
    return np.frombuffer(bytearray(data), dtype=dtypes.BYTE)


def make_feeder(reader, default_buffer_bytes=256, msg_size_bytes=16):
    return BatchFeeder(reader, msg_size_bytes=msg_size_bytes, default_buffer_bytes=default_buffer_bytes)


def chunks(count, size=40):
    return [bytes([i % 251]) * size for i in range(count)]


class TestPushView:
    def test_a_full_batch_is_distributed_and_the_overflowing_chunk_starts_the_next(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader)
        sent = chunks(200)
        for i, chunk in enumerate(sent):
            feeder.push_view(i + 1, view_of(chunk), len(chunk))
        feeder.close()

        assert len(reader.batches) > 1  # the batch filled up at least once
        assert [row for batch in reader.batches for row in batch] == sent

    def test_the_tuner_gets_each_full_batch_s_own_byte_count(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader)
        for i, chunk in enumerate(chunks(200)):
            feeder.push_view(i + 1, view_of(chunk), len(chunk))

        assert tuner_calls, "no batch filled up"
        distributes = reader.events
        assert len(distributes) == len(tuner_calls)
        for (_, d_bytes, d_rows), (_, u_bytes, u_rows) in zip(distributes, tuner_calls):
            # plain msg_cursor: the overflowing chunk is not counted here, it goes into the next batch
            assert (u_bytes, u_rows) == (d_bytes, d_rows)
        feeder.close()

    def test_distribute_comes_first(self, monkeypatch):
        reader = FakeReader()
        order = []
        real_update = ThroughputAutoTuner.update
        monkeypatch.setattr(
            ThroughputAutoTuner, "update", lambda self, *a: (order.append("update"), real_update(self, *a))[1]
        )
        real_distribute = reader.distribute
        reader.distribute = lambda batch: (order.append("distribute"), real_distribute(batch))[1]

        feeder = make_feeder(reader)
        feeder.push_view(1, view_of(b"x" * 10), 10)
        feeder.flush()

        assert order == ["distribute", "update"]
        feeder.close()

    def test_a_chunk_bigger_than_a_fresh_batch_grows_the_next_one(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader, default_buffer_bytes=256)
        big = bytes(range(256)) * 1024  # 256 KB into a batch sized for 256 bytes
        feeder.push_view(1, view_of(big), len(big))

        # The too-small empty batch is released, not distributed.
        assert reader.batches == [] and tuner_calls == []
        feeder.close()
        assert reader.batches == [[big]]
        assert reader.logger_link.errors == []

    def test_a_chunk_bigger_than_the_rest_of_a_used_batch_is_kept(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader, default_buffer_bytes=256)
        small, big = b"a" * 10, b"b" * 300_000
        feeder.push_view(1, view_of(small), len(small))
        feeder.push_view(2, view_of(big), len(big))
        feeder.close()

        assert reader.batches == [[small], [big]]
        assert reader.logger_link.errors == []


class TestPushBytes:
    def test_full_batches_are_handed_on_and_every_chunk_is_kept(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader)
        sent = chunks(200)
        for i, chunk in enumerate(sent):
            feeder.push(i + 1, chunk)
        feeder.close()

        assert len(reader.batches) > 1
        assert [row for batch in reader.batches for row in batch] == sent

    def test_a_chunk_bigger_than_a_fresh_batch_grows_the_next_one(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader, default_buffer_bytes=256)
        big = bytes(range(256)) * 1024
        feeder.push(1, big)
        feeder.close()

        assert reader.batches == [[big]]
        assert reader.logger_link.errors == []


class TestAppend:
    def test_append_extends_the_last_row(self):
        reader = FakeReader()
        feeder = make_feeder(reader)
        feeder.push(1, b"h")
        feeder.append(1, b"ello")
        feeder.close()

        assert reader.batches == [[b"hello"]]

    def test_a_burst_that_does_not_fit_becomes_a_new_row_in_a_batch_sized_for_it(self, tuner_calls):
        """uart's path: first byte as a row, then the rest of a big burst appended to it."""
        reader = FakeReader()
        feeder = make_feeder(reader, default_buffer_bytes=256)
        rest = b"r" * 300_000
        feeder.push(1, b"f")
        feeder.append(1, rest)
        feeder.close()

        assert reader.batches == [[b"f"], [rest]]
        assert tuner_calls == [("update", 1, 1)]
        assert reader.logger_link.errors == []


class TestFlush:
    def test_flush_distributes_and_starts_a_new_batch(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader)
        first = feeder.batch
        feeder.push_view(1, view_of(b"hello"), 5)

        new = feeder.flush()

        assert reader.batches == [[b"hello"]]
        assert tuner_calls == [("update", 5, 1)]
        assert new is feeder.batch and new is not first and len(new) == 0
        feeder.close()

    def test_flush_does_nothing_to_an_empty_batch(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader)
        batch = feeder.batch

        assert feeder.flush() is batch
        assert reader.batches == [] and tuner_calls == []
        feeder.close()


class TestMaybeFlush:
    def test_flushes_once_the_first_row_is_delay_old(self, tuner_calls):
        reader = FakeReader(delay=20)
        feeder = make_feeder(reader)
        feeder.push_view(1_000, view_of(b"row"), 3)

        feeder.maybe_flush(1_000 + 20_000_000 - 1)
        assert reader.batches == []

        feeder.maybe_flush(1_000 + 20_000_000)
        assert reader.batches == [[b"row"]]
        assert tuner_calls == [("update", 3, 1)]
        assert len(feeder.batch) == 0
        feeder.close()

    def test_never_fires_on_an_empty_batch(self):
        """An empty batch's start_ts is max int64, so now - start_ts is negative."""
        reader = FakeReader(delay=0)
        feeder = make_feeder(reader)
        batch = feeder.batch

        feeder.maybe_flush(10**18)

        assert reader.batches == [] and feeder.batch is batch
        feeder.close()


class TestClose:
    def test_close_distributes_what_is_left_without_a_tuner_update(self, tuner_calls):
        reader = FakeReader()
        feeder = make_feeder(reader)
        feeder.push_view(1, view_of(b"tail"), 4)

        feeder.close()

        assert reader.batches == [[b"tail"]]
        assert tuner_calls == []
        assert feeder.batch is None

    def test_close_releases_an_empty_batch(self):
        reader = FakeReader()
        feeder = make_feeder(reader)
        batch = feeder.batch

        feeder.close()

        assert reader.batches == []
        assert batch.bundle is None  # released back to the pool
        feeder.close()  # a second close is harmless


def test_delay_comes_from_the_reader():
    feeder = make_feeder(FakeReader(delay=20))
    assert feeder.delay_ns == 20_000_000
    feeder.close()

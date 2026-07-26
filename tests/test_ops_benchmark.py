# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ops.benchmark import nb_blast_benchmark_cache


def _make_cache(messages):
    flat = b"".join(messages)
    c_buf = np.frombuffer(flat, dtype=np.uint8)
    c_lens = np.array([len(m) for m in messages], dtype=np.uint32)
    c_offs = np.zeros(len(c_lens), dtype=np.uint32)
    offset = 0
    for i, length in enumerate(c_lens):
        c_offs[i] = offset
        offset += length
    return c_buf, c_offs, c_lens, len(messages)


def test_blast_benchmark_cache_writes_rows_from_cache_round_robin():
    pool = NumpyArrayPool()
    c_buf, c_offs, c_lens, c_items = _make_cache([b"aa", b"bbb", b"c"])

    batch = pool.create(PooledLogBatch, 5, 32)
    written = nb_blast_benchmark_cache(batch.bundle, 1000, 5, c_buf, c_offs, c_lens, c_items)

    assert batch.size == 5
    assert written == sum(c_lens[i % c_items] for i in range(5))

    # Round-robins through the 3 cached messages in order, twice + one more.
    expected = [b"aa", b"bbb", b"c", b"aa", b"bbb"]
    bundle = batch.bundle
    for i, msg in enumerate(expected):
        off = int(bundle.offsets[i])
        length = int(bundle.lengths[i])
        assert bundle.buffer[off : off + length].tobytes() == msg

    batch.release()


def test_blast_benchmark_cache_assigns_incrementing_timestamps():
    pool = NumpyArrayPool()
    c_buf, c_offs, c_lens, c_items = _make_cache([b"x"])

    batch = pool.create(PooledLogBatch, 3, 8)
    nb_blast_benchmark_cache(
        batch.bundle, start_ts=500, chunks=3, c_buf=c_buf, c_offs=c_offs, c_lens=c_lens, c_items=c_items
    )

    assert list(batch.bundle.timestamps[:3]) == [500, 501, 502]

    batch.release()


def test_blast_benchmark_cache_appends_after_existing_rows():
    """Cursors are read from bundle.size/msg_cursor, so a second call on a partially-filled
    batch must append after the existing rows rather than overwriting from zero."""
    pool = NumpyArrayPool()
    c_buf, c_offs, c_lens, c_items = _make_cache([b"ab"])

    batch = pool.create(PooledLogBatch, 4, 16)
    nb_blast_benchmark_cache(batch.bundle, 0, 1, c_buf, c_offs, c_lens, c_items)
    nb_blast_benchmark_cache(batch.bundle, 1, 1, c_buf, c_offs, c_lens, c_items)

    assert batch.size == 2
    bundle = batch.bundle
    assert int(bundle.offsets[1]) == int(bundle.lengths[0])  # second row starts after first
    assert bundle.buffer[bundle.offsets[1] : bundle.offsets[1] + bundle.lengths[1]].tobytes() == b"ab"

    batch.release()

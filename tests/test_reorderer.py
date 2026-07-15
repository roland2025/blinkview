# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
from numba.typed import List as NumbaList

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.reorderer import MergeChunk, nb_hybrid_merge_and_copy


def _make_batch(pool, capacity=8, buffer_bytes=256, **has_flags):
    return pool.create(PooledLogBatch, capacity, buffer_bytes, **has_flags)


def test_nb_hybrid_merge_and_copy_copies_pids_and_tids():
    """Regression test: the Reorder layer sits between the parser (which correctly writes
    pids/tids) and CentralStorage - this merge kernel used to silently drop them because it
    only copied levels/modules/devices/sequences, and its own output batch wasn't even
    allocated with has_pids/has_tids. Both are now fixed; this locks that in."""
    pool = NumpyArrayPool()

    src = _make_batch(pool, has_levels=True, has_modules=True, has_devices=True, has_pids=True, has_tids=True)
    src.insert(100, 100, b"a", level=1, module=2, device=3, pid=111, tid=222)
    src.insert(200, 200, b"b", level=1, module=2, device=3, pid=333, tid=444)

    out = _make_batch(pool, has_levels=True, has_modules=True, has_devices=True, has_pids=True, has_tids=True)

    chunks = NumbaList()
    chunks.append(MergeChunk(src.bundle, 0, src.size))

    n = src.size
    ts_scr = np.zeros(n, dtype=dtypes.TS_TYPE)
    b_idx_scr = np.zeros(n, dtype=np.uint32)
    r_idx_scr = np.zeros(n, dtype=np.uint32)
    sort_order = np.zeros(n, dtype=np.uint32)

    nb_hybrid_merge_and_copy(chunks, ts_scr, b_idx_scr, r_idx_scr, sort_order, out.bundle)

    assert out.size == 2
    assert list(out.bundle.pids[:2]) == [111, 333]
    assert list(out.bundle.tids[:2]) == [222, 444]

    src.release()
    out.release()


def test_nb_hybrid_merge_and_copy_skips_pids_when_source_lacks_them():
    """System-generated log rows (no ADB source) legitimately have has_pids=False - the merge
    must not crash or write garbage into the output's pids column for those rows."""
    pool = NumpyArrayPool()

    src = _make_batch(pool, has_levels=True, has_modules=True, has_devices=True)  # no pids/tids
    src.insert(100, 100, b"a", level=1, module=2, device=3)

    out = _make_batch(pool, has_levels=True, has_modules=True, has_devices=True, has_pids=True, has_tids=True)

    chunks = NumbaList()
    chunks.append(MergeChunk(src.bundle, 0, src.size))

    ts_scr = np.zeros(1, dtype=dtypes.TS_TYPE)
    b_idx_scr = np.zeros(1, dtype=np.uint32)
    r_idx_scr = np.zeros(1, dtype=np.uint32)
    sort_order = np.zeros(1, dtype=np.uint32)

    nb_hybrid_merge_and_copy(chunks, ts_scr, b_idx_scr, r_idx_scr, sort_order, out.bundle)

    assert out.size == 1
    assert int(out.bundle.pids[0]) == 0
    assert int(out.bundle.tids[0]) == 0

    src.release()
    out.release()

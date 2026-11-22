# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import threading
import time

import numpy as np
import pytest
from numba.typed import List as NumbaList

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.reorderer import MergeChunk, nb_hybrid_merge_and_copy
from tests.fakes.real_registry import make_real_registry

ALL_COLS = dict(
    has_levels=True,
    has_modules=True,
    has_devices=True,
    has_sequences=True,
    has_pids=True,
    has_tids=True,
    has_ext_u32_1=True,
    has_ext_u32_2=True,
    has_ext_u64_1=True,
)
OPTIONAL = ("levels", "modules", "devices", "sequences", "pids", "tids", "ext_u32_1", "ext_u32_2", "ext_u64_1")


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


def _merge(chunk_list, out_bundle):
    chunks = NumbaList()
    for bundle, s, e in chunk_list:
        chunks.append(MergeChunk(bundle, s, e))
    n = max(1, sum(e - s for _b, s, e in chunk_list))
    nb_hybrid_merge_and_copy(
        chunks,
        np.empty(n, dtype=dtypes.TS_TYPE),
        np.empty(n, dtype=np.uint32),
        np.empty(n, dtype=np.uint32),
        np.empty(n, dtype=np.uint32),
        out_bundle,
    )


def _fill_random_batch(pool, rng, n, ts_lo, ts_span, device, flags):
    """A batch of n rows with sorted timestamps from a narrow range (so ties are common), text of
    0-20 random bytes per row laid out with gaps (so the merge has to honour source offsets), and
    random values in every optional column it has."""
    b = pool.create(PooledLogBatch, max(n, 1), max(n, 1) * 24 + 8, **flags)
    bb = b.bundle
    ts = np.sort(rng.integers(ts_lo, ts_lo + ts_span + 1, size=n)).astype(np.int64)
    lengths = rng.integers(0, 21, size=n).astype(np.int64)
    gaps = rng.integers(0, 4, size=n)
    offsets = np.concatenate(([0], np.cumsum(lengths + gaps)[:-1])).astype(np.int64)[:n]
    end = int(offsets[-1] + lengths[-1]) if n else 0
    bb.buffer[:end] = rng.integers(0, 256, size=end, dtype=np.uint8)
    bb.timestamps[:n] = ts
    bb.rx_timestamps[:n] = ts + rng.integers(0, 1000, size=n)
    bb.offsets[:n] = offsets
    bb.lengths[:n] = lengths
    for name in OPTIONAL:
        if getattr(bb, "has_" + name):
            col = getattr(bb, name)
            col[:n] = device if name == "devices" else rng.integers(0, 2**31, size=n).astype(col.dtype)
    bb.size[0] = n
    bb.msg_cursor[0] = end
    return b


def _rows(bundle, s, e, out_flags):
    """Row tuples of bundle[s:e]: timestamps, text bytes, then each optional column the output
    has (out_flags), reading 0 where the source lacks it (the merge's fill for pids/tids)."""
    rows = []
    for j in range(s, e):
        off, n = int(bundle.offsets[j]), int(bundle.lengths[j])
        row = [int(bundle.timestamps[j]), int(bundle.rx_timestamps[j]), bytes(bundle.buffer[off : off + n])]
        for name in OPTIONAL:
            if out_flags["has_" + name]:
                row.append(int(getattr(bundle, name)[j]) if getattr(bundle, "has_" + name) else 0)
        rows.append(tuple(row))
    return rows


def _stable_sorted(rows):
    order = np.argsort(np.array([r[0] for r in rows], dtype=np.int64), kind="stable")
    return [rows[i] for i in order]


def _check_out_layout(out, start_row, start_bytes):
    """Output text is contiguous: each row's offset is where the previous one ended."""
    n = int(out.size[0])
    lengths = out.lengths[start_row:n].astype(np.int64)
    expect = start_bytes + np.concatenate(([0], np.cumsum(lengths)[:-1]))[: n - start_row]
    assert np.array_equal(out.offsets[start_row:n].astype(np.int64), expect)
    assert int(out.msg_cursor[0]) == start_bytes + int(lengths.sum())


@pytest.mark.parametrize("seed", range(40))
def test_merge_matches_stable_sort_reference(seed):
    """The merge must equal a stable sort by timestamp of the chunks' rows concatenated in list
    order, with every column and the text carried through. Random multi-source input: narrow
    timestamp ranges (many ties, within and across sources), same-source batches that sometimes
    overlap in time (so they cannot be merged as one run), empty batches and empty or partial
    chunk ranges, sources with and without pids/tids, a list either grouped by source (as
    Reorder.run builds it) or shuffled, and an output batch that already holds rows."""
    rng = np.random.default_rng(seed)
    pool = NumpyArrayPool()
    all_flags = {"has_" + n: True for n in OPTIONAL}
    chunk_list = []
    batches = []
    for src in range(int(rng.integers(1, 7))):
        flags = dict(ALL_COLS)
        if rng.random() < 0.3:
            flags["has_pids"] = False
        if rng.random() < 0.3:
            flags["has_tids"] = False
        ts_lo = int(rng.integers(0, 50))
        for _ in range(int(rng.integers(1, 6))):
            n = int(rng.choice([0, 1, 2, 7, 50, 300]))
            span = int(rng.integers(0, 60))
            b = _fill_random_batch(pool, rng, n, ts_lo, span, 10 + src, flags)
            batches.append(b)
            # Usually the next batch continues in time; sometimes it starts earlier (overlap).
            ts_lo = ts_lo + span if rng.random() < 0.7 else max(0, ts_lo - int(rng.integers(0, 30)))
            s = int(rng.integers(0, n + 1)) if rng.random() < 0.3 else 0
            e = int(rng.integers(s, n + 1)) if rng.random() < 0.3 else n
            chunk_list.append((b.bundle, s, e))
    if rng.random() < 0.5:
        chunk_list = [chunk_list[i] for i in rng.permutation(len(chunk_list))]

    total = sum(e - s for _b, s, e in chunk_list)
    out = pool.create(PooledLogBatch, total + 3, total * 24 + 64, **ALL_COLS)
    pre = _fill_random_batch(pool, rng, 3, 0, 5, 99, ALL_COLS)
    _merge([(pre.bundle, 0, 3)], out.bundle)
    before = _rows(out.bundle, 0, 3, all_flags)
    pre_bytes = int(out.bundle.msg_cursor[0])

    _merge(chunk_list, out.bundle)

    flat = [r for bundle, s, e in chunk_list for r in _rows(bundle, s, e, all_flags)]
    assert int(out.size) == 3 + total
    assert _rows(out.bundle, 0, 3, all_flags) == before
    assert _rows(out.bundle, 3, 3 + total, all_flags) == _stable_sorted(flat)
    _check_out_layout(out.bundle, 3, pre_bytes)

    for b in batches:
        b.release()
    pre.release()
    out.release()


def _random_chunks(rng, pool, run_len_hint):
    """Random multi-source chunks; run_len_hint > 1 stamps rows in equal-timestamp groups of about
    that size, so one source wins long stretches (the block-copy case)."""
    all_flags = dict(ALL_COLS)
    chunk_list, batches = [], []
    for src in range(int(rng.integers(1, 5))):
        flags = dict(all_flags)
        flags["has_pids"] = rng.random() < 0.7
        flags["has_tids"] = rng.random() < 0.7
        ts_lo = int(rng.integers(0, 2000))
        for _ in range(int(rng.integers(1, 4))):
            n = int(rng.choice([0, 1, 5, 200, 600]))
            b = _fill_random_batch(pool, rng, n, 0, 0, 10 + src, flags)
            if n:
                groups = np.sort(rng.integers(0, max(1, n // run_len_hint) + 1, size=n))
                b.bundle.timestamps[:n] = ts_lo + groups * 1000 + src
                ts_lo += int(groups[-1]) * 1000 + 1000
            batches.append(b)
            chunk_list.append((b.bundle, 0, n))
    return chunk_list, batches


@pytest.mark.parametrize("path", ["rows", "blocks"])
@pytest.mark.parametrize("run_len", [1, 50])
@pytest.mark.parametrize("seed", range(8))
def test_both_scatter_paths_match_reference(seed, run_len, path):
    """nb_hybrid_merge_and_copy picks the row or the block copy from the merge's mean segment
    length; each must give the same output on any input, so both are run directly here."""
    from blinkview.ops.reorder import (
        nb_merge_flatten_runs,
        nb_merge_order_runs,
        nb_merge_scatter_blocks,
        nb_merge_scatter_rows,
    )

    rng = np.random.default_rng(1000 + seed)
    pool = NumpyArrayPool()
    chunk_list, batches = _random_chunks(rng, pool, run_len)
    total = sum(e - s for _b, s, e in chunk_list)
    chunks = NumbaList()
    for bundle, s, e in chunk_list:
        chunks.append(MergeChunk(bundle, s, e))
    n = max(1, total)
    ts_scr = np.empty(n, dtype=dtypes.TS_TYPE)
    len_scr, dest_row, dest_off = (np.empty(n, dtype=np.uint32) for _ in range(3))
    runs_s, runs_e = np.empty(len(chunks), dtype=np.int64), np.empty(len(chunks), dtype=np.int64)
    out = pool.create(PooledLogBatch, total + 2, total * 24 + 64, **ALL_COLS)
    pre = _fill_random_batch(pool, rng, 2, 0, 5, 99, ALL_COLS)
    _merge([(pre.bundle, 0, 2)], out.bundle)
    pre_bytes = int(out.bundle.msg_cursor[0])

    n_runs, _rows_n = nb_merge_flatten_runs(chunks, ts_scr, len_scr, runs_s, runs_e)
    nb_merge_order_runs(n_runs, ts_scr, len_scr, runs_s, runs_e, dest_row, dest_off)
    (nb_merge_scatter_rows if path == "rows" else nb_merge_scatter_blocks)(chunks, dest_row, dest_off, out.bundle)

    all_flags = {"has_" + n: True for n in OPTIONAL}
    flat = [r for bundle, s, e in chunk_list for r in _rows(bundle, s, e, all_flags)]
    assert _rows(out.bundle, 2, 2 + total, all_flags) == _stable_sorted(flat)
    _check_out_layout(out.bundle, 2, pre_bytes)
    for b in batches + [pre, out]:
        b.release()


@pytest.mark.parametrize("seed", range(30))
def test_cut_rows_keeps_the_most_rows_up_to_one_timestamp(seed):
    """nb_merge_cut_rows trims every chunk at one timestamp: the kept rows are all at or before
    it, the rest all after, at most max_rows are kept unless the earliest timestamp alone has more
    (then exactly those), and the next timestamp would have pushed the count over max_rows."""
    from blinkview.ops.reorder import nb_merge_cut_rows

    rng = np.random.default_rng(seed)
    pool = NumpyArrayPool()
    chunk_list, batches = _random_chunks(rng, pool, int(rng.choice([1, 20])))
    # Partial ranges too, as a pass sees after earlier passes.
    chunk_list = [(b, int(rng.integers(0, e + 1)) if rng.random() < 0.3 else s, e) for b, s, e in chunk_list]
    chunks = NumbaList()
    for bundle, s, e in chunk_list:
        chunks.append(MergeChunk(bundle, s, e))
    all_ts = np.concatenate([b.timestamps[s:e] for b, s, e in chunk_list] + [np.empty(0, np.int64)])
    if len(all_ts) < 2:
        pytest.skip("needs rows")
    max_rows = int(rng.integers(1, len(all_ts)))
    ends = np.empty(len(chunks), dtype=np.int64)
    kept = int(nb_merge_cut_rows(chunks, max_rows, ends))

    kept_ts = np.concatenate([b.timestamps[s:end] for (b, s, _e), end in zip(chunk_list, ends)])
    left_ts = np.concatenate([b.timestamps[end:e] for (b, _s, e), end in zip(chunk_list, ends)])
    assert kept == len(kept_ts) and all(s <= end <= e for (_b, s, e), end in zip(chunk_list, ends))
    assert len(kept_ts) > 0, "a pass must make progress"
    if len(left_ts):
        assert kept_ts.max() < left_ts.min()
        lowest = all_ts.min()
        if np.count_nonzero(all_ts == lowest) > max_rows:
            assert kept == np.count_nonzero(all_ts == lowest)
        else:
            assert kept <= max_rows
            assert kept + np.count_nonzero(left_ts == left_ts.min()) > max_rows
    for b in batches:
        b.release()


def test_merge_of_only_empty_chunks_appends_nothing():
    pool = NumpyArrayPool()
    rng = np.random.default_rng(0)
    a = _fill_random_batch(pool, rng, 0, 0, 0, 1, ALL_COLS)
    b = _fill_random_batch(pool, rng, 5, 0, 10, 2, ALL_COLS)
    out = pool.create(PooledLogBatch, 8, 256, **ALL_COLS)
    _merge([(a.bundle, 0, 0), (b.bundle, 2, 2)], out.bundle)
    assert out.size == 0
    assert int(out.bundle.msg_cursor[0]) == 0
    for x in (a, b, out):
        x.release()


def test_merge_keeps_tied_rows_in_chunk_order_across_sources():
    """Equal timestamps from different sources come out in chunk-list order, and a later chunk of
    the same source that continues in time is not overtaken by another source's tied rows."""
    pool = NumpyArrayPool()
    cols = dict(has_levels=True, has_modules=True, has_devices=True)
    a1 = pool.create(PooledLogBatch, 4, 64, **cols)
    a2 = pool.create(PooledLogBatch, 4, 64, **cols)
    b1 = pool.create(PooledLogBatch, 4, 64, **cols)
    for ts, msg in ((5, b"a1"), (7, b"a2")):
        a1.insert(ts, ts, msg, device=1)
    for ts, msg in ((7, b"a3"), (9, b"a4")):
        a2.insert(ts, ts, msg, device=1)
    for ts, msg in ((5, b"b1"), (7, b"b2"), (9, b"b3")):
        b1.insert(ts, ts, msg, device=2)
    out = pool.create(PooledLogBatch, 16, 256, **cols)
    _merge([(a1.bundle, 0, 2), (a2.bundle, 0, 2), (b1.bundle, 0, 3)], out.bundle)
    ob = out.bundle
    msgs = [bytes(ob.buffer[int(o) : int(o) + int(n)]) for o, n in zip(ob.offsets[:7], ob.lengths[:7])]
    assert msgs == [b"a1", b"b1", b"a2", b"a3", b"b2", b"a4", b"b3"]
    for x in (a1, a2, b1, out):
        x.release()


def _write_reorder_only_profile(tmp_path):
    (tmp_path / "test_config.json").write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {},
                "pipelines": {},
                "plugins": {},
                "reorder": {"enabled": True, "type": "default", "delay": 20},
                "central": {"enabled": True, "type": "default", "logging": {"enabled": False}, "cold_max_pieces": 0},
            }
        )
    )


def test_reorder_thread_delivers_a_device_whose_clock_goes_backwards(tmp_path, monkeypatch):
    """A device whose next batch starts earlier than its previous one (a firmware clock reset)
    must not stall a capped pass: every row still comes out once. Under a backlog the earliest
    rows then sit behind a batch the cut trims to nothing."""
    import blinkview.core.reorderer as reorderer_mod

    monkeypatch.setattr(reorderer_mod, "REORDER_MAX_ROWS_PER_PASS", 10)
    _write_reorder_only_profile(tmp_path)
    reg = make_real_registry(tmp_path, "reorder_clock_reset", start=True)
    cols = dict(has_levels=True, has_modules=True, has_devices=True, has_pids=True, has_tids=True)
    out_flags = {"has_" + n: n in ("levels", "modules", "devices", "pids", "tids") for n in OPTIONAL}
    got = []
    got_lock = threading.Lock()

    class Collector:
        reference_id = "test_collector"

        def put(self, batch):
            rows = [r for r in _rows(batch.bundle, 0, int(batch.size), out_flags) if r[5] in (201, 202)]
            with got_lock:
                got.extend(rows)

    try:
        reorder = reg.reorder
        reorder.subscribe(Collector())
        pool = reorder.shared.array_pool
        rng = np.random.default_rng(3)
        base = reg.now_ns() + 300_000_000
        sent = []
        # Device 201: a small batch late in time (under the cap, so a pass walks on past it), then a
        # bigger one that jumps 50 ms back; device 202 in between.
        for d, lo, n in ((201, base + 50_000_000, 5), (201, base, 30), (202, base + 20_000_000, 30)):
            b = _fill_random_batch(pool, rng, n, lo, 1_000_000, d, cols)
            sent.extend(_rows(b.bundle, 0, n, out_flags))
            reorder.put(b)
            b.release()
        deadline = time.time() + 10
        while time.time() < deadline:
            with got_lock:
                if len(got) >= len(sent):
                    break
            time.sleep(0.02)
        with got_lock:
            assert sorted(got) == sorted(sent)
    finally:
        reg.stop()


@pytest.mark.parametrize("max_pass_rows", [None, 37])
def test_reorder_thread_merges_sources_in_time_order_into_central(tmp_path, monkeypatch, max_pass_rows):
    """End to end through a real Registry: batches from three devices go into the running
    Reorder (real run() loop, real merge kernel), come out in one global time order with every
    column intact, and land in central storage in that order. With a tiny per-pass row cap the
    same holds while each pass merges only a slice cut at one timestamp across all devices."""
    if max_pass_rows is not None:
        import blinkview.core.reorderer as reorderer_mod

        monkeypatch.setattr(reorderer_mod, "REORDER_MAX_ROWS_PER_PASS", max_pass_rows)
    _write_reorder_only_profile(tmp_path)
    reg = make_real_registry(tmp_path, "reorder_e2e", start=True)
    cols = dict(has_levels=True, has_modules=True, has_devices=True, has_pids=True, has_tids=True)
    out_flags = {"has_" + n: n in ("levels", "modules", "devices", "pids", "tids") for n in OPTIONAL}
    devices = (101, 102, 103)
    got = []
    got_lock = threading.Lock()

    class Collector:
        reference_id = "test_collector"

        def put(self, batch):
            b = batch.bundle
            _check_out_layout(b, 0, 0)
            # The registry's own system log rows go through the reorder too; keep only ours.
            rows = [r for r in _rows(b, 0, int(b.size[0]), out_flags) if r[5] in devices]
            with got_lock:
                got.extend(rows)

    try:
        reorder = reg.reorder
        reorder.subscribe(Collector())
        pool = reorder.shared.array_pool
        rng = np.random.default_rng(7)
        # Stamped far enough ahead that nothing is ready before every batch is queued, so the
        # output order must be global, not arrival order.
        base = reg.now_ns() + 400_000_000
        flat = []
        ts_lo = {d: base + int(rng.integers(0, 1000)) for d in devices}
        per_dev = {d: [] for d in devices}
        for _round in range(6):
            for d in devices:
                n = int(rng.integers(0, 400))
                span = int(rng.integers(0, 40_000_000))
                b = _fill_random_batch(pool, rng, n, ts_lo[d], span, d, cols)
                ts_lo[d] += span
                per_dev[d].extend(_rows(b.bundle, 0, n, out_flags))
                reorder.put(b)
                b.release()
        assert reg.now_ns() < base, "queuing took longer than the head start the test relies on"
        for d in devices:
            flat.extend(per_dev[d])
        expected = _stable_sorted(flat)

        deadline = time.time() + 10
        while time.time() < deadline:
            with got_lock:
                if len(got) >= len(expected):
                    break
            time.sleep(0.02)
        with got_lock:
            assert got == expected

        want = [(r[0], r[2], r[3], r[4], r[5]) for r in expected]
        stored = []
        while time.time() < deadline:
            stored = []
            with reg.central.log_pool.get_snapshot() as segments:
                for seg in segments:
                    for ts, msg, _rx, level, module, device, *_rest in seg:
                        if device in devices:
                            stored.append((ts, bytes(msg), level, module, device))
            if len(stored) >= len(want):
                break
            time.sleep(0.02)
        assert stored == want
    finally:
        reg.stop()

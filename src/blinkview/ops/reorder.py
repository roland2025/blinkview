# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.buffers import nb_memmove


class MergeChunk(NamedTuple):
    """Strictly typed instruction for the Numba merge kernel."""

    bundle: LogBundle
    start: int
    end: int


# Mean output segment length (rows in a row from one run) from which nb_hybrid_merge_and_copy
# copies rows in blocks instead of row by row. Row by row is ~10 cycles/row cheaper when sources
# alternate every row; blocks are 15-35% cheaper when same-source stretches are common.
BLOCK_COPY_MIN_SEGMENT = 4


@app_njit()
def nb_hybrid_merge_and_copy(chunks, ts_scr, len_scr, dest_row, dest_off, out_bundle):
    """Appends the rows of every chunk to out_bundle in timestamp order.

    The result is the stable sort of the chunks' rows concatenated in list order: equal
    timestamps keep chunk order, then row order. Each chunk must already be sorted.

    The four scratch arrays need room for the total row count: ts_scr int64, the other three
    uint32. Their contents on return are unspecified.

    Cost is O(rows x runs), where a run is a stretch of list-adjacent chunks that continue each
    other in time (normally one per source, since Reorder.run lists a device's queued batches
    together), and less when one run wins many rows in a row. Rows are copied chunk by chunk, so
    each chunk's bundle is read out of the typed list once, not once per row: that per-row read
    was ~300 cycles of a ~500-cycle row."""
    k = len(chunks)
    run_starts = np.empty(k, dtype=np.int64)
    run_ends = np.empty(k, dtype=np.int64)
    n_runs, num_rows = nb_merge_flatten_runs(chunks, ts_scr, len_scr, run_starts, run_ends)
    segments = nb_merge_order_runs(n_runs, ts_scr, len_scr, run_starts, run_ends, dest_row, dest_off)
    if segments * BLOCK_COPY_MIN_SEGMENT <= num_rows:
        nb_merge_scatter_blocks(chunks, dest_row, dest_off, out_bundle)
    else:
        nb_merge_scatter_rows(chunks, dest_row, dest_off, out_bundle)


@app_njit()
def nb_merge_flatten_runs(chunks, ts_scr, len_scr, run_starts, run_ends):
    """Copies the chunks' timestamps and lengths into flat scratch arrays, in list order, and
    groups list-adjacent chunks into runs: a chunk joins the previous run when its first
    timestamp is not earlier than that run's last. Fills run_starts/run_ends with flat
    [start, end) ranges and returns (run count, row count)."""
    cursor = 0
    n_runs = 0
    for i in range(len(chunks)):
        chunk = chunks[i]
        s = chunk.start
        e = chunk.end
        if e <= s:
            continue
        bundle = chunk.bundle
        ts_col = bundle.timestamps
        len_col = bundle.lengths
        first = cursor
        for j in range(s, e):
            ts_scr[cursor] = ts_col[j]
            len_scr[cursor] = len_col[j]
            cursor += 1
        if n_runs > 0 and ts_scr[first - 1] <= ts_scr[first]:
            run_ends[n_runs - 1] = cursor
        else:
            run_starts[n_runs] = first
            run_ends[n_runs] = cursor
            n_runs += 1
    return n_runs, cursor


@app_njit()
def nb_merge_order_runs(n_runs, ts_scr, len_scr, run_starts, run_ends, dest_row, dest_off):
    """Merges the sorted runs of ts_scr. For each flat row f, writes its output row (dest_row[f])
    and its byte offset in the output text (dest_off[f]), both relative to the merge's start.

    Each scan over the run heads finds the best and the second-best head; the best run then
    keeps emitting for as long as it still beats the second-best, so a stretch from one source
    costs one scan. Ties go to the lower run index, which keeps the merge stable. Returns the
    number of such stretches (output segments)."""
    num_rows = 0
    for r in range(n_runs):
        num_rows += run_ends[r] - run_starts[r]
    heads = run_starts[:n_runs].copy()
    out_i = 0
    byte_cur = 0
    segments = 0
    while out_i < num_rows:
        best = -1
        second = -1
        b_ts = ts_scr[0]  # typed placeholders; best/second == -1 marks them unset
        s_ts = ts_scr[0]
        for r in range(n_runs):
            c = heads[r]
            if c < run_ends[r]:
                t = ts_scr[c]
                if best == -1 or t < b_ts:
                    second = best
                    s_ts = b_ts
                    best = r
                    b_ts = t
                elif second == -1 or t < s_ts:
                    second = r
                    s_ts = t
        f = heads[best]
        e = run_ends[best]
        dest_row[f] = out_i
        dest_off[f] = byte_cur
        byte_cur += len_scr[f]
        out_i += 1
        g = f + 1
        if second == -1:
            stop = e
        elif best < second:
            stop = g
            while stop < e and ts_scr[stop] <= s_ts:
                stop += 1
        else:
            stop = g
            while stop < e and ts_scr[stop] < s_ts:
                stop += 1
        while g < stop:
            dest_row[g] = out_i
            dest_off[g] = byte_cur
            byte_cur += len_scr[g]
            out_i += 1
            g += 1
        heads[best] = stop
        segments += 1
    return segments


@app_njit()
def nb_merge_scatter_rows(chunks, dest_row, dest_off, out_bundle):
    """Writes every chunk row into the output slot nb_merge_order_runs gave it, appending after
    out_bundle's current size/msg_cursor. Reads each chunk sequentially, one column at a time;
    the best layout when output slots of one chunk are rarely adjacent."""
    out_base = out_bundle.size[0]
    out_msg_base = out_bundle.msg_cursor[0]
    o_ts = out_bundle.timestamps
    o_rx = out_bundle.rx_timestamps
    o_off = out_bundle.offsets
    o_len = out_bundle.lengths
    o_buf = out_bundle.buffer
    f0 = 0
    total_rows = 0
    total_bytes = 0
    for i in range(len(chunks)):
        chunk = chunks[i]
        s = chunk.start
        e = chunk.end
        if e <= s:
            continue
        src = chunk.bundle
        s_ts = src.timestamps
        s_rx = src.rx_timestamps
        s_off = src.offsets
        s_len = src.lengths
        s_buf = src.buffer
        f = f0
        for j in range(s, e):
            d = out_base + dest_row[f]
            o = out_msg_base + dest_off[f]
            n = s_len[j]
            o_ts[d] = s_ts[j]
            o_rx[d] = s_rx[j]
            o_off[d] = o
            o_len[d] = n
            nb_memmove(o_buf, o, s_buf, s_off[j], n)
            total_bytes += n
            f += 1
        nb_merge_scatter_optional(out_bundle, src, s, e, dest_row, f0, out_base)
        f0 = f
        total_rows += e - s
    out_bundle.size[0] = out_base + total_rows
    out_bundle.msg_cursor[0] = out_msg_base + total_bytes


@app_njit()
def nb_merge_scatter_blocks(chunks, dest_row, dest_off, out_bundle):
    """Same result as nb_merge_scatter_rows, for merges where one chunk's rows mostly land in
    adjacent output slots: a stretch of source rows that goes to consecutive output rows with
    contiguous source text is copied as one block (one text copy, contiguous column copies)."""
    out_base = out_bundle.size[0]
    out_msg_base = out_bundle.msg_cursor[0]
    o_ts = out_bundle.timestamps
    o_rx = out_bundle.rx_timestamps
    o_off = out_bundle.offsets
    o_len = out_bundle.lengths
    o_buf = out_bundle.buffer
    f = 0
    total_rows = 0
    total_bytes = 0
    for i in range(len(chunks)):
        chunk = chunks[i]
        s = chunk.start
        e = chunk.end
        if e <= s:
            continue
        src = chunk.bundle
        s_ts = src.timestamps
        s_rx = src.rx_timestamps
        s_off = src.offsets
        s_len = src.lengths
        s_buf = src.buffer
        do_lv = out_bundle.has_levels and src.has_levels
        do_mo = out_bundle.has_modules and src.has_modules
        do_dv = out_bundle.has_devices and src.has_devices
        do_sq = out_bundle.has_sequences and src.has_sequences
        do_e1 = out_bundle.has_ext_u32_1 and src.has_ext_u32_1
        do_e2 = out_bundle.has_ext_u32_2 and src.has_ext_u32_2
        do_e3 = out_bundle.has_ext_u64_1 and src.has_ext_u64_1
        do_pi = out_bundle.has_pids
        src_pi = src.has_pids
        do_ti = out_bundle.has_tids
        src_ti = src.has_tids
        j = s
        while j < e:
            d0 = dest_row[f]
            so = s_off[j]
            text_end = so + s_len[j]
            m = 1
            while j + m < e and dest_row[f + m] == d0 + m and s_off[j + m] == text_end:
                text_end += s_len[j + m]
                m += 1
            d = out_base + d0
            o = out_msg_base + dest_off[f]
            nb_memmove(o_ts, d, s_ts, j, m)
            nb_memmove(o_rx, d, s_rx, j, m)
            nb_memmove(o_len, d, s_len, j, m)
            for t in range(m):
                o_off[d + t] = o + (s_off[j + t] - so)
            nb_memmove(o_buf, o, s_buf, so, text_end - so)
            if do_lv:
                nb_memmove(out_bundle.levels, d, src.levels, j, m)
            if do_mo:
                nb_memmove(out_bundle.modules, d, src.modules, j, m)
            if do_dv:
                nb_memmove(out_bundle.devices, d, src.devices, j, m)
            if do_sq:
                nb_memmove(out_bundle.sequences, d, src.sequences, j, m)
            if do_e1:
                nb_memmove(out_bundle.ext_u32_1, d, src.ext_u32_1, j, m)
            if do_e2:
                nb_memmove(out_bundle.ext_u32_2, d, src.ext_u32_2, j, m)
            if do_e3:
                nb_memmove(out_bundle.ext_u64_1, d, src.ext_u64_1, j, m)
            # A source without pids/tids gives 0s; see nb_merge_scatter_optional.
            if do_pi:
                if src_pi:
                    nb_memmove(out_bundle.pids, d, src.pids, j, m)
                else:
                    out_bundle.pids[d : d + m] = 0
            if do_ti:
                if src_ti:
                    nb_memmove(out_bundle.tids, d, src.tids, j, m)
                else:
                    out_bundle.tids[d : d + m] = 0
            total_bytes += text_end - so
            j += m
            f += m
        total_rows += e - s
    out_bundle.size[0] = out_base + total_rows
    out_bundle.msg_cursor[0] = out_msg_base + total_bytes


@app_njit()
def nb_merge_scatter_optional(out_bundle, src, s, e, dest_row, f0, out_base):
    """Scatters src[s:e]'s optional columns (flat rows from f0), one loop per column so the loop
    bodies carry no per-row flag checks."""
    if out_bundle.has_levels and src.has_levels:
        nb_merge_scatter_column(out_bundle.levels, src.levels, s, e, dest_row, f0, out_base)
    if out_bundle.has_modules and src.has_modules:
        nb_merge_scatter_column(out_bundle.modules, src.modules, s, e, dest_row, f0, out_base)
    if out_bundle.has_devices and src.has_devices:
        nb_merge_scatter_column(out_bundle.devices, src.devices, s, e, dest_row, f0, out_base)
    if out_bundle.has_sequences and src.has_sequences:
        nb_merge_scatter_column(out_bundle.sequences, src.sequences, s, e, dest_row, f0, out_base)
    if out_bundle.has_ext_u32_1 and src.has_ext_u32_1:
        nb_merge_scatter_column(out_bundle.ext_u32_1, src.ext_u32_1, s, e, dest_row, f0, out_base)
    if out_bundle.has_ext_u32_2 and src.has_ext_u32_2:
        nb_merge_scatter_column(out_bundle.ext_u32_2, src.ext_u32_2, s, e, dest_row, f0, out_base)
    if out_bundle.has_ext_u64_1 and src.has_ext_u64_1:
        nb_merge_scatter_column(out_bundle.ext_u64_1, src.ext_u64_1, s, e, dest_row, f0, out_base)

    # Rows from a source without pids/tids (e.g. system-generated logs, CAN) must get an
    # explicit 0, not whatever stale value the array-pool-recycled output slot happened to
    # hold - array_pool.acquire() does not zero-fill, and out_bundle rows are reused across
    # many merge calls.
    if out_bundle.has_pids:
        if src.has_pids:
            nb_merge_scatter_column(out_bundle.pids, src.pids, s, e, dest_row, f0, out_base)
        else:
            nb_merge_scatter_zeros(out_bundle.pids, e - s, dest_row, f0, out_base)
    if out_bundle.has_tids:
        if src.has_tids:
            nb_merge_scatter_column(out_bundle.tids, src.tids, s, e, dest_row, f0, out_base)
        else:
            nb_merge_scatter_zeros(out_bundle.tids, e - s, dest_row, f0, out_base)


@app_njit()
def nb_merge_scatter_column(out_col, src_col, s, e, dest_row, f0, out_base):
    f = f0
    for j in range(s, e):
        out_col[out_base + dest_row[f]] = src_col[j]
        f += 1


@app_njit()
def nb_merge_scatter_zeros(out_col, n, dest_row, f0, out_base):
    for f in range(f0, f0 + n):
        out_col[out_base + dest_row[f]] = 0


@app_njit()
def nb_merge_count_upto(chunks, cut_ts):
    """Rows of the chunks with timestamp <= cut_ts."""
    total = 0
    for i in range(len(chunks)):
        c = chunks[i]
        if c.end > c.start:
            total += nb_find_split_idx(c.bundle.timestamps, c.start, c.end, cut_ts)
    return total


@app_njit()
def nb_merge_cut_rows(chunks, max_rows, ends):
    """Picks one cut timestamp for all chunks, the latest at which at most max_rows of their rows
    are at or before it, and writes each chunk's end trimmed to it into ends. Every row left out
    is later than every row kept, so merging the kept rows now and the rest later keeps time
    order. If more than max_rows rows share the earliest timestamp, the cut is that timestamp:
    all of them are kept, so a pass always makes progress. Returns the kept row count."""
    k = len(chunks)
    lo = np.int64(0)
    hi = np.int64(0)
    seen = False
    for i in range(k):
        c = chunks[i]
        if c.end > c.start:
            ts = c.bundle.timestamps
            first = ts[c.start]
            last = ts[c.end - 1]
            if not seen or first < lo:
                lo = first
            if not seen or last > hi:
                hi = last
            seen = True
    if nb_merge_count_upto(chunks, lo) > max_rows:
        cut = lo
    elif nb_merge_count_upto(chunks, hi) <= max_rows:
        cut = hi
    else:
        # count(a) <= max_rows < count(b)
        a = lo
        b = hi
        while b - a > 1:
            mid = a + (b - a) // 2
            if nb_merge_count_upto(chunks, mid) <= max_rows:
                a = mid
            else:
                b = mid
        cut = a
    kept = 0
    for i in range(k):
        c = chunks[i]
        n = 0
        if c.end > c.start:
            n = nb_find_split_idx(c.bundle.timestamps, c.start, c.end, cut)
        ends[i] = c.start + n
        kept += n
    return kept


@app_njit()
def nb_find_split_idx(timestamps, cursor, size, safe_ts):
    """Zero-allocation binary search replacing np.searchsorted."""
    left = cursor
    right = size
    while left < right:
        mid = (left + right) // 2
        if timestamps[mid] <= safe_ts:
            left = mid + 1
        else:
            right = mid
    return left - cursor


@app_njit()
def nb_sum_lengths(lengths, start, end):
    """Zero-allocation sum replacing np.sum(slice)."""
    total = 0
    for i in range(start, end):
        total += lengths[i]
    return total

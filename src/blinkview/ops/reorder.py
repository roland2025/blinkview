# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle


class MergeChunk(NamedTuple):
    """Strictly typed instruction for the Numba merge kernel."""

    bundle: LogBundle
    start: int
    end: int


@app_njit()
def nb_hybrid_merge_and_copy(chunks, ts_scr, b_idx_scr, r_idx_scr, sort_order, out_bundle):
    """
    1. Flattens data to bypass Numba object refcount overhead.
    2. Performs an O(N * K) k-way merge directly on the flat arrays.
    3. Copies chronologically.
    """
    k = len(chunks)
    cursor = 0

    cursors = np.zeros(k, dtype=np.uint32)
    ends = np.zeros(k, dtype=np.uint32)

    # 1. FILL SCRATCHPADS (Flattens the data into primitive 1D arrays)
    for i in range(k):
        chunk = chunks[i]
        bundle = chunk.bundle
        s = chunk.start
        e = chunk.end

        cursors[i] = cursor

        for j in range(s, e):
            ts_scr[cursor] = bundle.timestamps[j]
            b_idx_scr[cursor] = i
            r_idx_scr[cursor] = j
            cursor += 1

        ends[i] = cursor

    num_rows = cursor

    # 2. TINY-K MERGE INTO SORT_ORDER
    # (Replaces np.argsort with a fast linear scan over block heads)
    for out_i in range(num_rows):
        best_k = -1
        min_ts = ts_scr[0]  # Dummy init for Numba static type inference

        for i in range(k):
            c = cursors[i]
            if c < ends[i]:
                ts = ts_scr[c]
                if best_k == -1 or ts < min_ts:
                    min_ts = ts
                    best_k = i

        sort_order[out_i] = cursors[best_k]
        cursors[best_k] += 1

    # 3. COPY TO OUTPUT BUNDLE (Pure sequential memory copy)
    out_idx = out_bundle.size[0]
    out_msg_cursor = out_bundle.msg_cursor[0]

    for i in range(num_rows):
        idx = sort_order[i]
        b_id = b_idx_scr[idx]
        r_id = r_idx_scr[idx]

        src_bundle = chunks[b_id].bundle

        # Copy mandatory columns
        out_bundle.rx_timestamps[out_idx] = src_bundle.rx_timestamps[r_id]
        out_bundle.timestamps[out_idx] = src_bundle.timestamps[r_id]
        src_off = src_bundle.offsets[r_id]
        src_len = src_bundle.lengths[r_id]

        out_bundle.offsets[out_idx] = out_msg_cursor
        out_bundle.lengths[out_idx] = src_len

        # Raw Memory Copy for Bytes
        for b in range(src_len):
            out_bundle.buffer[out_msg_cursor + b] = src_bundle.buffer[src_off + b]
        out_msg_cursor += src_len

        # Copy optional columns
        if out_bundle.has_levels and src_bundle.has_levels:
            out_bundle.levels[out_idx] = src_bundle.levels[r_id]
        if out_bundle.has_modules and src_bundle.has_modules:
            out_bundle.modules[out_idx] = src_bundle.modules[r_id]
        if out_bundle.has_devices and src_bundle.has_devices:
            out_bundle.devices[out_idx] = src_bundle.devices[r_id]
        if out_bundle.has_sequences and src_bundle.has_sequences:
            out_bundle.sequences[out_idx] = src_bundle.sequences[r_id]

        # Rows from a source without pids/tids (e.g. system-generated logs, CAN) must get an
        # explicit 0, not whatever stale value the array-pool-recycled output slot happened to
        # hold - array_pool.acquire() does not zero-fill, and out_bundle rows are reused across
        # many merge calls.
        if out_bundle.has_pids:
            out_bundle.pids[out_idx] = src_bundle.pids[r_id] if src_bundle.has_pids else 0
        if out_bundle.has_tids:
            out_bundle.tids[out_idx] = src_bundle.tids[r_id] if src_bundle.has_tids else 0

        out_idx += 1

    # Write back the new sizes to the 1D arrays
    out_bundle.size[0] = out_idx
    out_bundle.msg_cursor[0] = out_msg_cursor


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

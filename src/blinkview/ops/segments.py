# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.dtypes import ID_UNSPECIFIED, LEVEL_UNSPECIFIED, SEQ_NONE, TS_UNSPECIFIED
from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.segments import LogSegmentParams


@app_njit()
def copy_batch_to_segment(segment: LogSegmentParams, batch: LogBundle, batch_start_idx: int, start_seq_id: int):
    # 1. READ INTERNAL STATE
    # We read the current write-head and count from the shared arrays
    seg_cursor = segment.msg_cursor[0]
    current_seg_count = segment.count[0]

    rows_to_copy = 0
    bytes_to_copy = 0
    seg_buf_len = segment.buffer.shape[0]
    batch_size = batch.size[0]

    # 2. SCAN: Calculate fit
    for i in range(batch_start_idx, batch_size):
        if current_seg_count + rows_to_copy >= segment.capacity:
            break

        msg_len = batch.lengths[i]
        if seg_cursor + bytes_to_copy + msg_len > seg_buf_len:
            break

        rows_to_copy += 1
        bytes_to_copy += msg_len

    if rows_to_copy == 0:
        return 0

    # 3. DEFINE BOUNDARIES
    s_start = current_seg_count
    s_end = current_seg_count + rows_to_copy
    b_start = batch_start_idx
    b_end = batch_start_idx + rows_to_copy
    b_byte_start = batch.offsets[b_start]
    b_byte_end = b_byte_start + bytes_to_copy

    # 4. BLOCK COPIES
    segment.timestamps[s_start:s_end] = batch.timestamps[b_start:b_end]
    segment.lengths[s_start:s_end] = batch.lengths[b_start:b_end]
    segment.buffer[seg_cursor : seg_cursor + bytes_to_copy] = batch.buffer[b_byte_start:b_byte_end]

    if batch.has_levels:
        segment.levels[s_start:s_end] = batch.levels[b_start:b_end]
    if batch.has_modules:
        segment.modules[s_start:s_end] = batch.modules[b_start:b_end]
    if batch.has_devices:
        segment.devices[s_start:s_end] = batch.devices[b_start:b_end]

    # 5. SHIFT OFFSETS & SEQUENCE IDS
    for i in range(rows_to_copy):
        segment.offsets[s_start + i] = seg_cursor + (batch.offsets[b_start + i] - b_byte_start)
        segment.sequence_ids[s_start + i] = start_seq_id + i + 1

    # --- THE KEY UPDATE ---
    # Update the counters in-place before exiting
    segment.count[0] += rows_to_copy
    segment.msg_cursor[0] += bytes_to_copy

    return rows_to_copy


@app_njit()
def filter_segment(
    segment,
    target_modules_arr,
    start_seq=SEQ_NONE,  # Ensure this is here
    start_ts=TS_UNSPECIFIED,
    end_ts=TS_UNSPECIFIED,
    target_level=LEVEL_UNSPECIFIED,
    target_module=ID_UNSPECIFIED,
    target_device=ID_UNSPECIFIED,
):

    count = segment.count[0]
    timestamps = segment.timestamps
    levels = segment.levels
    modules = segment.modules
    devices = segment.devices
    seqs = segment.sequence_ids

    target_modules_size = target_modules_arr.size

    matching_indices = np.empty(count, dtype=np.int64)
    match_count = 0
    use_multi_module = target_modules_size > 0

    for i in range(count):
        # 1. Sequence Check (Fastest exclusion)
        if start_seq != SEQ_NONE and seqs[i] <= start_seq:
            continue

        # 2. Module Filter
        if use_multi_module:
            found = False
            for m_idx in range(target_modules_size):
                if modules[i] == target_modules_arr[m_idx]:
                    found = True
                    break
            if not found:
                continue
        elif target_module != ID_UNSPECIFIED and modules[i] != target_module:
            continue

        # 3. Level/Device/Time filters...
        if target_level != LEVEL_UNSPECIFIED and levels[i] != target_level:
            continue
        if target_device != ID_UNSPECIFIED and devices[i] != target_device:
            continue
        if start_ts != -1 and timestamps[i] < start_ts:
            continue
        if end_ts != -1 and timestamps[i] > end_ts:
            continue

        matching_indices[match_count] = i
        match_count += 1

    return matching_indices[:match_count]

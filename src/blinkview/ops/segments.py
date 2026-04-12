# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle
from blinkview.core.types.segments import LogSegmentParams


@app_njit()
def copy_batch_to_segment(
    segment: LogSegmentParams, seg_cursor: int, batch: LogBundle, batch_start_idx: int, start_seq_id: int
):
    """
    Appends as many rows as possible from the batch starting at `batch_start_idx`.
    Returns a tuple: (rows_copied, bytes_copied).
    """
    rows_to_copy = 0
    bytes_to_copy = 0
    seg_buf_len = segment.buffer.shape[0]

    # 1. SCAN: Calculate fit based on row capacity and byte capacity
    for i in range(batch_start_idx, batch.size):
        if segment.count + rows_to_copy >= segment.capacity:
            break  # Row limit

        msg_len = batch.lengths[i]
        if seg_cursor + bytes_to_copy + msg_len > seg_buf_len:
            break  # Byte limit

        rows_to_copy += 1
        bytes_to_copy += msg_len

    if rows_to_copy == 0:
        return 0, 0

    # 2. DEFINE BOUNDARIES
    s_start = segment.count
    s_end = segment.count + rows_to_copy

    b_start = batch_start_idx
    b_end = batch_start_idx + rows_to_copy

    b_byte_start = batch.offsets[b_start]
    b_byte_end = b_byte_start + bytes_to_copy

    # 3. BLOCK COPIES (Vectorized)
    segment.timestamps[s_start:s_end] = batch.timestamps[b_start:b_end]
    segment.lengths[s_start:s_end] = batch.lengths[b_start:b_end]
    segment.buffer[seg_cursor : seg_cursor + bytes_to_copy] = batch.buffer[b_byte_start:b_byte_end]

    # 4. SHIFT OFFSETS
    # New offsets are relative to the segment's cursor
    for i in range(rows_to_copy):
        segment.offsets[s_start + i] = seg_cursor + (batch.offsets[b_start + i] - b_byte_start)

    # 5. HANDLE OPTIONAL COLUMNS
    # Level, Module, and Device columns are only copied if the batch has them
    if batch.has_levels:
        segment.levels[s_start:s_end] = batch.levels[b_start:b_end]
    else:
        segment.levels[s_start:s_end] = 0

    if batch.has_modules:
        segment.modules[s_start:s_end] = batch.modules[b_start:b_end]
    else:
        segment.modules[s_start:s_end] = 0

    if batch.has_devices:
        segment.devices[s_start:s_end] = batch.devices[b_start:b_end]
    else:
        segment.devices[s_start:s_end] = 0

    # 6. SEQUENCE IDS
    # Assign incrementing IDs and write them back to the batch if it has sequence memory
    for i in range(rows_to_copy):
        val = start_seq_id + i
        segment.sequence_ids[s_start + i] = val
        if batch.has_sequences:
            batch.sequences[b_start + i] = val

    return rows_to_copy, bytes_to_copy


@app_njit()
def filter_segment(
    segment,
    target_modules_arr,
    start_seq=-1,  # Ensure this is here
    start_ts=-1,
    end_ts=-1,
    target_level=0xFF,
    target_module=0xFFFF,
    target_device=0xFFFF,
):

    count = segment.count
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
        if start_seq != -1 and seqs[i] <= start_seq:
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
        elif target_module != 0xFFFF and modules[i] != target_module:
            continue

        # 3. Level/Device/Time filters...
        if target_level != 0xFF and levels[i] != target_level:
            continue
        if target_device != 0xFFFF and devices[i] != target_device:
            continue
        if start_ts != -1 and timestamps[i] < start_ts:
            continue
        if end_ts != -1 and timestamps[i] > end_ts:
            continue

        matching_indices[match_count] = i
        match_count += 1

    return matching_indices[:match_count]

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
from numba import types, uint32, uint64

from blinkview.core.dtypes import ID_TYPE, ID_UNSPECIFIED, LEVEL_UNSPECIFIED, SEQ_NONE, SEQ_TYPE, TS_UNSPECIFIED
from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle


@app_njit()
def copy_batch_to_segment(segment: LogBundle, batch: LogBundle, batch_start_idx: int, start_seq_id: int):
    # 1. READ INTERNAL STATE
    # We read the current write-head and count from the shared arrays
    seg_cursor = segment.msg_cursor[0]
    current_seg_count = segment.size[0]

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
    segment.rx_timestamps[s_start:s_end] = batch.rx_timestamps[b_start:b_end]
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
        segment.sequences[s_start + i] = start_seq_id + i + 1

    # --- THE KEY UPDATE ---
    # Update the counters in-place before exiting
    segment.size[0] += rows_to_copy
    segment.msg_cursor[0] += bytes_to_copy

    return rows_to_copy


@app_njit()
def filter_segment(
    segment: LogBundle,
    target_modules_arr,
    start_seq=SEQ_NONE,  # Ensure this is here
    start_ts=TS_UNSPECIFIED,
    end_ts=TS_UNSPECIFIED,
    target_level=LEVEL_UNSPECIFIED,
    target_module=ID_UNSPECIFIED,
    target_device=ID_UNSPECIFIED,
):

    count = segment.size[0]
    timestamps = segment.timestamps
    levels = segment.levels
    modules = segment.modules
    devices = segment.devices
    seqs = segment.sequences

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


@app_njit()
def nb_find_next_module_match(segment: LogBundle, target_module, start_seq):
    """
    Returns (seq_id, array_index) as (uint64, uint64).
    If not found, returns (0, 0).
    """
    count = segment.size[0]
    seqs = segment.sequences
    modules = segment.modules

    for i in range(count):
        # start_seq=0 (SEQ_NONE) allows the first record (ID 1) to pass
        if start_seq != 0 and seqs[i] <= start_seq:
            continue

        if modules[i] == target_module:
            # Found! Return both as uint64
            return seqs[i], np.uint64(i)

    # Not found: return the "Zero Tuple"
    return np.uint64(0), np.uint64(0)


@app_njit()
def _nb_bundle_push(
    bundle: LogBundle, ts_ns, rx_ts_ns, msg_bytes, level, module, device, seq, ext_u32_1, ext_u32_2, ext_u64_1
):
    # 1. Early Exit & Pre-flight
    size_ptr = bundle.size
    idx = size_ptr[0]
    if idx >= bundle.capacity:
        return False

    msg_len = len(msg_bytes)
    cursor_ptr = bundle.msg_cursor
    cursor = cursor_ptr[0]

    # Localize the buffer pointer for SIMD throughput
    bundle_buffer = bundle.buffer

    if cursor + msg_len > len(bundle_buffer):
        return False

    # 2. Metadata Writes (Structure of Arrays)
    bundle.timestamps[idx] = ts_ns
    bundle.rx_timestamps[idx] = rx_ts_ns
    bundle.offsets[idx] = cursor
    bundle.lengths[idx] = msg_len

    # Core Optional Columns
    if bundle.has_levels:
        bundle.levels[idx] = level
    if bundle.has_modules:
        bundle.modules[idx] = module
    if bundle.has_devices:
        bundle.devices[idx] = device
    if bundle.has_sequences:
        bundle.sequences[idx] = seq

    # Heterogeneous Extension Columns
    if bundle.has_ext_u32_1:
        bundle.ext_u32_1[idx] = ext_u32_1
    if bundle.has_ext_u32_2:
        bundle.ext_u32_2[idx] = ext_u32_2
    if bundle.has_ext_u64_1:
        bundle.ext_u64_1[idx] = ext_u64_1

    # 3. Vectorized Copy with Hoisted Pointer
    if msg_len > 0:
        for i in range(msg_len):
            bundle_buffer[cursor + i] = msg_bytes[i]

        cursor_ptr[0] += msg_len

    size_ptr[0] += 1
    return True


@app_njit()
def _nb_bundle_extend(bundle, msg_bytes):
    # 1. Access size and ensure there's something to append to
    size_ptr = bundle.size
    size = size_ptr[0]
    if size == 0:
        return False

    msg_len = len(msg_bytes)
    cursor_ptr = bundle.msg_cursor
    cursor = cursor_ptr[0]

    # Pointer Hoisting
    bundle_buffer = bundle.buffer

    if cursor + msg_len > len(bundle_buffer):
        return False

    # 2. Target the last entry in the SoA
    idx = size - 1

    # 3. Explicit Loop for Vectorized Copy
    if msg_len > 0:
        for i in range(msg_len):
            bundle_buffer[cursor + i] = msg_bytes[i]

        # Update metadata: increment the length of the LAST message
        # and move the global buffer cursor
        bundle.lengths[idx] += msg_len
        cursor_ptr[0] += msg_len

    return True

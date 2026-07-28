# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.dtypes import (
    SEQ_NONE,
    SEQ_TYPE,
    TS_TYPE,
    TS_UNSPECIFIED,
)
from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle
from blinkview.ops.constants import CHAR_EQUALS, CHAR_SPACE
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS, nb_row_matches_kv_conditions
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH, nb_bytes_contains_ci


@app_njit()
def nb_copy_batch_to_segment(segment: LogBundle, batch: LogBundle, batch_start_idx: int, start_seq_id: int):
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
    # Rows from a batch without pids/tids (e.g. system-generated logs, CAN) must get an explicit
    # 0, not whatever stale value the array-pool-recycled segment slot happened to hold -
    # array_pool.acquire() does not zero-fill, and segment backing arrays get reused across many
    # segment rotations.
    if segment.has_pids:
        if batch.has_pids:
            segment.pids[s_start:s_end] = batch.pids[b_start:b_end]
        else:
            segment.pids[s_start:s_end] = 0
    if segment.has_tids:
        if batch.has_tids:
            segment.tids[s_start:s_end] = batch.tids[b_start:b_end]
        else:
            segment.tids[s_start:s_end] = 0

    # 5. SHIFT OFFSETS & SEQUENCE IDS
    for i in range(rows_to_copy):
        segment.offsets[s_start + i] = seg_cursor + (batch.offsets[b_start + i] - b_byte_start)
        segment.sequences[s_start + i] = start_seq_id + i + 1

    # --- THE KEY UPDATE ---
    # Update the counters in-place before exiting
    segment.size[0] += rows_to_copy
    segment.msg_cursor[0] += bytes_to_copy

    return rows_to_copy


# ---------------------------------------------------------
# Inline Binary Search Helpers (Zero NumPy Overhead)
# ---------------------------------------------------------
@app_njit(inline="always")
def nb_fast_find_first_ge(arr, count, val):
    """Finds first index where arr[i] >= val"""
    left = 0
    right = count
    while left < right:
        mid = (left + right) >> 1
        if arr[mid] < val:
            left = mid + 1
        else:
            right = mid
    return left


@app_njit(inline="always")
def nb_fast_find_first_gt(arr, count, val):
    """Finds first index where arr[i] > val"""
    left = 0
    right = count
    while left < right:
        mid = (left + right) >> 1
        if arr[mid] <= val:
            left = mid + 1
        else:
            right = mid
    return left


def segment_filter_reversed(
    segment,  # LogBundle
    effective_mask,
    out_indices,
    max_matches,
    start_seq=SEQ_NONE,
    end_seq=SEQ_NONE,
    start_ts=TS_UNSPECIFIED,
    end_ts=TS_UNSPECIFIED,
    kv=EMPTY_KV_CONDITIONS,  # KvConditionArrays
    kv_field_delim=CHAR_SPACE,
    kv_kv_delim=CHAR_EQUALS,
    text=EMPTY_TEXT_SEARCH,  # TextSearchArrays
):
    return nb_segment_filter_reversed(
        segment,
        effective_mask,
        out_indices,
        max_matches,
        SEQ_TYPE(start_seq),
        SEQ_TYPE(end_seq),
        TS_TYPE(start_ts),
        TS_TYPE(end_ts),
        kv,
        kv_field_delim,
        kv_kv_delim,
        text,
    )


@app_njit()
def nb_segment_filter_reversed(
    segment,  # LogBundle
    effective_mask,
    out_indices,
    max_matches,
    start_seq,
    end_seq,
    start_ts,
    end_ts,
    kv,
    kv_field_delim,
    kv_kv_delim,
    text,
):
    count = segment.size[0]
    timestamps = segment.timestamps
    levels = segment.levels
    modules = segment.modules
    devices = segment.devices
    seqs = segment.sequences

    # 1. Zero-Overhead Logarithmic Boundary Finding
    loop_start = 0
    loop_end = count

    if start_seq != SEQ_NONE:
        idx = nb_fast_find_first_gt(seqs, count, start_seq)
        if idx > loop_start:
            loop_start = idx

    # end_seq is an INCLUSIVE upper bound (only rows with seq <= end_seq are eligible) - used to
    # anchor a "history before X" backward scan that must not include rows at/after the anchor.
    if end_seq != SEQ_NONE:
        idx = nb_fast_find_first_gt(seqs, count, end_seq)
        if idx < loop_end:
            loop_end = idx

    if start_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_ge(timestamps, count, start_ts)
        if idx > loop_start:
            loop_start = idx

    if end_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_gt(timestamps, count, end_ts)
        if idx < loop_end:
            loop_end = idx

    if loop_start >= loop_end:
        return 0

    match_count = 0

    # 2. Scan BACKWARDS from the newest valid log
    for i in range(loop_end - 1, loop_start - 1, -1):
        level_ok = levels[i] >= effective_mask[modules[i]]
        kv_ok = kv.num_conditions == 0 or nb_row_matches_kv_conditions(
            segment.buffer,
            segment.offsets[i],
            segment.lengths[i],
            kv.cond_keys_buf,
            kv.cond_keys_off,
            kv.cond_keys_len,
            kv.cond_vals_buf,
            kv.cond_vals_off,
            kv.cond_vals_len,
            kv.num_conditions,
            kv_field_delim,
            kv_kv_delim,
        )
        text_ok = text.needle_len == 0 or (
            (devices[i] < len(text.dev_mask) and text.dev_mask[devices[i]])
            or (modules[i] < len(text.mod_mask) and text.mod_mask[modules[i]])
            or nb_bytes_contains_ci(
                segment.buffer, segment.offsets[i], segment.lengths[i], text.needle_buf, text.needle_len
            )
        )
        is_match = level_ok and kv_ok and text_ok

        if is_match:
            out_indices[match_count] = i
            match_count += 1
            if match_count >= max_matches:
                break

    # 3. Reverse indices in-place so the formatting engine processes them chronologically
    left = 0
    right = match_count - 1
    while left < right:
        tmp = out_indices[left]
        out_indices[left] = out_indices[right]
        out_indices[right] = tmp
        left += 1
        right -= 1

    return match_count


@app_njit()
def nb_segment_extract_fields(
    segment,  # LogBundle
    indices: np.ndarray,
    count,
    out_bundle,  # LogBundle - written fixed-stride, row * max_msg_bytes, not via msg_cursor
    out_row_offset: int,
    max_msg_bytes: int,
) -> int:
    """
    Copies `count` rows referenced by `indices` (as produced by nb_segment_filter_reversed)
    out of `segment` into `out_bundle`'s flat structured columns, starting at `out_row_offset`.
    Messages longer than `max_msg_bytes` are truncated. Returns the number of rows written.
    """
    s_ts = segment.timestamps
    s_rx_ts = segment.rx_timestamps
    s_devs = segment.devices
    s_lvls = segment.levels
    s_mods = segment.modules
    s_seqs = segment.sequences
    s_offs = segment.offsets
    s_lens = segment.lengths
    s_buf = segment.buffer

    out_ts = out_bundle.timestamps
    out_rx_ts = out_bundle.rx_timestamps
    out_dev = out_bundle.devices
    out_lvl = out_bundle.levels
    out_mod = out_bundle.modules
    out_seq = out_bundle.sequences
    out_offs = out_bundle.offsets
    out_lens = out_bundle.lengths
    out_buf = out_bundle.buffer

    copy_pids = segment.has_pids and out_bundle.has_pids
    copy_tids = segment.has_tids and out_bundle.has_tids
    s_pids = segment.pids
    s_tids = segment.tids
    out_pids = out_bundle.pids
    out_tids = out_bundle.tids

    for i in range(count):
        src_idx = indices[i]
        row = out_row_offset + i

        out_ts[row] = s_ts[src_idx]
        out_rx_ts[row] = s_rx_ts[src_idx]
        out_dev[row] = s_devs[src_idx]
        out_lvl[row] = s_lvls[src_idx]
        out_mod[row] = s_mods[src_idx]
        out_seq[row] = s_seqs[src_idx]
        if copy_pids:
            out_pids[row] = s_pids[src_idx]
        if copy_tids:
            out_tids[row] = s_tids[src_idx]

        msg_len = s_lens[src_idx]
        copy_len = msg_len if msg_len < max_msg_bytes else max_msg_bytes

        src_off = s_offs[src_idx]
        dst_off = row * max_msg_bytes
        out_offs[row] = dst_off
        out_lens[row] = copy_len
        for b in range(copy_len):
            out_buf[dst_off + b] = s_buf[src_off + b]

    return count


def segment_filter(
    segment,  # LogBundle
    effective_mask,
    out_indices,
    max_matches,
    start_seq=SEQ_NONE,
    start_ts=TS_UNSPECIFIED,
    end_ts=TS_UNSPECIFIED,
    kv=EMPTY_KV_CONDITIONS,  # KvConditionArrays
    kv_field_delim=CHAR_SPACE,
    kv_kv_delim=CHAR_EQUALS,
    text=EMPTY_TEXT_SEARCH,  # TextSearchArrays
):
    return nb_filter_segment(
        segment,  # LogBundle
        effective_mask,
        out_indices,
        max_matches,
        SEQ_TYPE(start_seq),
        TS_TYPE(start_ts),
        TS_TYPE(end_ts),
        kv,
        kv_field_delim,
        kv_kv_delim,
        text,
    )


@app_njit()
def nb_filter_segment(
    segment,  # LogBundle
    effective_mask,
    out_indices,
    max_matches,
    start_seq,
    start_ts,
    end_ts,
    kv,
    kv_field_delim,
    kv_kv_delim,
    text,
):
    count = segment.size[0]
    timestamps = segment.timestamps
    levels = segment.levels
    modules = segment.modules
    devices = segment.devices
    seqs = segment.sequences

    # 1. Zero-Overhead Logarithmic Boundary Finding
    loop_start = 0
    loop_end = count

    if start_seq != SEQ_NONE:
        idx = nb_fast_find_first_gt(seqs, count, start_seq)
        if idx > loop_start:
            loop_start = idx

    if start_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_ge(timestamps, count, start_ts)
        if idx > loop_start:
            loop_start = idx

    if end_ts != TS_UNSPECIFIED:
        idx = nb_fast_find_first_gt(timestamps, count, end_ts)
        if idx < loop_end:
            loop_end = idx

    if loop_start >= loop_end:
        return 0

    match_count = 0
    for i in range(loop_start, loop_end):
        # The ultimate O(1) check:
        # Is the log level >= the threshold baked for this module?
        level_ok = levels[i] >= effective_mask[modules[i]]
        kv_ok = kv.num_conditions == 0 or nb_row_matches_kv_conditions(
            segment.buffer,
            segment.offsets[i],
            segment.lengths[i],
            kv.cond_keys_buf,
            kv.cond_keys_off,
            kv.cond_keys_len,
            kv.cond_vals_buf,
            kv.cond_vals_off,
            kv.cond_vals_len,
            kv.num_conditions,
            kv_field_delim,
            kv_kv_delim,
        )
        text_ok = text.needle_len == 0 or (
            (devices[i] < len(text.dev_mask) and text.dev_mask[devices[i]])
            or (modules[i] < len(text.mod_mask) and text.mod_mask[modules[i]])
            or nb_bytes_contains_ci(
                segment.buffer, segment.offsets[i], segment.lengths[i], text.needle_buf, text.needle_len
            )
        )
        is_match = level_ok and kv_ok and text_ok

        # Branchless Append
        out_indices[match_count] = i
        match_count += is_match

        if match_count >= max_matches:
            break

    return match_count


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
    return SEQ_NONE, np.uint64(0)


@app_njit()
def nb_find_next_module_index(segment: LogBundle, target_module, start_idx):
    """
    Returns (seq_id, array_index) as (uint64, uint64).
    If not found, returns (0, 0).
    """
    count = segment.size[0]
    modules = segment.modules

    for i in range(start_idx, count):
        if modules[i] == target_module:
            # Found! Return both as uint64
            return True, np.uint64(i)

    # Not found: return the "Zero Tuple"
    return False, np.uint64(0)


@app_njit()
def nb_bundle_push(
    bundle: LogBundle,
    ts_ns,
    rx_ts_ns,
    msg_bytes,
    level,
    module,
    device,
    seq,
    ext_u32_1,
    ext_u32_2,
    ext_u64_1,
    pid=0,
    tid=0,
):
    """
    Convenience wrapper that infers the length directly from the buffer
    and delegates execution to the core length-aware Numba kernel.
    """
    msg_len = len(msg_bytes)

    return nb_bundle_push_len(
        bundle,
        ts_ns,
        rx_ts_ns,
        msg_bytes,
        msg_len,
        level,
        module,
        device,
        seq,
        ext_u32_1,
        ext_u32_2,
        ext_u64_1,
        pid,
        tid,
    )


@app_njit()
def nb_can_push(
    bundle: LogBundle,
    raw_timestamp: float,
    offset_ns: int,
    arb_id: int,
    data: np.ndarray,  # Expects a uint8 view
    is_ext: bool,
    is_rem: bool,
    is_err: bool,
    is_fd: bool,
    is_rx: bool,
    brs: bool,
    esi: bool,
) -> bool:
    # 1. Project Monotonic/Boot time to Epoch Nanoseconds
    # $$T_{ns} = T_{offset} + \text{int}(T_{raw} \times 10^9)$$
    ts_ns = offset_ns + int(raw_timestamp * 1_000_000_000)

    # 2. Fast Bit-Packing for ext_u32_2
    flags = 0
    if is_ext:
        flags |= 0x01  # Bit 0: Extended vs Standard
    if is_rem:
        flags |= 0x02  # Bit 1: Remote Frame
    if is_err:
        flags |= 0x04  # Bit 2: Error Frame
    if is_fd:
        flags |= 0x08  # Bit 3: CAN FD Frame
    if is_rx:
        flags |= 0x10  # Bit 4: Rx vs Tx
    if brs:
        flags |= 0x20  # Bit 5: Bit Rate Switch
    if esi:
        flags |= 0x40  # Bit 6: Error State Indicator

    # 3. Direct push into the bundle
    # level, module, device, seq are 0 for raw CAN ingress
    return nb_bundle_push(bundle, ts_ns, ts_ns, data, 0, 0, 0, 0, arb_id, flags, 0)


@app_njit(inline="always")
def nb_bundle_push_len(
    bundle: LogBundle,
    ts_ns,
    rx_ts_ns,
    msg_bytes,
    msg_len,
    level,
    module,
    device,
    seq,
    ext_u32_1,
    ext_u32_2,
    ext_u64_1,
    pid=0,
    tid=0,
):
    # 1. Early Exit & Pre-flight
    size_ptr = bundle.size
    idx = size_ptr[0]
    if idx >= bundle.capacity:
        return False

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
    if bundle.has_pids:
        bundle.pids[idx] = pid
    if bundle.has_tids:
        bundle.tids[idx] = tid

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
def nb_bundle_extend(bundle, msg_bytes):
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

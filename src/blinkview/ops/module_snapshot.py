# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple, Tuple

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle

# Constant defining the maximum payload allocation per module in bytes
MAX_MSG_BYTES = 512


class ModuleSnapshotParams(NamedTuple):
    """Numba-compatible view of a module snapshot's memory."""

    timestamps: np.ndarray  # dtypes.TS_TYPE
    sequence_ids: np.ndarray  # dtypes.SEQ_TYPE
    levels: np.ndarray  # dtypes.LEVEL_TYPE
    lengths: np.ndarray  # dtypes.LEN_TYPE
    buffer: np.ndarray  # dtypes.BYTE

    count: int
    capacity: int


@app_njit()
def nb_copy_snapshot_state(
    old_b: ModuleSnapshotParams,
    new_b: ModuleSnapshotParams,
):
    """Fast compiled memory copy from the old snapshot to the new one."""
    old_cnt = old_b.count

    new_b.timestamps[:old_cnt] = old_b.timestamps[:old_cnt]
    new_b.levels[:old_cnt] = old_b.levels[:old_cnt]
    new_b.lengths[:old_cnt] = old_b.lengths[:old_cnt]
    new_b.sequence_ids[:old_cnt] = old_b.sequence_ids[:old_cnt]

    bytes_to_copy = old_cnt * MAX_MSG_BYTES
    new_b.buffer[:bytes_to_copy] = old_b.buffer[:bytes_to_copy]

    # CLEANSE THE TAIL:
    if new_b.capacity > old_cnt:
        new_b.sequence_ids[old_cnt:] = 0
        new_b.lengths[old_cnt:] = 0
        new_b.timestamps[old_cnt:] = 0
        new_b.levels[old_cnt:] = 0


@app_njit()
def nb_update_master_arrays_reverse(
    seg_b: LogBundle,
    snap_b: ModuleSnapshotParams,
    module_count: int,
    last_known_seq: int,
    is_initialized: bool,
) -> bool:
    """
    Scans a segment from back-to-front.
    Returns True if we hit the 'last_known_seq' (time to stop everything).
    """
    row_count = seg_b.size[0]
    seg_b_modules = seg_b.modules
    seg_b_timestamps = seg_b.timestamps
    seg_b_levels = seg_b.levels
    seg_b_lengths = seg_b.lengths
    seg_b_offsets = seg_b.offsets
    seg_b_buffer = seg_b.buffer
    seg_b_sequences = seg_b.sequences

    snap_b_timestamps = snap_b.timestamps
    snap_b_sequence_ids = snap_b.sequence_ids
    snap_b_levels = snap_b.levels
    snap_b_lengths = snap_b.lengths
    snap_b_buffer = snap_b.buffer

    for i in range(row_count - 1, -1, -1):
        seq = seg_b_sequences[i]

        if is_initialized and seq <= last_known_seq:
            return True

        mod_id = seg_b_modules[i]
        # Protect against out-of-bounds or newly registered modules
        # not yet accounted for in this update cycle
        if mod_id >= module_count:
            continue

        if seq > snap_b_sequence_ids[mod_id]:
            snap_b_timestamps[mod_id] = seg_b_timestamps[i]
            snap_b_sequence_ids[mod_id] = seq
            snap_b_levels[mod_id] = seg_b_levels[i]

            m_len = seg_b_lengths[i]
            # Cap at MAX_MSG_BYTES - 1 to guarantee room for the 0-terminator
            if m_len > MAX_MSG_BYTES - 1:
                m_len = MAX_MSG_BYTES - 1

            s_off = seg_b_offsets[i]
            m_off = mod_id * MAX_MSG_BYTES  # Computed on the fly

            # Copy the message payload
            snap_b_buffer[m_off : m_off + m_len] = seg_b_buffer[s_off : s_off + m_len]
            snap_b_lengths[mod_id] = m_len

            # Always 0-terminate the string, regardless of original length
            snap_b_buffer[m_off + m_len] = 0

    return False


@app_njit()
def nb_build_snapshot_as_of(
    seg_b: LogBundle,
    snap_b: ModuleSnapshotParams,
    module_count: int,
    max_ts_ns: int,
    found_mask: np.ndarray,  # bool_, len module_count, in/out
    remaining: int,
) -> Tuple[bool, int]:
    """
    Playback-scrub counterpart to nb_update_master_arrays_reverse: instead of resuming
    from a forward sequence watermark, rebuilds "latest message per module" from scratch as
    of an arbitrary point in the past (`max_ts_ns`). Scans a segment back-to-front, skipping
    rows newer than max_ts_ns (they're in the log's future relative to the playhead) and
    filling in the first (i.e. latest-before-max_ts_ns) row seen per not-yet-found module.
    `found_mask`/`remaining` carry state across segments so a caller can stop early once
    every module has been resolved instead of always scanning back to the start of the log.
    Returns (all_found, remaining).
    """
    row_count = seg_b.size[0]
    seg_b_modules = seg_b.modules
    seg_b_timestamps = seg_b.timestamps
    seg_b_levels = seg_b.levels
    seg_b_lengths = seg_b.lengths
    seg_b_offsets = seg_b.offsets
    seg_b_buffer = seg_b.buffer
    seg_b_sequences = seg_b.sequences

    snap_b_timestamps = snap_b.timestamps
    snap_b_sequence_ids = snap_b.sequence_ids
    snap_b_levels = snap_b.levels
    snap_b_lengths = snap_b.lengths
    snap_b_buffer = snap_b.buffer

    for i in range(row_count - 1, -1, -1):
        if remaining <= 0:
            return True, remaining

        ts = seg_b_timestamps[i]
        if ts > max_ts_ns:
            continue

        mod_id = seg_b_modules[i]
        if mod_id >= module_count:
            continue

        if found_mask[mod_id]:
            continue

        snap_b_timestamps[mod_id] = ts
        snap_b_sequence_ids[mod_id] = seg_b_sequences[i]
        snap_b_levels[mod_id] = seg_b_levels[i]

        m_len = seg_b_lengths[i]
        if m_len > MAX_MSG_BYTES - 1:
            m_len = MAX_MSG_BYTES - 1

        s_off = seg_b_offsets[i]
        m_off = mod_id * MAX_MSG_BYTES

        snap_b_buffer[m_off : m_off + m_len] = seg_b_buffer[s_off : s_off + m_len]
        snap_b_lengths[mod_id] = m_len
        snap_b_buffer[m_off + m_len] = 0

        found_mask[mod_id] = True
        remaining -= 1

    return remaining <= 0, remaining

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.core.types.modules import MODULE_ID_FULL, MODULE_ID_UNKNOWN, MODULE_TEMP_ID_BASE
from blinkview.utils.fnv1a_64 import fnv1a_64_fast


@app_njit()
def resolve_module_id(name_buffer, name_start, name_len, table, tracker):
    if name_len == 0:
        return MODULE_ID_UNKNOWN

    name_hash = fnv1a_64_fast(name_buffer[name_start : name_start + name_len])

    # Check Permanent Registry (ByteMap)
    bm_buffer = table.buffer
    bm_offsets = table.offsets
    bm_lens = table.lens
    bm_hashes = table.hashes
    count = table.count

    for mod_id in range(count):
        if bm_hashes[mod_id] == name_hash and bm_lens[mod_id] == name_len:
            offset = bm_offsets[mod_id]
            is_match = True
            for j in range(name_len):
                if bm_buffer[offset + j] != name_buffer[name_start + j]:
                    is_match = False
                    break
            if is_match:
                return mod_id

    # 2. Check Temporary Cache (Tracker)
    t_count = tracker.count[0]
    t_starts = tracker.starts
    t_lens = tracker.lengths
    t_hashes = tracker.hashes
    t_buffer = tracker.name_bytes

    for i in range(t_count):
        if t_hashes[i] == name_hash and t_lens[i] == name_len:
            offset = t_starts[i]
            is_match = True
            for j in range(name_len):
                if t_buffer[offset + j] != name_buffer[name_start + j]:
                    is_match = False
                    break
            if is_match:
                return MODULE_TEMP_ID_BASE + i  # Return the existing Temp ID!

    # 3. Not found anywhere, promote it to a NEW Temporary ID!
    if t_count >= len(t_starts):
        return MODULE_ID_FULL  # Signal: Tracker is completely full

    # The string is already sitting in tracker.name_bytes (written by parse_fixed_width_name).
    # We just need to save its coordinates and advance the cursor so it isn't overwritten.
    t_starts[t_count] = name_start
    t_lens[t_count] = name_len
    t_hashes[t_count] = name_hash

    tracker.count[0] += 1
    tracker.bytes_cursor[0] += name_len

    return MODULE_TEMP_ID_BASE + t_count


@app_njit()
def resolve_module_id_(name_buffer, name_start, name_len, table, tracker):
    if name_len == 0:
        return MODULE_ID_UNKNOWN

    # Capture first byte to act as a fast rejection filter
    first_byte = name_buffer[name_start]

    # 1. Check Permanent Registry (ByteMap)
    bm_buffer = table.buffer
    bm_offsets = table.offsets
    count = table.count

    for mod_id in range(count):
        offset = bm_offsets[mod_id]

        # Fast path: Check first byte before doing full iteration
        if bm_buffer[offset] != first_byte:
            continue

        is_match = True
        # Start from index 1 since index 0 matched
        for j in range(1, name_len):
            if bm_buffer[offset + j] != name_buffer[name_start + j]:
                is_match = False
                break

        # Null Terminator Check
        if is_match and bm_buffer[offset + name_len] == 0:
            return mod_id

    # 2. Check Temporary Cache (Tracker)
    t_count = tracker.count[0]
    t_starts = tracker.starts
    t_buffer = tracker.name_bytes

    for i in range(t_count):
        offset = t_starts[i]

        # Fast path: Check first byte
        if t_buffer[offset] != first_byte:
            continue

        is_match = True
        for j in range(1, name_len):
            if t_buffer[offset + j] != name_buffer[name_start + j]:
                is_match = False
                break

        # Null Terminator Check
        if is_match and t_buffer[offset + name_len] == 0:
            return MODULE_TEMP_ID_BASE + i

            # 3. Not found anywhere, promote it to a NEW Temporary ID!
    if t_count >= len(t_starts):
        return MODULE_ID_FULL  # Signal: Tracker is completely full

    # Register the coordinates (Hashes are omitted entirely)
    t_starts[t_count] = name_start
    tracker.lengths[t_count] = name_len

    tracker.count[0] += 1

    # Advance cursor by name_len + 1 to lock in the trailing '\0' byte
    tracker.bytes_cursor[0] += name_len + 1

    return MODULE_TEMP_ID_BASE + t_count

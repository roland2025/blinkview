# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.modules import MODULE_ID_FULL, MODULE_ID_UNKNOWN, MODULE_TEMP_ID_BASE
from blinkview.ops.discovery import nb_resolve_module_id
from blinkview.ops.strings import nb_fnv1a_64_fast


@app_njit(inline="always")
def nb_mix_device_hash(name_hash, device_id):
    """Combines a device id into a name hash (bit-shifted into the high bits and XORed) so a
    single shared temp-tracker can disambiguate the same module name string registered under
    different devices - module names are only unique *per device* (see DeviceIdentity's own
    per-device modules_table), unlike module ids, which are global. Used for the tracker's
    within-batch dedup only; the permanent table (see nb_resolve_scoped_id) stays keyed by the
    plain per-module name hash and is disambiguated via its `values` column instead, so this
    mixing never has to agree with anything nb_insert_item/IndexedStringTable computes."""
    return name_hash ^ (np.uint64(device_id) << np.uint64(40))


@app_njit()
def nb_resolve_scoped_id(name_buffer, name_start, name_len, device_id, table, tracker):
    """Like ops/discovery.py's nb_resolve_module_id, but for names that are only unique within
    a `device_id` scope (module names under a device) rather than globally unique (module ids
    themselves, or device names) - two devices can register a byte-identical module name and
    must resolve to two different ids.

    `table` (a StringTableParams) is expected to have been built via ordinary
    IndexedStringTable.register_name(module_id, name, value=device_id) calls - i.e. insertion
    is completely unmodified, keyed by the plain per-name hash exactly like nb_resolve_module_id
    handles it. The scoping happens on the *lookup* side: a hash+length+byte match is only
    accepted once `table.values[mod_id] == device_id` also holds - continuing the linear probe
    past any same-named-different-device entry instead of returning it. The temp tracker (for
    names not yet in the permanent table) additionally mixes device_id into the hash used for
    its own dedup (see nb_mix_device_hash), since ModuleTrackerState carries no separate
    per-entry owner field of its own.
    """
    if name_len == 0:
        return MODULE_ID_UNKNOWN

    name_hash = nb_fnv1a_64_fast(name_buffer, name_start, name_len)

    # 1. Check the permanent table - plain (unmixed) hash, since that's how it was inserted.
    bm_buffer = table.buffer
    bm_offsets = table.offsets
    bm_lens = table.lens
    bm_hashes = table.hashes
    bm_values = table.values

    hash_index = table.hash_index
    index_mask = len(hash_index) - 1
    idx = name_hash & index_mask

    while True:
        mod_id = hash_index[idx]
        if mod_id == -1:
            break

        if bm_hashes[mod_id] == name_hash and bm_lens[mod_id] == name_len:
            offset = bm_offsets[mod_id]
            is_match = True
            for j in range(name_len):
                if bm_buffer[offset + j] != name_buffer[name_start + j]:
                    is_match = False
                    break
            # A hash+length+byte match still isn't enough on its own - module names are only
            # unique per device, so keep probing past a different device's same-named entry.
            if is_match and bm_values[mod_id] == device_id:
                return mod_id

        idx = (idx + 1) & index_mask

    # 2. Check the temporary (this-batch) tracker - mixed hash, so different devices'
    # identically-named pending entries land as distinct tracker slots.
    combined_hash = nb_mix_device_hash(name_hash, device_id)

    t_count = tracker.count[0]
    t_starts = tracker.starts
    t_lens = tracker.lengths
    t_hashes = tracker.hashes
    t_buffer = tracker.name_bytes

    for i in range(t_count):
        if t_hashes[i] == combined_hash and t_lens[i] == name_len:
            offset = t_starts[i]
            is_match = True
            for j in range(name_len):
                if t_buffer[offset + j] != name_buffer[name_start + j]:
                    is_match = False
                    break
            if is_match:
                return MODULE_TEMP_ID_BASE + i

    # 3. Not found anywhere - promote it to a new temporary id.
    if t_count >= len(t_starts):
        return MODULE_ID_FULL

    t_starts[t_count] = name_start
    t_lens[t_count] = name_len
    t_hashes[t_count] = combined_hash

    tracker.count[0] += 1
    tracker.bytes_cursor[0] += name_len

    return MODULE_TEMP_ID_BASE + t_count


@app_njit()
def nb_resolve_names_batch(buf, off_arr, len_arr, row_count, table, tracker, out_id):
    """Batch driver for nb_resolve_module_id (device names - no per-row scoping needed, one
    flat global namespace)."""
    for i in range(row_count):
        out_id[i] = nb_resolve_module_id(buf, off_arr[i], len_arr[i], table, tracker)


@app_njit()
def nb_resolve_scoped_names_batch(buf, off_arr, len_arr, device_id_arr, row_count, table, tracker, out_id):
    """Batch driver for nb_resolve_scoped_id (module names - scoped per already-resolved
    device_id_arr[i], see nb_resolve_scoped_id)."""
    for i in range(row_count):
        out_id[i] = nb_resolve_scoped_id(buf, off_arr[i], len_arr[i], device_id_arr[i], table, tracker)

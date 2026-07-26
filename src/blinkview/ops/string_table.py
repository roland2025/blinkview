# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit
from blinkview.ops.strings import nb_fnv1a_64_fast


@app_njit()
def nb_insert_item(name_array, identity_id, buffer, offsets, lens, hashes, hash_index, cursor, count):
    n_len = len(name_array)
    name_hash = nb_fnv1a_64_fast(name_array, 0, n_len)

    start = cursor

    # 1. Write Bytes (Numba handles array slicing beautifully)
    buffer[start : start + n_len] = name_array
    buffer[start + n_len] = 0  # Null terminator

    # 2. Write Metadata
    offsets[identity_id] = start
    lens[identity_id] = n_len
    hashes[identity_id] = name_hash

    # 3. Hash Index Linear Probing
    mask = len(hash_index) - 1
    idx = name_hash & mask

    while hash_index[idx] != -1:
        if hash_index[idx] == identity_id:
            break
        idx = (idx + 1) & mask

    hash_index[idx] = identity_id

    # 4. Calculate new boundaries
    new_cursor = cursor + n_len + 1
    new_count = count if count > identity_id + 1 else identity_id + 1

    return new_cursor, new_count


@app_njit()
def nb_insert_item_no_index(name_array, identity_id, buffer, offsets, lens, hashes, cursor, count):
    n_len = len(name_array)
    name_hash = nb_fnv1a_64_fast(name_array, 0, n_len)

    start = cursor

    # 1. Write Bytes
    buffer[start : start + n_len] = name_array
    buffer[start + n_len] = 0  # Null terminator

    # 2. Write Metadata
    offsets[identity_id] = start
    lens[identity_id] = n_len
    hashes[identity_id] = name_hash

    # 3. Calculate new boundaries
    new_cursor = cursor + n_len + 1
    new_count = count if count > identity_id + 1 else identity_id + 1

    return new_cursor, new_count


@app_njit()
def nb_rebuild_index(hashes, hash_index, count, mask):
    for i in range(count):
        h = hashes[i]
        idx = h & mask

        # Linear probing to find an empty slot
        while hash_index[idx] != -1:
            idx = (idx + 1) & mask

        hash_index[idx] = i

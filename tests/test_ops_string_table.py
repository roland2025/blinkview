# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.ops.string_table import nb_insert_item, nb_insert_item_no_index, nb_rebuild_index

# IndexedStringTable (tests/test_indexed_string_table.py) exercises these kernels
# indirectly end-to-end; these tests call them directly to pin down the raw
# buffer/offset/hash-index bookkeeping contract.


def _name_array(s):
    return np.frombuffer(s.encode("utf-8"), dtype=dtypes.BYTE)


def _fresh_arrays(cap=8, buf_size=64, index_size=16):
    buffer = np.zeros(buf_size, dtype=dtypes.BYTE)
    offsets = np.zeros(cap, dtype=dtypes.OFFSET_TYPE)
    lens = np.zeros(cap, dtype=dtypes.LEN_TYPE)
    hashes = np.zeros(cap, dtype=dtypes.HASH_TYPE)
    hash_index = np.full(index_size, -1, dtype=dtypes.INDEX_TYPE)
    return buffer, offsets, lens, hashes, hash_index


def test_insert_item_writes_bytes_and_null_terminator():
    buffer, offsets, lens, hashes, hash_index = _fresh_arrays()

    cursor, count = nb_insert_item(_name_array("abc"), 0, buffer, offsets, lens, hashes, hash_index, 0, 0)

    assert offsets[0] == 0
    assert lens[0] == 3
    assert buffer[0:3].tobytes() == b"abc"
    assert buffer[3] == 0  # null terminator
    assert cursor == 4  # 3 bytes + terminator
    assert count == 1


def test_insert_item_advances_cursor_across_multiple_inserts():
    buffer, offsets, lens, hashes, hash_index = _fresh_arrays()

    cursor, count = nb_insert_item(_name_array("ab"), 0, buffer, offsets, lens, hashes, hash_index, 0, 0)
    cursor, count = nb_insert_item(_name_array("cde"), 1, buffer, offsets, lens, hashes, hash_index, cursor, count)

    assert offsets[1] == 3  # starts right after "ab\0"
    assert lens[1] == 3
    assert buffer[3:6].tobytes() == b"cde"
    assert cursor == 7
    assert count == 2


def test_insert_item_count_tracks_max_identity_id_plus_one():
    buffer, offsets, lens, hashes, hash_index = _fresh_arrays()

    # Insert out of order / with gaps: count should reflect the highest id seen + 1.
    cursor, count = nb_insert_item(_name_array("a"), 0, buffer, offsets, lens, hashes, hash_index, 0, 0)
    cursor, count = nb_insert_item(_name_array("b"), 5, buffer, offsets, lens, hashes, hash_index, cursor, count)
    assert count == 6

    cursor, count = nb_insert_item(_name_array("c"), 2, buffer, offsets, lens, hashes, hash_index, cursor, count)
    assert count == 6  # inserting a lower id does not shrink count


def test_insert_item_populates_hash_index_lookup():
    buffer, offsets, lens, hashes, hash_index = _fresh_arrays()

    cursor, count = nb_insert_item(_name_array("wifi"), 0, buffer, offsets, lens, hashes, hash_index, 0, 0)

    mask = len(hash_index) - 1
    idx = hashes[0] & mask
    assert hash_index[idx] == 0


def test_insert_item_re_registering_same_name_does_not_duplicate_slot():
    """Re-inserting the same identity_id with the same name (same hash, same bucket) must stop
    at its own existing slot rather than probing forward and creating a duplicate entry."""
    buffer, offsets, lens, hashes, hash_index = _fresh_arrays()

    cursor, count = nb_insert_item(_name_array("wifi"), 0, buffer, offsets, lens, hashes, hash_index, 0, 0)
    nb_insert_item(_name_array("wifi"), 0, buffer, offsets, lens, hashes, hash_index, cursor, count)

    occurrences = np.sum(hash_index == 0)
    assert occurrences == 1


def test_insert_item_no_index_skips_hash_bookkeeping():
    buffer, offsets, lens, hashes = _fresh_arrays()[:4]

    cursor, count = nb_insert_item_no_index(_name_array("abc"), 0, buffer, offsets, lens, hashes, 0, 0)

    assert offsets[0] == 0
    assert lens[0] == 3
    assert buffer[0:3].tobytes() == b"abc"
    assert cursor == 4
    assert count == 1


def test_rebuild_index_reconstructs_lookup_from_hashes():
    buffer, offsets, lens, hashes, hash_index = _fresh_arrays()

    cursor, count = nb_insert_item_no_index(_name_array("a"), 0, buffer, offsets, lens, hashes, 0, 0)
    cursor, count = nb_insert_item_no_index(_name_array("b"), 1, buffer, offsets, lens, hashes, cursor, count)

    mask = len(hash_index) - 1
    nb_rebuild_index(hashes, hash_index, count, mask)

    idx0 = hashes[0] & mask
    idx1 = hashes[1] & mask
    # Either lands in its own bucket, or was probed forward if there was a collision.
    assert 0 in hash_index
    assert 1 in hash_index


def test_rebuild_index_handles_collision_via_linear_probing():
    hash_index = np.full(4, -1, dtype=dtypes.INDEX_TYPE)
    mask = 3
    # Force two entries into the same bucket (hash & mask collides).
    hashes = np.array([0, 4], dtype=dtypes.HASH_TYPE)  # both & 3 == 0

    nb_rebuild_index(hashes, hash_index, 2, mask)

    assert 0 in hash_index
    assert 1 in hash_index
    # They must not occupy the same slot.
    assert list(hash_index).count(0) == 1
    assert list(hash_index).count(1) == 1

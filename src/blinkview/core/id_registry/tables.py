# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.types import StringTableParams
from blinkview.core.numba_config import app_njit
from blinkview.core.types.empty import EMPTY_ID, EMPTY_INDEX
from blinkview.core.types.parsing import UnifiedParserConfig
from blinkview.utils.fnv1a_64 import fnv1a_64_fast


@app_njit()
def nb_insert_item(name_array, identity_id, buffer, offsets, lens, hashes, hash_index, cursor, count):
    n_len = len(name_array)
    name_hash = fnv1a_64_fast(name_array, 0, n_len)

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
    name_hash = fnv1a_64_fast(name_array, 0, n_len)

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


class IndexedStringTable:
    __slots__ = (
        "_offsets",
        "_lens",
        "_hashes",
        "_values",
        "_hash_index",
        "index_size",
        "_buffer",
        "cursor",
        "count",
        "_bundle",
        "use_hashes",
    )

    def __init__(
        self,
        initial_capacity: int = 2048,
        buffer_size_bytes: int = 128 * 1024,
        values_dtype: Optional[Any] = None,
        use_hashes: bool = True,
    ):
        # Initialize with standard numpy arrays
        self._offsets = np.empty(initial_capacity, dtype=dtypes.OFFSET_TYPE)
        self._lens = np.empty(initial_capacity, dtype=dtypes.LEN_TYPE)
        self._hashes = np.empty(initial_capacity, dtype=dtypes.HASH_TYPE)

        self.use_hashes = use_hashes

        # Conditionally allocate the Hash Index
        if self.use_hashes:
            target_index_size = initial_capacity * 2
            self.index_size = 1 << (target_index_size - 1).bit_length()
            self._hash_index = np.full(self.index_size, -1, dtype=dtypes.INDEX_TYPE)
        else:
            self.index_size = 0
            self._hash_index = None

        self._values = None
        if values_dtype is not None:
            self._values = np.empty(initial_capacity, dtype=values_dtype)

        self._buffer = np.empty(buffer_size_bytes, dtype=dtypes.BYTE)

        self.cursor = 0
        self.count = 0
        self._bundle = None

    def _rebuild_index(self):
        """Called when capacity grows to maintain O(1) lookups."""
        if not self.use_hashes:
            return  # Skip entirely if we don't need hashes

        # 1. Calculate safe power-of-2 size
        target_index_size = len(self._offsets) * 2
        self.index_size = 1 << (target_index_size - 1).bit_length()

        # 2. Allocate memory in Python
        self._hash_index = np.full(self.index_size, -1, dtype=dtypes.INDEX_TYPE)
        mask = self.index_size - 1

        # 3. Offload the heavy looping to Numba (Modifies _hash_index in-place)
        nb_rebuild_index(self._hashes, self._hash_index, self.count, mask)

    def register_name(self, identity_id: int, name: str, value: Any = None):
        name_bytes = name.encode("utf-8")
        n_len = len(name_bytes)
        total_space = n_len + 1
        name_array = np.frombuffer(name_bytes, dtype=dtypes.BYTE)

        # 1. Grow Metadata Capacity
        current_cap = len(self._offsets)
        if identity_id >= current_cap:
            new_cap = max(identity_id + 1, current_cap * 2)
            self._offsets = np.resize(self._offsets, new_cap)
            self._lens = np.resize(self._lens, new_cap)
            self._hashes = np.resize(self._hashes, new_cap)
            if self._values is not None:
                self._values = np.resize(self._values, new_cap)

            self._rebuild_index()
            self._bundle = None

        # 2. Grow Byte Buffer
        if self.cursor + total_space > len(self._buffer):
            new_buf_size = max(len(self._buffer) * 2, self.cursor + total_space)
            self._buffer = np.resize(self._buffer, new_buf_size)
            self._bundle = None

        if self.use_hashes:
            self.cursor, self.count = nb_insert_item(
                name_array,
                identity_id,
                self._buffer,
                self._offsets,
                self._lens,
                self._hashes,
                self._hash_index,
                self.cursor,
                self.count,
            )
        else:
            self.cursor, self.count = nb_insert_item_no_index(
                name_array,
                identity_id,
                self._buffer,
                self._offsets,
                self._lens,
                self._hashes,
                self.cursor,
                self.count,
            )

        if (v := self._values) is not None and value is not None:
            v[identity_id] = value

        # Reset bundle since data was written
        self._bundle = None

    def bundle(self) -> StringTableParams:
        if (b := self._bundle) is not None:
            return b

        self._bundle = StringTableParams(
            self._buffer,
            self._offsets,
            self._lens,
            self._hashes,
            v if (v := self._values) is not None else EMPTY_ID,
            self.count,
            self._hash_index if self.use_hashes else EMPTY_INDEX,
        )
        return self._bundle

    def release(self):
        """Clears references to allow GC to reclaim memory."""
        self._bundle = None
        self._offsets = None
        self._lens = None
        self._hashes = None
        self._buffer = None
        self._values = None

    def debug_print(self, title):
        print(f"--- StringTable '{title}' (count={self.count} cursor={self.cursor}) ---")
        for i in range(self.count):
            off = self._offsets[i]
            n_len = self._lens[i]
            h_val = self._hashes[i]

            raw_bytes = self._buffer[off : off + n_len].tobytes()
            name = raw_bytes.decode("utf-8", errors="replace")
            val = self._values[i] if self._values is not None else "n/a"

            print(f'id={i:<4} off={off:<6} len={n_len:<3} hash={h_val:016x} val={val:<5} name="{name}"')
        print(f"--- End Dump '{title}' ---")

    def get_string(self, identity_id: int) -> str:
        """Retrieves and decodes the string for a given ID."""
        if identity_id < 0 or identity_id >= self.count:
            return ""

        off = self._offsets[identity_id]
        n_len = self._lens[identity_id]

        # Slice the buffer, convert to bytes, and decode to utf-8
        # 'replace' handles any potential corruption gracefully
        return self._buffer[off : off + n_len].tobytes().decode("utf-8", errors="replace")

    def __getitem__(self, identity_id: int) -> str:
        """Syntactic sugar for get_string: name = table[5]"""
        return self.get_string(identity_id)

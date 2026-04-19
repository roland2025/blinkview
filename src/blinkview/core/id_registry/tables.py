# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.types import StringTableParams
from blinkview.core.types.empty import EMPTY_ID
from blinkview.core.types.parsing import UnifiedParserConfig
from blinkview.utils.fnv1a_64 import fnv1a_64_fast

# Placeholder for when values are disabled to keep NamedTuple stable


class IndexedStringTable:
    __slots__ = ("_offsets", "_lens", "_hashes", "_values", "_buffer", "cursor", "count", "_bundle")

    def __init__(
        self,
        initial_capacity: int = 1024,
        buffer_size_kb: int = 128,
        values_dtype: Optional[Any] = None,
    ):
        # Initialize with standard numpy arrays
        self._offsets = np.empty(initial_capacity, dtype=dtypes.OFFSET_TYPE)
        self._lens = np.empty(initial_capacity, dtype=dtypes.LEN_TYPE)
        self._hashes = np.empty(initial_capacity, dtype=dtypes.HASH_TYPE)

        self._values = None
        if values_dtype is not None:
            self._values = np.empty(initial_capacity, dtype=values_dtype)

        self._buffer = np.empty(buffer_size_kb * 1024, dtype=dtypes.BYTE)

        self.cursor = 0
        self.count = 0
        self._bundle = None

    def register_name(self, identity_id: int, name: str, value: Any = None):
        name_bytes = name.encode("utf-8")
        n_len = len(name_bytes)
        total_space = n_len + 1
        name_array = np.frombuffer(name_bytes, dtype=dtypes.BYTE)

        # 1. Grow Metadata Capacity
        current_cap = len(self._offsets)
        if identity_id >= current_cap:
            new_cap = max(identity_id + 1, current_cap * 2)

            # np.resize handles the copy and re-allocation
            self._offsets = np.resize(self._offsets, new_cap)
            self._lens = np.resize(self._lens, new_cap)
            self._hashes = np.resize(self._hashes, new_cap)

            if self._values is not None:
                self._values = np.resize(self._values, new_cap)

            self._bundle = None  # Invalidate cache

        # 2. Grow Byte Buffer
        if self.cursor + total_space > len(self._buffer):
            new_buf_size = max(len(self._buffer) * 2, self.cursor + total_space)
            self._buffer = np.resize(self._buffer, new_buf_size)
            self._bundle = None  # Invalidate cache

        # 3. Write Data
        start = self.cursor
        self._buffer[start : start + n_len] = name_array
        self._buffer[start + n_len] = 0  # Explicit null terminator

        self._offsets[identity_id] = start
        self._lens[identity_id] = n_len
        self._hashes[identity_id] = fnv1a_64_fast(name_array)

        if self._values is not None and value is not None:
            self._values[identity_id] = value

        self.cursor += total_space
        self.count = max(self.count, identity_id + 1)

        # Reset bundle if count changed or data was written
        self._bundle = None

    def bundle(self) -> StringTableParams:
        if self._bundle is None:
            vals = self._values if self._values is not None else EMPTY_ID

            # Pass the full arrays; StringTableParams uses self.count to bound access
            self._bundle = StringTableParams(self._buffer, self._offsets, self._lens, self._hashes, vals, self.count)
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

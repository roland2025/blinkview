# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.types import StringTableParams
from blinkview.utils.fnv1a_64 import fnv1a_64_fast

# Placeholder for when values are disabled to keep NamedTuple stable


class IndexedStringTable:
    __slots__ = ("_pool", "_offsets_h", "_lens_h", "_hashes_h", "_values_h", "_buffer_h", "cursor", "count", "_bundle")

    _EMPTY_INT_ARRAY = np.zeros(0, dtype=dtypes.VALUES_TYPE)

    def __init__(
        self,
        pool,
        initial_capacity: int = 1024,
        buffer_size_kb: int = 128,
        values_dtype: Optional[Any] = None,  # Default to None (Disabled)
    ):
        self._pool = pool

        # Core Metadata
        self._offsets_h = pool.acquire(initial_capacity, dtype=dtypes.OFFSET_TYPE)
        self._lens_h = pool.acquire(initial_capacity, dtype=dtypes.LEN_TYPE)
        self._hashes_h = pool.acquire(initial_capacity, dtype=dtypes.HASH_TYPE)

        # Optional side-car values
        self._values_h = None
        if values_dtype is not None:
            self._values_h = pool.acquire(initial_capacity, dtype=values_dtype)

        self._buffer_h = pool.acquire(buffer_size_kb * 1024, dtype=dtypes.BYTE)

        self.cursor = 0
        self.count = 0
        self._bundle = None

    def register_name(self, identity_id: int, name: str, value: Any = None):
        name_bytes = name.encode("utf-8")
        n_len = len(name_bytes)
        name_array = np.frombuffer(name_bytes, dtype=dtypes.BYTE)

        # 1. Grow Metadata Capacity
        current_cap = len(self._offsets_h.array)
        if identity_id >= current_cap:
            new_cap = max(identity_id + 1, current_cap * 2)

            # Grow standard arrays
            self._offsets_h = self._grow_handle(self._offsets_h, new_cap)
            self._lens_h = self._grow_handle(self._lens_h, new_cap)
            self._hashes_h = self._grow_handle(self._hashes_h, new_cap)

            # Grow values only if enabled
            if self._values_h is not None:
                self._values_h = self._grow_handle(self._values_h, new_cap)

        # 2. Grow Byte Buffer
        if self.cursor + n_len > len(self._buffer_h.array):
            new_buf_size = max(len(self._buffer_h.array) * 2, self.cursor + n_len)
            self._buffer_h = self._grow_handle(self._buffer_h, new_buf_size, copy_len=self.cursor)

        # 3. Write Data
        start = self.cursor
        self._buffer_h.array[start : start + n_len] = name_array

        self._offsets_h.array[identity_id] = start
        self._lens_h.array[identity_id] = n_len
        self._hashes_h.array[identity_id] = fnv1a_64_fast(name_array)

        if self._values_h is not None and value is not None:
            self._values_h.array[identity_id] = value

        self.cursor += n_len
        self.count = max(self.count, identity_id + 1)
        self._bundle = None

    def _grow_handle(self, handle, new_cap, copy_len=None):
        """Helper to grow a pool handle."""
        new_h = self._pool.acquire(new_cap, dtype=handle.dtype)
        limit = copy_len if copy_len is not None else len(handle.array)
        new_h.array[:limit] = handle.array[:limit]
        handle.release()
        return new_h

    def bundle(self) -> StringTableParams:
        if self._bundle is None:
            # If values are disabled, we pass the empty placeholder
            vals = self._values_h.array if self._values_h is not None else self._EMPTY_INT_ARRAY

            self._bundle = StringTableParams(
                self._buffer_h.array, self._offsets_h.array, self._lens_h.array, self._hashes_h.array, vals, self.count
            )
        return self._bundle

    def release(self):
        self._bundle = None
        self._offsets_h.release()
        self._lens_h.release()
        self._hashes_h.release()
        self._buffer_h.release()
        if self._values_h is not None:
            self._values_h.release()

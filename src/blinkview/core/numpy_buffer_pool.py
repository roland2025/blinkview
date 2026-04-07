# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from queue import Empty, SimpleQueue

import numpy as np


class PooledTelemetryBatch:
    """
    A lightweight, slotted transport object.
    It fetches its heavy arrays from the global NumpyArrayPool and manages 2D reshaping.
    """

    __slots__ = (
        "_pool",  # Reference to the global NumpyArrayPool
        "_times_handle",  # The PooledArrayHandle for the 1D times array
        "_values_handle",  # The PooledArrayHandle for the 2D values array
        "times",  # The tightly sliced 1D view for the GUI
        "values",  # The tightly sliced 2D view for the GUI
        "latest_seq",
        "target_cols",
    )

    def __init__(self, pool):
        self._pool = pool
        self._times_handle = None
        self._values_handle = None
        self.times = None
        self.values = None
        self.latest_seq = -1
        self.target_cols = 0

    def allocate(self, required_size: int, target_cols: int):
        """Acquires appropriately sized memory slabs from the central pool."""
        self.target_cols = target_cols

        # Calculate byte requirements
        itemsize = np.dtype(np.float64).itemsize
        times_bytes = required_size * itemsize
        values_bytes = required_size * target_cols * itemsize

        # Release old handles if they aren't large enough for the new requirements
        if self._times_handle and self._times_handle.array.nbytes < times_bytes:
            self._times_handle.release()
            self._times_handle = None

        if self._values_handle and self._values_handle.array.nbytes < values_bytes:
            self._values_handle.release()
            self._values_handle = None

        # Acquire new memory slabs (returns instantly if cached in the pool)
        if self._times_handle is None:
            self._times_handle = self._pool.acquire(times_bytes, dtype=np.float64)

        if self._values_handle is None:
            self._values_handle = self._pool.acquire(values_bytes, dtype=np.float64)

    def set_views(self, idx: int, latest_seq: int):
        """Slices the 1D power-of-two slabs and reshapes into exact GUI views."""
        self.latest_seq = latest_seq

        # 1D Slice for times
        self.times = self._times_handle.array[:idx]

        # 2D Reshape & Slice for values:
        # Extract exactly the elements needed, then view as a 2D matrix
        flat_values = self._values_handle.array[: idx * self.target_cols]
        self.values = flat_values.reshape(idx, self.target_cols)

    def release(self):
        """Releases the underlying arrays back to the pool."""
        self.times = None
        self.values = None

        if self._times_handle:
            self._times_handle.release()
            self._times_handle = None

        if self._values_handle:
            self._values_handle.release()
            self._values_handle = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

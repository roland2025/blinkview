# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from queue import Empty, SimpleQueue


class PooledTelemetryBatch:
    """A slotted transport object that automatically returns itself to the pool."""

    __slots__ = (
        "base_times",
        "base_values",  # The massive, pre-allocated C-arrays
        "times",
        "values",  # The tightly sliced views for the GUI
        "latest_seq",
        "target_cols",  # Metadata
        "_pool",  # Reference back to the parent pool
    )

    def __init__(self, pool):
        self._pool = pool
        self.base_times = None
        self.base_values = None
        self.times = None
        self.values = None
        self.latest_seq = -1
        self.target_cols = 0

    def allocate(self, required_size: int, target_cols: int):
        """The Learning Step: Resizes native arrays only if the existing ones are too small."""
        import numpy as np

        # Check if reallocation is needed
        if self.base_times is None or len(self.base_times) < required_size or self.base_values.shape[1] != target_cols:
            self.base_times = np.empty(required_size, dtype=np.float64)
            self.base_values = np.empty((required_size, target_cols), dtype=np.float64)

        self.target_cols = target_cols

    def set_views(self, idx: int, latest_seq: int):
        """Sets the tight slices used by the GUI to read the valid data."""
        self.times = self.base_times[:idx]
        self.values = self.base_values[:idx]
        self.latest_seq = latest_seq

    def release(self):
        """Clears view references and returns this object to the pool."""
        self.times = None
        self.values = None
        self._pool._queue.put(self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()


class NumpyBufferPool:
    def __init__(self):
        from queue import SimpleQueue

        self._queue = SimpleQueue()

    def acquire(self, required_size: int, target_cols: int) -> PooledTelemetryBatch:
        from queue import Empty

        try:
            batch = self._queue.get_nowait()
        except Empty:
            batch = PooledTelemetryBatch(self)

        batch.allocate(required_size, target_cols)
        return batch

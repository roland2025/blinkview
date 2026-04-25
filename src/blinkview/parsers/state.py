# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.frames import FrameStateParams


class FrameState:
    __slots__ = ("_pool_handle", "_ts_handle", "bundle")

    def __init__(self, pool, size_bytes=4096):
        self._pool_handle = pool.acquire(size_bytes, dtype=dtypes.BYTE)

        self._ts_handle = pool.acquire(size_bytes, dtype=dtypes.TS_TYPE)

        # Initialize the bundle directly.
        # All trackers live here; 'self' only keeps the reference.
        self.bundle = FrameStateParams(
            buffer=self._pool_handle.array,
            ts_buffer=self._ts_handle.array,
            offset=np.zeros(1, dtype=np.int64),
            in_idx=np.zeros(1, dtype=np.int64),
            in_offset=np.zeros(1, dtype=np.int64),
            in_frame=np.zeros(1, dtype=np.bool_),
        )

    def reset_batch_trackers(self):
        """Resets trackers using walrus to avoid redundant self.bundle lookups."""
        b = self.bundle  # Capture local reference
        b.in_idx[0] = 0
        b.in_offset[0] = 0

    def clear_stitch_state(self):
        """Clears state using walrus to minimize attribute access overhead."""
        if b := self.bundle:
            b.in_frame[0] = False
            b.offset[0] = 0

    def release(self):
        """Release the memory back to the pool."""
        self.bundle = None
        self._pool_handle.release()
        self._pool_handle = None

        self._ts_handle.release()
        self._ts_handle = None

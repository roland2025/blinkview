# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.types.frames import FrameStateParams


class FrameState:
    def __init__(self, pool, size_kb=4):
        self._pool_handle = pool.acquire(size_kb * 1024, dtype=np.uint8)
        self.buffer = self._pool_handle.array

        self.write_offset = np.zeros(1, dtype=np.int64)
        self.in_idx = np.zeros(1, dtype=np.int64)
        self.in_offset = np.zeros(1, dtype=np.int64)
        self.in_frame = np.zeros(1, dtype=np.bool_)

    def reset_batch_trackers(self):
        """Resets the trackers for a new incoming batch."""
        self.in_idx[0] = 0
        self.in_offset[0] = 0

    def clear_stitch_state(self):
        """Clears any incomplete frame data (used after warmup or connection loss)."""
        self.in_frame[0] = False
        self.write_offset[0] = 0

    def release(self):
        self._pool_handle.release()

    def bundle(self):
        return FrameStateParams(
            buffer=self.buffer,
            offset=self.write_offset,
            in_idx=self.in_idx,
            in_offset=self.in_offset,
            in_frame=self.in_frame,
        )

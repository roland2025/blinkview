# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.types.telemetry import TelemetryBufferBundle

if TYPE_CHECKING:
    # This prevents circular imports if TelemetryBatch
    # is defined in a file that imports this one.
    from blinkview.core.types.log_batch import TelemetryBatch


@dataclass
class ModuleBuffer:
    """Holds the rolling buffers for a specific module."""

    max_points: int
    num_channels: int
    last_seq: dtypes.SEQ_TYPE = SEQ_NONE

    # Arrays initialized internally
    x_data: np.ndarray = field(init=False)
    x_data_int64: np.ndarray = field(init=False)
    y_data: np.ndarray = field(init=False)
    temp_floats: np.ndarray = field(init=False)

    # State tracking
    head: int = 0
    size: int = 0
    ptr: int = 0
    is_dirty: bool = False
    is_dirty_overview: bool = False
    last_fetch_ns: int = 0

    def __post_init__(self):
        total_capacity = self.max_points * 2

        from blinkview.core import dtypes

        self.x_data = np.zeros(total_capacity, dtype=dtypes.PLOT_TS_TYPE)
        self.x_data_int64 = np.zeros(total_capacity, dtype=dtypes.TS_TYPE)

        self.y_data = np.zeros((total_capacity, self.num_channels), order="F", dtype=dtypes.PLOT_VAL_TYPE)

        from blinkview.core.numpy_log import allocate_telemetry_workspace

        self.temp_floats = allocate_telemetry_workspace(self.num_channels)

    def update(self, batch: "TelemetryBatch") -> bool:
        """Entry point that updates state using the JITed logic."""
        if batch.times.size == 0:
            return False

        from blinkview.ops.telemetry import fast_insert_mirrored_buffer

        new_head, new_size = fast_insert_mirrored_buffer(
            self.x_data,
            self.x_data_int64,
            self.y_data,
            self.head,
            self.size,
            batch,
            self.max_points,
        )

        self.head = new_head
        self.size = new_size
        self.is_dirty = True
        self.is_dirty_overview = True
        return True

    def bundle(self) -> TelemetryBufferBundle:
        """Returns a lightweight bundle for Numba downsampling kernels."""
        # In the mirrored buffer, if we haven't wrapped, start at 0.
        # Once full, the start of the valid window is at 'head'.
        start_idx = self.head if self.size >= self.max_points else 0

        return TelemetryBufferBundle(
            x_data=self.x_data,
            x_data_int64=self.x_data_int64,
            y_data=self.y_data,
            data_start=start_idx,
            data_size=self.size,
        )

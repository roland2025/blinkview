# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

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

        from blinkview.ops.telemetry import nb_fast_insert_mirrored_buffer

        new_head, new_size = nb_fast_insert_mirrored_buffer(
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


@dataclass
class ReplayWindowBuffer:
    """Holds a single playback-scrub window of telemetry for one module - populated by
    fetch_telemetry_window (core/numpy_log.py), re-centered on the global playback clock's
    current_ts_ns, as opposed to ModuleBuffer's forward-accumulating ring keyed off a live-fetch
    sequence watermark.

    Deliberately NOT a ring buffer: REPLAY re-centers rather than appends, so there's no
    wraparound/head/watermark bookkeeping to maintain - each update() call is a flat overwrite
    of however many samples fetch_telemetry_window actually returned that tick.

    Exposes the exact same .bundle() -> TelemetryBufferBundle contract as ModuleBuffer, so
    TelemetryPlotter._update_plots/_update_overview/nb_slice_and_downsample need no changes to
    consume either buffer type interchangeably.
    """

    capacity: int
    num_channels: int

    x_data: np.ndarray = field(init=False)
    x_data_int64: np.ndarray = field(init=False)
    y_data: np.ndarray = field(init=False)
    temp_floats: np.ndarray = field(init=False)

    size: int = 0
    last_fetch_ns: int = 0
    is_dirty: bool = False
    is_dirty_overview: bool = False

    def __post_init__(self):
        self._allocate(self.capacity)

    def _allocate(self, capacity: int):
        self.x_data = np.zeros(capacity, dtype=dtypes.PLOT_TS_TYPE)
        self.x_data_int64 = np.zeros(capacity, dtype=dtypes.TS_TYPE)
        self.y_data = np.zeros((capacity, self.num_channels), order="F", dtype=dtypes.PLOT_VAL_TYPE)

        from blinkview.core.numpy_log import allocate_telemetry_workspace

        self.temp_floats = allocate_telemetry_workspace(self.num_channels)

    def ensure_capacity(self, capacity: int):
        """Grows the backing arrays in place if a larger window is requested (e.g. the
        follow-window -> browse-window upgrade on manual pan) - never shrinks, so a one-time
        upgrade doesn't get reallocated back down on every subsequent follow tick. The old,
        now-undersized data is discarded (size reset to 0) since the caller always re-fetches
        immediately after a capacity change anyway."""
        if capacity <= self.capacity:
            return
        self.capacity = capacity
        self._allocate(capacity)
        self.size = 0

    def update(self, batch: "TelemetryBatch") -> bool:
        """Flat overwrite from a fetch_telemetry_window result - no ring/wrap logic needed
        since REPLAY re-centers rather than appends. Truncates to capacity defensively (should
        already be capped by fetch_telemetry_window's before_cap+after_cap, but this keeps the
        buffer's own invariant self-contained rather than trusting the caller)."""
        n = min(batch.times.size, self.capacity)
        self.size = n
        if n == 0:
            return False

        self.x_data[:n] = batch.times[:n]
        self.x_data_int64[:n] = batch.times_int64[:n]
        self.y_data[:n, :] = batch.values[:n, :]
        self.is_dirty = True
        self.is_dirty_overview = True
        return True

    def bundle(self) -> TelemetryBufferBundle:
        return TelemetryBufferBundle(
            x_data=self.x_data,
            x_data_int64=self.x_data_int64,
            y_data=self.y_data,
            data_start=0,
            data_size=self.size,
        )

    @property
    def head(self) -> int:
        """Matches ModuleBuffer.head's meaning (index right after the newest valid sample) for
        code in plotter.py that reads either buffer type interchangeably via
        TelemetryPlotter._active_buffer - a flat, non-wrapping buffer's "newest" index is simply
        its size."""
        return self.size

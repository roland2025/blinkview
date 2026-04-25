# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np


class LogBundle(NamedTuple):
    timestamps: np.ndarray  # dtypes.TS_TYPE (int64)
    rx_timestamps: np.ndarray  # System/PC Arrival Time #dtypes.TS_TYPE (int64)

    offsets: np.ndarray  # dtypes.OFFSET_TYPE (uint32)
    lengths: np.ndarray  # dtypes.LEN_TYPE (uint32)
    buffer: np.ndarray  # dtypes.BYTE (uint8)

    # Optional columns (Pass empty arrays if not used)
    levels: np.ndarray  # dtypes.LEVEL_TYPE (uint8)
    modules: np.ndarray  # dtypes.ID_TYPE (uint32)
    devices: np.ndarray  # dtypes.ID_TYPE (uint32)
    sequences: np.ndarray  # dtypes.SEQ_TYPE (uint64)

    # --- New Heterogeneous Extension Columns ---
    ext_u32_1: np.ndarray  # dtypes.UINT32 (uint32)
    ext_u32_2: np.ndarray  # dtypes.UINT32 (uint32)
    ext_u64_1: np.ndarray  # dtypes.UINT64 (uint64)

    size: np.ndarray  # number of rows
    msg_cursor: np.ndarray  # bytes used in buffer
    capacity: int  # MAX number of rows before full

    # Status flags
    has_levels: bool
    has_modules: bool
    has_devices: bool
    has_sequences: bool
    has_ext_u32_1: bool
    has_ext_u32_2: bool
    has_ext_u64_1: bool


class TelemetryBatch(NamedTuple):
    times: np.ndarray
    times_int64: np.ndarray
    values: np.ndarray
    watermark: int

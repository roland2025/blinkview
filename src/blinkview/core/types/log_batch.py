# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np


class LogBundle(NamedTuple):
    timestamps: np.ndarray  # dtypes.TS_TYPE (int64)
    offsets: np.ndarray  # dtypes.OFFSET_TYPE (uint32)
    lengths: np.ndarray  # dtypes.LEN_TYPE (uint32)
    buffer: np.ndarray  # dtypes.BYTE (uint8)

    # Optional columns (Pass empty arrays if not used)
    levels: np.ndarray  # dtypes.LEVEL_TYPE (uint8)
    modules: np.ndarray  # dtypes.ID_TYPE (uint32)
    devices: np.ndarray  # dtypes.ID_TYPE (uint32)
    sequences: np.ndarray  # dtypes.SEQ_TYPE (uint64)

    size: int  # number of rows
    msg_cursor: int  # bytes used in buffer

    # Status flags
    has_levels: bool
    has_modules: bool
    has_devices: bool
    has_sequences: bool

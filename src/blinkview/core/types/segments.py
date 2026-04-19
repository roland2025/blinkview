# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core import dtypes


class LogSegmentParams(NamedTuple):
    """Numba-compatible view of a single memory segment."""

    timestamps: np.ndarray  # dtypes.TS_TYPE
    levels: np.ndarray  # dtypes.LEVEL_TYPE
    modules: np.ndarray  # dtypes.ID_TYPE
    devices: np.ndarray  # dtypes.ID_TYPE
    sequence_ids: np.ndarray  # dtypes.SEQ_TYPE
    offsets: np.ndarray  # dtypes.OFFSET_TYPE
    lengths: np.ndarray  # dtypes.LEN_TYPE
    buffer: np.ndarray  # dtypes.BYTE

    count: np.ndarray  # np.int64, shape=(1,)
    msg_cursor: np.ndarray  # np.int64, shape=(1,)

    capacity: int

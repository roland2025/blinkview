# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""State of the raw hex dump (ops/hex_dump.py, parsers/raw_hex.py)."""

from typing import NamedTuple

import numpy as np


class HexDumpState(NamedTuple):
    in_idx: np.ndarray  # int64[1]: input row to continue with, reset to 0 for each new batch
    in_pos: np.ndarray  # int64[1]: byte within that row to continue with
    stream_offset: np.ndarray  # int64[1]: bytes dumped so far - the offset column of the next row
    capped: np.ndarray  # uint8[1]: 1 once max_bytes was reached; nothing is dumped after that


def create_hex_dump_state() -> HexDumpState:
    return HexDumpState(
        in_idx=np.zeros(1, dtype=np.int64),
        in_pos=np.zeros(1, dtype=np.int64),
        stream_offset=np.zeros(1, dtype=np.int64),
        capped=np.zeros(1, dtype=np.uint8),
    )

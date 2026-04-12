# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np


class FrameStateParams(NamedTuple):
    buffer: np.ndarray
    offset: np.ndarray
    in_idx: np.ndarray
    in_offset: np.ndarray
    in_frame: np.ndarray


class FrameConfig(NamedTuple):
    decode_func: any  # The JIT function (decode_newline_frame, etc.)
    delimiter: int  # e.g., ord('\n')
    length_fixed: bool  # If True, only frames of exactly 'length' bytes are valid. If False, frames between length_min and length_max are valid.
    length_min: int  # Minimum frame size to process
    length_max: int  # Maximum frame size to process (for sanity checking, not a hard limit)
    length: int
    filter_printable: bool
    filter_ansi: bool
    filter_trim_r: bool
    report_error: bool

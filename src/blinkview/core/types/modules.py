# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core.id_registry.types import StringTableParams
from blinkview.core.types.empty import EMPTY_BYTES, EMPTY_HASH, EMPTY_LEN, EMPTY_OFF, ZERO_COUNT, ZERO_CURSOR

MODULE_TEMP_ID_BASE = 0xF0000000

# Signal that the discovery tracker has reached its memory limit.
MODULE_ID_FULL = 0xFFFFFFFE

# Standard "Null" or "Unknown" value (All bits set).
MODULE_ID_UNKNOWN = 0xFFFFFFFF


class TrackerState(NamedTuple):
    count: np.ndarray
    bytes_cursor: np.ndarray


class ModuleTrackerState(NamedTuple):
    count: np.ndarray = ZERO_COUNT  # [0] = number of unresolved names in current chunk
    bytes_cursor: np.ndarray = ZERO_CURSOR  # [0] = write position in name_bytes
    starts: np.ndarray = EMPTY_OFF  # Indices where each raw name starts in name_bytes
    lengths: np.ndarray = EMPTY_LEN  # Length of each raw name
    hashes: np.ndarray = EMPTY_HASH  # Optional: pre-calculated hashes
    name_bytes: np.ndarray = EMPTY_BYTES  # The raw byte buffer


EmptyModuleTrackerState = ModuleTrackerState()

#
# class ModuleTrackerConfig(NamedTuple):
#     starts: np.ndarray
#     lengths: np.ndarray
#     hashes: np.ndarray
#     name_bytes: np.ndarray
#     state: TrackerState


class FixedWidthConfig(NamedTuple):
    width: int


class DynamicWidthConfig(NamedTuple):
    max_length: int = 0
    max_depth: int = 0
    enable_brackets: bool = 0
    enable_dot_separator: bool = 0


EmptyDynamicWidthConfig = DynamicWidthConfig()

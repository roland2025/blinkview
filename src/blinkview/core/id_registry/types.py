# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core import dtypes


class StringTableParams(NamedTuple):
    buffer: np.ndarray  # dtype: dtypes.BYTE
    offsets: np.ndarray  # dtype: dtypes.OFFSET_TYPE
    lens: np.ndarray  # dtype: dtypes.LEN_TYPE
    hashes: np.ndarray  # dtype: dtypes.HASH_TYPE
    values: np.ndarray  # dtype: ?
    count: int


class RegistryParams(NamedTuple):
    """A complete snapshot of the ID Registry state for JIT kernels."""

    levels: StringTableParams
    modules: StringTableParams
    devices: StringTableParams

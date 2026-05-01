# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core import dtypes
from blinkview.core.types.empty import EMPTY_BYTES, EMPTY_HASH, EMPTY_ID, EMPTY_LEN, EMPTY_OFF


class StringTableParams(NamedTuple):
    buffer: np.ndarray = EMPTY_BYTES  # dtype: dtypes.BYTE
    offsets: np.ndarray = EMPTY_OFF  # dtype: dtypes.OFFSET_TYPE
    lens: np.ndarray = EMPTY_LEN  # dtype: dtypes.LEN_TYPE
    hashes: np.ndarray = EMPTY_HASH  # dtype: dtypes.HASH_TYPE
    values: np.ndarray = EMPTY_ID  # dtype: ?
    count: int = 0


EmptyStringTableParams = StringTableParams()


class RegistryParams(NamedTuple):
    """A complete snapshot of the ID Registry state for JIT kernels."""

    levels: StringTableParams
    modules: StringTableParams
    devices: StringTableParams
    parents: np.ndarray

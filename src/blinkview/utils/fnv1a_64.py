# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.numba_config import app_njit

np.seterr(over="ignore")


def fnv1a_64_python(buffer, start, length) -> int:
    data = memoryview(buffer)[start : start + length]
    hval = 0xCBF29CE484222325
    prime = 0x100000001B3
    mask = 0xFFFFFFFFFFFFFFFF
    for b in data:
        hval = ((hval ^ b) * prime) & mask
    return hval


@app_njit(fallback=fnv1a_64_python)
def nb_fnv1a_64_fast(buffer, start, length) -> int:
    """Numba-compiled FNV-1a. Refactored to prevent array slice allocations."""
    hash_val = np.uint64(14695981039346656037)
    fnv_prime = np.uint64(1099511628211)

    for i in range(length):
        hash_val ^= np.uint64(buffer[start + i])
        hash_val *= fnv_prime

    return hash_val

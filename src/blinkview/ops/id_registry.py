# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.numba_config import app_njit

NO_PARENT = np.iinfo(dtypes.ID_TYPE).max


@app_njit()
def nb_get_descendants(target_id: int, parent_array: np.ndarray, num_modules: int) -> np.ndarray:
    max_possible = num_modules - target_id - 1
    if max_possible <= 0:
        return np.empty(0, dtype=dtypes.ID_TYPE)

    results = np.empty(max_possible, dtype=dtypes.ID_TYPE)
    count = 0

    for current_id in range(target_id + 1, num_modules):
        parent = parent_array[current_id]

        while parent != NO_PARENT:
            if parent == target_id:
                results[count] = current_id
                count += 1
                break
            if parent < target_id:
                break
            parent = parent_array[parent]

    return results[:count]

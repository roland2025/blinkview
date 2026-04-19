# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes

# We pre-create these so we aren't allocating new objects during setup
EMPTY_BYTES = np.empty(0, dtype=dtypes.BYTE)
EMPTY_OFF = np.empty(0, dtype=dtypes.OFFSET_TYPE)
EMPTY_LEN = np.empty(0, dtype=dtypes.LEN_TYPE)
EMPTY_HASH = np.empty(0, dtype=dtypes.HASH_TYPE)
EMPTY_ID = np.empty(0, dtype=dtypes.ID_TYPE)  # Assuming values uses ID_TYPE

ZERO_COUNT = np.zeros(1, dtype=dtypes.ID_TYPE)
ZERO_CURSOR = np.zeros(1, dtype=dtypes.OFFSET_TYPE)

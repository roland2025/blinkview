# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes

EMPTY_BYTES = np.empty(0, dtype=dtypes.BYTE)
EMPTY_OFF = np.empty(0, dtype=dtypes.OFFSET_TYPE)
EMPTY_LEN = np.empty(0, dtype=dtypes.LEN_TYPE)
EMPTY_HASH = np.empty(0, dtype=dtypes.HASH_TYPE)
EMPTY_ID = np.empty(0, dtype=dtypes.ID_TYPE)
EMPTY_LEVEL = np.empty(0, dtype=dtypes.LEVEL_TYPE)
EMPTY_SEQ = np.empty(0, dtype=dtypes.SEQ_TYPE)
EMPTY_INDEX = np.empty(0, dtype=dtypes.INDEX_TYPE)


ZERO_COUNT = np.zeros(1, dtype=dtypes.ID_TYPE)
ZERO_CURSOR = np.zeros(1, dtype=dtypes.OFFSET_TYPE)
ZERO_UTC_OFFSET = np.zeros(1, dtype=dtypes.TS_TYPE)

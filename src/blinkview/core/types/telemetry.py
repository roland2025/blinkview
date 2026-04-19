# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np


class TelemetryBufferBundle(NamedTuple):
    x_data: np.ndarray
    x_data_int64: np.ndarray
    y_data: np.ndarray
    data_start: int
    data_size: int

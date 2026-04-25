# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.numba_config import app_njit
from blinkview.core.types.parsing import SyncState


@app_njit(inline="always")
def parse_iso8601_to_ns(buffer, start, offset_sec):
    """
    Parses 'YYYY-MM-DD HH:MM:SS.mmm' starting at 'start'.
    Returns UTC nanoseconds as int64.
    """
    # 1. Extraction (Fixed offsets relative to YYYY)
    y = (
        (buffer[start + 0] - 48) * 1000
        + (buffer[start + 1] - 48) * 100
        + (buffer[start + 2] - 48) * 10
        + (buffer[start + 3] - 48)
    )
    m = (buffer[start + 5] - 48) * 10 + (buffer[start + 6] - 48)
    d = (buffer[start + 8] - 48) * 10 + (buffer[start + 9] - 48)

    hh = (buffer[start + 11] - 48) * 10 + (buffer[start + 12] - 48)
    mm = (buffer[start + 14] - 48) * 10 + (buffer[start + 15] - 48)
    ss = (buffer[start + 17] - 48) * 10 + (buffer[start + 18] - 48)
    ms = (buffer[start + 20] - 48) * 100 + (buffer[start + 21] - 48) * 10 + (buffer[start + 22] - 48)

    # 2. Julian Day Number Algorithm
    # Formula: $$JDN = d + \lfloor\frac{153m + 2}{5}\rfloor + 365y + \lfloor\frac{y}{4}\rfloor - \lfloor\frac{y}{100}\rfloor + \lfloor\frac{y}{400}\rfloor - 32045$$
    temp_a = (14 - m) // 12
    temp_y = y + 4800 - temp_a
    temp_m = m + 12 * temp_a - 3

    jdn = d + (153 * temp_m + 2) // 5 + 365 * temp_y + temp_y // 4 - temp_y // 100 + temp_y // 400 - 32045
    days_since_1970 = jdn - 2440588

    # 3. Epoch Math
    res_ns = (days_since_1970 * 86400 + hh * 3600 + mm * 60 + ss) * 1_000_000_000
    res_ns += ms * 1_000_000

    return res_ns - (offset_sec * 1_000_000_000)


@app_njit(inline="always")
def nb_project_synced_ns(raw_ns, rx_ns, sync: SyncState):
    # If the app hasn't provided a high-precision anchor yet,
    # we use the PC's arrival time so the logs appear in 'roughly' the right place.
    if not sync.enabled[0]:
        return rx_ns

    i = sync.active_idx[0]
    target_anchor = sync.ref_time[i]
    pc_anchor = sync.offset[i]

    # Calculate distance from anchor and apply hardware drift
    delta = np.int64(raw_ns) - target_anchor

    # Use float64 for the drift math to prevent integer overflow during scaling
    drift = np.float64(sync.drift_m[i]) / np.float64(sync.drift_d[i])

    return dtypes.TS_TYPE(pc_anchor + np.int64(np.float64(delta) * drift))

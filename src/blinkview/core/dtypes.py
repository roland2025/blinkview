# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

# --- System Primitives ---
BYTE = np.uint8
HASH_TYPE = np.uint64
TS_TYPE = np.int64  # Timestamps

# --- Registry / Table Types ---
OFFSET_TYPE = np.uint32
LEN_TYPE = np.uint32
ID_TYPE = np.uint32  # Global IDs (Devices, Modules)
VALUES_TYPE = np.uint32

# --- Log Specifics ---
LEVEL_TYPE = np.uint8
SEQ_TYPE = np.uint64

UINT32 = np.uint32
UINT64 = np.uint64

# Unified naming for processed telemetry arrays
PLOT_TS_TYPE = np.float64  # Transformed timestamps (seconds)
PLOT_VAL_TYPE = np.float64  # Extracted numeric values

# Sentinel: No data, uninitialized, or ignore
SEQ_NONE: SEQ_TYPE = SEQ_TYPE(0)
# The very first ID assigned to a log
SEQ_START: SEQ_TYPE = SEQ_TYPE(1)

LEVEL_UNSPECIFIED = LEVEL_TYPE(0xFF)

TS_UNSPECIFIED = TS_TYPE(-1)
ID_UNSPECIFIED = ID_TYPE(0xFFFFFFFF)

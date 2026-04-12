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

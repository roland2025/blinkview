# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit


@app_njit()
def parse_log_level(buffer, start_cursor, end_cursor, out_b, out_idx, state, config):
    """
    Parses a log level string from the input buffer.
    Universal Signature: (buffer, start, end, out, idx, state, config)
    """
    # Hoist attributes from 'config' (The StringTableParams)
    count = config.count
    lens = config.lens
    offsets = config.offsets
    values = config.values
    ref_buffer = config.buffer  # The baked table of levels (INFO, WARN, etc.)

    for i in range(count):
        length = lens[i]

        # 1. Bounds check
        if length == 0 or (start_cursor + length > end_cursor):
            continue

        match = True
        offset = offsets[i]

        # 2. Byte-by-byte comparison (Inner loop)
        for j in range(length):
            if buffer[start_cursor + j] != ref_buffer[offset + j]:
                match = False
                break

        if match:
            # 3. Success: Write to output and return advanced cursor
            out_b.levels[out_idx] = values[i]
            return start_cursor + length

    # No match found
    return -1

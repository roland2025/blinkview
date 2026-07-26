# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit


@app_njit()
def nb_blast_benchmark_cache(
    bundle,  # LogBundle (NamedTuple of Numpy arrays)
    start_ts,
    chunks,  # Generation params
    c_buf,
    c_offs,
    c_lens,
    c_items,  # Compiled cache arrays
):
    """Numba kernel to bulk-insert cached messages using LogBundle."""
    # Read current cursors from the 1-element arrays
    row_cursor = bundle.size[0]
    byte_cursor = bundle.msg_cursor[0]

    written_bytes = 0

    for i in range(chunks):
        idx = i % c_items

        # 1. Write Metadata
        bundle.timestamps[row_cursor] = start_ts + i
        bundle.offsets[row_cursor] = byte_cursor

        c_len = c_lens[idx]
        bundle.lengths[row_cursor] = c_len

        # 2. Fast byte copy into the contiguous buffer
        c_off = c_offs[idx]
        bundle.buffer[byte_cursor : byte_cursor + c_len] = c_buf[c_off : c_off + c_len]

        byte_cursor += c_len
        row_cursor += 1
        written_bytes += c_len

    # Update the batch's internal trackers directly via the array references!
    bundle.size[0] = row_cursor
    bundle.msg_cursor[0] = byte_cursor

    # We only need to return written_bytes for the global benchmark counters
    return written_bytes

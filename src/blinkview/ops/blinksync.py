# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""BlinkSync pong parsing, shared by the serial (Zephyr `bsync`) and logcat (`blinksync.pong`) time
syncers. Both transports log the same line (wiki:specs/blinksync-time-sync.md):

    r s=<seq> m=<mono> b=<boot> w=<wall>

`m`, `b` and `w` may be omitted (read as 0); `w` is not used by the sync engine."""

import numpy as np

from blinkview.core import dtypes
from blinkview.core.numba_config import app_njit
from blinkview.ops.segments import nb_find_next_module_index
from blinkview.ops.strings import nb_find_and_parse_int

# Read-only (np.frombuffer of bytes): every caller passes these same arrays, so one compiled signature
PONG_PREFIX = np.frombuffer(b"r s=", dtype=dtypes.BYTE)
KEY_SEQ = np.frombuffer(b"s", dtype=dtypes.BYTE)
KEY_MONO = np.frombuffer(b"m", dtype=dtypes.BYTE)
KEY_BOOT = np.frombuffer(b"b", dtype=dtypes.BYTE)


@app_njit()
def nb_parse_pong_payload(bundle, index, pong_prefix, key_seq, key_mono, key_boot):
    """Checks that row `index` starts with the pong prefix and reads its seq, mono and boot.
    Returns (is_pong, seq, mono, boot); seq is -1 and the clocks 0 when absent."""
    offset = bundle.offsets[index]
    length = bundle.lengths[index]
    buffer = bundle.buffer

    prefix_len = pong_prefix.shape[0]
    if length < prefix_len:
        return False, -1, 0, 0

    for i in range(prefix_len):
        if buffer[offset + i] != pong_prefix[i]:
            return False, -1, 0, 0

    has_seq, seq = nb_find_and_parse_int(buffer, offset, length, key_seq)
    has_mono, mono = nb_find_and_parse_int(buffer, offset, length, key_mono)
    has_boot, boot = nb_find_and_parse_int(buffer, offset, length, key_boot)

    return True, seq if has_seq else -1, mono if has_mono else 0, boot if has_boot else 0


@app_njit()
def nb_find_and_parse_next_pong(bundle, module_id, cursor, pong_prefix, key_seq, key_mono, key_boot):
    """Finds the next row of `module_id` at or after `cursor` and parses it as a pong.
    Returns (found, is_pong, next_cursor, seq, mono, boot, rx_ts); next_cursor is the row after the
    match, so a caller loops until found is False."""
    found, idx = nb_find_next_module_index(bundle, module_id, cursor)
    if not found:
        return False, False, cursor, -1, 0, 0, 0

    is_pong, seq, mono, boot = nb_parse_pong_payload(bundle, idx, pong_prefix, key_seq, key_mono, key_boot)

    # uint64 + an int literal would unify to float64 in Numba
    return True, is_pong, idx + np.uint64(1), seq, mono, boot, bundle.rx_timestamps[idx]

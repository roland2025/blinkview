# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.numba_config import app_njit

NOT_FOUND = -1


@app_njit()
def nb_resolve_id_at(start_row: int, ts_ns: int, valid_from, valid_until, prev_row) -> int:
    """Walks `prev_row` backward from `start_row`, returning the row whose
    [valid_from, valid_until) interval contains `ts_ns`, or NOT_FOUND if the chain ends without a
    match. Mirrors nb_get_descendants' backward-chain-walk shape (ops/id_registry.py) - chain
    length is bounded by how many times a given key was reused, expected O(1)-O(few)."""
    row = start_row
    while row != NOT_FOUND:
        if valid_from[row] <= ts_ns < valid_until[row]:
            return row
        row = prev_row[row]
    return NOT_FOUND

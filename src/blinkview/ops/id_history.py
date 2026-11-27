# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import NamedTuple

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.core.types.empty import EMPTY_BYTES_RO

NOT_FOUND = -1


class IdHistoryTable(NamedTuple):
    """Read-only snapshot of an IdHistory (core/id_history.py) that kernels can resolve against:
    "which name did key K hold at timestamp T", per row, without calling back into Python.

    keys/latest_rows are the key -> newest-row index, sorted by key for a binary search;
    valid_from/valid_until/prev_row/name_ids are the history rows themselves (see IdHistory).
    name_buf/name_offs/name_lens hold the interned names' UTF-8 bytes, indexed by name id."""

    keys: np.ndarray  # uint64, ascending
    latest_rows: np.ndarray  # int32, parallel to keys
    valid_from: np.ndarray  # int64
    valid_until: np.ndarray  # int64
    prev_row: np.ndarray  # int32
    name_ids: np.ndarray  # int32
    name_buf: np.ndarray  # uint8, read-only (np.frombuffer)
    name_offs: np.ndarray  # int64
    name_lens: np.ndarray  # int64
    max_name_len: int


# Same dtypes and read-only-ness as a real IdHistory.table(), so both hit one compiled
# specialization (numba-njit skill Sec 15).
EMPTY_ID_HISTORY_TABLE = IdHistoryTable(
    keys=np.empty(0, dtype=np.uint64),
    latest_rows=np.empty(0, dtype=np.int32),
    valid_from=np.empty(0, dtype=np.int64),
    valid_until=np.empty(0, dtype=np.int64),
    prev_row=np.empty(0, dtype=np.int32),
    name_ids=np.empty(0, dtype=np.int32),
    name_buf=EMPTY_BYTES_RO,
    name_offs=np.empty(0, dtype=np.int64),
    name_lens=np.empty(0, dtype=np.int64),
    max_name_len=0,
)


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


@app_njit(inline="always")
def nb_resolve_name_id(table: IdHistoryTable, key, ts_ns) -> int:
    """Name id `key` held at `ts_ns` in `table`, or NOT_FOUND."""
    keys = table.keys
    n = keys.shape[0]
    lo = 0
    hi = n
    while lo < hi:
        mid = (lo + hi) >> 1
        if keys[mid] < key:
            lo = mid + 1
        else:
            hi = mid
    if lo >= n or keys[lo] != key:
        return NOT_FOUND

    row = nb_resolve_id_at(table.latest_rows[lo], ts_ns, table.valid_from, table.valid_until, table.prev_row)
    if row == NOT_FOUND:
        return NOT_FOUND
    return table.name_ids[row]

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING, Optional

import numpy as np

from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.id_history import NOT_FOUND, nb_resolve_id_at

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

# "Still valid as of the last update" - a max-int sentinel (rather than -1) so `ts < valid_until`
# comparisons work naturally without a branch, consistent with how the rest of this codebase uses
# max-int/-1-style sentinels (e.g. dtypes.ID_UNSPECIFIED, ops/id_registry.py's NO_PARENT).
OPEN_UNTIL = np.iinfo(np.int64).max

_NO_ROW = -1


class IdHistory:
    """Remembers which name an arbitrary uint64 key held over time, and resolves "what name did
    key K have at timestamp T". Built for reused/recycled numeric identifiers (PIDs, TIDs, or
    anything similar from any log source) - this module has no notion of "namespace" or "kind"
    and no domain-specific vocabulary; callers are responsible for constructing collision-free
    keys themselves (e.g. bit-packing a device id with a locally-scoped id) if their id space
    needs disambiguating across multiple sources.

    Storage is one row per (key, name) *transition*, not per log line - update() is expected to
    be called at most a handful of times per key over a whole session (process/thread lifecycle
    events are rare relative to log volume), so growth and key->row lookup stay in plain Python
    (a dict is simpler and fast enough at this frequency). Only the timestamp resolution walk -
    the part a future hot-path caller (bulk row rendering, a logfmt-style filter) might call a
    lot - is Numba-backed."""

    def __init__(self, initial_capacity: int = 64):
        self._capacity = max(1, initial_capacity)
        self._count = 0

        self._keys = np.zeros(self._capacity, dtype=np.uint64)
        self._name_ids = np.zeros(self._capacity, dtype=np.int32)
        self._valid_from = np.zeros(self._capacity, dtype=np.int64)
        self._valid_until = np.full(self._capacity, OPEN_UNTIL, dtype=np.int64)
        self._prev_row = np.full(self._capacity, _NO_ROW, dtype=np.int32)

        self._latest_row: dict[int, int] = {}  # key -> most recent row index

        self._names: list[str] = []
        self._name_to_id: dict[str, int] = {}

    def _intern(self, name: str) -> int:
        name_id = self._name_to_id.get(name)
        if name_id is not None:
            return name_id
        name_id = len(self._names)
        self._names.append(name)
        self._name_to_id[name] = name_id
        return name_id

    def _grow(self):
        new_cap = self._capacity * 2

        new_keys = np.zeros(new_cap, dtype=np.uint64)
        new_keys[: self._capacity] = self._keys
        new_name_ids = np.zeros(new_cap, dtype=np.int32)
        new_name_ids[: self._capacity] = self._name_ids
        new_valid_from = np.zeros(new_cap, dtype=np.int64)
        new_valid_from[: self._capacity] = self._valid_from
        new_valid_until = np.full(new_cap, OPEN_UNTIL, dtype=np.int64)
        new_valid_until[: self._capacity] = self._valid_until
        new_prev_row = np.full(new_cap, _NO_ROW, dtype=np.int32)
        new_prev_row[: self._capacity] = self._prev_row

        self._keys = new_keys
        self._name_ids = new_name_ids
        self._valid_from = new_valid_from
        self._valid_until = new_valid_until
        self._prev_row = new_prev_row
        self._capacity = new_cap

    def _append(self, key: int, name_id: int, ts_ns: int, prev_row: int) -> int:
        if self._count >= self._capacity:
            self._grow()

        row = self._count
        self._keys[row] = key
        self._name_ids[row] = name_id
        self._valid_from[row] = ts_ns
        self._valid_until[row] = OPEN_UNTIL
        self._prev_row[row] = prev_row
        self._count += 1
        return row

    def update(self, key: int, name: str, ts_ns: int) -> None:
        """Records that `key` is (now) named `name` as of `ts_ns`. A no-op if `key`'s currently
        open interval already has this name (avoids opening a pointless new row every poll
        tick). Otherwise closes the previous open interval (if any) at `ts_ns` and appends a new
        open one."""
        name_id = self._intern(name)
        prev = self._latest_row.get(key, _NO_ROW)

        if prev != _NO_ROW and self._valid_until[prev] == OPEN_UNTIL:
            if self._name_ids[prev] == name_id:
                return  # unchanged - nothing to record
            self._valid_until[prev] = ts_ns

        self._latest_row[key] = self._append(key, name_id, ts_ns, prev)

    def close(self, key: int, ts_ns: int) -> None:
        """Marks `key` as no longer valid as of `ts_ns` (e.g. process death), with no replacement
        interval opened."""
        row = self._latest_row.get(key, _NO_ROW)
        if row != _NO_ROW and self._valid_until[row] == OPEN_UNTIL:
            self._valid_until[row] = ts_ns

    def resolve(self, key: int, ts_ns: int) -> Optional[str]:
        """Returns the name `key` held at `ts_ns`, or None if `key` is unknown or had no name at
        that time."""
        row = self._latest_row.get(key, _NO_ROW)
        if row == _NO_ROW:
            return None

        found = nb_resolve_id_at(row, ts_ns, self._valid_from, self._valid_until, self._prev_row)
        if found == NOT_FOUND:
            return None

        return self._names[self._name_ids[found]]

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for nb_resolve_id_at against a tiny dummy history."""
        print("[Warmup] IdHistory ...")

        history = IdHistory(initial_capacity=4)
        history.update(1, "warmup", 0)
        history.update(1, "warmup", 100)  # unchanged - exercises the no-op path
        history.update(1, "warmup_renamed", 200)  # exercises the close+open path
        history.resolve(1, 50)
        history.resolve(1, 999)
        history.resolve(999, 0)  # unknown key

        print("[Warmup] IdHistory ... done")

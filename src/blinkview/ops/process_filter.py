# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Per-row process-name matching for the scan kernels (ops/segments.py): a row's process is the
name its (device, pid) held at the row's timestamp in pid_history (core/id_history.py, fed by
AdbReader's ps poll). Two uses, baked together into one ProcessFilter by LogFilter:

- text_mask: names matching the free-text search - ORed with the device/module/message matches.
- require_mask: names matching every `process=` term of the kv box - ANDed with the other
  conditions; a row with no known process never passes it."""

from typing import NamedTuple, Sequence

import numpy as np

from blinkview.core.numba_config import app_njit
from blinkview.ops.id_history import EMPTY_ID_HISTORY_TABLE, NOT_FOUND, IdHistoryTable, nb_resolve_name_id

EMPTY_NAME_MASK = np.zeros(0, dtype=np.bool_)


class ProcessFilter(NamedTuple):
    table: IdHistoryTable
    text_mask: np.ndarray  # bool, by name id: name matches the free-text search
    text_active: bool  # some name matches the search - resolve rows for it at all
    require: bool  # a `process=` condition is present
    require_mask: np.ndarray  # bool, by name id: name contains every `process=` term


EMPTY_PROC_FILTER = ProcessFilter(EMPTY_ID_HISTORY_TABLE, EMPTY_NAME_MASK, False, False, EMPTY_NAME_MASK)


def build_process_filter(
    table: IdHistoryTable, names: Sequence[str], text: str, require_terms: Sequence[str]
) -> ProcessFilter:
    """Bakes the masks over `names` (pid_history's interned names, by id). `text` and the terms
    are matched as case-insensitive substrings. Pure Python - runs once per filter or history
    change, not per row."""
    text = (text or "").strip().lower()
    terms = [t.lower() for t in require_terms]
    if not text and not terms:
        return EMPTY_PROC_FILTER

    lowered = [name.lower() for name in names]
    text_mask = np.fromiter((bool(text) and text in n for n in lowered), dtype=np.bool_, count=len(lowered))
    require_mask = np.fromiter((all(t in n for t in terms) for n in lowered), dtype=np.bool_, count=len(lowered))

    return ProcessFilter(
        table=table,
        text_mask=text_mask,
        text_active=bool(text_mask.any()),
        require=bool(terms),
        require_mask=require_mask,
    )


@app_njit(inline="always")
def nb_row_process_name_id(table: IdHistoryTable, segment, i) -> int:
    """pid_history name id of row `i`'s process, or NOT_FOUND (no pid, or not known then)."""
    if not segment.has_pids:
        return NOT_FOUND
    pid = segment.pids[i]
    if pid == 0:
        return NOT_FOUND
    key = (np.uint64(segment.devices[i]) << np.uint64(32)) | np.uint64(pid)
    return nb_resolve_name_id(table, key, segment.timestamps[i])


@app_njit(inline="always")
def nb_row_process_in_mask(table: IdHistoryTable, mask, segment, i) -> bool:
    name_id = nb_row_process_name_id(table, segment, i)
    return name_id != NOT_FOUND and name_id < mask.shape[0] and mask[name_id]

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.core.types.modules import MODULE_ID_FULL, MODULE_ID_UNKNOWN, MODULE_TEMP_ID_BASE, ModuleTrackerState
from blinkview.ops.discovery import nb_resolve_module_id


def _name_buffer(*names):
    """Packs names back-to-back and returns (buffer, {name: (start, len)})."""
    joined = "".join(names).encode("utf-8")
    buffer = np.frombuffer(joined, dtype=dtypes.BYTE)
    spans = {}
    cursor = 0
    for name in names:
        n = len(name.encode("utf-8"))
        spans[name] = (cursor, n)
        cursor += n
    return buffer, spans


def _tracker(capacity=8, name_bytes=None):
    if name_bytes is None:
        name_bytes = np.zeros(256, dtype=dtypes.BYTE)
    return ModuleTrackerState(
        count=np.zeros(1, dtype=np.int64),
        bytes_cursor=np.zeros(1, dtype=np.int64),
        starts=np.zeros(capacity, dtype=dtypes.OFFSET_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        hashes=np.zeros(capacity, dtype=dtypes.HASH_TYPE),
        name_bytes=name_bytes,
    )


def test_resolve_module_id_empty_name_returns_unknown():
    buffer, spans = _name_buffer("x")
    table = IndexedStringTable(initial_capacity=4).bundle()
    tracker = _tracker()

    result = nb_resolve_module_id(buffer, 0, 0, table, tracker)

    assert result == MODULE_ID_UNKNOWN


def test_resolve_module_id_finds_match_in_permanent_registry():
    table_obj = IndexedStringTable(initial_capacity=4, use_hashes=True)
    table_obj.register_name(0, "wifi")
    table_obj.register_name(1, "ble")
    table = table_obj.bundle()

    buffer, spans = _name_buffer("ble")
    start, length = spans["ble"]
    tracker = _tracker()

    result = nb_resolve_module_id(buffer, start, length, table, tracker)

    assert result == 1


def test_resolve_module_id_unregistered_name_promotes_to_new_temp_id():
    table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
    name_bytes = np.zeros(64, dtype=dtypes.BYTE)
    b = "newmod".encode("utf-8")
    name_bytes[: len(b)] = np.frombuffer(b, dtype=dtypes.BYTE)
    tracker = _tracker(name_bytes=name_bytes)

    result = nb_resolve_module_id(name_bytes, 0, len(b), table, tracker)

    assert result == MODULE_TEMP_ID_BASE + 0
    assert tracker.count[0] == 1
    assert tracker.bytes_cursor[0] == len(b)
    assert tracker.starts[0] == 0
    assert tracker.lengths[0] == len(b)


def test_resolve_module_id_reuses_existing_temp_id_for_repeated_name():
    table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
    name_bytes = np.zeros(64, dtype=dtypes.BYTE)
    b = "newmod".encode("utf-8")
    name_bytes[: len(b)] = np.frombuffer(b, dtype=dtypes.BYTE)
    tracker = _tracker(name_bytes=name_bytes)

    first = nb_resolve_module_id(name_bytes, 0, len(b), table, tracker)
    # Same name occurring again elsewhere in the buffer must resolve to the same temp id,
    # not allocate a second one.
    second = nb_resolve_module_id(name_bytes, 0, len(b), table, tracker)

    assert first == second
    assert tracker.count[0] == 1


def test_resolve_module_id_returns_full_when_tracker_capacity_exhausted():
    table = IndexedStringTable(initial_capacity=4, use_hashes=True).bundle()
    name_bytes = np.zeros(64, dtype=dtypes.BYTE)
    b = "abc".encode("utf-8")
    name_bytes[:3] = np.frombuffer(b, dtype=dtypes.BYTE)
    tracker = _tracker(capacity=1, name_bytes=name_bytes)

    first = nb_resolve_module_id(name_bytes, 0, 3, table, tracker)
    assert first == MODULE_TEMP_ID_BASE + 0

    # A different, never-seen name with the tracker already full must signal MODULE_ID_FULL.
    b2 = "xyz".encode("utf-8")
    name_bytes[3:6] = np.frombuffer(b2, dtype=dtypes.BYTE)
    second = nb_resolve_module_id(name_bytes, 3, 3, table, tracker)

    assert second == MODULE_ID_FULL


def test_resolve_module_id_permanent_registry_takes_precedence_over_tracker():
    table_obj = IndexedStringTable(initial_capacity=4, use_hashes=True)
    table_obj.register_name(0, "known")
    table = table_obj.bundle()

    name_bytes = np.zeros(64, dtype=dtypes.BYTE)
    b = "known".encode("utf-8")
    name_bytes[: len(b)] = np.frombuffer(b, dtype=dtypes.BYTE)
    tracker = _tracker(name_bytes=name_bytes)

    result = nb_resolve_module_id(name_bytes, 0, len(b), table, tracker)

    assert result == 0  # resolves to the permanent id, never becomes a temp id
    assert tracker.count[0] == 0

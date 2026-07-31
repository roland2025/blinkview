# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core import dtypes
from blinkview.ops.module_snapshot import (
    MAX_MSG_BYTES,
    ModuleSnapshotParams,
    nb_build_snapshot_as_of,
    nb_copy_live_valid_modules,
    nb_copy_snapshot_state,
    nb_update_master_arrays_reverse,
)
from tests.fakes.log_bundle import make_log_bundle


def _make_snapshot(capacity, count=0):
    return ModuleSnapshotParams(
        timestamps=np.zeros(capacity, dtype=dtypes.TS_TYPE),
        sequence_ids=np.zeros(capacity, dtype=dtypes.SEQ_TYPE),
        levels=np.zeros(capacity, dtype=dtypes.LEVEL_TYPE),
        lengths=np.zeros(capacity, dtype=dtypes.LEN_TYPE),
        buffer=np.zeros(capacity * MAX_MSG_BYTES, dtype=dtypes.BYTE),
        count=count,
        capacity=capacity,
    )


def _msg_at(snap, module_id):
    off = module_id * MAX_MSG_BYTES
    length = int(snap.lengths[module_id])
    return snap.buffer[off : off + length].tobytes().decode("utf-8")


def _make_first_seen(capacity):
    return np.zeros(capacity, dtype=dtypes.TS_TYPE), np.zeros(capacity, dtype=dtypes.SEQ_TYPE)


# ---------------------------------------------------------------------------
# nb_copy_snapshot_state
# ---------------------------------------------------------------------------


def test_copy_snapshot_state_copies_populated_rows():
    old = _make_snapshot(4, count=2)
    old.timestamps[:2] = [10, 20]
    old.levels[:2] = [1, 2]
    old.sequence_ids[:2] = [100, 200]
    old.lengths[0] = 3
    old.buffer[0:3] = np.frombuffer(b"abc", dtype=dtypes.BYTE)

    new = _make_snapshot(8)

    nb_copy_snapshot_state(old, new)

    assert list(new.timestamps[:2]) == [10, 20]
    assert list(new.levels[:2]) == [1, 2]
    assert list(new.sequence_ids[:2]) == [100, 200]
    assert new.buffer[0:3].tobytes() == b"abc"


def test_copy_snapshot_state_cleanses_tail_beyond_old_count():
    old = _make_snapshot(2, count=1)
    old.timestamps[0] = 5
    old.levels[0] = 9
    old.sequence_ids[0] = 50

    new = _make_snapshot(4)
    # Poison the tail to prove it gets cleared
    new.timestamps[:] = 999
    new.levels[:] = 9
    new.sequence_ids[:] = 999
    new.lengths[:] = 5

    nb_copy_snapshot_state(old, new)

    assert new.timestamps[0] == 5
    assert list(new.timestamps[1:]) == [0, 0, 0]
    assert list(new.sequence_ids[1:]) == [0, 0, 0]
    assert list(new.lengths[1:]) == [0, 0, 0]


# ---------------------------------------------------------------------------
# nb_update_master_arrays_reverse
# ---------------------------------------------------------------------------


def test_update_master_arrays_reverse_keeps_latest_sequence_per_module():
    bundle = make_log_bundle(
        timestamps=[100, 200, 300],
        devices=[0, 0, 0],
        levels=[1, 2, 3],
        modules=[0, 0, 1],
        sequences=[1, 2, 3],
        messages=["first", "second", "third"],
    )
    snap = _make_snapshot(2)
    first_seen_ts, first_seen_seq = _make_first_seen(2)

    hit_watermark = nb_update_master_arrays_reverse(
        bundle,
        snap,
        module_count=2,
        last_known_seq=0,
        is_initialized=False,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )

    assert hit_watermark is False
    # Module 0's latest by sequence is row 1 ("second", seq=2), not row 0.
    assert snap.sequence_ids[0] == 2
    assert snap.timestamps[0] == 200
    assert _msg_at(snap, 0) == "second"

    assert snap.sequence_ids[1] == 3
    assert _msg_at(snap, 1) == "third"


def test_update_master_arrays_reverse_stops_at_last_known_seq():
    bundle = make_log_bundle(
        timestamps=[100, 200, 300],
        devices=[0, 0, 0],
        levels=[1, 1, 1],
        modules=[0, 0, 0],
        sequences=[1, 2, 3],
        messages=["a", "b", "c"],
    )
    snap = _make_snapshot(1)
    first_seen_ts, first_seen_seq = _make_first_seen(1)

    hit_watermark = nb_update_master_arrays_reverse(
        bundle,
        snap,
        module_count=1,
        last_known_seq=2,
        is_initialized=True,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )

    assert hit_watermark is True
    # Only row 2 (seq=3) should have been applied before the scan stopped at seq<=2.
    assert snap.sequence_ids[0] == 3
    assert _msg_at(snap, 0) == "c"


def test_update_master_arrays_reverse_ignores_out_of_range_modules():
    bundle = make_log_bundle(
        timestamps=[100],
        devices=[0],
        levels=[1],
        modules=[5],  # >= module_count
        sequences=[1],
        messages=["a"],
    )
    snap = _make_snapshot(2)
    first_seen_ts, first_seen_seq = _make_first_seen(2)

    nb_update_master_arrays_reverse(
        bundle,
        snap,
        module_count=2,
        last_known_seq=0,
        is_initialized=False,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )

    assert list(snap.sequence_ids) == [0, 0]


def test_update_master_arrays_reverse_truncates_and_null_terminates_long_message():
    long_msg = "x" * (MAX_MSG_BYTES + 50)
    bundle = make_log_bundle(
        timestamps=[100],
        devices=[0],
        levels=[1],
        modules=[0],
        sequences=[1],
        messages=[long_msg],
    )
    snap = _make_snapshot(1)
    first_seen_ts, first_seen_seq = _make_first_seen(1)

    nb_update_master_arrays_reverse(
        bundle,
        snap,
        module_count=1,
        last_known_seq=0,
        is_initialized=False,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )

    assert snap.lengths[0] == MAX_MSG_BYTES - 1
    assert snap.buffer[MAX_MSG_BYTES - 1] == 0  # null terminator within the module's slot


# ---------------------------------------------------------------------------
# nb_update_master_arrays_reverse - first_seen_ts/first_seen_seq tracking
# ---------------------------------------------------------------------------


def test_first_seen_is_set_on_first_sighting():
    bundle = make_log_bundle(
        timestamps=[100, 200],
        devices=[0, 0],
        levels=[1, 1],
        modules=[0, 1],
        sequences=[1, 2],
        messages=["a", "b"],
    )
    snap = _make_snapshot(2)
    first_seen_ts, first_seen_seq = _make_first_seen(2)

    nb_update_master_arrays_reverse(
        bundle,
        snap,
        module_count=2,
        last_known_seq=0,
        is_initialized=False,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )

    assert list(first_seen_seq) == [1, 2]
    assert list(first_seen_ts) == [100, 200]


def test_first_seen_keeps_the_smallest_seq_seen_across_multiple_calls():
    """Simulates two successive update() ticks over a contiguous, non-overlapping seq range -
    first_seen must converge to the module's true first-ever occurrence (the smaller seq),
    regardless of which tick it was visited in."""
    snap = _make_snapshot(1)
    first_seen_ts, first_seen_seq = _make_first_seen(1)

    # Tick 1: module 0's only row so far is seq=5.
    bundle1 = make_log_bundle(timestamps=[500], devices=[0], levels=[1], modules=[0], sequences=[5], messages=["mid"])
    nb_update_master_arrays_reverse(
        bundle1,
        snap,
        module_count=1,
        last_known_seq=0,
        is_initialized=False,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )
    assert first_seen_seq[0] == 5

    # Tick 2: a *newer* row (seq=9) arrives - must NOT overwrite the earlier first_seen.
    bundle2 = make_log_bundle(timestamps=[900], devices=[0], levels=[1], modules=[0], sequences=[9], messages=["new"])
    nb_update_master_arrays_reverse(
        bundle2,
        snap,
        module_count=1,
        last_known_seq=5,
        is_initialized=True,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )
    assert first_seen_seq[0] == 5
    assert first_seen_ts[0] == 500


def test_first_seen_untouched_for_modules_with_no_rows_in_a_call():
    bundle = make_log_bundle(timestamps=[100], devices=[0], levels=[1], modules=[0], sequences=[1], messages=["a"])
    snap = _make_snapshot(2)
    first_seen_ts, first_seen_seq = _make_first_seen(2)

    nb_update_master_arrays_reverse(
        bundle,
        snap,
        module_count=2,
        last_known_seq=0,
        is_initialized=False,
        first_seen_ts=first_seen_ts,
        first_seen_seq=first_seen_seq,
    )

    assert first_seen_seq[0] == 1
    assert first_seen_seq[1] == 0  # module 1 never appeared - still unconfirmed


# ---------------------------------------------------------------------------
# nb_build_snapshot_as_of
# ---------------------------------------------------------------------------


def test_build_snapshot_as_of_only_considers_rows_at_or_before_max_ts():
    bundle = make_log_bundle(
        timestamps=[100, 200, 300],
        devices=[0, 0, 0],
        levels=[1, 1, 1],
        modules=[0, 0, 0],
        sequences=[1, 2, 3],
        messages=["a", "b", "future"],
    )
    snap = _make_snapshot(1)
    found_mask = np.zeros(1, dtype=np.bool_)

    all_found, remaining = nb_build_snapshot_as_of(
        bundle,
        snap,
        module_count=1,
        max_ts_ns=200,
        min_ts_ns_exclusive=dtypes.TS_UNSPECIFIED,
        found_mask=found_mask,
        remaining=1,
    )

    assert all_found is True
    assert remaining == 0
    assert snap.timestamps[0] == 200  # latest row at or before max_ts_ns
    assert _msg_at(snap, 0) == "b"


def test_build_snapshot_as_of_skips_already_found_modules():
    bundle = make_log_bundle(
        timestamps=[100, 200],
        devices=[0, 0],
        levels=[1, 1],
        modules=[0, 0],
        sequences=[1, 2],
        messages=["old", "new"],
    )
    snap = _make_snapshot(1)
    found_mask = np.array([True], dtype=np.bool_)  # module 0 already resolved

    all_found, remaining = nb_build_snapshot_as_of(
        bundle,
        snap,
        module_count=1,
        max_ts_ns=200,
        min_ts_ns_exclusive=dtypes.TS_UNSPECIFIED,
        found_mask=found_mask,
        remaining=0,
    )

    assert all_found is True
    assert remaining == 0
    # Snapshot untouched since the module was already marked found.
    assert snap.timestamps[0] == 0


def test_build_snapshot_as_of_reports_not_all_found_when_remaining_positive():
    bundle = make_log_bundle(
        timestamps=[100],
        devices=[0],
        levels=[1],
        modules=[0],
        sequences=[1],
        messages=["a"],
    )
    snap = _make_snapshot(2)
    found_mask = np.zeros(2, dtype=np.bool_)

    all_found, remaining = nb_build_snapshot_as_of(
        bundle,
        snap,
        module_count=2,
        max_ts_ns=1000,
        min_ts_ns_exclusive=dtypes.TS_UNSPECIFIED,
        found_mask=found_mask,
        remaining=2,
    )

    assert all_found is False


# ---------------------------------------------------------------------------
# nb_copy_live_valid_modules
# ---------------------------------------------------------------------------


def test_copy_live_valid_modules_copies_modules_at_or_before_max_ts():
    current = _make_snapshot(2, count=2)
    current.timestamps[:2] = [100, 200]
    current.sequence_ids[:2] = [1, 2]
    current.levels[:2] = [1, 2]
    current.lengths[0] = 3
    current.buffer[0:3] = np.frombuffer(b"abc", dtype=dtypes.BYTE)
    current.lengths[1] = 3
    current.buffer[MAX_MSG_BYTES : MAX_MSG_BYTES + 3] = np.frombuffer(b"xyz", dtype=dtypes.BYTE)

    snap = _make_snapshot(2)
    found_mask = np.zeros(2, dtype=np.bool_)

    remaining = nb_copy_live_valid_modules(
        current, snap, module_count=2, max_ts_ns=150, found_mask=found_mask, remaining=2
    )

    assert remaining == 1
    assert list(found_mask) == [True, False]
    assert snap.timestamps[0] == 100
    assert snap.sequence_ids[0] == 1
    assert _msg_at(snap, 0) == "abc"
    # Module 1's live occurrence (ts=200) is after max_ts_ns=150 - must stay unresolved.
    assert snap.sequence_ids[1] == 0


def test_copy_live_valid_modules_skips_already_found_and_empty_modules():
    current = _make_snapshot(2, count=2)
    current.timestamps[:2] = [50, 0]
    current.sequence_ids[:2] = [1, 0]  # module 1 has no live data (seq=0)

    snap = _make_snapshot(2)
    found_mask = np.array([True, False], dtype=np.bool_)  # module 0 already resolved elsewhere

    remaining = nb_copy_live_valid_modules(
        current, snap, module_count=2, max_ts_ns=1000, found_mask=found_mask, remaining=1
    )

    assert remaining == 1  # neither module changed remaining
    assert snap.sequence_ids[0] == 0  # untouched - was already found before this call
    assert snap.sequence_ids[1] == 0  # untouched - live data is empty (seq==0)
    assert list(found_mask) == [True, False]
    assert remaining == 1  # module 0 resolved, module 1 still outstanding

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""IdHistory's start-time backdating, change counter and Numba-readable table snapshot (what the
log viewers' process-name column and process filter resolve against)."""

import threading

import numpy as np
import pytest

from blinkview.core.id_history import IdHistory
from blinkview.ops.id_history import EMPTY_ID_HISTORY_TABLE, NOT_FOUND, nb_resolve_name_id


@pytest.fixture
def history():
    return IdHistory(initial_capacity=2)


def _resolve_via_table(history, key, ts):
    table = history.table()
    name_id = nb_resolve_name_id(table, np.uint64(key), np.int64(ts))
    return None if name_id == NOT_FOUND else history.names()[name_id]


class TestStartedAtBackdating:
    def test_new_key_interval_starts_at_started_ns(self, history):
        history.update(1, "app", 1_000, started_ns=400)

        assert history.resolve(1, 400) == "app"
        assert history.resolve(1, 399) is None

    def test_started_ns_after_ts_is_clamped_to_ts(self, history):
        history.update(1, "app", 1_000, started_ns=5_000)

        assert history.resolve(1, 1_000) == "app"
        assert history.resolve(1, 999) is None

    def test_unchanged_name_does_not_move_the_start(self, history):
        history.update(1, "app", 1_000, started_ns=400)
        history.update(1, "app", 2_000, started_ns=100)

        assert history.resolve(1, 399) is None

    def test_reused_key_closes_previous_owner_at_new_start(self, history):
        """The old owner was only seen dead at the next poll (2_000), but the new process says it
        started at 1_500 - the PID can't have belonged to both, so the old interval ends there."""
        history.update(1, "old", 1_000)
        history.update(1, "new", 2_000, started_ns=1_500)

        assert history.resolve(1, 1_499) == "old"
        assert history.resolve(1, 1_500) == "new"

    def test_reused_key_after_close_trims_the_closed_interval(self, history):
        history.update(1, "old", 1_000)
        history.close(1, 2_000)  # death observed at the 2_000 poll
        history.update(1, "new", 3_000, started_ns=1_800)

        assert history.resolve(1, 1_799) == "old"
        assert history.resolve(1, 1_900) == "new"

    def test_new_start_never_precedes_previous_interval_start(self, history):
        history.update(1, "old", 1_000)
        history.update(1, "new", 2_000, started_ns=500)  # bogus elapsed time

        assert history.resolve(1, 999) is None
        assert history.resolve(1, 1_000) == "new"


class TestVersion:
    def test_bumps_on_real_changes_only(self, history):
        v0 = history.version
        history.update(1, "app", 1_000)
        v1 = history.version
        history.update(1, "app", 2_000)  # unchanged
        v2 = history.version
        history.close(1, 3_000)
        v3 = history.version
        history.close(1, 4_000)  # already closed
        v4 = history.version

        assert v0 < v1 == v2 < v3 == v4


class TestTable:
    def test_empty_history_returns_the_empty_table(self, history):
        assert history.table() is EMPTY_ID_HISTORY_TABLE

    def test_table_is_cached_until_history_changes(self, history):
        history.update(1, "app", 1_000)
        first = history.table()
        assert history.table() is first

        history.update(2, "other", 1_000)
        assert history.table() is not first

    def test_table_resolves_like_resolve(self, history):
        history.update(7, "a", 100)
        history.update(7, "b", 200)
        history.update((3 << 32) | 9, "c", 50)
        history.close((3 << 32) | 9, 80)

        for key, ts in [(7, 99), (7, 150), (7, 250), ((3 << 32) | 9, 60), ((3 << 32) | 9, 90), (12345, 100)]:
            assert _resolve_via_table(history, key, ts) == history.resolve(key, ts)

    def test_table_survives_growth(self, history):
        for pid in range(40):  # initial_capacity=2: grows several times
            history.update(pid, f"proc{pid}", 1_000)

        for pid in range(40):
            assert _resolve_via_table(history, pid, 1_000) == f"proc{pid}"

    def test_table_names_and_max_len(self, history):
        history.update(1, "short", 0)
        history.update(2, "a-much-longer-name", 0)
        table = history.table()

        names = [
            bytes(table.name_buf[table.name_offs[i] : table.name_offs[i] + table.name_lens[i]]).decode()
            for i in range(len(table.name_lens))
        ]
        assert names == list(history.names())
        assert table.max_name_len == len("a-much-longer-name")

    def test_table_arrays_match_empty_table_types(self, history):
        """One Numba specialization for empty and real tables: same dtypes, same read-only-ness
        (numba-njit skill Sec 15)."""
        history.update(1, "app", 0)
        real = history.table()
        for field in real._fields:
            real_v, empty_v = getattr(real, field), getattr(EMPTY_ID_HISTORY_TABLE, field)
            if isinstance(real_v, np.ndarray):
                assert real_v.dtype == empty_v.dtype, field
                assert real_v.flags.writeable == empty_v.flags.writeable, field

    def test_concurrent_updates_while_snapshotting(self, history):
        """The ADB poll thread writes while the UI thread snapshots - no exception, and every
        snapshot is internally consistent (each listed key resolves)."""
        stop = threading.Event()
        errors = []

        def writer():
            pid = 0
            while not stop.is_set():
                history.update(pid % 500, f"p{pid}", pid)
                pid += 1

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            for _ in range(300):
                try:
                    table = history.table()
                    names = history.names()
                    for row in table.latest_rows:
                        assert 0 <= table.name_ids[row] < len(names) or len(names) == 0
                except Exception as exc:  # noqa: BLE001 - collected for the assert below
                    errors.append(exc)
        finally:
            stop.set()
            thread.join()

        assert errors == []

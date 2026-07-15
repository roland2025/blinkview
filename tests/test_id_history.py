# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.id_history import OPEN_UNTIL, IdHistory


@pytest.fixture
def history():
    return IdHistory(initial_capacity=2)


def test_unknown_key_resolves_to_none(history):
    assert history.resolve(42, 0) is None


def test_single_interval_resolves(history):
    history.update(1, "proc_a", 100)

    assert history.resolve(1, 99) is None  # before valid_from
    assert history.resolve(1, 100) == "proc_a"  # inclusive lower bound
    assert history.resolve(1, 5000) == "proc_a"  # still open


def test_update_with_same_name_is_a_noop(history):
    history.update(1, "proc_a", 100)
    history.update(1, "proc_a", 200)

    assert history._count == 1
    assert history._valid_from[0] == 100
    assert history._valid_until[0] == OPEN_UNTIL


def test_update_with_changed_name_closes_and_opens(history):
    history.update(1, "proc_a", 100)
    history.update(1, "proc_b", 300)

    assert history._count == 2
    assert history.resolve(1, 250) == "proc_a"
    assert history.resolve(1, 300) == "proc_b"
    assert history.resolve(1, 999) == "proc_b"


def test_close_ends_the_open_interval_without_replacement(history):
    history.update(1, "proc_a", 100)
    history.close(1, 400)

    assert history.resolve(1, 399) == "proc_a"
    assert history.resolve(1, 400) is None
    assert history.resolve(1, 500) is None


def test_close_on_unknown_key_is_a_noop(history):
    history.close(999, 0)  # should not raise
    assert history.resolve(999, 0) is None


def test_reused_key_multiple_intervals_resolve_by_timestamp(history):
    """PID reuse: same key, three separate owners over time."""
    history.update(7, "app_a", 0)
    history.update(7, "app_b", 100)
    history.update(7, "app_c", 200)

    assert history.resolve(7, 50) == "app_a"
    assert history.resolve(7, 150) == "app_b"
    assert history.resolve(7, 250) == "app_c"


def test_different_keys_do_not_interfere(history):
    history.update(1, "proc_a", 0)
    history.update(2, "proc_b", 0)

    assert history.resolve(1, 10) == "proc_a"
    assert history.resolve(2, 10) == "proc_b"


def test_name_interning_deduplicates_repeated_names(history):
    history.update(1, "relaunched_app", 0)
    history.update(1, "other", 100)
    history.update(1, "relaunched_app", 200)  # same name string as the first interval

    assert len(history._names) == 2  # not 3 - "relaunched_app" reused its existing name_id


def test_capacity_grows_and_open_sentinel_fills_new_tail(history):
    # initial_capacity=2 - force growth well past it
    for i in range(10):
        history.update(1, f"name{i}", i * 10)

    assert history._capacity > 2
    assert history._count == 10
    # Every row's valid_until is either OPEN_UNTIL (the last one) or was closed by the next update.
    assert history._valid_until[history._count - 1] == OPEN_UNTIL
    for i in range(history._count - 1):
        assert history._valid_until[i] != OPEN_UNTIL

    # Resolution still works correctly after growth.
    assert history.resolve(1, 5) == "name0"
    assert history.resolve(1, 95) == "name9"

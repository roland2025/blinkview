# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.dtypes import BYTE
from blinkview.ops.kv_filter import (
    EMPTY_KV_CONDITIONS,
    build_kv_condition_arrays,
    nb_row_matches_kv_conditions,
)

FIELD_DELIM = 32  # space
KV_DELIM = 61  # '='


def make_row(message: str):
    """Builds a standalone (buffer, offset, length) row for direct kernel testing."""
    b = message.encode("utf-8")
    return np.frombuffer(b, dtype=BYTE), 0, len(b)


def matches(message: str, conditions) -> bool:
    buf, off, length = make_row(message)
    arrays = build_kv_condition_arrays(conditions)
    return bool(nb_row_matches_kv_conditions(buf, off, length, *arrays, FIELD_DELIM, KV_DELIM))


def test_no_conditions_always_matches():
    assert matches("anything at all", [])


def test_single_condition_match():
    assert matches("status=ok user_id=42", [(b"status", b"ok")])


def test_single_condition_no_match_wrong_value():
    assert not matches("status=ok user_id=42", [(b"status", b"bad")])


def test_condition_key_absent_from_row():
    assert not matches("status=ok user_id=42", [(b"missing_key", b"x")])


def test_multiple_conditions_all_match_is_anded():
    conditions = [(b"status", b"ok"), (b"user_id", b"42")]
    assert matches("status=ok user_id=42", conditions)


def test_multiple_conditions_one_missing_fails_and():
    conditions = [(b"status", b"ok"), (b"user_id", b"99")]
    assert not matches("status=ok user_id=42", conditions)


def test_quoted_value_containing_the_delimiter():
    # The value itself contains '=' and a space, both inside quotes.
    assert matches('query="a=b c" status=ok', [(b"query", b"a=b c"), (b"status", b"ok")])


def test_conditions_match_regardless_of_pair_order_in_message():
    assert matches("b=2 a=1 c=3", [(b"c", b"3"), (b"a", b"1")])


def test_key_present_but_value_partial_prefix_does_not_match():
    # "status=5000" must not satisfy a condition for "status=500" (no substring matching).
    assert not matches("status=5000", [(b"status", b"500")])


def test_build_kv_condition_arrays_empty_returns_shared_empty_singleton():
    assert build_kv_condition_arrays([]) is EMPTY_KV_CONDITIONS


def test_build_kv_condition_arrays_flattens_offsets_and_lengths():
    arrays = build_kv_condition_arrays([(b"status", b"ok"), (b"id", b"42")])

    assert arrays.num_conditions == 2
    assert arrays.cond_keys_len.tolist() == [6, 2]
    assert arrays.cond_vals_len.tolist() == [2, 2]

    k0 = arrays.cond_keys_buf[arrays.cond_keys_off[0] : arrays.cond_keys_off[0] + arrays.cond_keys_len[0]]
    assert k0.tobytes() == b"status"
    v1 = arrays.cond_vals_buf[arrays.cond_vals_off[1] : arrays.cond_vals_off[1] + arrays.cond_vals_len[1]]
    assert v1.tobytes() == b"42"


def test_build_kv_condition_arrays_caps_at_max_conditions():
    from blinkview.ops.kv_filter import MAX_KV_CONDITIONS

    conditions = [(f"k{i}".encode(), b"v") for i in range(MAX_KV_CONDITIONS + 5)]
    arrays = build_kv_condition_arrays(conditions)
    assert arrays.num_conditions == MAX_KV_CONDITIONS

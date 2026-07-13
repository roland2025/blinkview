# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry.registry import IDRegistry
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel


@pytest.fixture
def log_filter():
    return LogFilter(IDRegistry(NumpyArrayPool()), log_level=LogLevel.ALL.name_conf)


def test_starts_with_no_kv_conditions(log_filter):
    assert log_filter.kv_conditions == []
    assert log_filter.bake_kv_arrays() is EMPTY_KV_CONDITIONS


def test_set_kv_filter_parses_multiple_pairs(log_filter):
    log_filter.set_kv_filter("status=ok user_id=42")
    assert log_filter.kv_conditions == [(b"status", b"ok"), (b"user_id", b"42")]


def test_set_kv_filter_supports_quoted_values_with_spaces(log_filter):
    log_filter.set_kv_filter('msg="hello world" status=ok')
    assert log_filter.kv_conditions == [(b"msg", b"hello world"), (b"status", b"ok")]


def test_set_kv_filter_ignores_malformed_quoting(log_filter):
    log_filter.set_kv_filter('status=ok "unbalanced')
    assert log_filter.kv_conditions == []


def test_set_kv_filter_ignores_tokens_without_equals(log_filter):
    log_filter.set_kv_filter("status=ok justaword")
    assert log_filter.kv_conditions == [(b"status", b"ok")]


def test_set_kv_filter_empty_text_clears_conditions(log_filter):
    log_filter.set_kv_filter("status=ok")
    assert log_filter.kv_conditions

    log_filter.set_kv_filter("")
    assert log_filter.kv_conditions == []
    assert log_filter.bake_kv_arrays() is EMPTY_KV_CONDITIONS


def test_bake_kv_arrays_is_cached_until_conditions_change(log_filter):
    log_filter.set_kv_filter("status=ok")
    baked_first = log_filter.bake_kv_arrays()
    baked_second = log_filter.bake_kv_arrays()
    assert baked_first is baked_second

    log_filter.set_kv_filter("status=bad")
    baked_third = log_filter.bake_kv_arrays()
    assert baked_third is not baked_first


def test_bake_kv_arrays_flattens_conditions(log_filter):
    log_filter.set_kv_filter("status=ok id=42")
    arrays = log_filter.bake_kv_arrays()

    assert arrays.num_conditions == 2
    k0 = arrays.cond_keys_buf[arrays.cond_keys_off[0] : arrays.cond_keys_off[0] + arrays.cond_keys_len[0]]
    assert k0.tobytes() == b"status"


@pytest.mark.parametrize(
    "text,expected",
    [
        ("", True),
        ("status", True),  # still typing the key, no '=' yet
        ("status=", False),  # just typed '=', no value character yet
        ("status=o", True),  # one value character written
        ("status=ok", True),
        ("status=ok id=", False),  # first pair complete, second pair mid-'='
        ("status=ok id=4", True),
        ('msg="hello world" status=', False),
        ("status=ok ", True),  # trailing space after a complete pair
    ],
)
def test_is_kv_query_ready(text, expected):
    assert LogFilter.is_kv_query_ready(text) is expected

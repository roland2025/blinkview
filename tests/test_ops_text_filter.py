# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.dtypes import BYTE
from blinkview.ops.text_filter import (
    EMPTY_TEXT_SEARCH,
    build_text_search_arrays,
    nb_bytes_contains_ci,
)


def make_row(message: str):
    b = message.encode("utf-8")
    return np.frombuffer(b, dtype=BYTE), 0, len(b)


def contains(message: str, needle: str) -> bool:
    buf, off, length = make_row(message)
    needle_bytes = needle.lower().encode("utf-8")
    needle_buf = np.frombuffer(needle_bytes, dtype=BYTE)
    return bool(nb_bytes_contains_ci(buf, off, length, needle_buf, len(needle_bytes)))


class Identity:
    def __init__(self, name):
        self.name = name


def test_nb_bytes_contains_ci_empty_needle_always_matches():
    assert contains("anything", "")


def test_nb_bytes_contains_ci_finds_substring():
    assert contains("connection lost", "lost")


def test_nb_bytes_contains_ci_is_case_insensitive():
    assert contains("Connection LOST", "lost")
    assert contains("connection lost", "LOST")


def test_nb_bytes_contains_ci_no_match():
    assert not contains("all good", "lost")


def test_nb_bytes_contains_ci_needle_longer_than_haystack():
    assert not contains("hi", "hello world")


def test_nb_bytes_contains_ci_match_at_start_and_end():
    assert contains("needle at start", "needle")
    assert contains("ends with needle", "needle")


def test_build_text_search_arrays_empty_text_returns_shared_empty_singleton():
    assert build_text_search_arrays("", {}, {}) is EMPTY_TEXT_SEARCH
    assert build_text_search_arrays(None, {}, {}) is EMPTY_TEXT_SEARCH
    assert build_text_search_arrays("   ", {}, {}) is EMPTY_TEXT_SEARCH


def test_build_text_search_arrays_bakes_needle_lowercased():
    arrays = build_text_search_arrays("HELLO", {}, {})
    assert arrays.needle_buf.tobytes() == b"hello"
    assert arrays.needle_len == 5


def test_build_text_search_arrays_device_mask_marks_matching_names():
    devices = {0: Identity("esp32"), 1: Identity("nrf52"), 5: Identity("esp32_secondary")}
    arrays = build_text_search_arrays("esp32", devices, {})

    assert arrays.dev_mask[0]
    assert not arrays.dev_mask[1]
    assert arrays.dev_mask[5]
    assert len(arrays.dev_mask) == 6  # sized to max id + 1


def test_build_text_search_arrays_module_mask_marks_matching_names():
    modules = {0: Identity("esp32.wifi"), 1: Identity("esp32.ble")}
    arrays = build_text_search_arrays("wifi", {}, modules)

    assert arrays.mod_mask[0]
    assert not arrays.mod_mask[1]


def test_build_text_search_arrays_name_match_is_case_insensitive():
    devices = {0: Identity("ESP32")}
    arrays = build_text_search_arrays("esp32", devices, {})
    assert arrays.dev_mask[0]

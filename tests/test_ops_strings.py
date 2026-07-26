# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core import dtypes
from blinkview.ops.strings import (
    fnv1a_64_python,
    nb_bundle_find_and_parse_int,
    nb_filter_ansi_inplace,
    nb_filter_printable_inplace,
    nb_find_and_parse_int,
    nb_fnv1a_64_fast,
    nb_is_alpha,
    nb_is_digit,
    nb_is_whitespace,
    nb_skip_n_words,
    nb_skip_non_whitespace,
    nb_skip_whitespace,
    nb_skip_whitespace_reverse,
    nb_squash_spaces_inplace,
    nb_to_lower,
    nb_to_upper,
    nb_trim_spaces,
)
from tests.fakes.log_bundle import make_log_bundle


def _buf(s: str):
    return np.frombuffer(s.encode("utf-8"), dtype=dtypes.BYTE)


def _text(buf, start=0, end=None):
    end = len(buf) if end is None else end
    return buf[start:end].tobytes().decode("utf-8")


# ---------------------------------------------------------------------------
# character classifiers
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("char", [ord(" "), ord("\t"), ord("\n"), ord("\r")])
def test_is_whitespace_true_for_space_tab_lf_cr(char):
    assert bool(nb_is_whitespace(char))


@pytest.mark.parametrize("char", [ord("a"), ord("0"), 0, 31, 33])
def test_is_whitespace_false_for_others(char):
    assert not bool(nb_is_whitespace(char))


@pytest.mark.parametrize("char", [ord("0"), ord("5"), ord("9")])
def test_is_digit_true_for_digits(char):
    assert bool(nb_is_digit(char))


@pytest.mark.parametrize("char", [ord("a"), ord(":"), ord("/")])
def test_is_digit_false_for_non_digits(char):
    assert not bool(nb_is_digit(char))


@pytest.mark.parametrize("char", [ord("A"), ord("Z"), ord("a"), ord("z")])
def test_is_alpha_true_for_letters(char):
    assert bool(nb_is_alpha(char))


@pytest.mark.parametrize("char", [ord("0"), ord(" "), ord("@")])
def test_is_alpha_false_for_non_letters(char):
    assert not bool(nb_is_alpha(char))


def test_to_lower_converts_uppercase():
    assert nb_to_lower(ord("A")) == ord("a")
    assert nb_to_lower(ord("Z")) == ord("z")


def test_to_lower_leaves_lowercase_and_digits_unchanged():
    assert nb_to_lower(ord("a")) == ord("a")
    assert nb_to_lower(ord("5")) == ord("5")


def test_to_upper_converts_lowercase():
    assert nb_to_upper(ord("a")) == ord("A")
    assert nb_to_upper(ord("z")) == ord("Z")


def test_to_upper_leaves_uppercase_and_digits_unchanged():
    assert nb_to_upper(ord("A")) == ord("A")
    assert nb_to_upper(ord("5")) == ord("5")


# ---------------------------------------------------------------------------
# nb_filter_printable_inplace
# ---------------------------------------------------------------------------


def test_filter_printable_inplace_drops_non_printable_bytes():
    buf = bytearray(b"ab\x01cd\x7f")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    new_end = nb_filter_printable_inplace(arr, 0, len(arr))

    assert arr[:new_end].tobytes() == b"abcd"


def test_filter_printable_inplace_keeps_all_printable_bytes_unchanged():
    buf = bytearray(b"hello world")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    new_end = nb_filter_printable_inplace(arr, 0, len(arr))

    assert arr[:new_end].tobytes() == b"hello world"


# ---------------------------------------------------------------------------
# nb_filter_ansi_inplace
# ---------------------------------------------------------------------------


def test_filter_ansi_inplace_strips_csi_sequence():
    buf = bytearray(b"\x1b[31mred\x1b[0m")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    new_end = nb_filter_ansi_inplace(arr, 0, len(arr))

    assert arr[:new_end].tobytes() == b"red"


def test_filter_ansi_inplace_drops_orphan_esc_without_eating_next_char():
    buf = bytearray(b"a\x1bb")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    new_end = nb_filter_ansi_inplace(arr, 0, len(arr))

    assert arr[:new_end].tobytes() == b"ab"


def test_filter_ansi_inplace_no_escape_sequences_is_a_no_op():
    buf = bytearray(b"plain text")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    new_end = nb_filter_ansi_inplace(arr, 0, len(arr))

    assert arr[:new_end].tobytes() == b"plain text"


# ---------------------------------------------------------------------------
# whitespace cursor helpers
# ---------------------------------------------------------------------------


def test_skip_whitespace_advances_past_leading_spaces_and_tabs():
    arr = _buf("  \tabc")
    assert nb_skip_whitespace(arr, 0, len(arr)) == 3


def test_skip_whitespace_reverse_retreats_past_trailing_spaces():
    arr = _buf("abc  ")
    assert nb_skip_whitespace_reverse(arr, 0, len(arr)) == 3


def test_skip_whitespace_reverse_stops_at_start_boundary():
    arr = _buf("   ")
    assert nb_skip_whitespace_reverse(arr, 1, len(arr)) == 1


def test_skip_non_whitespace_advances_to_next_space():
    arr = _buf("word next")
    assert nb_skip_non_whitespace(arr, 0, len(arr)) == 4


# ---------------------------------------------------------------------------
# nb_squash_spaces_inplace / nb_trim_spaces
# ---------------------------------------------------------------------------


def test_squash_spaces_inplace_collapses_internal_runs_and_strips_ends():
    buf = bytearray(b"  a   b  c  ")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    start, end = nb_squash_spaces_inplace(arr, 0, len(arr))

    assert _text(arr, start, end) == "a b c"


def test_squash_spaces_inplace_all_spaces_yields_empty_range():
    buf = bytearray(b"    ")
    arr = np.frombuffer(buf, dtype=dtypes.BYTE)

    start, end = nb_squash_spaces_inplace(arr, 0, len(arr))

    assert start == end


def test_trim_spaces_strips_leading_and_trailing_without_copying():
    arr = _buf("   hello world   ")
    start, end = nb_trim_spaces(arr, 0, len(arr))
    assert _text(arr, start, end) == "hello world"


def test_trim_spaces_no_surrounding_spaces_is_unchanged():
    arr = _buf("hello")
    start, end = nb_trim_spaces(arr, 0, len(arr))
    assert (start, end) == (0, len(arr))


# ---------------------------------------------------------------------------
# nb_skip_n_words
# ---------------------------------------------------------------------------


def test_skip_n_words_lands_after_the_nth_word():
    arr = _buf("one two three four")
    cursor = nb_skip_n_words(arr, 0, len(arr), 2)
    assert _text(arr, cursor) == "three four"


def test_skip_n_words_zero_words_returns_start_after_whitespace_trim():
    arr = _buf("one two")
    cursor = nb_skip_n_words(arr, 0, len(arr), 0)
    assert cursor == 0


def test_skip_n_words_more_words_than_available_reaches_end():
    arr = _buf("one two")
    cursor = nb_skip_n_words(arr, 0, len(arr), 10)
    assert cursor == len(arr)


# ---------------------------------------------------------------------------
# nb_find_and_parse_int / nb_bundle_find_and_parse_int
# ---------------------------------------------------------------------------


def _key(s):
    return np.frombuffer(s.encode("utf-8"), dtype=dtypes.BYTE)


def test_find_and_parse_int_finds_positive_value():
    arr = _buf("level=3 pid=42 done")
    found, value = nb_find_and_parse_int(arr, 0, len(arr), _key("pid"))
    assert found
    assert value == 42


def test_find_and_parse_int_finds_negative_value():
    arr = _buf("offset=-15 end")
    found, value = nb_find_and_parse_int(arr, 0, len(arr), _key("offset"))
    assert found
    assert value == -15


def test_find_and_parse_int_key_not_present_returns_false():
    arr = _buf("level=3 pid=42")
    found, value = nb_find_and_parse_int(arr, 0, len(arr), _key("tid"))
    assert not found
    assert value == 0


def test_find_and_parse_int_requires_preceding_space_boundary():
    """A key embedded inside a larger token (e.g. 'xpid=1') must not match 'pid'."""
    arr = _buf("xpid=1 real")
    found, value = nb_find_and_parse_int(arr, 0, len(arr), _key("pid"))
    assert not found


def test_find_and_parse_int_no_digits_after_equals_returns_false():
    arr = _buf("pid= next")
    found, value = nb_find_and_parse_int(arr, 0, len(arr), _key("pid"))
    assert not found


def test_bundle_find_and_parse_int_reads_from_log_bundle_row():
    bundle = make_log_bundle(
        timestamps=[1],
        devices=[0],
        levels=[1],
        modules=[0],
        sequences=[1],
        messages=["tag pid=7 more"],
    )
    found, value = nb_bundle_find_and_parse_int(bundle, 0, _key("pid"))
    assert found
    assert value == 7


# ---------------------------------------------------------------------------
# nb_fnv1a_64_fast
# ---------------------------------------------------------------------------


def test_fnv1a_64_fast_matches_python_reference():
    arr = _buf("hello world")
    fast = int(nb_fnv1a_64_fast(arr, 0, len(arr)))
    reference = fnv1a_64_python(arr, 0, len(arr))
    assert fast == reference


def test_fnv1a_64_fast_respects_start_and_length_window():
    arr = _buf("xxhelloyy")
    fast = int(nb_fnv1a_64_fast(arr, 2, 5))
    reference = fnv1a_64_python(_buf("hello"), 0, 5)
    assert fast == reference


def test_fnv1a_64_fast_different_inputs_produce_different_hashes():
    a = int(nb_fnv1a_64_fast(_buf("wifi"), 0, 4))
    b = int(nb_fnv1a_64_fast(_buf("ble"), 0, 3))
    assert a != b

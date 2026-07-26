# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.ops.reorder import nb_find_split_idx, nb_sum_lengths

# nb_hybrid_merge_and_copy is exercised directly in tests/test_reorderer.py
# (via its blinkview.core.reorderer re-export); these cover the two small
# helper kernels that aren't hit by those tests.


def test_find_split_idx_all_ready():
    timestamps = np.array([1, 2, 3, 4], dtype=np.int64)
    assert nb_find_split_idx(timestamps, 0, 4, safe_ts=10) == 4


def test_find_split_idx_none_ready():
    timestamps = np.array([10, 20, 30], dtype=np.int64)
    assert nb_find_split_idx(timestamps, 0, 3, safe_ts=5) == 0


def test_find_split_idx_partial_with_cursor_offset():
    timestamps = np.array([1, 2, 3, 4, 5, 100], dtype=np.int64)
    # Only rows with timestamps[i] <= safe_ts, starting the count from cursor=2
    assert nb_find_split_idx(timestamps, 2, 6, safe_ts=4) == 2  # covers idx 2,3 (values 3,4)


def test_find_split_idx_boundary_equal_is_included():
    timestamps = np.array([5, 5, 6], dtype=np.int64)
    assert nb_find_split_idx(timestamps, 0, 3, safe_ts=5) == 2


def test_sum_lengths_basic():
    lengths = np.array([3, 5, 7, 11], dtype=np.uint32)
    assert nb_sum_lengths(lengths, 1, 3) == 12


def test_sum_lengths_empty_range():
    lengths = np.array([3, 5, 7], dtype=np.uint32)
    assert nb_sum_lengths(lengths, 1, 1) == 0

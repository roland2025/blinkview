# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core.numba_config import app_njit
from blinkview.ops.buffers import nb_copy_buf, nb_find_byte, nb_move_buf


@app_njit()
def _find(buf, start, end, value):
    return nb_find_byte(buf, start, end, value)


@app_njit()
def _move(buf, src, dst, n):
    nb_move_buf(buf, src, dst, n)


@app_njit()
def _copy(src, src_off, dst, dst_off, n):
    nb_copy_buf(src, src_off, dst, dst_off, n)


def _reference_find(buf, start, end, value):
    for i in range(start, end):
        if buf[i] == value:
            return i
    return end


class TestFindByte:
    @pytest.mark.parametrize("length", [0, 1, 7, 8, 9, 15, 16, 17, 63, 64, 65])
    def test_matches_reference_for_every_position_and_offset(self, length):
        rng = np.random.default_rng(length)
        for start in (0, 1, 3):
            buf = rng.integers(1, 255, size=length + start + 5, dtype=np.uint8)
            end = start + length
            assert _find(buf, start, end, 0) == end  # absent
            for pos in range(start, end):
                probe = buf.copy()
                probe[pos] = 0
                assert _find(probe, start, end, 0) == pos == _reference_find(probe, start, end, 0)

    def test_first_of_several_matches_wins(self):
        buf = np.full(40, 1, dtype=np.uint8)
        buf[[13, 14, 30]] = 10
        assert _find(buf, 0, 40, 10) == 13

    def test_match_beyond_end_is_ignored(self):
        buf = np.full(32, 1, dtype=np.uint8)
        buf[20] = 0
        assert _find(buf, 0, 20, 0) == 20

    def test_high_bit_bytes_do_not_false_positive(self):
        buf = np.full(64, 0x81, dtype=np.uint8)
        assert _find(buf, 0, 64, 0x01) == 64
        assert _find(buf, 0, 64, 0x80) == 64
        buf[37] = 0x80
        assert _find(buf, 0, 64, 0x80) == 37


class TestMoveAndCopy:
    @pytest.mark.parametrize("src,dst", [(10, 0), (0, 10), (5, 7), (7, 5), (20, 20)])
    @pytest.mark.parametrize("n", [0, 1, 9, 33, 100])
    def test_move_handles_overlap_like_memmove(self, src, dst, n):
        buf = np.arange(256, dtype=np.uint8)
        expected = buf.copy()
        expected[dst : dst + n] = buf[src : src + n].copy()

        _move(buf, src, dst, n)

        assert (buf == expected).all()

    def test_copy_between_arrays_and_zero_length(self):
        src = np.arange(64, dtype=np.uint8)
        dst = np.zeros(64, dtype=np.uint8)

        _copy(src, 5, dst, 20, 30)
        _copy(src, 0, dst, 0, 0)

        assert (dst[20:50] == src[5:35]).all()
        assert dst[:20].sum() == 0 and dst[50:].sum() == 0

    def test_copy_respects_item_size(self):
        src = np.arange(16, dtype=np.int64)
        dst = np.zeros(16, dtype=np.int64)

        _copy(src, 2, dst, 4, 6)

        assert (dst[4:10] == src[2:8]).all() and dst[:4].sum() == 0 and dst[10:].sum() == 0

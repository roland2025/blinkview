# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.parsers.state import FrameState


@pytest.fixture
def pool():
    return NumpyArrayPool()


def test_bundle_arrays_start_zeroed(pool):
    state = FrameState(pool, size_bytes=64)

    assert state.bundle.offset[0] == 0
    assert state.bundle.in_idx[0] == 0
    assert state.bundle.in_offset[0] == 0
    assert state.bundle.in_frame[0] == False  # noqa: E712

    state.release()


def test_buffer_and_ts_buffer_are_at_least_the_requested_size(pool):
    state = FrameState(pool, size_bytes=64)

    assert state.bundle.buffer.nbytes >= 64
    assert state.bundle.ts_buffer.shape[0] >= 64

    state.release()


def test_reset_batch_trackers_clears_in_idx_and_in_offset_only(pool):
    state = FrameState(pool, size_bytes=64)
    state.bundle.in_idx[0] = 5
    state.bundle.in_offset[0] = 7
    state.bundle.in_frame[0] = True
    state.bundle.offset[0] = 9

    state.reset_batch_trackers()

    assert state.bundle.in_idx[0] == 0
    assert state.bundle.in_offset[0] == 0
    # reset_batch_trackers only touches in_idx/in_offset - in_frame/offset are untouched
    assert state.bundle.in_frame[0] == True  # noqa: E712
    assert state.bundle.offset[0] == 9

    state.release()


def test_clear_stitch_state_clears_in_frame_and_offset_only(pool):
    state = FrameState(pool, size_bytes=64)
    state.bundle.in_idx[0] = 5
    state.bundle.in_offset[0] = 7
    state.bundle.in_frame[0] = True
    state.bundle.offset[0] = 9

    state.clear_stitch_state()

    assert state.bundle.in_frame[0] == False  # noqa: E712
    assert state.bundle.offset[0] == 0
    # clear_stitch_state only touches in_frame/offset - in_idx/in_offset are untouched
    assert state.bundle.in_idx[0] == 5
    assert state.bundle.in_offset[0] == 7

    state.release()


def test_release_clears_bundle_and_frees_handles(pool):
    state = FrameState(pool, size_bytes=64)

    state.release()

    assert state.bundle is None
    assert state._pool_handle is None
    assert state._ts_handle is None


def test_clear_stitch_state_after_release_is_a_noop():
    pool = NumpyArrayPool()
    state = FrameState(pool, size_bytes=64)
    state.release()

    state.clear_stitch_state()  # must not raise

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
from numba.typed import List as NumbaList

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.frames import FrameConfig
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import ParserConfig, ParserPipelineBundle, pipeline_bundle_type
from blinkview.ops.dispatch import nb_process_batch_kernel
from blinkview.parsers.state import FrameState
from blinkview.utils.log_level import LogLevel


def _line_decoder_config(**overrides):
    defaults = dict(
        decode_id=10,  # CodecID.NEWLINE
        delimiter=ord("\n"),
        length_fixed=False,
        length_min=1,
        length_max=1024,
        length=0,
        filter_printable=False,
        filter_ansi=False,
        filter_trim_r=True,
        report_error=True,
    )
    defaults.update(overrides)
    return FrameConfig(**defaults)


def _empty_parser_bundle():
    p_config = ParserConfig(
        level_default=LogLevel.INFO.value,
        level_error=LogLevel.ERROR.value,
        module_log=0,
        module_unknown=0,
        device_id=0,
        report_error=True,
        filter_squash_spaces=False,
    )
    empty_pipeline = NumbaList.empty_list(pipeline_bundle_type)
    return ParserPipelineBundle(config=p_config, pipeline=empty_pipeline)


def _run_kernel(pool, frame_state, lines, frame_config=None):
    """Feeds each of `lines` as its own inserted row into an input batch (mirroring how a real
    reader batches separate OS-read chunks together), runs the real kernel once, and returns the
    decoded message bytes for every emitted output row."""
    in_batch = pool.create(PooledLogBatch, 16, 4096)
    for line in lines:
        in_batch.insert(1000, 1000, line)

    out_batch = pool.create(
        PooledLogBatch, 16, 4096, has_levels=True, has_modules=True, has_devices=True
    )

    o_cfg = OutputConfig(compact_buffer=True)
    parser_bundle = _empty_parser_bundle()

    nb_process_batch_kernel(
        frame_config or _line_decoder_config(),
        frame_state.bundle,
        in_batch.bundle,
        parser_bundle,
        o_cfg,
        out_batch.bundle,
    )

    messages = [bytes(msg) for _ts, msg, *_rest in out_batch]

    in_batch.release()
    out_batch.release()
    return messages


class TestFirstFrameDroppedOnFreshState:
    """Regression test documenting a real data-loss bug (not an edge case): the very first
    newline-delimited frame processed against a brand-new FrameState is silently discarded rather
    than emitted, because nb_process_batch_kernel's frame-boundary state machine starts with
    in_frame=False - the first delimiter match only flips in_frame=True and advances read_offset
    past it (priming the "we're mid-frame" state), without ever setting process_frame=True. Only
    the *second* delimiter match actually copies bytes to the output, and by then read_offset has
    already skipped past the first frame's content - it is gone, not merely delayed.

    Confirmed with a real BinkParser thread end-to-end (2026-07-26): a batch of "line1\n",
    "line2\n", "line3\n" fed through a fresh parser produces only "line2" and "line3" downstream -
    "line1" never reaches any subscriber. This reproduces the same loss directly against the
    kernel, isolated from the rest of the parser pipeline.

    Practical impact: on every new device connection, reconnect, or parser restart (anything that
    allocates a fresh FrameState), the first log line/frame from that source is silently lost.
    """

    def test_first_of_three_newline_frames_is_silently_dropped(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096)

        messages = _run_kernel(pool, frame_state, [b"line1\n", b"line2\n", b"line3\n"])

        # This asserts the CURRENT (buggy) behavior, not the desired one - if this test starts
        # failing because "line1" is now included, the kernel bug has been fixed and this test
        # (and the memory note describing it) should be updated/removed accordingly.
        assert messages == [b"line2", b"line3"]

        frame_state.release()

    def test_a_single_frame_on_fresh_state_produces_no_output_at_all(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096)

        messages = _run_kernel(pool, frame_state, [b"only line\n"])

        assert messages == []  # the sole frame is entirely swallowed by the priming pass

        frame_state.release()

    def test_a_throwaway_priming_frame_unblocks_all_real_frames_afterward(self):
        """The practical workaround other tests in this session use: feed one disposable frame
        first (on a fresh FrameState) so real frames of interest land on the second-and-later
        delimiter matches, which do get processed correctly."""
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096)

        messages = _run_kernel(pool, frame_state, [b"__priming__\n", b"real one\n", b"real two\n"])

        assert messages == [b"real one", b"real two"]

        frame_state.release()

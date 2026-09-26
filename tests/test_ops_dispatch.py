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
from blinkview.core.types.modules import DynamicWidthConfig
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import (
    TS_PRECISION_MS,
    ParserConfig,
    ParserID,
    ParserPipelineBundle,
    TimeParserState,
    UnifiedParserConfig,
    UnifiedParserState,
    UnusedSyncState,
    pipeline_bundle_type,
)
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

    out_batch = pool.create(PooledLogBatch, 16, 4096, has_levels=True, has_modules=True, has_devices=True)

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


# ---------------------------------------------------------------------------------------------------
# Stage-major kernel behaviour: dropped frames, error rows, compaction, column shifting, resume
# ---------------------------------------------------------------------------------------------------

LEVEL_DEFAULT = LogLevel.INFO.value
LEVEL_ERROR = LogLevel.ERROR.value
MODULE_LOG = 11
MODULE_UNKNOWN = 12
BAD_PARSER_ID = 999


def _ts_step():
    state = UnifiedParserState(timestamp=TimeParserState(sync=UnusedSyncState))
    state.timestamp.utc_offset[0] = 0
    config = UnifiedParserConfig(timestamp_precision=TS_PRECISION_MS, timestamp_unix=True)
    return ParserID.TS_INTEGER, state, config


def _skip_words_step(count):
    config = UnifiedParserConfig(module_config=DynamicWidthConfig(max_length=count))
    return ParserID.SKIP_WORDS, UnifiedParserState(), config


def _parser_bundle(steps=(), report_error=True, squash=False):
    p_config = ParserConfig(
        level_default=LEVEL_DEFAULT,
        level_error=LEVEL_ERROR,
        module_log=MODULE_LOG,
        module_unknown=MODULE_UNKNOWN,
        device_id=7,
        report_error=report_error,
        filter_squash_spaces=squash,
    )
    pipeline = NumbaList.empty_list(pipeline_bundle_type)
    for step in steps:
        pipeline.append(step)
    return ParserPipelineBundle(config=p_config, pipeline=pipeline)


class _Rig:
    """Drives nb_process_batch_kernel like BinaryParser.run(): feeds `lines` after a priming frame, and
    keeps calling the kernel with a fresh output batch while it reports out_full."""

    def __init__(self, *, steps=(), report_error=True, squash=False, compact=True, frame_config=None, has_pids=False):
        self.pool = NumpyArrayPool()
        self.frame_state = FrameState(self.pool, size_bytes=4096)
        self.parser = _parser_bundle(steps, report_error, squash)
        self.o_cfg = OutputConfig(compact_buffer=compact)
        self.frame_config = frame_config or _line_decoder_config()
        self.has_pids = has_pids
        self.calls = 0

    def feed(self, chunks, out_capacity=64, out_bytes=8192):
        rows = []
        for chunk in chunks:
            in_batch = self.pool.create(PooledLogBatch, 4, len(chunk) + 16)
            in_batch.insert(1000, 1000, chunk)
            self.frame_state.reset_batch_trackers()
            full = True
            while self.frame_state.bundle.in_idx[0] < 1 or full:
                out = self.pool.create(
                    PooledLogBatch,
                    out_capacity,
                    out_bytes,
                    has_levels=True,
                    has_modules=True,
                    has_devices=True,
                    has_pids=self.has_pids,
                )
                # the pool rounds capacities up, so trim the column views to exactly what the test asked for
                row_columns = (
                    "timestamps",
                    "rx_timestamps",
                    "offsets",
                    "lengths",
                    "levels",
                    "modules",
                    "devices",
                    "pids",
                )
                b = out.bundle._replace(
                    buffer=out.bundle.buffer[:out_bytes],
                    **{c: getattr(out.bundle, c)[:out_capacity] for c in row_columns},
                )
                if self.has_pids:
                    b.pids[:] = np.arange(100, 100 + len(b.pids))
                full = nb_process_batch_kernel(
                    self.frame_config,
                    self.frame_state.bundle,
                    in_batch.bundle,
                    self.parser,
                    self.o_cfg,
                    b,
                )
                self.calls += 1
                for i in range(int(b.size[0])):
                    off, length = int(b.offsets[i]), int(b.lengths[i])
                    rows.append(
                        dict(
                            msg=bytes(b.buffer[off : off + length]),
                            ts=int(b.timestamps[i]),
                            level=int(b.levels[i]),
                            module=int(b.modules[i]),
                            device=int(b.devices[i]),
                            pid=int(b.pids[i]) if self.has_pids else None,
                        )
                    )
                out.release()
                if not full:
                    break
            in_batch.release()
        return rows

    def close(self):
        self.frame_state.release()


def _rows(prime=b"__prime__\n", lines=(), **rig_kwargs):
    feed_kwargs = {k: rig_kwargs.pop(k) for k in ("out_capacity", "out_bytes") if k in rig_kwargs}
    rig = _Rig(**rig_kwargs)
    try:
        return rig.feed([prime + b"".join(lines)], **feed_kwargs), rig
    finally:
        rig.close()


class TestStageMajorKernel:
    def test_timestamps_and_payloads_follow_frames_across_dropped_frames(self):
        rows, _ = _rows(
            lines=[b"1000 first\n", b"2000   \n", b"3000 third\n", b"4000    \n", b"5000 fifth\n"],
            steps=[_ts_step()],
        )

        assert [(r["msg"], r["ts"]) for r in rows] == [
            (b"first", 1_000_000_000),
            (b"third", 3_000_000_000),
            (b"fifth", 5_000_000_000),
        ]
        assert all(r["level"] == LEVEL_DEFAULT and r["module"] == MODULE_LOG and r["device"] == 7 for r in rows)

    def test_failed_step_is_reported_as_error_row_when_enabled(self):
        rows, _ = _rows(lines=[b"1 ok\n", b"bad line\n", b"2 ok2\n"], steps=[_ts_step()], report_error=True)

        assert [r["msg"] for r in rows] == [b"ok", b"bad line", b"ok2"]
        assert rows[1]["level"] == LEVEL_ERROR and rows[1]["module"] == MODULE_UNKNOWN
        assert rows[0]["level"] == LEVEL_DEFAULT and rows[2]["level"] == LEVEL_DEFAULT

    def test_failed_step_is_dropped_when_error_reporting_disabled(self):
        rows, _ = _rows(lines=[b"1 ok\n", b"bad line\n", b"2 ok2\n"], steps=[_ts_step()], report_error=False)

        assert [r["msg"] for r in rows] == [b"ok", b"ok2"]

    def test_failed_step_stops_later_steps_for_that_frame_only(self):
        rows, _ = _rows(
            lines=[b"1 skipme keep1\n", b"nope skipme keep\n", b"3 skipme keep3\n"],
            steps=[_ts_step(), _skip_words_step(1)],
            report_error=True,
        )

        # the failing frame is emitted untouched (raw bytes), not passed through the skip-words step
        assert [r["msg"] for r in rows] == [b"keep1", b"nope skipme keep", b"keep3"]

    def test_unknown_pipeline_step_marks_frames_as_failed(self):
        bad_step = (BAD_PARSER_ID, UnifiedParserState(), UnifiedParserConfig())

        rows, _ = _rows(lines=[b"abc\n", b"def\n"], steps=[bad_step], report_error=True)
        assert [(r["msg"], r["level"]) for r in rows] == [(b"abc", LEVEL_ERROR), (b"def", LEVEL_ERROR)]

        rows, _ = _rows(lines=[b"abc\n", b"def\n"], steps=[bad_step], report_error=False)
        assert rows == []

    def test_zero_step_pipeline_still_drops_empty_frames(self):
        rows, _ = _rows(lines=[b"one\n", b"   \n", b"\n", b"two\n"])

        assert [r["msg"] for r in rows] == [b"one", b"two"]

    def test_frame_length_errors_reported_only_when_enabled(self):
        lines = [b"long enough\n", b"ab\n", b"another long one\n"]

        rows, _ = _rows(lines=lines, frame_config=_line_decoder_config(length_min=6, report_error=True))
        assert [(r["msg"], r["level"]) for r in rows] == [
            (b"long enough", LEVEL_DEFAULT),
            (b"ab", LEVEL_ERROR),
            (b"another long one", LEVEL_DEFAULT),
        ]

        rows, _ = _rows(lines=lines, frame_config=_line_decoder_config(length_min=6, report_error=False))
        assert [r["msg"] for r in rows] == [b"long enough", b"another long one"]

    def test_compact_buffer_false_keeps_payloads_intact(self):
        lines = [b"1000    spaced   \n", b"2000   \n", b"bad\n", b"3000 tail\n"]

        rows, _ = _rows(lines=lines, steps=[_ts_step()], compact=False)
        assert [r["msg"] for r in rows] == [b"spaced", b"bad", b"tail"]

        compact_rows, _ = _rows(lines=lines, steps=[_ts_step()], compact=True)
        assert rows == compact_rows

    def test_squash_spaces(self):
        rows, _ = _rows(lines=[b"1 a   b    c  \n", b"2    \n"], steps=[_ts_step()], squash=True)

        assert [r["msg"] for r in rows] == [b"a b c"]

    def test_optional_columns_move_with_their_rows(self):
        rows, _ = _rows(
            lines=[b"1 a\n", b"2   \n", b"3 c\n", b"4   \n", b"5 e\n"],
            steps=[_ts_step()],
            has_pids=True,
        )

        # pids were prefilled with the row slot (100 + slot); dropped frames leave holes that must be closed
        assert [(r["msg"], r["pid"]) for r in rows] == [(b"a", 100), (b"c", 102), (b"e", 104)]

    def test_output_full_resumes_without_losing_or_duplicating_frames(self):
        lines = [b"%d msg%d\n" % (i + 1, i) for i in range(20)]

        rows, rig = _rows(lines=lines, steps=[_ts_step()], out_capacity=3)

        assert [r["msg"] for r in rows] == [b"msg%d" % i for i in range(20)]
        assert rig.calls > 1

    def test_output_full_resume_with_dropped_frames(self):
        lines = []
        for i in range(30):
            lines.append(b"%d keep%d\n" % (i + 1, i) if i % 3 == 0 else b"%d   \n" % (i + 1))

        rows, _ = _rows(lines=lines, steps=[_ts_step()], out_capacity=4)

        assert [r["msg"] for r in rows] == [b"keep%d" % i for i in range(0, 30, 3)]

    def test_scratch_capacity_caps_frames_per_call_and_resumes(self):
        lines = [b"%d msg%d\n" % (i + 1, i) for i in range(50)]
        rig = _Rig(steps=[_ts_step()])
        rig.frame_state = FrameState(rig.pool, size_bytes=4096, max_frames=8)
        try:
            rows = rig.feed([b"__prime__\n" + b"".join(lines)], out_capacity=64)
        finally:
            rig.close()

        assert [r["msg"] for r in rows] == [b"msg%d" % i for i in range(50)]
        assert rig.calls >= 7

    def test_frames_split_across_input_chunks(self):
        rig = _Rig(steps=[_ts_step()])
        try:
            rows = rig.feed([b"__prime__\n1 hel", b"lo wor", b"ld\n2 second\n3 thi", b"rd\n"])
        finally:
            rig.close()

        assert [r["msg"] for r in rows] == [b"hello world", b"second", b"third"]

    def test_pre_framed_input(self):
        rig = _Rig(steps=[_ts_step()], frame_config=_line_decoder_config(decode_id=0))
        try:
            rows = rig.feed([b"__prime__", b"1 alpha", b"bad", b"2 beta"])
        finally:
            rig.close()

        assert [r["msg"] for r in rows] == [b"alpha", b"bad", b"beta"]

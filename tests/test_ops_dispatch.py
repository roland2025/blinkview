# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The stage-major pipeline as BinaryParser.run() drives it: a frame decoder kernel (ops/decode_loop.py), one
stage kernel per pipeline step, then nb_finish_frames (ops/dispatch.py)."""

import numpy as np

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.frames import FrameConfig
from blinkview.core.types.output import OutputConfig
from blinkview.core.types.parsing import TS_PRECISION_MS, ParserConfig, UnusedSyncState
from blinkview.ops.codecs import nb_decode_frames_newline, nb_decode_frames_passthrough
from blinkview.ops.dispatch import nb_finish_frames
from blinkview.ops.generic import nb_skip_words_parser_stage
from blinkview.ops.stage_loop import FS_OK, FS_STEP_FAILED
from blinkview.ops.timestamps import nb_parse_int_timestamp_stage
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


def _run_pipeline(frame_config, f_state, in_b, p_config, o_cfg, out_b, steps=()):
    """One pass of BinaryParser.run()'s inner loop: decode, run every step's stage kernel, finish.
    Returns out_full."""
    decode = nb_decode_frames_passthrough if frame_config.decode_id == 0 else nb_decode_frames_newline
    out_full, n = decode(frame_config, f_state, in_b, p_config, o_cfg, out_b)
    if n:
        for step in steps:
            step(out_b, f_state, n)
        nb_finish_frames(
            f_state.fstart,
            f_state.fcur,
            f_state.fend,
            f_state.ftotal,
            f_state.fstatus,
            p_config,
            o_cfg.compact_buffer,
            out_b,
            n,
        )
    return out_full


def _empty_parser_config():
    return ParserConfig(
        level_default=LogLevel.INFO.value,
        level_error=LogLevel.ERROR.value,
        module_log=0,
        module_unknown=0,
        device_id=0,
        report_error=True,
        filter_squash_spaces=False,
    )


def _run_kernel(pool, frame_state, lines, frame_config=None):
    """Feeds each of `lines` as its own inserted row into an input batch (mirroring how a real
    reader batches separate OS-read chunks together), runs the real kernel once, and returns the
    decoded message bytes for every emitted output row."""
    in_batch = pool.create(PooledLogBatch, 16, 4096)
    for line in lines:
        in_batch.insert(1000, 1000, line)

    out_batch = pool.create(PooledLogBatch, 16, 4096, has_levels=True, has_modules=True, has_devices=True)

    o_cfg = OutputConfig(compact_buffer=True)

    _run_pipeline(
        frame_config or _line_decoder_config(),
        frame_state.bundle,
        in_batch.bundle,
        _empty_parser_config(),
        o_cfg,
        out_batch.bundle,
    )

    messages = [bytes(msg) for _ts, msg, *_rest in out_batch]

    in_batch.release()
    out_batch.release()
    return messages


class TestStartupResync:
    """A fresh FrameState either resyncs (default, start_synced=False: everything up to the first
    delimiter is discarded, since a UART-like stream may have been joined mid-frame) or assumes the
    stream starts on a frame boundary (start_synced=True: the first frame is emitted too). Driven by
    the frame decoder's `frame_resync_on_start` setting via BinaryParser.run()."""

    def test_resync_discards_everything_before_the_first_delimiter(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096)

        messages = _run_kernel(pool, frame_state, [b"tail of a partial line\n", b"line2\n", b"line3\n"])

        assert messages == [b"line2", b"line3"]

        frame_state.release()

    def test_resync_swallows_a_lone_first_frame(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096)

        assert _run_kernel(pool, frame_state, [b"only line\n"]) == []

        frame_state.release()

    def test_start_synced_emits_the_first_frame(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096, start_synced=True)

        messages = _run_kernel(pool, frame_state, [b"line1\n", b"line2\n", b"line3\n"])

        assert messages == [b"line1", b"line2", b"line3"]

        frame_state.release()

    def test_start_synced_skips_a_leading_empty_frame(self):
        """A stream that opens with a delimiter (e.g. SLIP's leading END byte) yields an empty first
        frame, which produces no row."""
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096, start_synced=True)

        assert _run_kernel(pool, frame_state, [b"\nline1\n"]) == [b"line1"]

        frame_state.release()

    def test_start_synced_still_resyncs_after_an_oversized_frame(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096, start_synced=True)

        messages = _run_kernel(
            pool, frame_state, [b"x" * 20 + b"\n", b"ok\n"], frame_config=_line_decoder_config(length_max=8)
        )

        assert messages == [b"ok"]

        frame_state.release()

    def test_oversized_frame_does_not_take_the_next_frame_with_it(self):
        """The oversized frame's own delimiter ends it, so the decoder is still on a boundary afterwards."""
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096, start_synced=True)

        messages = _run_kernel(
            pool,
            frame_state,
            [b"x" * 20 + b"\n", b"ok\n", b"ok2\n"],
            frame_config=_line_decoder_config(length_max=8),
        )

        assert messages == [b"ok", b"ok2"]

        frame_state.release()

    def test_oversized_frame_split_across_rows_resyncs_at_its_delimiter(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096, start_synced=True)

        messages = _run_kernel(
            pool, frame_state, [b"x" * 10, b"y" * 10 + b"\n", b"ok\n"], frame_config=_line_decoder_config(length_max=8)
        )

        assert messages == [b"ok"]

        frame_state.release()

    def test_clear_stitch_state_restores_the_start_mode(self):
        pool = NumpyArrayPool()
        frame_state = FrameState(pool, size_bytes=4096, start_synced=True)
        frame_state.bundle.in_frame[0] = False

        frame_state.clear_stitch_state()

        assert frame_state.bundle.in_frame[0]

        frame_state.release()


# ---------------------------------------------------------------------------------------------------
# Stage-major pipeline behaviour: dropped frames, error rows, compaction, column shifting, resume
# ---------------------------------------------------------------------------------------------------

LEVEL_DEFAULT = LogLevel.INFO.value
LEVEL_ERROR = LogLevel.ERROR.value
MODULE_LOG = 11
MODULE_UNKNOWN = 12


def _ts_step(out_b, f_state, n):
    nb_parse_int_timestamp_stage(out_b, f_state, n, UnusedSyncState, TS_PRECISION_MS, True)


def _skip_words_step(count):
    def step(out_b, f_state, n):
        nb_skip_words_parser_stage(out_b, f_state, n, count)

    return step


def _failing_step(out_b, f_state, n):
    """A step that rejects every frame still in play, like a stage kernel whose step returned -1 for each."""
    status = f_state.fstatus[:n]
    status[status == FS_OK] = FS_STEP_FAILED


def _parser_config(report_error=True, squash=False):
    return ParserConfig(
        level_default=LEVEL_DEFAULT,
        level_error=LEVEL_ERROR,
        module_log=MODULE_LOG,
        module_unknown=MODULE_UNKNOWN,
        device_id=7,
        report_error=report_error,
        filter_squash_spaces=squash,
    )


class _Rig:
    """Drives the pipeline like BinaryParser.run(): feeds `lines` to a FrameState that starts synced (no resync
    discard), and keeps running it with a fresh output batch while the decoder reports out_full."""

    def __init__(self, *, steps=(), report_error=True, squash=False, compact=True, frame_config=None, has_pids=False):
        self.pool = NumpyArrayPool()
        self.frame_state = FrameState(self.pool, size_bytes=4096, start_synced=True)
        self.steps = tuple(steps)
        self.p_config = _parser_config(report_error, squash)
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
                full = _run_pipeline(
                    self.frame_config,
                    self.frame_state.bundle,
                    in_batch.bundle,
                    self.p_config,
                    self.o_cfg,
                    b,
                    self.steps,
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


def _rows(lines=(), **rig_kwargs):
    feed_kwargs = {k: rig_kwargs.pop(k) for k in ("out_capacity", "out_bytes") if k in rig_kwargs}
    rig = _Rig(**rig_kwargs)
    try:
        return rig.feed([b"".join(lines)], **feed_kwargs), rig
    finally:
        rig.close()


class TestStageMajorPipeline:
    def test_timestamps_and_payloads_follow_frames_across_dropped_frames(self):
        rows, _ = _rows(
            lines=[b"1000 first\n", b"2000   \n", b"3000 third\n", b"4000    \n", b"5000 fifth\n"],
            steps=[_ts_step],
        )

        assert [(r["msg"], r["ts"]) for r in rows] == [
            (b"first", 1_000_000_000),
            (b"third", 3_000_000_000),
            (b"fifth", 5_000_000_000),
        ]
        assert all(r["level"] == LEVEL_DEFAULT and r["module"] == MODULE_LOG and r["device"] == 7 for r in rows)

    def test_failed_step_is_reported_as_error_row_when_enabled(self):
        rows, _ = _rows(lines=[b"1 ok\n", b"bad line\n", b"2 ok2\n"], steps=[_ts_step], report_error=True)

        assert [r["msg"] for r in rows] == [b"ok", b"bad line", b"ok2"]
        assert rows[1]["level"] == LEVEL_ERROR and rows[1]["module"] == MODULE_UNKNOWN
        assert rows[0]["level"] == LEVEL_DEFAULT and rows[2]["level"] == LEVEL_DEFAULT

    def test_failed_step_is_dropped_when_error_reporting_disabled(self):
        rows, _ = _rows(lines=[b"1 ok\n", b"bad line\n", b"2 ok2\n"], steps=[_ts_step], report_error=False)

        assert [r["msg"] for r in rows] == [b"ok", b"ok2"]

    def test_failed_step_stops_later_steps_for_that_frame_only(self):
        rows, _ = _rows(
            lines=[b"1 skipme keep1\n", b"nope skipme keep\n", b"3 skipme keep3\n"],
            steps=[_ts_step, _skip_words_step(1)],
            report_error=True,
        )

        # the failing frame is emitted untouched (raw bytes), not passed through the skip-words step
        assert [r["msg"] for r in rows] == [b"keep1", b"nope skipme keep", b"keep3"]

    def test_step_failed_frames_become_error_rows_or_are_dropped(self):
        rows, _ = _rows(lines=[b"abc\n", b"def\n"], steps=[_failing_step], report_error=True)
        assert [(r["msg"], r["level"]) for r in rows] == [(b"abc", LEVEL_ERROR), (b"def", LEVEL_ERROR)]

        rows, _ = _rows(lines=[b"abc\n", b"def\n"], steps=[_failing_step], report_error=False)
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

        rows, _ = _rows(lines=lines, steps=[_ts_step], compact=False)
        assert [r["msg"] for r in rows] == [b"spaced", b"bad", b"tail"]

        compact_rows, _ = _rows(lines=lines, steps=[_ts_step], compact=True)
        assert rows == compact_rows

    def test_squash_spaces(self):
        rows, _ = _rows(lines=[b"1 a   b    c  \n", b"2    \n"], steps=[_ts_step], squash=True)

        assert [r["msg"] for r in rows] == [b"a b c"]

    def test_optional_columns_move_with_their_rows(self):
        rows, _ = _rows(
            lines=[b"1 a\n", b"2   \n", b"3 c\n", b"4   \n", b"5 e\n"],
            steps=[_ts_step],
            has_pids=True,
        )

        # pids were prefilled with the row slot (100 + slot); dropped frames leave holes that must be closed
        assert [(r["msg"], r["pid"]) for r in rows] == [(b"a", 100), (b"c", 102), (b"e", 104)]

    def test_output_full_resumes_without_losing_or_duplicating_frames(self):
        lines = [b"%d msg%d\n" % (i + 1, i) for i in range(20)]

        rows, rig = _rows(lines=lines, steps=[_ts_step], out_capacity=3)

        assert [r["msg"] for r in rows] == [b"msg%d" % i for i in range(20)]
        assert rig.calls > 1

    def test_output_full_resume_with_dropped_frames(self):
        lines = []
        for i in range(30):
            lines.append(b"%d keep%d\n" % (i + 1, i) if i % 3 == 0 else b"%d   \n" % (i + 1))

        rows, _ = _rows(lines=lines, steps=[_ts_step], out_capacity=4)

        assert [r["msg"] for r in rows] == [b"keep%d" % i for i in range(0, 30, 3)]

    def test_scratch_capacity_caps_frames_per_call_and_resumes(self):
        lines = [b"%d msg%d\n" % (i + 1, i) for i in range(50)]
        rig = _Rig(steps=[_ts_step])
        rig.frame_state = FrameState(rig.pool, size_bytes=4096, max_frames=8, start_synced=True)
        try:
            rows = rig.feed([b"".join(lines)], out_capacity=64)
        finally:
            rig.close()

        assert [r["msg"] for r in rows] == [b"msg%d" % i for i in range(50)]
        assert rig.calls >= 7

    def test_frames_split_across_input_chunks(self):
        rig = _Rig(steps=[_ts_step])
        try:
            rows = rig.feed([b"1 hel", b"lo wor", b"ld\n2 second\n3 thi", b"rd\n"])
        finally:
            rig.close()

        assert [r["msg"] for r in rows] == [b"hello world", b"second", b"third"]

    def test_pre_framed_input(self):
        rig = _Rig(steps=[_ts_step], frame_config=_line_decoder_config(decode_id=0))
        try:
            rows = rig.feed([b"1 alpha", b"bad", b"2 beta"])
        finally:
            rig.close()

        assert [r["msg"] for r in rows] == [b"alpha", b"bad", b"beta"]

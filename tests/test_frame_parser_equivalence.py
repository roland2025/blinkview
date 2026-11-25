# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Differential tests for the frame parser hot path.

The real FramePipelineParser.run() parses random Zephyr deferred lines (workload A) and `adb logcat -v long`
frames (workload B), cut at random byte offsets into input rows and batches, so frames split across rows and
batches. Every column of every output row must come out the same:

* whatever the decoder call's frame budget is (FrameState's max_frames, MAX_FRAMES_PER_CALL by default),
  including an input batch that yields exactly max_frames frames and one that yields one more (the "frame
  trackers ran out" re-call in run());
* with the adb long-frame decoder's frame function as it is and with the frozen copy at the bottom of this
  file, taken before its fast paths (word-at-a-time scans, memmove copies, resuming the next-header search
  where the last call on the same pending frame stopped) were added.

The text is synthesized; only its shape follows the real formats.
"""

import functools
import random
from types import SimpleNamespace

import numpy as np
import pytest

import blinkview.parsers.frame_pipeline_parser as fpp
from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.id_registry import IDRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.numba_config import app_njit
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.frames import FrameStateParams
from blinkview.core.types.parsing import STATE_COMPLETE, STATE_ERROR, STATE_INCOMPLETE
from blinkview.ops.buffers import nb_copy_buf, nb_find_byte, nb_report_error, nb_sync_push, nb_sync_shift_leftovers
from blinkview.ops.codec_adb_long import (
    nb_decode_adb_long_frame,
    nb_decode_frames_adb_long,
    nb_is_adb_long_header_monotonic,
)
from blinkview.ops.constants import (
    CHAR_COLON,
    CHAR_COMMA,
    CHAR_CR,
    CHAR_DOT,
    CHAR_ESC,
    CHAR_LBRACKET,
    CHAR_LF,
    CHAR_NINE,
    CHAR_RBRACKET,
    CHAR_RS,
    CHAR_SPACE,
    CHAR_TAB,
    CHAR_TILDE,
    CHAR_UPPER_A,
    CHAR_ZERO,
)
from blinkview.ops.discovery import nb_resolve_module_id
from blinkview.ops.modules import nb_normalize_name_inplace
from blinkview.ops.stage_loop import FS_DECODER_ERR, FS_DROP, FS_FRAME_ERR, FS_OK, nb_stage_loop_a1, nb_stage_loop_a2
from blinkview.ops.strings import nb_skip_whitespace
from blinkview.ops.timestamps import nb_project_synced_ns
from blinkview.ops.views import nb_log_bundle_views, nb_string_table_views, nb_sync_views, nb_tracker_views, nb_view
from blinkview.parsers.frame_decoders import FrameDecoderFactory
from blinkview.parsers.frame_parsers import FrameParserFactory, FrameSectionParserFactory
from blinkview.parsers.state import MAX_FRAMES_PER_CALL, FrameState

# A fixed clock: the sync state (and so every projected timestamp) is anchored to it
FIXED_NOW = 1_700_000_000_000_000_000
RX_START = FIXED_NOW - 10_000_000_000

ZEPHYR_DEFERRED = {
    "frame_parser": {
        "type": "default",
        "parser_errors_hidden": False,
        "steps": [
            {"type": "timestamp_zephyr_uptime_formatted"},
            {"type": "log_level_zephyr"},
            {"type": "module_name_normalizer", "max_depth": 8, "max_length": 64, "enable_dot_separator": True},
        ],
        "filter_squash_spaces": False,
    },
    "frame_decoder": {
        "type": "line_decoder",
        "frame_errors_hidden": False,
        "filter_trim_r": True,
        "filter_printable": True,
        "filter_ansi": True,
        "frame_length_dynamic": True,
        "frame_length": 0,
        "frame_length_minimum": 29,
        "frame_length_maximum": 1024,
    },
    "max_batch": 2000,
}

ADB_LONG = {
    "frame_parser": {
        "type": "default",
        "parser_errors_hidden": False,
        "steps": [
            {"type": "timestamp_adb_long_frame"},
            {"type": "process_pid_tid_adb_long_frame"},
            {"type": "log_level_adb_long_frame"},
            {"type": "module_name_adb_long_frame"},
        ],
        "filter_squash_spaces": False,
    },
    "frame_decoder": {
        "type": "decode_adb_long_frame",
        "frame_errors_hidden": False,
        "filter_trim_r": True,
        "filter_printable": True,
        "filter_ansi": True,
        "frame_length_dynamic": True,
        "frame_length": 0,
        "frame_length_minimum": 1,
        "frame_length_maximum": 4096,
    },
}

WORKLOADS = {"zephyr": (fpp.ZephyrDeferredParser, ZEPHYR_DEFERRED), "adb": (fpp.FramePipelineParser, ADB_LONG)}


# ---------------------------------------------------------------------------------------------------------
# Driving the real run() synchronously


class _ListQueue:
    """Hands run() the batches in order; once they are gone, stops the parser (run() then flushes and exits)."""

    def __init__(self, parser, batches):
        self._parser = parser
        self._batches = list(batches)

    def get(self, timeout=None):
        if self._batches:
            return self._batches.pop(0)
        self._parser._stop_event.set()
        return None


class _Capture:
    """Copies every output row out of the batches the parser distributes (they are released afterwards)."""

    def __init__(self, with_pid_tid):
        self.rows = []
        self.with_pid_tid = with_pid_tid
        self.module_unknown = None

    def put(self, batch):
        b = batch.bundle
        n = batch.size
        buf = b.buffer
        for i in range(n):
            off, ln = int(b.offsets[i]), int(b.lengths[i])
            module = int(b.modules[i])
            row = (
                bytes(buf[off : off + ln]),
                int(b.timestamps[i]),
                int(b.rx_timestamps[i]),
                int(b.levels[i]),
                module,
                int(b.devices[i]),
            )
            # pid/tid are pool memory unless the pid/tid step wrote them (it does not on error rows)
            if self.with_pid_tid and module != self.module_unknown:
                row += (int(b.pids[i]), int(b.tids[i]))
            self.rows.append(row)


class _RecordingLogger(PrintLogger):
    """Keeps what run() logs as a failure (it catches everything), so the test can fail on it."""

    __slots__ = ("failures",)

    def exception(self, fmt, *args, exc=None):
        self.failures.append(exc if exc is not None else fmt)


def _shared(id_registry):
    registry = FactoryRegistry()
    registry.register("frame_decoder", FrameDecoderFactory)
    registry.register("frame_parser", FrameParserFactory)
    registry.register("frame_section_parser", FrameSectionParserFactory)
    return SimpleNamespace(
        array_pool=NumpyArrayPool(), time_ns=lambda: FIXED_NOW, factories=registry, id_registry=id_registry
    )


def run_parser(workload, batches, max_frames=MAX_FRAMES_PER_CALL, decoder_kernel=None, step_kernels=None):
    """Parses `batches` (a list of batches, each a list of input rows as bytes) with a freshly built parser of
    the given workload; returns every output row. `decoder_kernel` replaces the decoder's kernel;
    `step_kernels` maps a step's class name to `fn(step, octx, fctx, n)`, which replaces that step's kernel."""
    cls, config = WORKLOADS[workload]
    id_registry = IDRegistry(NumpyArrayPool())
    shared = _shared(id_registry)
    parser = cls()
    parser.logger = logger = _RecordingLogger("test.frame_parser_equivalence")
    logger.failures = []
    parser.shared = shared
    parser.local = SimpleNamespace(device_id=id_registry.get_device("equivalence"))
    parser.apply_config(parser.hydrate_config(dict(config)))
    if decoder_kernel is not None:
        parser._frame_codec._kernel = decoder_kernel
    for name, fn in (step_kernels or {}).items():
        steps = [s for s in parser._frame_parser.pipeline if type(s).__name__ == name]
        assert steps, name
        for step in steps:
            step.kernel = functools.partial(fn, step)

    capture = _Capture(with_pid_tid=workload == "adb")
    capture.module_unknown = parser.local.device_id.get_module("unknown").id
    parser.subscribe(capture)

    pool = shared.array_pool
    rx = RX_START
    in_batches = []
    for rows in batches:
        batch = pool.create(PooledLogBatch, max(1, len(rows)), max(1, sum(len(r) for r in rows)))
        for row in rows:
            rx += 1_000_003
            assert batch.insert(rx, rx, row)
        in_batches.append(batch)
    parser.input_queue = _ListQueue(parser, in_batches)

    original_frame_state = fpp.FrameState
    fpp.FrameState = functools.partial(FrameState, max_frames=max_frames)
    try:
        parser.run()
    finally:
        fpp.FrameState = original_frame_state
    assert not logger.failures, logger.failures
    # module ids are handed out in order of discovery, so two runs could agree on them and still disagree on
    # the names: compare the names too
    names = {m: id_registry.module_from_int(m).name for m in {r[4] for r in capture.rows}}
    return [r + (names[r[4]],) for r in capture.rows]


def cut(raw, rng, max_row=2048, max_rows_per_batch=40):
    """Cuts a byte stream into input rows of random size (now and then a big one) and groups them into
    batches of a random number of rows."""
    rows = []
    pos = 0
    while pos < len(raw):
        n = rng.randint(1, max_row) if rng.random() < 0.9 else rng.randint(max_row, 16 * max_row)
        rows.append(raw[pos : pos + n])
        pos += n
    batches = []
    i = 0
    while i < len(rows):
        k = rng.randint(1, max_rows_per_batch)
        batches.append(rows[i : i + k])
        i += k
    return batches


# ---------------------------------------------------------------------------------------------------------
# Synthetic input

_LETTERS = "abcdefghijklmnopqrstuvwxyz"


def _filler(rng, n):
    words = []
    size = 0
    while size < n:
        w = "".join(rng.choice(_LETTERS) for _ in range(rng.randint(1, 9)))
        if rng.random() < 0.15:
            w = "%s=%d" % (w, rng.randint(0, 99999))
        words.append(w)
        size += len(w) + 1
    return " ".join(words)[:n]


def _zephyr_modules(rng):
    names = ["".join(rng.choice(_LETTERS + "_") for _ in range(rng.randint(3, 15))) for _ in range(24)]
    names += [rng.choice(_LETTERS) * 2 + "." + "".join(rng.choice(_LETTERS) for _ in range(5)) for _ in range(8)]
    names += ["a.b.c.d.e.f.g.h.i.j.k", "x" * 70, "net.l2." + "y" * 60]  # deeper / longer than the step allows
    return names


def zephyr_line(rng, t_us, modules):
    """One line of `[hh:mm:ss.mmm,uuu] <lvl> module: message`, now and then broken in one of the ways the
    decoder and the steps have to handle."""
    h, rem = divmod(t_us, 3_600_000_000)
    m, rem = divmod(rem, 60_000_000)
    s, us = divmod(rem, 1_000_000)
    ts = "[%02d:%02d:%02d.%03d,%03d]" % (h, m, s, us // 1000, us % 1000)
    level = rng.choices(["inf", "dbg", "wrn", "err", "xyz"], [80, 9, 6, 4, 1])[0]
    msg = _filler(rng, rng.choice([5, 10, 18, 20, 22, 25, 32, 43, 55, 58, 61]))
    line = "%s <%s> %s: %s" % (ts, level, rng.choice(modules), msg)
    kind = rng.random()
    if kind < 0.08:
        line = "\x1b[1;31m" + line + "\x1b[0m"
    elif kind < 0.10:
        line = line[:30] + rng.choice(["\x07", "\t", "\x7f", "\xc3\xa9", "\x1b[K", "\x1b"]) + line[30:]
    elif kind < 0.115:
        line = "%s <inf> a:" % ts  # decodes to less than frame_length_minimum
    elif kind < 0.12:
        line = "%s <inf> big: %s" % (ts, _filler(rng, rng.randint(1030, 2500)))  # over frame_length_maximum
    elif kind < 0.13:
        line = "no timestamp on this line at all " + msg
    elif kind < 0.14:
        line = ""
    elif kind < 0.145:
        line = "%s <inf> %s" % (ts, msg)  # no module separator
    data = line.encode("latin-1") if any(ord(c) > 127 for c in line) else line.encode("ascii")
    return data + (b"\n" if rng.random() < 0.02 else b"\r\n")


def zephyr_stream(rng, n_lines):
    modules = _zephyr_modules(rng)
    t = 1_000_000
    out = []
    for _ in range(n_lines):
        t += rng.randint(0, 1500)
        out.append(zephyr_line(rng, t, modules))
    return b"".join(out)


def _adb_tags(rng):
    tags = [
        "".join(rng.choice(_LETTERS + "ABCDEFGHIJKLMNOPQRSTUVWXYZ_.") for _ in range(rng.randint(3, 28)))
        for _ in range(40)
    ]
    tags += ["vri: Settings", "VRI[NotificationShade]", "bt: ", "[BT]", "x"]  # the tag step's special cases
    return tags


def adb_frame(rng, t_ms, tags):
    """One `adb logcat -v long` frame (the header syntax of tests/test_stage_kernels.py's adb_long case), now
    and then a multi-line, oversized or broken one."""
    pid = rng.choice([1, 57, 345, 2680, 12345, 31000])
    tid = pid + rng.randint(0, 300)
    level = rng.choices(["I", "D", "W", "E", "V", "F", "Q"], [50, 30, 10, 5, 2, 1, 1])[0]
    # logcat right-aligns the seconds in 5 columns: padded on a device up for under 10000 s (the stream's
    # clock starts past that, so a padded header gets a small seconds value)
    padded = rng.random() < 0.3
    header = ("[ " + ("%5d" if padded else "%d") + ".%03d  %d: %d %s/%s ]") % (
        (t_ms // 1000) % 1000 if padded else t_ms // 1000,
        t_ms % 1000,
        pid,
        tid,
        level,
        rng.choice(tags),
    )
    n = rng.choice([1, 20, 36, 56, 80, 101, 144, 200]) if rng.random() < 0.97 else rng.randint(300, 3900)
    msg = _filler(rng, n)
    kind = rng.random()
    if kind < 0.06:
        lines = [_filler(rng, rng.randint(30, 90)) for _ in range(rng.randint(2, 12))]
        msg = "\n".join(lines[:1] + ["\tat " + ln.replace(" ", ".") for ln in lines[1:]])
    elif kind < 0.08:
        msg = "\x1b[31m" + msg + "\x1b[0m"
    elif kind < 0.10:
        msg = msg.replace(" ", "\r\n", 2)
    elif kind < 0.11:
        msg = msg[:10] + "\x01\x7f\x1b" + msg[10:]
    elif kind < 0.115:
        msg = _filler(rng, rng.randint(4100, 6000))  # over frame_length_maximum
    elif kind < 0.12:
        msg = msg + "\n[ 9 looks like a header line"
    elif kind < 0.125:
        header = "[ %d.%03d  %d: %d %s/no closing bracket" % (t_ms // 1000, t_ms % 1000, pid, tid, level)
    elif kind < 0.13:
        msg = ""
    elif kind < 0.135:
        header = "[ 14.000  1 :2 I/X ]"
    elif kind < 0.14:
        msg = msg + "\n\n\n"
    elif kind < 0.142:
        header = "[ 1\n" + header  # a short line that starts like a header
    frame = header + "\n" + msg + "\n\n"
    return frame.encode("ascii")


def adb_stream(rng, n_frames):
    tags = _adb_tags(rng)
    t = 123_456_789
    out = [b"garbage before the first header\n"]
    for _ in range(n_frames):
        t += rng.randint(0, 5)
        out.append(adb_frame(rng, t, tags))
    return b"".join(out)


STREAMS = {"zephyr": zephyr_stream, "adb": adb_stream}


# ---------------------------------------------------------------------------------------------------------
# Tests


@pytest.mark.parametrize("workload", ["zephyr", "adb"])
@pytest.mark.parametrize("seed", [1, 2, 3])
def test_frame_budget_does_not_change_the_output(workload, seed):
    rng = random.Random(seed)
    raw = STREAMS[workload](rng, 6000)
    batches = cut(raw, rng)

    expected = run_parser(workload, batches, max_frames=MAX_FRAMES_PER_CALL)
    assert len(expected) > 4000
    for max_frames in (1, 7, 64, 1000):
        assert run_parser(workload, batches, max_frames=max_frames) == expected, max_frames


@pytest.mark.parametrize("workload", ["zephyr", "adb"])
def test_one_batch_over_many_calls_matches_one_call(workload):
    """A batch far bigger than one call's frame budget comes out the same as when one call takes it all."""
    rng = random.Random(7)
    raw = STREAMS[workload](rng, 30_000)
    batches = [[raw[i : i + 65536] for i in range(0, len(raw), 65536)]]

    assert run_parser(workload, batches, max_frames=8192) == run_parser(workload, batches, max_frames=1 << 18)


def _frames_batch(workload, count, first=True):
    """Input rows that yield exactly `count` frames from the decoder: (in the first batch) a line the zephyr
    decoder's resync drops, then `count` lines; or for adb, the rest of the pending frame (in a later batch),
    then `count` frames whose last one is cut by the header after it (an adb frame ends where the next
    header starts)."""
    rng = random.Random(count)
    if workload == "zephyr":
        lines = [b"resync\n"] if first else []
        lines += [b"[00:00:01.%03d,000] <inf> main: line %d\r\n" % (i % 1000, i) for i in range(count)]
    else:
        lines = [] if first else [b"pending message\n\n"]
        n = count if first else count - 1
        lines += [b"[ %d.000  1: 2 I/Tag ]\nmessage %d\n\n" % (100 + i, i) for i in range(n)]
        lines.append(b"[ 999.000  3: 4 W/Next ]\n")
    data = b"".join(lines)
    rows = []
    pos = 0
    while pos < len(data):
        n = rng.randint(1, 4096)
        rows.append(data[pos : pos + n])
        pos += n
    return rows


@pytest.mark.parametrize("workload", ["zephyr", "adb"])
@pytest.mark.parametrize("extra", [0, 1])
def test_a_batch_of_exactly_max_frames_frames_and_one_more(workload, extra):
    max_frames = 64
    rows = _frames_batch(workload, max_frames + extra)
    tail = _frames_batch(workload, 3, first=False)  # a second batch after the re-call

    expected = run_parser(workload, [rows, tail], max_frames=MAX_FRAMES_PER_CALL)
    got = run_parser(workload, [rows, tail], max_frames=max_frames)

    assert len(expected) == max_frames + extra + 3
    assert got == expected


@pytest.mark.parametrize("seed", [1, 2, 3, 4])
def test_adb_frame_decoder_matches_the_reference_copy(seed):
    rng = random.Random(seed)
    batches = cut(adb_stream(rng, 6000), rng)

    expected = run_parser("adb", batches, decoder_kernel=nb_ref_decode_frames_adb_long)
    assert len(expected) > 4000
    assert run_parser("adb", batches) == expected


# Lines that start like a header ('[', '[ ', '[ 1') and are shorter than the header test's 5 bytes, so whether
# the line feed before them starts a header is only known once more bytes arrive.
_SHORT_LINES = [b"", b"[", b"[ ", b"[ 1", b"[ 12", b"[ x", b"x", b" [ 1", b"[ 1.5 ]", b"\r", b"[ 1\r"]
# ...and their padded forms (logcat right-aligns the seconds), up to and past the padding the header test takes
_SHORT_LINES += [b"[  ", b"[    ", b"[     1", b"[      1", b"[       1", b"[        1", b"[    x", b"[   \r"]


def short_line_stream(rng, n_lines):
    """adb frames whose bodies are mostly lines of 0-4 bytes that look like the start of a header."""
    out = []
    for i in range(n_lines):
        if rng.random() < 0.25:
            seconds = b"%5d" % (i % 100) if rng.random() < 0.5 else b"%d" % i  # padded as on a fresh boot
            out.append(b"[ %s.%03d  %d: %d I/Tag%d ]" % (seconds, i % 1000, 1 + i % 7, 2 + i % 5, i % 9))
        else:
            out.append(rng.choice(_SHORT_LINES))
    return b"\n".join(out) + b"\n"


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_adb_decoder_resumed_scan_matches_a_full_scan(seed):
    """Grows a pending frame line by line as the decode loop does: after an INCOMPLETE answer the decoder gets
    the frame again with one more line and seen_end set to the old end. Every answer and every decoded byte
    must match the full scan (seen_end = start) and the frozen reference."""
    rng = random.Random(seed)
    data = short_line_stream(rng, 4000)
    lines = data.split(b"\n")[:-1]
    f_cfg = _adb_frame_config()
    out_new = np.zeros(8192, dtype=dtypes.BYTE)
    out_full = np.zeros(8192, dtype=dtypes.BYTE)
    out_ref = np.zeros(8192, dtype=dtypes.BYTE)

    pending = b""
    seen = 0
    completes = 0
    resumed = 0
    for line in lines:
        pending += line + b"\n"
        buf = np.frombuffer(pending, dtype=dtypes.BYTE).copy()
        new = nb_decode_adb_long_frame(buf, 0, len(buf), seen, out_new, 0, f_cfg)
        full = nb_decode_adb_long_frame(buf, 0, len(buf), 0, out_full, 0, f_cfg)
        ref = nb_ref_decode_adb_long_frame(buf, 0, len(buf), out_ref, 0, f_cfg)
        assert new == full == ref, (pending, seen)
        assert bytes(out_new[: new[1]]) == bytes(out_full[: full[1]]) == bytes(out_ref[: ref[1]])
        resumed += seen > 0
        if new[0] == STATE_INCOMPLETE:
            seen = len(buf)
        else:
            completes += 1
            pending = pending[new[2] :]
            seen = 0
    assert completes > 500 and resumed > 1000


def _adb_frame_config():
    """The adb workload's FrameDecoderParams, as the parser builds them."""
    cls, config = WORKLOADS["adb"]
    parser = cls()
    parser.logger = PrintLogger("test.frame_parser_equivalence")
    parser.shared = _shared(IDRegistry(NumpyArrayPool()))
    parser.local = SimpleNamespace(device_id=parser.shared.id_registry.get_device("cfg"))
    parser.apply_config(parser.hydrate_config(dict(config)))
    return parser._frame_codec._bundle


@pytest.mark.parametrize("seed", [1, 2])
def test_adb_short_line_stream_matches_the_reference_copy(seed):
    """The same short-line stream through the real run(), cut into rows and batches, with tiny frame budgets
    so pending frames cross decoder calls."""
    rng = random.Random(seed)
    batches = cut(short_line_stream(rng, 20_000), rng, max_row=64)

    expected = run_parser("adb", batches, decoder_kernel=nb_ref_decode_frames_adb_long)
    assert len(expected) > 3000
    for max_frames in (1, 7, MAX_FRAMES_PER_CALL):
        assert run_parser("adb", batches, max_frames=max_frames) == expected, max_frames


def test_a_pending_frame_that_never_ends_does_not_stall_the_parser():
    """An adb frame ends where the next header starts. An emulator's padded headers ("[     4.560...") used to
    match no header, so the pending frame grew to frame_length_maximum; with an output buffer no bigger than
    that (an untrained tuner), every decoder call then said "full" with no frame decoded and run() spun at
    100% CPU forever. The over-long frame must be dropped and the parser carry on."""
    data = b"".join(b"a line with no header after it %05d\n" % i for i in range(2000))  # ~74 kB
    data += b"[ 1.000  1: 1 I/Tag ]\nfirst\n\n[ 2.000  1: 1 I/Tag ]\nsecond\n\n[ 3.000  1: 1 I/Tag ]\n"
    rows = [data[i : i + 64] for i in range(0, len(data), 64)]
    batches = [rows[i : i + 16] for i in range(0, len(rows), 16)]

    calls = 0

    def bounded_kernel(*args):
        nonlocal calls
        calls += 1
        if calls > 50_000:  # the input is ~1200 rows: a stalled loop runs into this, not into a hang
            raise RuntimeError("the decoder keeps being called without consuming the input")
        return nb_decode_frames_adb_long(*args)

    out = run_parser("adb", batches, decoder_kernel=bounded_kernel)

    assert [r[0] for r in out if r[-1] == "tag"] == [b"first", b"second"]  # the tag step lowercases names


_TAG_CHARS = "abcXYZ019 ._-:[]/\t\x01\x7f\xe9"


def tag_stream(rng, n_frames):
    """adb frames whose tags are drawn from a pool of odd ones: separators at either end, runs of them,
    spaces, ':' and '[' (the tag step's metadata cases), ']' and stray bytes. The header is not filtered."""
    pool = ["".join(rng.choice(_TAG_CHARS) for _ in range(rng.randint(0, 24))) for _ in range(300)]
    pool += ["a..b", "__x__", ".lead", "trail.", "A.B_C", "x [y]", "x:y", "x: [y]", "x:", " ", ""]
    out = []
    for i in range(n_frames):
        tag = rng.choice(pool)
        out.append("[ %d.%03d  %d: %d I/%s ]\nmessage %d\n\n" % (i, i % 1000, 1 + i % 7, 2 + i % 5, tag, i))
    return "".join(out).encode("latin-1")


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_adb_tag_step_matches_the_reference_copy(seed):
    rng = random.Random(seed)
    batches = cut(tag_stream(rng, 8000), rng)
    ref = {"AdbModuleName": ref_adb_tag_kernel}

    expected = run_parser("adb", batches, step_kernels=ref)
    assert len(expected) > 7000
    assert run_parser("adb", batches) == expected

    rng = random.Random(seed)
    batches = cut(adb_stream(rng, 6000), rng)
    assert run_parser("adb", batches) == run_parser("adb", batches, step_kernels=ref)


_NOISE = ["\x1b[1;31m", "\x1b[0m", "\x1b[K", "\x1b", "\x1b[", "\x1b[12", "\x07", "\t", "\x7f", "\xc3\xa9", "\x00", "\r"]


def noisy_zephyr_stream(rng, n_lines):
    """Zephyr lines with ANSI sequences (whole, cut short or left open at the end of the line), control and
    high bytes at random places, including the first and last byte of the line."""
    modules = _zephyr_modules(rng)
    out = []
    for i in range(n_lines):
        line = zephyr_line(rng, 1_000_000 + i * 700, modules).decode("latin-1")
        body, nl = line.rstrip("\r\n"), line[len(line.rstrip("\r\n")) :]
        for _ in range(rng.choice([0, 0, 1, 2, 5])):
            k = rng.randint(0, len(body))
            body = body[:k] + rng.choice(_NOISE) + body[k:]
        out.append(body + nl)
    return "".join(out).encode("latin-1")


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_line_decoder_matches_the_reference_copy(seed):
    rng = random.Random(seed)
    batches = cut(noisy_zephyr_stream(rng, 8000), rng)
    expected = run_parser("zephyr", batches, decoder_kernel=nb_ref_decode_frames_newline)
    assert len(expected) > 7000
    assert run_parser("zephyr", batches) == expected

    rng = random.Random(seed)
    batches = cut(zephyr_stream(rng, 6000), rng)
    assert run_parser("zephyr", batches) == run_parser("zephyr", batches, decoder_kernel=nb_ref_decode_frames_newline)


def odd_timestamp(rng, t_us):
    """A Zephyr uptime timestamp, now and then off the '[hh:mm:ss.mmm,uuu]' layout in one place: a digit
    replaced by another byte, a field one digit shorter or longer, a separator swapped or missing, a ']' early,
    a space, or more hours than two digits hold."""
    h, rem = divmod(t_us, 3_600_000_000)
    m, rem = divmod(rem, 60_000_000)
    s, us = divmod(rem, 1_000_000)
    ts = "[%02d:%02d:%02d.%03d,%03d]" % (h, m, s, us // 1000, us % 1000)
    kind = rng.random()
    if kind < 0.5:
        return ts
    if kind < 0.6:
        return "[%d:%02d:%02d.%03d,%03d]" % (h + rng.choice([100, 1000]), m, s, us // 1000, us % 1000)
    k = rng.randint(1, len(ts) - 1)
    if kind < 0.75:
        return ts[:k] + rng.choice("0123456789:.,] /a\x00") + ts[k + 1 :]
    if kind < 0.85:
        return ts[:k] + ts[k + 1 :]
    return ts[:k] + rng.choice("0123456789:.,] ") + ts[k:]


def odd_timestamp_stream(rng, n_lines):
    modules = _zephyr_modules(rng)[:20]
    out = []
    for i in range(n_lines):
        t_us = 1_000_000 + i * 700 + rng.choice([0, 0, 3_600_000_000 * 20, 3_600_000_000 * 99])
        out.append("%s <inf> %s: message %d\r\n" % (odd_timestamp(rng, t_us), rng.choice(modules), i))
    return "".join(out).encode("latin-1")


@pytest.mark.parametrize("seed", [1, 2, 3])
def test_zephyr_timestamp_step_matches_the_reference_copy(seed):
    ref = {"ZephyrUptimeFormattedParser": ref_zephyr_uptime_kernel}
    rng = random.Random(seed)
    batches = cut(odd_timestamp_stream(rng, 8000), rng)
    expected = run_parser("zephyr", batches, step_kernels=ref)
    assert len(expected) > 7000
    assert run_parser("zephyr", batches) == expected

    rng = random.Random(seed)
    batches = cut(zephyr_stream(rng, 6000), rng)
    assert run_parser("zephyr", batches) == run_parser("zephyr", batches, step_kernels=ref)


def test_the_streams_exercise_the_edge_cases():
    """Guards against the generators drifting into only producing clean frames."""
    rng = random.Random(1)
    rows = run_parser("zephyr", cut(zephyr_stream(rng, 6000), rng))
    levels = {r[3] for r in rows}
    assert len(levels) >= 4  # inf/dbg/wrn/err (and error rows)
    assert any(len(r[0]) > 0 and r[0].startswith(b"[") for r in rows)  # frame / parser error rows keep the raw text

    rng = random.Random(1)
    rows = run_parser("adb", cut(adb_stream(rng, 6000), rng))
    assert any(b"\x1e" in r[0] for r in rows)  # multi-line messages (line feeds become RS)
    assert len({r[6] for r in rows if len(r) > 7}) >= 5  # varied pids (rows with pid/tid have 9 columns)


# ---------------------------------------------------------------------------------------------------------
# Frozen reference copy of the decode loop, before it decoded multi-chunk frames in place (it copied every
# chunk of a pending frame to f_buf and shifted the unconsumed tail there) and before seen_end. Every reference
# decoder kernel below runs through it. Do not optimise it either.


@app_njit(inline="always")
def nb_ref_decode_loop(decode_frame, f_cfg, f_state: FrameStateParams, in_b0, p_cfg, o_cfg, out_b0):
    """nb_decode_loop before the frames of a row were decoded in place and before seen_end."""
    # Meminfo-free views of the arrays used per frame (see ops/views.py); they never leave this call.
    in_b = nb_log_bundle_views(in_b0)
    out_b = nb_log_bundle_views(out_b0)
    curr_write = f_state.offset[0]
    in_idx = f_state.in_idx[0]
    read_offset = f_state.in_offset[0]
    in_frame = f_state.in_frame[0]
    f_buf = nb_view(f_state.buffer)

    in_size = in_b.size[0]

    frame_delimiter = f_cfg.delimiter
    frame_length_min = f_cfg.length_min
    frame_length_max = f_cfg.length_max
    frame_length_fixed = f_cfg.length_fixed
    frame_length = f_cfg.length
    report_frame_error = f_cfg.report_error
    is_pre_framed = f_cfg.decode_id == 0

    report_parser_error = p_cfg.report_error
    default_level = p_cfg.level_default
    default_module = p_cfg.module_log

    start_out_idx = out_b.size[0]
    curr_out_idx = start_out_idx  # rows reserved so far (one per decoded frame)
    start_out_cursor = out_b.msg_cursor[0]
    curr_out_cursor = start_out_cursor  # uncompacted decode position

    out_cap = out_b.timestamps.shape[0]
    out_buf_cap = out_b.buffer.shape[0]
    report_errors = report_frame_error or report_parser_error
    out_full = False

    # per-frame bookkeeping (preallocated in FrameState), indexed by frame ordinal k = row slot - start_out_idx.
    # Its length caps the rows this call may reserve, so it doubles as an output-capacity limit.
    fstart = f_state.fstart
    fcur = f_state.fcur
    fend = f_state.fend
    ftotal = f_state.ftotal
    fstatus = f_state.fstatus
    out_cap = min(out_cap, start_out_idx + fstatus.shape[0])
    nframes = 0

    # ---------------- Phase A: decode frames, reserve a row slot for each ----------------
    while in_idx < in_size:
        in_len = in_b.lengths[in_idx]
        in_off = in_b.offsets[in_idx]
        ts_in = in_b.timestamps[in_idx]

        scan_idx = read_offset

        while scan_idx < in_len:
            found_frame = False
            chunk_len = 0

            if is_pre_framed:
                chunk_len = in_len
                scan_idx = in_len - 1
                found_frame = True
            else:
                # jump straight to the next delimiter (or the end of this input row)
                scan_idx = nb_find_byte(in_b.buffer, in_off + scan_idx, in_off + in_len, frame_delimiter) - in_off
                if scan_idx < in_len:
                    chunk_len = (scan_idx - read_offset) + 1
                    found_frame = True

            if found_frame:
                if in_frame:
                    if (curr_out_idx >= out_cap) or (curr_out_cursor + curr_write + chunk_len > out_buf_cap):
                        out_full = True
                        break

                target_buf = f_buf
                target_start = 0
                target_end = 0
                process_frame = False
                is_zero_copy = False

                if in_frame and curr_write == 0 and chunk_len <= frame_length_max:
                    target_buf = in_b.buffer
                    target_start = in_off + read_offset
                    target_end = target_start + chunk_len
                    process_frame = True
                    is_zero_copy = True
                else:
                    if in_frame:
                        if curr_write + chunk_len <= frame_length_max:
                            src_view = in_b.buffer[in_off + read_offset : in_off + read_offset + chunk_len]
                            nb_sync_push(f_buf, curr_write, src_view, chunk_len)

                            curr_write += chunk_len
                            target_buf = f_buf
                            target_start = 0
                            target_end = curr_write
                            process_frame = True
                        else:
                            # oversized: drop it; its delimiter was just found, so we are still on a boundary
                            curr_write = 0
                    else:
                        in_frame = True
                        curr_write = 0

                if process_frame:
                    if is_pre_framed:
                        nb_copy_buf(target_buf, target_start, out_b.buffer, curr_out_cursor, chunk_len)
                        final_cursor = curr_out_cursor + chunk_len
                        bytes_consumed = chunk_len
                        decoder_state = STATE_COMPLETE
                    else:
                        decoder_state, final_cursor, bytes_consumed = decode_frame(
                            target_buf, target_start, target_end, out_b.buffer, curr_out_cursor, f_cfg
                        )

                    if decoder_state == STATE_INCOMPLETE:
                        if is_zero_copy:
                            src_view = in_b.buffer[in_off + read_offset : in_off + scan_idx + 1]
                            nb_sync_push(f_buf, 0, src_view, chunk_len)
                            curr_write = chunk_len

                        read_offset = scan_idx + 1
                        scan_idx += 1
                        continue

                    elif decoder_state == STATE_ERROR:
                        mangled_len = target_end - target_start
                        if report_errors and curr_out_idx < out_cap and mangled_len > 0:
                            # the raw bytes are not in the output buffer yet; nb_report_error copies them in
                            # place at curr_out_cursor and takes a row slot, so it doubles as a frame here
                            k = curr_out_idx - start_out_idx
                            new_cursor = nb_report_error(
                                out_b,
                                curr_out_idx,
                                curr_out_cursor,
                                target_buf,
                                target_start,
                                mangled_len,
                                p_cfg.level_error,
                                p_cfg.module_unknown,
                            )
                            fstart[k] = curr_out_cursor
                            ftotal[k] = new_cursor - curr_out_cursor  # nb_report_error may truncate
                            fstatus[k] = FS_DECODER_ERR  # already fully written
                            curr_out_cursor = new_cursor
                            curr_out_idx += 1
                            nframes += 1
                        bytes_consumed = target_end - target_start

                    else:
                        total_frame_length = final_cursor - curr_out_cursor
                        if total_frame_length > 0:
                            k = curr_out_idx - start_out_idx
                            out_b.rx_timestamps[curr_out_idx] = ts_in
                            out_b.timestamps[curr_out_idx] = ts_in
                            out_b.levels[curr_out_idx] = default_level
                            out_b.modules[curr_out_idx] = default_module
                            fstart[k] = curr_out_cursor
                            fcur[k] = curr_out_cursor
                            fend[k] = final_cursor
                            ftotal[k] = total_frame_length
                            valid = True
                            if frame_length_fixed != 0:
                                valid = total_frame_length == frame_length
                            else:
                                valid = total_frame_length >= frame_length_min
                            if valid:
                                fstatus[k] = FS_OK
                            elif report_frame_error:
                                fstatus[k] = FS_FRAME_ERR
                            else:
                                fstatus[k] = FS_DROP
                            curr_out_idx += 1
                            nframes += 1
                            curr_out_cursor = final_cursor

                    unconsumed = (target_end - target_start) - bytes_consumed
                    if unconsumed > 0:
                        nb_sync_shift_leftovers(f_buf, target_buf, target_start + bytes_consumed, unconsumed)
                        curr_write = unconsumed
                        in_frame = True
                    else:
                        curr_write = 0

                read_offset = scan_idx + 1

            scan_idx += 1

        if out_full:
            break

        remaining = in_len - read_offset
        if remaining > 0 and in_frame:
            if curr_write + remaining <= frame_length_max:
                src_view = in_b.buffer[in_off + read_offset : in_off + in_len]
                nb_sync_push(f_buf, curr_write, src_view, remaining)
                curr_write += remaining
            else:
                in_frame = False

        in_idx += 1
        read_offset = 0

    f_state.offset[0] = curr_write
    f_state.in_idx[0] = in_idx
    f_state.in_offset[0] = read_offset
    f_state.in_frame[0] = in_frame

    return out_full, nframes


# ---------------------------------------------------------------------------------------------------------
# Frozen reference copy of the adb long-frame decoder's frame function, before its fast paths. Do not
# optimise it: it is what the current one is compared against.


@app_njit(inline="always")
def nb_ref_decode_adb_long_frame(f_buf, start, end, out_buf, out_cursor, f_cfg):
    search_start = start + 2 if nb_is_adb_long_header_monotonic(f_buf, start, end) else start

    second_header_idx = -1
    for i in range(search_start, end):
        if f_buf[i] == CHAR_LF:
            if nb_is_adb_long_header_monotonic(f_buf, i + 1, end):
                second_header_idx = i + 1
                break

    if second_header_idx == -1:
        return STATE_INCOMPLETE, out_cursor, 0

    bytes_consumed = second_header_idx - start

    true_end = second_header_idx - 1
    while true_end > start and (f_buf[true_end - 1] == CHAR_LF or f_buf[true_end - 1] == CHAR_CR):
        true_end -= 1

    true_start = start
    while true_start < true_end and (
        f_buf[true_start] == CHAR_LF or f_buf[true_start] == CHAR_CR or f_buf[true_start] == CHAR_SPACE
    ):
        true_start += 1

    if true_start >= true_end:
        return STATE_COMPLETE, out_cursor, bytes_consumed

    cursor = out_cursor
    buf_cap = out_buf.shape[0]

    header_end = -1
    for i in range(true_start, true_end - 1):
        if f_buf[i] == CHAR_SPACE and f_buf[i + 1] == CHAR_RBRACKET:
            header_end = i + 1
            break

    if header_end != -1:
        if f_buf[true_start] != CHAR_LBRACKET:
            if cursor < buf_cap:
                out_buf[cursor] = CHAR_LBRACKET
                cursor += 1
        for i in range(true_start, header_end + 1):
            if cursor < buf_cap:
                out_buf[cursor] = f_buf[i]
                cursor += 1
        msg_ptr = header_end + 1
    else:
        msg_ptr = true_start

    if cursor < buf_cap:
        out_buf[cursor] = CHAR_SPACE
        cursor += 1

    while msg_ptr < true_end and (
        f_buf[msg_ptr] == CHAR_SPACE or f_buf[msg_ptr] == CHAR_CR or f_buf[msg_ptr] == CHAR_LF
    ):
        msg_ptr += 1

    f_ansi = f_cfg.filter_ansi
    f_print = f_cfg.filter_printable
    ansi_state = 0

    for i in range(msg_ptr, true_end):
        if cursor >= buf_cap:
            break
        val = f_buf[i]
        if f_ansi:
            if ansi_state == 0:
                if val == CHAR_ESC:
                    ansi_state = 1
                    continue
            elif ansi_state == 1:
                if val == CHAR_LBRACKET:
                    ansi_state = 2
                    continue
                else:
                    ansi_state = 0
            elif ansi_state == 2:
                if CHAR_UPPER_A <= val <= CHAR_TILDE:
                    ansi_state = 0
                continue
        if f_print:
            if not (CHAR_SPACE <= val <= CHAR_TILDE or val == CHAR_LF or val == CHAR_TAB):
                continue
        if val == CHAR_CR:
            continue
        if val == CHAR_LF:
            val = CHAR_RS
        out_buf[cursor] = val
        cursor += 1

    return STATE_COMPLETE, cursor, bytes_consumed


@app_njit()
def nb_ref_decode_frames_adb_long(f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0):
    return nb_ref_decode_loop(nb_ref_decode_adb_long_frame, f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0)


# ---------------------------------------------------------------------------------------------------------
# Frozen reference copy of the adb tag step (module_name_adb_long_frame), before its fast paths. Do not
# optimise it either.


@app_njit(inline="always")
def nb_ref_parse_adb_tag(buffer, start_cursor, end_cursor, out_b, out_idx, tracker, string_table):
    t_bytes = tracker.name_bytes
    write_pos = tracker.bytes_cursor[0]

    header_delimiter_idx = -1
    for i in range(start_cursor, end_cursor - 1):
        if buffer[i] == CHAR_SPACE and buffer[i + 1] == CHAR_RBRACKET:
            header_delimiter_idx = i
            break
    if header_delimiter_idx == -1:
        return -1

    actual_end = header_delimiter_idx
    meta_type = 0
    for i in range(start_cursor, header_delimiter_idx):
        val = buffer[i]
        if val == CHAR_COLON:
            actual_end = i
            meta_type = 1
            break
        elif val == CHAR_LBRACKET and i > start_cursor:
            actual_end = i
            meta_type = 2
            break

    tag_limit = actual_end
    if meta_type == 0:
        while tag_limit > start_cursor and buffer[tag_limit - 1] == CHAR_SPACE:
            tag_limit -= 1
    tag_len = tag_limit - start_cursor

    if tag_len > 0:
        if write_pos + tag_len > len(t_bytes):
            return -1
        t_bytes[write_pos : write_pos + tag_len] = buffer[start_cursor:tag_limit]
        squashed_len = int(nb_normalize_name_inplace(t_bytes, write_pos, tag_len))
        if squashed_len > 0:
            t_bytes[write_pos + squashed_len] = 0
            mod_id = nb_resolve_module_id(t_bytes, write_pos, squashed_len, string_table, tracker)
            if mod_id == -1:
                return -1
            out_b.modules[out_idx] = mod_id
        else:
            out_b.modules[out_idx] = 0
    else:
        out_b.modules[out_idx] = 0

    if meta_type == 1:
        peek_idx = actual_end + 1
        while peek_idx < header_delimiter_idx and buffer[peek_idx] == CHAR_SPACE:
            peek_idx += 1
        if peek_idx == header_delimiter_idx:
            return nb_skip_whitespace(buffer, header_delimiter_idx + 2, end_cursor)
        if buffer[peek_idx] == CHAR_LBRACKET:
            return nb_skip_whitespace(buffer, peek_idx, end_cursor)
        buffer[actual_end] = CHAR_LBRACKET
        return nb_skip_whitespace(buffer, actual_end, end_cursor)
    elif meta_type == 2:
        prefix_len = header_delimiter_idx - actual_end
        shift_dist = 2
        for i in range(prefix_len - 1, -1, -1):
            buffer[actual_end + shift_dist + i] = buffer[actual_end + i]
        return nb_skip_whitespace(buffer, actual_end + shift_dist, end_cursor)
    return nb_skip_whitespace(buffer, header_delimiter_idx + 2, end_cursor)


@app_njit()
def nb_ref_parse_adb_tag_stage(out_b0, f_state, n, tracker0, table0):
    nb_stage_loop_a2(
        nb_ref_parse_adb_tag, out_b0, f_state, n, nb_tracker_views(tracker0), nb_string_table_views(table0)
    )


def ref_adb_tag_kernel(step, octx, fctx, n):
    nb_ref_parse_adb_tag_stage(octx, fctx, n, step.tracker_state, step.table_bundle())


# ---------------------------------------------------------------------------------------------------------
# Frozen reference copy of the line decoder's frame function (workload A), before its fast paths. Do not
# optimise it either.


@app_njit(inline="always")
def nb_ref_decode_newline_frame(f_buf, start, end, out_buf, out_cursor, f_cfg):
    bytes_consumed = end - start
    true_end = end - 1
    if f_cfg.filter_trim_r and true_end > start and f_buf[true_end - 1] == 13:
        true_end -= 1

    buf_cap = out_buf.shape[0]
    cursor = out_cursor
    filter_ansi = f_cfg.filter_ansi
    filter_printable = f_cfg.filter_printable

    available = buf_cap - cursor
    frame_len = true_end - start
    process_len = frame_len if frame_len < available else available
    if process_len <= 0:
        return STATE_COMPLETE, cursor, bytes_consumed

    ansi_state = 0
    for i in range(start, start + process_len):
        val = f_buf[i]
        if filter_ansi:
            if ansi_state == 0:
                if val == 27:
                    ansi_state = 1
                    continue
            elif ansi_state == 1:
                if val == 91:
                    ansi_state = 2
                    continue
                else:
                    ansi_state = 0
            elif ansi_state == 2:
                if 64 <= val <= 126:
                    ansi_state = 0
                continue
        if filter_printable:
            if not (32 <= val <= 126):
                continue
        out_buf[cursor] = val
        cursor += 1

    return STATE_COMPLETE, cursor, bytes_consumed


@app_njit()
def nb_ref_decode_frames_newline(f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0):
    return nb_ref_decode_loop(nb_ref_decode_newline_frame, f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0)


# ---------------------------------------------------------------------------------------------------------
# Frozen reference copy of the Zephyr uptime timestamp step (timestamp_zephyr_uptime_formatted), before its
# fast path. Do not optimise it either.


@app_njit(inline="always")
def nb_ref_parse_zephyr_uptime_formatted(buffer, start_cursor, end_cursor, out_b, out_idx, sync):
    if start_cursor + 10 > end_cursor or buffer[start_cursor] != CHAR_LBRACKET:
        return -1

    ts_end = -1
    for i in range(start_cursor + 1, end_cursor):
        if buffer[i] == CHAR_RBRACKET:
            ts_end = i
            break
    if ts_end == -1:
        return -1

    raw_ns = 0
    current_val = 0
    parse_state = 0
    for i in range(start_cursor + 1, ts_end):
        c = buffer[i]
        if CHAR_ZERO <= c <= CHAR_NINE:
            current_val = current_val * 10 + (c - CHAR_ZERO)
        elif c == CHAR_COLON:
            if parse_state == 0:
                raw_ns += current_val * 3_600_000_000_000
                parse_state = 1
            elif parse_state == 1:
                raw_ns += current_val * 60_000_000_000
                parse_state = 2
            current_val = 0
        elif c == CHAR_DOT:
            raw_ns += current_val * 1_000_000_000
            parse_state = 3
            current_val = 0
        elif c == CHAR_COMMA:
            raw_ns += current_val * 1_000_000
            parse_state = 4
            current_val = 0
        elif c == CHAR_SPACE:
            continue

    if parse_state == 4:
        raw_ns += current_val * 1_000
    else:
        return -1

    rx_ns = out_b.rx_timestamps[out_idx]
    out_b.timestamps[out_idx] = nb_project_synced_ns(raw_ns, rx_ns, sync)
    return nb_skip_whitespace(buffer, ts_end + 1, end_cursor)


@app_njit()
def nb_ref_parse_zephyr_uptime_formatted_stage(out_b0, f_state, n, sync0):
    nb_stage_loop_a1(nb_ref_parse_zephyr_uptime_formatted, out_b0, f_state, n, nb_sync_views(sync0))


def ref_zephyr_uptime_kernel(step, octx, fctx, n):
    nb_ref_parse_zephyr_uptime_formatted_stage(octx, fctx, n, step.sync_state)

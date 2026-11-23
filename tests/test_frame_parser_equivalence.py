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
  file, taken before its fast paths (word-at-a-time scans, memmove copies) were added.

The text is synthesized; only its shape follows the real formats.
"""

import functools
import random
from types import SimpleNamespace

import pytest

import blinkview.parsers.frame_pipeline_parser as fpp
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.id_registry import IDRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.numba_config import app_njit
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.parsing import STATE_COMPLETE, STATE_INCOMPLETE
from blinkview.ops.codec_adb_long import nb_is_adb_long_header_monotonic
from blinkview.ops.constants import (
    CHAR_CR,
    CHAR_ESC,
    CHAR_LBRACKET,
    CHAR_LF,
    CHAR_RBRACKET,
    CHAR_RS,
    CHAR_SPACE,
    CHAR_TAB,
    CHAR_TILDE,
    CHAR_UPPER_A,
)
from blinkview.ops.decode_loop import nb_decode_loop
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


def run_parser(workload, batches, max_frames=MAX_FRAMES_PER_CALL, decoder_kernel=None):
    """Parses `batches` (a list of batches, each a list of input rows as bytes) with a freshly built parser of
    the given workload; returns every output row. `decoder_kernel` replaces the decoder's kernel."""
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
    return capture.rows


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
    header = "[ %d.%03d  %d: %d %s/%s ]" % (t_ms // 1000, t_ms % 1000, pid, tid, level, rng.choice(tags))
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
    assert len({r[6] for r in rows if len(r) > 6}) >= 5  # varied pids


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
    return nb_decode_loop(nb_ref_decode_adb_long_frame, f_cfg, f_state, in_b0, p_cfg, o_cfg, out_b0)

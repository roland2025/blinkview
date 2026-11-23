# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The line caches the Benchmark source replays: its original "Random: %d" lines, or synthetic frames in a real
log format, so a profile can drive a real parser configuration at full rate.

Each template is a deterministic list of frames grouped into chunks of FRAMES_PER_CHUNK frames; the source counts
one sent message per frame. The text is synthesized; only its shape and length distribution follow real logs:

* "zephyr_deferred": `[hh:mm:ss.mmm,uuu] <lvl> module: message\\r\\n` lines for the `serial_zephyr_deferred`
  parser. Message lengths from one real RTT device, mean frame ~67 B, 10% of lines (wrn/err) ANSI-coloured.
* "adb_long": `adb logcat -v long` frames (`[ sec.ms  pid: tid L/Tag ]`, the message, a blank line) for the
  `decode_adb_long_frame` decoder with the adb_long_frame steps. Message lengths sampled from two real ADB
  sessions (mean ~93 B, median ~56, p99 ~850, max ~3950), some multi-line; mean frame ~138 B.

The device clock in a template advances slowly (20 ms over a Zephyr cache, 200 ms over an adb one) and starts
over every tick, so a parsed timestamp never jumps back by the second that the time projection would take for a
device reboot.
"""

import random
from typing import NamedTuple

import numpy as np

FRAMES_PER_CHUNK = 50
TEMPLATE_FRAMES = 20_000

TEMPLATES = ("random", "zephyr_deferred", "adb_long")


class BenchmarkCache(NamedTuple):
    buf: np.ndarray  # uint8: every chunk, back to back
    offsets: np.ndarray  # uint32: start of each chunk in buf
    lengths: np.ndarray  # uint32: bytes of each chunk
    frames_per_chunk: int  # messages the parser makes of one chunk


def build_cache(template: str) -> BenchmarkCache:
    """The chunk cache of a template (see the module docstring); each chunk is whole frames."""
    if template == "random":
        frames = [b"Random: %d\n" % i for i in range(1000)]
    elif template == "zephyr_deferred":
        frames = zephyr_deferred_frames(TEMPLATE_FRAMES)
    elif template == "adb_long":
        frames = adb_long_frames(TEMPLATE_FRAMES)
    else:
        raise ValueError(f"unknown benchmark template {template!r} (expected one of {', '.join(TEMPLATES)})")

    chunks = [b"".join(frames[i : i + FRAMES_PER_CHUNK]) for i in range(0, len(frames), FRAMES_PER_CHUNK)]
    lengths = np.array([len(c) for c in chunks], dtype=np.uint32)
    offsets = np.zeros(len(chunks), dtype=np.uint32)
    offsets[1:] = np.cumsum(lengths[:-1], dtype=np.uint64)
    return BenchmarkCache(np.frombuffer(b"".join(chunks), dtype=np.uint8), offsets, lengths, FRAMES_PER_CHUNK)


_LETTERS = "abcdefghijklmnopqrstuvwxyz"


def _vocabulary():
    r = random.Random(12345)
    words = [
        "".join(r.choice(_LETTERS) for _ in range(r.choice([2, 3, 3, 4, 4, 5, 5, 6, 7, 8, 9, 11]))) for _ in range(400)
    ]
    words += ["0x%04x" % r.randrange(65536) for _ in range(40)]
    words += ["%d" % r.randrange(100000) for _ in range(40)]
    words += ["%s=%d" % (r.choice(words[:400]), r.randrange(1000)) for _ in range(60)]
    return words


def _filler(rng, words, n):
    """`n` bytes of printable filler words, with no ':' or '[' (so no parser step sees a field in them)."""
    out = []
    size = 0
    while size < n:
        w = rng.choice(words)
        out.append(w)
        size += len(w) + 1
    s = " ".join(out)[:n]
    return s[:-1] + "x" if s.endswith(" ") else s


def _pick(rng, weighted):
    x = rng.random()
    acc = 0.0
    for value, weight in weighted:
        acc += weight
        if x < acc:
            return value
    return weighted[-1][0]


# Message lengths of 36 lines from one real RTT device
ZEPHYR_MSG_LENGTHS = (
    5,
    5,
    5,
    5,
    10,
    12,
    16,
    18,
    18,
    18,
    19,
    19,
    20,
    20,
    20,
    20,
    20,
    21,
    22,
    22,
    23,
    25,
    32,
    37,
    43,
    52,
    55,
    58,
    58,
    58,
    58,
    58,
    58,
    60,
    60,
    61,
)
ZEPHYR_LEVELS = (("inf", 0.82), ("dbg", 0.08), ("wrn", 0.06), ("err", 0.04))
ZEPHYR_COLOURS = {"wrn": "\x1b[1;33m", "err": "\x1b[1;31m"}


def _zephyr_modules(rng, count=36):
    names = set()
    while len(names) < count:
        if rng.random() < 0.3:
            a = "".join(rng.choice(_LETTERS) for _ in range(rng.randint(2, 4)))
            b = "".join(rng.choice(_LETTERS + "_") for _ in range(rng.randint(2, 6)))
            name = (a + "." + b)[:15]
        else:
            length = rng.choice([3, 3, 4, 4, 4, 5, 5, 6, 7, 9, 12, 15])
            name = "".join(rng.choice(_LETTERS + "_") for _ in range(length)).strip("_")
            if len(name) < 3:
                name += "abc"
        names.add(name)
    return sorted(names)


def zephyr_deferred_frames(count, seed=1):
    rng = random.Random(seed)
    words = _vocabulary()
    modules = _zephyr_modules(rng)
    frames = []
    t_us = 1_000_000
    for _ in range(count):
        t_us += 1
        h, rem = divmod(t_us, 3_600_000_000)
        m, rem = divmod(rem, 60_000_000)
        s, us = divmod(rem, 1_000_000)
        level = _pick(rng, ZEPHYR_LEVELS)
        line = "[%02d:%02d:%02d.%03d,%03d] <%s> %s: %s" % (
            h,
            m,
            s,
            us // 1000,
            us % 1000,
            level,
            rng.choice(modules),
            _filler(rng, words, rng.choice(ZEPHYR_MSG_LENGTHS)),
        )
        if level in ZEPHYR_COLOURS:
            line = ZEPHYR_COLOURS[level] + line + "\x1b[0m"
        frames.append((line + "\r\n").encode("ascii"))
    return frames


# 20-byte buckets 0-19, 20-39, ... 280-299, 300+ of message lengths from two real ADB sessions (20 K rows)
ADB_MSG_HISTOGRAM = (1363, 4416, 4843, 2888, 1429, 2163, 776, 395, 378, 146, 189, 112, 99, 84, 117, 600)
ADB_LEVELS = (("I", 0.50), ("D", 0.35), ("W", 0.10), ("E", 0.05))
_ADB_SYLLABLES = (
    "Act",
    "ivity",
    "Man",
    "ager",
    "Sys",
    "tem",
    "Serv",
    "ice",
    "Win",
    "dow",
    "Pow",
    "er",
    "Net",
    "work",
    "Blue",
    "tooth",
    "Wifi",
    "Ctrl",
    "Input",
    "Disp",
    "lay",
    "Audio",
    "Flin",
    "ger",
    "Surf",
    "ace",
    "Pack",
    "age",
    "Media",
    "Codec",
    "Gnss",
    "Hal",
    "Sensor",
    "Camera",
    "Batt",
    "ery",
    "Stat",
    "Job",
    "Sched",
    "uler",
    "Alarm",
    "Conn",
    "Telephony",
    "Radio",
    "Usb",
    "Vold",
    "Zygote",
    "Art",
)


def _adb_msg_len(rng):
    x = rng.random() * sum(ADB_MSG_HISTOGRAM)
    acc = 0
    for bucket, n in enumerate(ADB_MSG_HISTOGRAM):
        acc += n
        if x < acc:
            break
    if bucket < len(ADB_MSG_HISTOGRAM) - 1:
        return rng.randint(max(1, bucket * 20), bucket * 20 + 19)
    # 300+: 2/3 up to the p99 (849), 1/3 up to the longest (3947), both log-shaped
    if rng.random() < 2 / 3:
        return int(300 * (849 / 300) ** rng.random())
    return int(849 * (3947 / 849) ** (rng.random() ** 2.2))


def _adb_message(rng, words, n):
    """A message of about `n` bytes; long ones are often several lines, like a stack trace."""
    if n >= 300 and rng.random() < 0.6:
        lines = [_filler(rng, words, rng.randint(40, 90))]
        size = len(lines[0])
        while size < n:
            line = "\tat " + _filler(rng, words, rng.randint(30, 100)).replace(" ", ".")
            lines.append(line)
            size += len(line) + 1
        return "\n".join(lines)[:n].rstrip("\n") or "x"
    if n >= 100 and rng.random() < 0.05:
        half = n // 2
        return _filler(rng, words, half) + "\n" + _filler(rng, words, max(1, n - half - 1))
    return _filler(rng, words, n)


def adb_long_frames(count, seed=2):
    rng = random.Random(seed)
    words = _vocabulary()
    tags = set()
    while len(tags) < 120:
        target = max(3, min(32, int(rng.gauss(15, 5))))
        tag = ""
        while len(tag) < target:
            tag += rng.choice(_ADB_SYLLABLES)
        tags.add(tag[:target])
    tags = sorted(tags)
    tag_weights = [1.0 / (i + 1) ** 0.8 for i in range(len(tags))]
    procs = []
    for _ in range(40):
        digits = rng.choice([1, 2, 3, 3, 4, 4, 4, 5, 5])
        pid = rng.randint(10 ** (digits - 1), 10**digits - 1) if digits > 1 else rng.randint(1, 9)
        procs.append((pid, [pid] + [min(99999, pid + rng.randint(1, 400)) for _ in range(rng.randint(1, 8))]))

    frames = []
    for i in range(count):
        t_ms = 123_456_789 + i // 100
        pid, tids = rng.choice(procs)
        header = "[ %d.%03d  %d: %d %s/%s ]" % (
            t_ms // 1000,
            t_ms % 1000,
            pid,
            rng.choice(tids),
            _pick(rng, ADB_LEVELS),
            rng.choices(tags, tag_weights)[0],
        )
        frames.append((header + "\n" + _adb_message(rng, words, _adb_msg_len(rng)) + "\n\n").encode("ascii"))
    return frames

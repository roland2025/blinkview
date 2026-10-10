# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Manual benchmark of the raw hex dump kernel (ops/hex_dump.nb_hex_dump, behind the `raw_hex`
pipeline). Run it by hand; it is not part of the test suite:

    uv run scripts/bench_hex_dump.py

It answers what the dump's extras cost. nb_hex_dump writes, per row, an offset column, padded
hex columns, a text column and the row's metadata, and checks the row width, `split_on` and
`max_bytes` on the way. Next to it run:

* nb_bytes_to_hex, a kernel defined here that only turns bytes into "xx " triples in one flat
  buffer - the least a Numba kernel can do for the same bytes;
* bytes.hex(" "), CPython's own C implementation, as a reference outside Numba.

All three are given the same 1 MiB of random bytes. Throughput is input bytes per second, best
of several runs. With BLINKVIEW_DISABLE_NUMBA=1 both kernels run as plain Python and the numbers
only show how slow that is."""

import os
import time

import numpy as np

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.dtypes import BYTE
from blinkview.core.numba_config import app_njit
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.types.hex_dump import create_hex_dump_state
from blinkview.ops.hex_dump import HEX_DIGITS, hex_dump_line_length, nb_hex_dump

TOTAL_BYTES = 1 << 20
REPEATS = 15
NEWLINE = 10

# (label, bytes per read, bytes_per_row, split_on)
CASES = [
    ("4 KiB reads, 16/row", 4096, 16, -1),
    ("4 KiB reads,  8/row", 4096, 8, -1),
    ("4 KiB reads, 32/row", 4096, 32, -1),
    ("4 KiB reads, 16/row, split on \\n", 4096, 16, NEWLINE),
    ("64 B reads,  16/row", 64, 16, -1),
    ("7 B reads,   16/row", 7, 16, -1),
]


@app_njit()
def nb_bytes_to_hex(src, length, dst):
    """The dedicated kernel: `length` bytes of src as "xx " triples into dst."""
    for i in range(length):
        value = np.int64(src[i])
        dst[i * 3] = HEX_DIGITS[value >> 4]
        dst[i * 3 + 1] = HEX_DIGITS[value & 0xF]
        dst[i * 3 + 2] = 32
    return length * 3


def best_seconds(func, repeats: int = REPEATS) -> float:
    best = float("inf")
    for _ in range(repeats):
        start = time.perf_counter()
        func()
        best = min(best, time.perf_counter() - start)
    return best


def make_input(pool, data: bytes, read_size: int) -> PooledLogBatch:
    """`data` as a source hands it over: one input row per read."""
    reads = [data[i : i + read_size] for i in range(0, len(data), read_size)]
    batch = pool.create(PooledLogBatch, len(reads), len(data))
    for n, chunk in enumerate(reads):
        batch.insert(n, n, chunk)
    return batch


def bench_hex_dump(pool, data: bytes, read_size: int, bytes_per_row: int, split_on: int):
    """Returns (seconds, rows written, output bytes) for one pass of nb_hex_dump over `data`.
    The output batch holds the whole dump, so no flush is part of the measurement."""
    batch_in = make_input(pool, data, read_size)
    max_rows = len(data) if split_on >= 0 else batch_in.size * (-(-read_size // bytes_per_row))
    batch_out = pool.create(
        PooledLogBatch,
        max_rows,
        max_rows * hex_dump_line_length(bytes_per_row),
        has_levels=True,
        has_modules=True,
        has_devices=True,
    )
    state = create_hex_dump_state()
    in_bundle, out_bundle = batch_in.bundle, batch_out.bundle

    def run():
        out_bundle.size[0] = 0
        out_bundle.msg_cursor[0] = 0
        state.in_idx[0] = 0
        state.in_pos[0] = 0
        state.stream_offset[0] = 0
        full = nb_hex_dump(in_bundle, state, out_bundle, bytes_per_row, split_on, 0, 0, 1, 1)
        assert not full and state.stream_offset[0] == len(data)

    run()  # compiles, or loads the cached kernel
    seconds = best_seconds(run)
    rows, out_bytes = int(out_bundle.size[0]), int(out_bundle.msg_cursor[0])
    batch_in.release()
    batch_out.release()
    return seconds, rows, out_bytes


def main():
    data = os.urandom(TOTAL_BYTES)
    megabytes = TOTAL_BYTES / 1e6
    pool = NumpyArrayPool()

    src = np.frombuffer(bytearray(data), dtype=BYTE)
    dst = np.empty(TOTAL_BYTES * 3, dtype=BYTE)
    nb_bytes_to_hex(src, TOTAL_BYTES, dst)
    assert bytes(dst[:-1]).decode() == data.hex(" ")  # the dedicated kernel is right
    dedicated = best_seconds(lambda: nb_bytes_to_hex(src, TOTAL_BYTES, dst))
    builtin = best_seconds(lambda: data.hex(" "))

    print(f"{megabytes:.2f} MB of random bytes, best of {REPEATS} runs\n")
    print(f"{'':<46} | {'MB/s in':>8} | {'ns/byte':>7} | {'rows':>7} | {'out/in':>6} | {'vs dedicated':>12}")
    print("-" * 102)

    def line(label, seconds, rows="", ratio=""):
        slower = f"{seconds / dedicated:.1f}x slower" if seconds > dedicated * 1.05 else "-"
        print(
            f"{label:<46} | {megabytes / seconds:>8.0f} | {seconds / TOTAL_BYTES * 1e9:>7.2f} | {rows:>7} | "
            f"{ratio:>6} | {slower:>12}"
        )

    line("nb_bytes_to_hex (dedicated)", dedicated, ratio="3.0")
    line('bytes.hex(" ") (CPython)', builtin, ratio="3.0")
    for label, read_size, bytes_per_row, split_on in CASES:
        seconds, rows, out_bytes = bench_hex_dump(pool, data, read_size, bytes_per_row, split_on)
        line(f"nb_hex_dump, {label}", seconds, rows, f"{out_bytes / TOTAL_BYTES:.1f}")


if __name__ == "__main__":
    main()

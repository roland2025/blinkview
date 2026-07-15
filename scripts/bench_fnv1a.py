# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import time

import numpy as np

from blinkview.utils.fnv1a_64 import fnv1a_64_python, nb_fnv1a_64_fast

# Note: nb_fnv1a_64_fast is decorated with @app_njit(fallback=fnv1a_64_mv), so if this
# script is run with BLINKVIEW_DISABLE_NUMBA=1, the "numba" column below will actually be
# running fnv1a_64_mv too and both columns will show ~identical timing.

ITERATIONS = 100_000
BUFFER_SIZES = [1, 8, 16, 32, 256]


def time_calls(func, buffer, iterations: int) -> float:
    """Returns average nanoseconds per call."""
    length = len(buffer)
    start = time.perf_counter_ns()
    for _ in range(iterations):
        func(buffer, 0, length)
    end = time.perf_counter_ns()
    return (end - start) / iterations


def main():
    print(f"{'size':>6} | {'numba ns/call':>14} | {'mv ns/call':>11} | {'speedup':>8}")
    print("-" * 50)

    for size in BUFFER_SIZES:
        buffer = np.frombuffer(os.urandom(size), dtype=np.uint8)

        # Warm up the JIT so compile time isn't included in the timed loop.
        nb_fnv1a_64_fast(buffer, 0, size)

        numba_ns = time_calls(nb_fnv1a_64_fast, buffer, ITERATIONS)
        mv_ns = time_calls(fnv1a_64_python, buffer, ITERATIONS)
        speedup = mv_ns / numba_ns

        print(f"{size:>6} | {numba_ns:>14.1f} | {mv_ns:>11.1f} | {speedup:>7.1f}x")


if __name__ == "__main__":
    main()

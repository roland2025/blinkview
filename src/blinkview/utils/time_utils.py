# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import localtime, perf_counter_ns, strftime, time_ns
from typing import Callable


class TimeUtils:
    __slots__ = (
        "now",
        "now_ns",
        "_anchors",  # [time_start_ns, perf_start_ns]
    )

    def __init__(self):
        # Initial sync using nanosecond versions
        # [0] = Unix Wall Clock (ns), [1] = Monotonic Perf Clock (ns)
        self._anchors = [0, 0]
        self.resync()
        self.bake()

    def bake(self):
        """bake the time functions using nanosecond integer math"""
        # Capture the list reference and function into the closure
        anchors = self._anchors
        perf_fn = perf_counter_ns

        # ---Ultra-High Speed 'now' ---
        def fast_now() -> float:
            # (CurrentPerf - StartPerf) + StartWall = CurrentWall (in ns)
            # Dividing by 1e9 at the VERY end preserves max precision
            return (anchors[0] + (perf_fn() - anchors[1])) / 1_000_000_000.0

        self.now = fast_now

        def fast_now_ns() -> int:
            # (CurrentPerf - StartPerf) + StartWall = CurrentWall (in ns)
            # Dividing by 1e9 at the VERY end preserves max precision
            return anchors[0] + (perf_fn() - anchors[1])

        self.now_ns = fast_now_ns

    def resync(self):
        """Update anchors in-place to correct for system sleep/drift."""
        time_fn = time_ns
        perf_fn = perf_counter_ns

        t0, p0 = time_fn(), perf_fn()
        self._anchors[:] = [t0, p0]

    # Type Hinting for IDE support
    now: Callable[[], float]
    now_ns: Callable[[], int]


class ConsoleTimestampFormatter:
    __slots__ = "format"

    def __init__(self):
        # Cache for the "HH:MM:SS" part
        last_sec = -1
        last_sec_str = ""

        _strftime = strftime
        _localtime = localtime
        _divmod = divmod

        def fast_fmt(ts_ns: int) -> str:
            nonlocal last_sec, last_sec_str

            # Extract seconds and sub-seconds using integer math
            # // is integer floor division; % is modulo
            current_sec, nsec = _divmod(ts_ns, 1_000_000_000)

            # Update string cache only once per second
            if current_sec != last_sec:
                last_sec = current_sec
                # We only convert to float here because localtime requires it
                last_sec_str = _strftime("%H:%M:%S", _localtime(current_sec))

            # Extract milliseconds from the remaining nanoseconds
            # 1,000,000 nanoseconds = 1 millisecond
            ms = nsec // 1_000_000

            # Fast string assembly
            return f"{last_sec_str}.{ms:03d}"

        self.format = fast_fmt

    format: Callable[[int], str]


class ISO8601TimestampFormatter:
    """Prints full ISO 8601 timestamps (μs precision) with tiered caching."""

    __slots__ = ("format",)

    def __init__(self):
        # Tier 1 Cache: Date (YYYY-MM-DD)
        last_day = -1
        date_str = ""

        # Tier 2 Cache: Time (HH:MM:SS)
        last_sec = -1
        time_str = ""

        _strftime = strftime
        _localtime = localtime
        _divmod = divmod

        def fast_fmt(ts_ns: int) -> str:
            nonlocal last_day, date_str, last_sec, time_str

            # Total seconds and the ns remainder
            total_sec, nsec = _divmod(ts_ns, 1_000_000_000)

            # Update Date Cache (Tier 1) - Every 86400 seconds
            # total_sec // 86400 gives days since epoch
            current_day = total_sec // 86400
            if current_day != last_day:
                last_day = current_day
                # Full date prefix with the 'T' separator
                date_str = _strftime("%Y-%m-%dT", _localtime(total_sec))

            # Update Time Cache (Tier 2) - Every 1 second
            if total_sec != last_sec:
                last_sec = total_sec
                time_str = _strftime("%H:%M:%S", _localtime(total_sec))

            # Extract Microseconds (us)
            # 1,000 nanoseconds = 1 microsecond
            us = nsec // 1_000

            # Fast string assembly with Zulu suffix
            # Result: 2026-02-21T14:02:01.000123Z
            return f"{date_str}{time_str}.{us:06d}Z"

        self.format = fast_fmt

    format: Callable[[int], str]

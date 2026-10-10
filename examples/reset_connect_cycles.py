# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Resets the device N times through a running BlinkView and reports how long each boot took to
reach a given log line (e.g. "network connected").

    uv run examples/reset_connect_cycles.py "network connected"
    uv run examples/reset_connect_cycles.py "network connected" --cycles 5 --device board --timeout 180

Times are measured on this script's clock, from just before the reset command to the reply that
carries the line, so each includes the reset itself and up to about 0.1 s of polling."""

import argparse
import statistics
import sys
import time

from blinkview.client import AgentApiError, BlinkView


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("text", help="text of the log line that means 'connected'")
    parser.add_argument("--cycles", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=120.0, help="seconds to wait per cycle (default 120)")
    parser.add_argument("--command", default="reset", help="source command that resets the device (default reset)")
    parser.add_argument("--source", help="source id or name; needed when the profile has several")
    parser.add_argument("--device", help="only look at this device's lines")
    args = parser.parse_args()

    try:
        bv = BlinkView.connect()
        filters = {"device": args.device} if args.device else {}
        times = []
        for cycle in range(1, args.cycles + 1):
            started = time.monotonic()
            seq = bv.command(args.command, source=args.source)
            match = bv.wait_for(args.text, after_seq=seq, timeout_s=args.timeout, **filters)
            elapsed = time.monotonic() - started
            if match is None:
                print(f"{cycle:3d}  TIMEOUT after {elapsed:.1f} s")
            else:
                times.append(elapsed)
                print(f"{cycle:3d}  {elapsed:7.2f} s  {match.text}")
    except AgentApiError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    failed = args.cycles - len(times)
    print(f"\n{len(times)} of {args.cycles} cycles reached '{args.text}'")
    if times:
        print(
            f"min {min(times):.2f} s   mean {statistics.mean(times):.2f} s   "
            f"median {statistics.median(times):.2f} s   max {max(times):.2f} s"
        )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())

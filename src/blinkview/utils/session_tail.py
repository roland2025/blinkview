# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink tail`: follows a running BlinkView's log from the shell, with `blink export`'s filters
and line format.

It reads the running instance through the agent API (blinkview.client, the `read_logs` tool with
a sequence cursor), not the session's part files: it sees rows as soon as central storage has
them (no file-logger flush delay), and the cursor carries over part and session rotation, where a
`tail -F` on a part file goes silent once the part is compressed to .zst.

Light to import, like session_export.py: no numba, Qt or id registry."""

import errno
import os
import sys
import time
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from pathlib import Path
from typing import Optional

from blinkview.client import AgentApiError, BlinkView
from blinkview.core.agent_api import list_instances
from blinkview.utils.session_export import (
    FILTER_RULES,
    PRESETS_HELP,
    ExportError,
    ExportFilter,
    FilterError,
    add_filter_arguments,
    filter_from_args,
    keep_line,
    load_preset,
    preset_files_for,
)

# How long each read_logs call waits for new rows. The server answers as soon as one arrives, so
# this doesn't add latency; it bounds how long Ctrl+C goes unnoticed - on Windows it can't
# interrupt a blocking socket read, which a 60 s long poll (MAX_WAIT_SECONDS) would make a
# minute. One request a second on localhost while idle.
WAIT_SECONDS_PER_CALL = 1.0
# Rows per read_logs reply (the server's export_format cap). A busier log pages: the next call
# returns at once while `truncated` is set.
PAGE_LINES = 50_000
MIN_LEVELS = ("ALL", "TRACE", "DEBUG", "INFO", "WARN", "ERROR", "FATAL", "CRITICAL")
SYSTEM_DEVICE = "system"

TAIL_DESCRIPTION = (
    """\
Follow a running BlinkView's log from the shell, filtered - for watching
devices from scripts and AI tools, e.g. in a background shell where each line
that comes out is an event.

INSTANCE
  Reads the BlinkView running in this project, through its agent API: the
  profile needs an enabled `mcp_server` pipeline (`blink init --agent` sets
  one up). With several running, pick one with --instance PID (`blink mcp`'s
  `status`, or the PIDs this command lists when it can't choose).

  Lines come out as BlinkView stores them, without the session log's flush
  delay, and the follow carries on across part and session rotation - unlike
  `tail -F` on a session.NNNN.log, which goes silent once that part is
  compressed.

OUTPUT
  The same lines as `blink export`:

    2026-10-02T13:28:27.664524Z I board app.sensor: temperature 21.5 C
    <timestamp, UTC>            L <device> <module>: <message>

  stdout is flushed after every batch, so each line arrives as it happens.
  stderr gets the instance and session at the start, a line when the session
  rotates, a warning if lines were lost (BlinkView dropped them from memory
  before they were read - or a rotation moved them to the old session, where
  `blink export OLD_SESSION` has them), and on exit the cursor:
  `next_seq N` - `--from-seq N` carries on from there. After --max-matches it
  is past the whole batch the last match came in, so a resume skips that
  batch's later matches.

  Exit status: 0 after --timeout, --max-matches or Ctrl+C; 1 when the
  instance is gone (or there is none); 2 for a bad command line.

FILTERING
  As `blink export`, plus --min-level, which BlinkView applies itself.
  BlinkView's own lines (device `system`) are left out unless you pass
  --include-system or --device system.

"""
    + FILTER_RULES
    + "\n"
    + PRESETS_HELP.replace("the one the session was recorded with, else", "the running session's, else")
)

TAIL_EPILOG = """\
typical use:
  blink tail --device board -e "(?i)fault|panic|reset"   watch for events
  blink tail --device board --min-level WARN             warnings and worse
  blink tail --last-n 200 --device board --max-matches 1 -e "connected"
  blink tail --preset quiet --exclude "heartbeat" --timeout 1800
  blink tail --from-seq 123456                           resume where one ended
"""


def setup_tail_parser(parser: ArgumentParser) -> None:
    parser.description = TAIL_DESCRIPTION
    parser.epilog = TAIL_EPILOG
    parser.formatter_class = RawDescriptionHelpFormatter
    add_filter_arguments(parser)
    parser.add_argument(
        "--min-level",
        type=str.upper,
        choices=MIN_LEVELS,
        default=None,
        metavar="LEVEL",
        help=f"keep only this level and worse: {', '.join(MIN_LEVELS[1:])}",
    )
    parser.add_argument(
        "--include-system", action="store_true", help="also BlinkView's own lines (device system); off by default"
    )
    start = parser.add_mutually_exclusive_group()
    start.add_argument(
        "--from-seq", type=int, default=None, metavar="N", help="start after cursor N (a printed next_seq)"
    )
    start.add_argument(
        "--last-n", type=int, default=None, metavar="N", help="start N lines back (all devices, before the filters)"
    )
    parser.add_argument("--instance", type=int, default=None, metavar="PID", help="the BlinkView to follow")
    parser.add_argument("--timeout", type=float, default=None, metavar="S", help="stop after S seconds")
    parser.add_argument("--max-matches", type=int, default=None, metavar="N", help="stop after N lines out")


class _TailExit(Exception):
    def __init__(self, code: int, message: Optional[str] = None):
        super().__init__(message)
        self.code = code
        self.message = message


def _note(message: str) -> None:
    print(f"blink tail: {message}", file=sys.stderr, flush=True)


def run_tail(args) -> None:
    tail = _Tail(args)
    try:
        try:
            tail.run()
        except KeyboardInterrupt:
            raise _TailExit(0)
    except _TailExit as e:
        if e.message:
            _note(e.message)
        if tail.cursor is not None and e.code != 2:
            _note(f"next_seq {tail.cursor}")
        sys.exit(e.code)


class _Tail:
    def __init__(self, args):
        self.args = args
        self.cursor: Optional[int] = None
        self.session_id: Optional[str] = None
        self.printed = 0

    def run(self) -> None:
        args = self.args
        try:
            filter_from_args(args)  # a bad flag is reported before anything else happens
        except FilterError as e:
            raise _TailExit(2, str(e))
        for flag, value in (("--last-n", args.last_n), ("--from-seq", args.from_seq)):
            if value is not None and value < 0:
                raise _TailExit(2, f"{flag} {value}: must not be negative")
        if args.max_matches is not None and args.max_matches < 1:
            raise _TailExit(2, f"--max-matches {args.max_matches}: must be at least 1")
        if args.timeout is not None and args.timeout <= 0:
            raise _TailExit(2, f"--timeout {args.timeout:g}: must be more than 0")

        try:
            bv = BlinkView.connect(pid=args.instance)
            status = bv.status()
        except AgentApiError as e:
            raise _TailExit(1, str(e))
        self.session_id = status.get("session_id")
        seq = int(status.get("seq") or 0)

        options = self._filter(status)
        server_args = self._server_args(bv, options)

        if args.from_seq is not None:
            if args.from_seq > seq:
                raise _TailExit(
                    2, f"--from-seq {args.from_seq} is ahead of this instance's log (at {seq}): from an earlier run?"
                )
            self.cursor = args.from_seq
        elif args.last_n is not None:
            self.cursor = max(seq - args.last_n, 0)
        else:
            self.cursor = seq
        _note(
            f"following pid {bv.pid} (project {bv.project}, profile {bv.profile}), "
            f"session {self.session_id}, after seq {self.cursor}"
        )

        self._follow(bv, options, server_args)

    def _filter(self, status: dict) -> ExportFilter:
        preset = None
        if self.args.preset is not None:
            session_dir = status.get("session_dir")
            try:
                preset = load_preset(self.args.preset, preset_files_for(Path(session_dir)) if session_dir else [])
            except ExportError as e:
                raise _TailExit(2, str(e))
            _note(f"preset {preset.name!r} from {preset.file}")
        try:
            return filter_from_args(self.args, preset)
        except FilterError as e:
            raise _TailExit(2, str(e))

    def _server_args(self, bv: BlinkView, options: ExportFilter) -> dict:
        """What read_logs filters itself: less traffic. Everything is filtered again with
        keep_line(), so these only narrow what comes over."""
        server = {"export_format": True, "max_lines": PAGE_LINES}
        if self.args.min_level is not None:
            server["min_level"] = self.args.min_level
        devices = {device.decode("utf-8", errors="surrogateescape") for device in options.devices}
        if self.args.include_system or SYSTEM_DEVICE in devices:
            server["include_system"] = True
        if len(devices) == 1:
            # read_logs refuses a device it doesn't know yet (one that hasn't logged): then the
            # device is only filtered here, so the follow can start before the device does.
            (device,) = devices
            try:
                known = {d["name"] for d in bv.list_modules(include_system=True, max_modules=1)["devices"]}
            except AgentApiError as e:
                raise _TailExit(1, str(e))
            if device in known:
                server["device"] = device
        return server

    def _follow(self, bv: BlinkView, options: ExportFilter, server_args: dict) -> None:
        args = self.args
        deadline = None if args.timeout is None else time.monotonic() + args.timeout
        out = sys.stdout.buffer
        while True:
            wait_s = WAIT_SECONDS_PER_CALL
            if deadline is not None:
                wait_s = min(wait_s, deadline - time.monotonic())
                if wait_s <= 0:
                    raise _TailExit(0, f"--timeout {args.timeout:g} s reached")
            try:
                result = bv.read_logs(after_seq=self.cursor, wait_s=wait_s, **server_args)
            except AgentApiError as e:
                if bv.pid not in {instance.get("pid") for instance in list_instances()}:
                    raise _TailExit(1, f"instance pid {bv.pid} is gone ({e})")
                raise _TailExit(1, str(e))

            self._report(result)
            self.cursor = int(result["next_seq"])
            text = result.get("text") or ""
            if not text:
                continue
            lines = text.encode("utf-8", errors="surrogateescape").split(b"\n")
            if lines[-1] == b"":
                lines.pop()
            kept = [line for line in lines if keep_line(line, options)]
            if args.max_matches is not None:
                kept = kept[: args.max_matches - self.printed]
            if kept:
                self._write(out, kept)
                self.printed += len(kept)
            if args.max_matches is not None and self.printed >= args.max_matches:
                raise _TailExit(0, f"--max-matches {args.max_matches} reached")

    def _report(self, result: dict) -> None:
        old_session = self.session_id
        new_session = result.get("session_id")
        rotated = new_session is not None and new_session != old_session
        if rotated:
            _note(f"session rotated: {old_session} -> {new_session}")
            self.session_id = new_session
        lost = result.get("lost_rows")
        if lost:
            where = f"; if the rotation took them, `blink export {old_session}` has them" if rotated else ""
            _note(
                f"warning: {lost} line(s) after seq {self.cursor} were lost - BlinkView dropped them from memory "
                f"before they were read (any device, before the filters){where}"
            )
        if result.get("cursor_reset"):
            _note(f"warning: cursor {self.cursor} was ahead of the log; following from the oldest line held")

    @staticmethod
    def _write(out, lines: list) -> None:
        try:
            out.write(b"\n".join(lines) + b"\n")
            out.flush()
        except OSError as e:
            # The reader went away (`blink tail ... | head -1`); on Windows EINVAL rather than EPIPE.
            if not isinstance(e, BrokenPipeError) and e.errno != errno.EINVAL:
                raise
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, sys.stdout.fileno())
            raise _TailExit(0)

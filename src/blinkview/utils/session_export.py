# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink export`: a recorded session's unified log written out as filtered plain text.

Lives in utils/ for the same reason as session_lister.py: the command must stay light to import
(no numba/ops/parsers/Qt/id registry), so it reads the session.NNNN.log[.zst] parts itself with
plain Python and zstandard instead of going through UnifiedLogReplay.

Works in bytes end to end - lines are never decoded, so the output is byte-identical to what was
stored (invalid UTF-8 included)."""

import errno
import os
import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, Optional, Sequence

import zstandard

from blinkview.utils.session_lister import (
    ARCHIVE_SUFFIX,
    SessionInfo,
    existing_part,
    part_index,
    resolve_log_root,
    resolve_session,
    unified_log_parts,
)

# Plain parts are read in chunks of this many bytes.
PLAIN_CHUNK_BYTES = 1 << 20
# BlinkView's own throughput diagnostics: the names every logger.stats_child(...) call in src/ uses
# (Speedometer / ThroughputAutoTuner output), dropped from `system` unless --include-stats. A new
# stats_child name must be added here - tests/test_session_export.py checks the two agree. It
# describes what recorded sessions contain, so it isn't tied to dev mode: sessions from before dev
# mode, or recorded with it on, have these lines.
STATS_LOGGER_NAMES = ("stats", "stats_in", "stats_out", "tuner", "tuner_out")
_STATS_LOGGER_NAMES_BYTES = frozenset(name.encode() for name in STATS_LOGGER_NAMES)

# Compressed bytes fed to the decompressor per call. decompressobj().decompress() has no output
# cap, so this bounds each burst of decompressed text (unified logs compress ~11x) and how much a
# corrupt block can take down with it - the call that hits the corruption returns nothing.
ZST_INPUT_CHUNK_BYTES = 1 << 16


@dataclass
class ExportFilter:
    """Which lines keep_line() drops."""

    include_stats: bool = False


def keep_line(line: bytes, options: ExportFilter) -> bool:
    """Whether a unified log line (no b"\\n") goes to the export.

    Line grammar: `<timestamp> <level> <device> <module>: <message>`. A line with fewer than
    four fields isn't a log line, but it's still data - kept, never filtered."""
    fields = line.split(b" ", 4)
    if len(fields) < 4:
        return True
    device = fields[2]
    module = fields[3].removesuffix(b":")

    # The last segment, so `source.nrf_rtt.stats` and `parser.nrf.stats_in` match while
    # `foo.stats_extra` or `statsd` don't. Only on `system`: a firmware module may well be
    # called `stats`.
    if not options.include_stats and device == b"system":
        if module.rpartition(b".")[2] in _STATS_LOGGER_NAMES_BYTES:
            return False

    return True


@dataclass
class PartReport:
    """What happened reading one listed part. Filled in as the part is read."""

    listed: Path
    path: Optional[Path] = None  # the file actually read (existing_part's pick); None if it vanished
    compressed: bool = False
    ends_partial: bool = False  # the last line had no b"\n" (a live part, or a truncated archive)
    warning: Optional[str] = None


@dataclass
class SessionRead:
    """Reports for one session's read. Complete once iter_session_lines() is exhausted."""

    parts: list[PartReport] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)  # session-level, e.g. gaps in numbering

    def warning_lines(self) -> list[str]:
        lines = list(self.warnings)
        lines += [f"{part.listed.name}: {part.warning}" for part in self.parts if part.warning]
        return lines


def iter_session_lines(parts: Sequence[Path], report: SessionRead, check_gaps: bool = True) -> Iterator[bytes]:
    """Yields every line of `parts` (unified_log_parts() order), without its b"\\n".

    Never raises for a bad part: a vanished part, a truncated or corrupt archive, or trailing
    garbage is recorded as a warning in `report` and reading goes on with the next part, keeping
    every line read up to the problem. `check_gaps` is off for a single part file given on its
    own, where missing neighbours aren't a gap."""
    if check_gaps:
        report.warnings.extend(_gap_warnings(parts))

    for listed in parts:
        part = PartReport(listed=listed)
        report.parts.append(part)
        f = _open_part(part)
        if f is None:
            part.warning = "vanished before it could be read, skipped"
            continue
        with f:
            chunks = _zst_chunks(f, part) if part.compressed else _plain_chunks(f)
            yield from _split_lines(chunks, part)


def _open_part(part: PartReport) -> Optional[BinaryIO]:
    # A plain part can be compressed and unlinked between existing_part() and open() (a live
    # session rotating) - resolving once more then finds its .zst.
    for _ in range(2):
        path = existing_part(part.listed)
        if path is None:
            return None
        try:
            f = open(path, "rb")
        except FileNotFoundError:
            continue
        part.path = path
        part.compressed = path.name.endswith(ARCHIVE_SUFFIX)
        return f
    return None


def _plain_chunks(f: BinaryIO) -> Iterator[bytes]:
    # Only up to the size at open: a live part keeps growing, and chasing it could read forever.
    # A line cut off at that point is just a partial last line.
    remaining = os.fstat(f.fileno()).st_size
    while remaining > 0:
        chunk = f.read(min(PLAIN_CHUNK_BYTES, remaining))
        if not chunk:
            return
        remaining -= len(chunk)
        yield chunk


def _zst_chunks(f: BinaryIO, part: PartReport) -> Iterator[bytes]:
    # Not decompress_file_to_buffer (raises on a truncated archive, keeping nothing), and not
    # stream_reader: on a truncated archive it just stops early without an error. decompressobj's
    # eof tells a complete frame from a cut one.
    decompressor = zstandard.ZstdDecompressor().decompressobj()
    try:
        while not decompressor.eof:
            data = f.read(ZST_INPUT_CHUNK_BYTES)
            if not data:
                break
            out = decompressor.decompress(data)
            if out:
                yield out
    except zstandard.ZstdError as e:
        part.warning = f"corrupt zstd data, kept the lines before it ({e})"
        return

    if not decompressor.eof:
        part.warning = "ends early (truncated zstd archive), kept the lines up to the break"
    elif decompressor.unused_data or f.read(1):
        # compress_file writes exactly one frame; anything after it isn't ours.
        part.warning = "data after the end of the zstd frame, ignored"


def _split_lines(chunks: Iterable[bytes], part: PartReport) -> Iterator[bytes]:
    tail = b""
    for chunk in chunks:
        lines = (tail + chunk).split(b"\n")
        tail = lines.pop()
        yield from lines
    if tail:
        part.ends_partial = True
        yield tail


def _gap_warnings(parts: Sequence[Path]) -> list[str]:
    indexes = {index for index in map(part_index, parts) if index is not None}
    if not indexes:
        return []
    missing = sorted(set(range(max(indexes) + 1)) - indexes)
    return [f"missing parts {_format_ranges(missing)}"] if missing else []


def _format_ranges(numbers: list[int]) -> str:
    """[0, 1, 2, 5] -> "0-2, 5" - a session whose first part is 5000 shouldn't list 5000 numbers."""
    ranges = []
    start = prev = numbers[0]
    for n in numbers[1:]:
        if n != prev + 1:
            ranges.append((start, prev))
            start = n
        prev = n
    ranges.append((start, prev))
    return ", ".join(f"{a:04d}" if a == b else f"{a:04d}-{b:04d}" for a, b in ranges)


# --- Command line ---

EXPORT_DESCRIPTION = """\
Write a recorded session's unified log out as plain text, filtered.

SESSION is a session folder, a single session.NNNN.log[.zst] part file, or a
session id or display name looked up like `blink replay` (in this project's
log folder, or under --logdir). A path that exists wins over a name. A name
lookup also matches part of a name, so the session it resolved to is printed
on stderr. --last exports the newest session instead.

Output is the lines exactly as stored, one per log row:

  YYYY-MM-DDTHH:MM:SS.uuuuuuZ <level> <device> <module>: <message>

Timestamps are UTC; level is a single letter. One session goes to stdout
unless -o is given. With -o, each session is written to
OUTDIR/<session folder name>.log. Plain and compressed (.zst) parts are read
in order; a truncated, corrupt or missing part is reported on stderr and the
rest is still exported.

Filtering: BlinkView's own throughput diagnostics are dropped - `system` lines
whose module ends in .stats, .stats_in, .stats_out, .tuner or .tuner_out.
Sessions recorded before dev mode, or with it on, are mostly these. This also
drops a benchmark source's `stats` output. --include-stats keeps them.

All other `system` lines are kept on purpose: they record host actions that
explain firmware behaviour (commands sent, resets, J-Link/RTT connection loss,
target power).
"""

# Kept lines are written in batches of this many.
_WRITE_BATCH_LINES = 4096


def setup_export_parser(parser: ArgumentParser) -> None:
    parser.description = EXPORT_DESCRIPTION
    parser.formatter_class = RawDescriptionHelpFormatter
    parser.add_argument("sessions", nargs="*", metavar="SESSION", help="session folder, part file, id or name")
    parser.add_argument("--last", action="store_true", help="export the most recently recorded session")
    parser.add_argument("-o", "--outdir", default=None, help="write OUTDIR/<session>.log per session instead of stdout")
    parser.add_argument("-l", "--logdir", default=None, help="base log directory for name lookups (as blink replay)")
    parser.add_argument("--include-stats", action="store_true", help="keep BlinkView's own stats/tuner lines")


@dataclass
class ExportSource:
    name: str  # session folder name - the output file is <name>.log
    parts: list[Path]
    check_gaps: bool


class ExportError(Exception):
    pass


def run_export(args) -> None:
    try:
        sources = _resolve_sources(args)
        options = ExportFilter(include_stats=args.include_stats)
        if args.outdir is None:
            _export_to_stdout(sources[0], options)
        else:
            _export_to_dir(sources, Path(args.outdir), options)
    except ExportError as e:
        sys.exit(f"blink export: {e}")


def _resolve_sources(args) -> list[ExportSource]:
    if args.last and args.sessions:
        raise ExportError("give either SESSION arguments or --last, not both")
    if not args.last and not args.sessions:
        raise ExportError("give a SESSION (folder, part file, id or name) or --last")

    if args.last:
        log_dir, project_name = resolve_log_root(log_dir=args.logdir)
        session = resolve_session(log_dir, project_name, last=True)
        if session is None:
            raise ExportError(f"no recorded session with a unified log in {Path(log_dir) / project_name}")
        print(f"--last: {session.session_id}", file=sys.stderr)
        sources = [_source_for_folder(session.path)]
    else:
        sources = [_resolve_one(arg, args.logdir) for arg in args.sessions]

    if len(sources) > 1 and args.outdir is None:
        raise ExportError("several sessions need -o OUTDIR (stdout takes one)")
    names = [source.name for source in sources]
    clashes = sorted({name for name in names if names.count(name) > 1})
    if clashes:
        raise ExportError(f"several arguments would write the same output file: {', '.join(clashes)}")
    return sources


def _resolve_one(arg: str, logdir: Optional[str]) -> ExportSource:
    path = Path(arg)
    if path.is_file():
        return ExportSource(name=path.resolve().parent.name, parts=[path], check_gaps=False)
    if path.is_dir():
        return _source_for_folder(path)

    log_dir, project_name = resolve_log_root(log_dir=logdir)
    session = resolve_session(log_dir, project_name, name=arg)
    if session is None:
        raise ExportError(
            f"no session folder, part file or recorded session matching {arg!r} "
            f"(looked in {Path(log_dir) / project_name})"
        )
    print(f"{arg}: {session.session_id}", file=sys.stderr)
    return _source_for_folder(session.path)


def _source_for_folder(folder: Path) -> ExportSource:
    # unified_log_parts() only looks at the folder; the rest of SessionInfo is metadata.json's.
    info = SessionInfo(folder.name, folder, folder.name, "", "unknown", None, None, None)
    parts = unified_log_parts(info)
    if not parts:
        raise ExportError(f"no session.NNNN.log[.zst] parts in {folder}")
    return ExportSource(name=folder.resolve().name, parts=parts, check_gaps=True)


def _export_to_stdout(source: ExportSource, options: ExportFilter) -> None:
    out = sys.stdout.buffer
    try:
        report, _, _ = _export_session(source, out, options)
        out.flush()
    except OSError as e:
        # The reader went away (`blink export X | head`). On Windows a write to a closed pipe can
        # be EINVAL rather than EPIPE.
        if not isinstance(e, BrokenPipeError) and e.errno != errno.EINVAL:
            raise
        # Point stdout at devnull so the interpreter's own flush at exit doesn't fail again.
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(0)
    _print_warnings(source, report)


def _export_to_dir(sources: list[ExportSource], outdir: Path, options: ExportFilter) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    for source in sources:
        out_path = outdir / f"{source.name}.log"
        with open(out_path, "wb") as out:
            report, read, kept = _export_session(source, out, options)
        print(f"{out_path}: {kept:,} of {read:,} lines", file=sys.stderr)
        _print_warnings(source, report)


def _export_session(source: ExportSource, out: BinaryIO, options: ExportFilter) -> tuple[SessionRead, int, int]:
    report = SessionRead()
    read = kept = 0
    batch: list[bytes] = []
    for line in iter_session_lines(source.parts, report, check_gaps=source.check_gaps):
        read += 1
        if keep_line(line, options):
            batch.append(line)
            if len(batch) >= _WRITE_BATCH_LINES:
                kept += len(batch)
                _write_lines(out, batch)
    kept += len(batch)
    _write_lines(out, batch)
    return report, read, kept


def _write_lines(out: BinaryIO, batch: list[bytes]) -> None:
    # Every line gets its b"\n" back, including a partial last line of a live/truncated part.
    if batch:
        out.write(b"\n".join(batch) + b"\n")
        batch.clear()


def _print_warnings(source: ExportSource, report: SessionRead) -> None:
    for warning in report.warning_lines():
        print(f"warning: {source.name}: {warning}", file=sys.stderr)

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
import json
import os
import sys
from argparse import ArgumentParser, RawDescriptionHelpFormatter
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, Optional, Sequence

import zstandard

from blinkview.utils.session_lister import (
    ARCHIVE_SUFFIX,
    SessionInfo,
    existing_part,
    part_index,
    resolve_active_profile_dir,
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
    """Which lines keep_line() drops. Build it with make_filter() from command-line style values."""

    include_stats: bool = False
    drop_modules: tuple[bytes, ...] = ()  # module prefixes, on any device
    drop_tags: tuple[bytes, ...] = ()  # b"<device> <module prefix>" prefixes; b"<device> " drops a device
    since: Optional[bytes] = None  # inclusive, in the line's own 27-byte UTC timestamp format
    until: Optional[bytes] = None  # exclusive


class FilterError(ValueError):
    pass


def make_filter(
    include_stats: bool = False,
    drop: Sequence[str] = (),
    drop_device: Sequence[str] = (),
    since: Optional[str] = None,
    until: Optional[str] = None,
) -> ExportFilter:
    """`drop` entries are a module prefix (`app.bms`, any device) or `"DEVICE module-prefix"`
    (`"iot battery."`, one device) - logprep.py's rule. Raises FilterError on a value that would
    silently do the wrong thing, like an empty prefix (which would drop every line)."""
    drop_modules, drop_tags = [], []
    for entry in drop:
        words = entry.split(" ")
        if not entry or "" in words or len(words) > 2:
            raise FilterError(f'--drop {entry!r}: expected "MODULE-PREFIX" or "DEVICE MODULE-PREFIX"')
        (drop_modules if len(words) == 1 else drop_tags).append(entry.encode())
    for device in drop_device:
        if not device or " " in device:
            raise FilterError(f"--drop-device {device!r}: expected a single device name")
        drop_tags.append(device.encode() + b" ")

    since_ts = _parse_bound("--since", since)
    until_ts = _parse_bound("--until", until)
    if since_ts is not None and until_ts is not None and since_ts >= until_ts:
        raise FilterError(f"--since {since} is not before --until {until}")

    return ExportFilter(include_stats, tuple(drop_modules), tuple(drop_tags), since_ts, until_ts)


def _parse_bound(flag: str, text: Optional[str]) -> Optional[bytes]:
    """An ISO 8601 time as the 27-byte UTC prefix lines start with, so bounds compare as bytes
    (fixed-width UTC sorts as text). With a zone (`Z`, `+03:00`) it's used as given; without one
    it's local time."""
    if text is None:
        return None
    try:
        moment = datetime.fromisoformat(text)
    except ValueError:
        raise FilterError(f"{flag} {text!r}: expected an ISO 8601 time, e.g. 2026-10-01T14:30 or 2026-10-01T11:30Z")
    if moment.tzinfo is None:
        moment = moment.astimezone()  # naive -> local
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ").encode()


def line_timestamp(line: bytes) -> Optional[bytes]:
    """The line's 27-byte UTC timestamp prefix, or None if it doesn't start with one (a cut-off
    line, or not a log line at all). A shape check, not a full parse - it's run per line."""
    ts = line[:27]
    if len(ts) == 27 and ts[26] == 0x5A and ts[10] == 0x54 and ts[4] == 0x2D and ts[19] == 0x2E:  # Z T - .
        return ts
    return None


def keep_line(line: bytes, options: ExportFilter) -> bool:
    """Whether a unified log line (no b"\\n") goes to the export.

    Line grammar: `<timestamp> <level> <device> <module>: <message>`. A line with fewer than
    four fields isn't a log line, but it's still data - kept, never filtered by stats or drops.
    Time bounds only apply to lines that start with a timestamp."""
    if options.since is not None or options.until is not None:
        ts = line_timestamp(line)
        if ts is not None:
            if options.since is not None and ts < options.since:
                return False
            if options.until is not None and ts >= options.until:
                return False

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

    if options.drop_modules and module.startswith(options.drop_modules):
        return False
    if options.drop_tags and (device + b" " + module).startswith(options.drop_tags):
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

More filters, each repeatable (one value per flag):

  --drop app.bms            drop modules starting with app.bms, on any device
  --drop "iot battery."     drop modules starting with battery., on device iot
  --drop-device iot         drop every line of device iot

Prefixes are plain text: --drop app.bms also drops app.bmsx. Derived devices
(parsed from another device's lines) are never dropped unless asked for.

  --since 2026-10-01T14:30  keep lines at or after this time
  --until 2026-10-01T15:00  keep lines before this time

A time with a zone (Z, +03:00) is used as given; without one it's local time.
Lines that don't start with a timestamp are kept.

--summary prints per session to stderr: lines read and kept, first and last
timestamp, the parts read, whether the session was recorded in dev mode, and
the 40 most frequent kept `device module` tags - a quick way to pick --drop
values.

Presets: --preset NAME applies a named set of drops kept in an
export_presets.json file next to the profile JSON
(.blinkview/profiles/<profile>/export_presets.json):

  {
    "analysis": {
      "description": "what an analysis needs",
      "drop": ["app.bms", "iot battery."],
      "drop_device": ["can0"]
    }
  }

The file is looked up per session: first in the folder of the profile the
session was recorded with (metadata.json), then in the active profile's
folder. --drop and --drop-device add to the preset.
"""

# Kept lines are written in batches of this many.
_WRITE_BATCH_LINES = 4096
SUMMARY_TOP_TAGS = 40
PRESETS_FILE_NAME = "export_presets.json"
_PRESET_KEYS = {"description", "drop", "drop_device"}


def setup_export_parser(parser: ArgumentParser) -> None:
    parser.description = EXPORT_DESCRIPTION
    parser.formatter_class = RawDescriptionHelpFormatter
    parser.add_argument("sessions", nargs="*", metavar="SESSION", help="session folder, part file, id or name")
    parser.add_argument("--last", action="store_true", help="export the most recently recorded session")
    parser.add_argument("-o", "--outdir", default=None, help="write OUTDIR/<session>.log per session instead of stdout")
    parser.add_argument("-l", "--logdir", default=None, help="base log directory for name lookups (as blink replay)")
    parser.add_argument("--include-stats", action="store_true", help="keep BlinkView's own stats/tuner lines")
    parser.add_argument(
        "--drop", action="append", default=[], metavar="TAG", help='drop "MODULE-PREFIX" or "DEVICE MODULE-PREFIX"'
    )
    parser.add_argument("--drop-device", action="append", default=[], metavar="DEV", help="drop all lines of DEV")
    parser.add_argument("--since", default=None, metavar="TIME", help="keep lines at or after TIME (ISO 8601)")
    parser.add_argument("--until", default=None, metavar="TIME", help="keep lines before TIME (ISO 8601)")
    parser.add_argument("--summary", action="store_true", help="print a per-session summary to stderr")
    parser.add_argument("--preset", default=None, metavar="NAME", help=f"apply the drops of preset NAME ({PRESETS_FILE_NAME})")


@dataclass
class ExportSource:
    name: str  # session folder name - the output file is <name>.log
    folder: Path  # where metadata.json is
    parts: list[Path]
    check_gaps: bool


@dataclass
class ExportResult:
    report: SessionRead
    read: int = 0
    kept: int = 0
    # Only filled with --summary:
    first_ts: Optional[bytes] = None
    last_ts: Optional[bytes] = None
    tags: Optional[Counter] = None


class ExportError(Exception):
    pass


@dataclass
class Preset:
    name: str
    file: Path
    drop: list[str]
    drop_device: list[str]


def run_export(args) -> None:
    try:
        try:
            # Checked before anything is resolved or read: a typo shouldn't cost a whole read.
            make_filter(args.include_stats, args.drop, args.drop_device, args.since, args.until)
        except FilterError as e:
            raise ExportError(str(e))
        sources = _resolve_sources(args)
        # Every session's filter is built before the first byte is written, so a missing or
        # broken preset for the third session doesn't leave two exports behind.
        jobs = [(source, _filter_for(source, args)) for source in sources]
        if args.outdir is None:
            _export_to_stdout(*jobs[0], args.summary)
        else:
            _export_to_dir(jobs, Path(args.outdir), args.summary)
    except ExportError as e:
        sys.exit(f"blink export: {e}")


def _filter_for(source: ExportSource, args) -> ExportFilter:
    drop, drop_device = list(args.drop), list(args.drop_device)
    if args.preset is not None:
        preset = load_preset(args.preset, preset_files_for(source.folder))
        print(f"{source.name}: preset {preset.name!r} from {preset.file}", file=sys.stderr)
        drop = preset.drop + drop
        drop_device = preset.drop_device + drop_device
    return make_filter(args.include_stats, drop, drop_device, args.since, args.until)


def preset_files_for(session_folder: Path) -> list[Path]:
    """Where to look for the session's export_presets.json, in order: the folder of the profile
    it was recorded with (metadata.json config.source_file), then the active profile's folder.
    Duplicates removed; whether each exists is load_preset()'s business."""
    candidates = []
    source_file = _read_metadata(session_folder).get("config", {}).get("source_file")
    if isinstance(source_file, str) and source_file:
        candidates.append(Path(source_file).parent / PRESETS_FILE_NAME)
    candidates.append(resolve_active_profile_dir() / PRESETS_FILE_NAME)
    unique = []
    for path in candidates:
        if path not in unique:
            unique.append(path)
    return unique


def load_preset(name: str, candidates: Sequence[Path]) -> Preset:
    """The preset `name` from the first existing file in `candidates`. A preset missing from that
    file is an error - not a reason to look further, which would mix two profiles' presets."""
    file = next((path for path in candidates if path.is_file()), None)
    if file is None:
        looked = ", ".join(str(path) for path in candidates)
        raise ExportError(f"--preset {name}: no {PRESETS_FILE_NAME} found (looked in: {looked})")
    try:
        presets = json.loads(file.read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        raise ExportError(f"--preset {name}: can't read {file}: {e}")
    if not isinstance(presets, dict):
        raise ExportError(f"{file}: expected an object of preset name: preset")
    if name not in presets:
        available = ", ".join(sorted(presets)) or "none"
        raise ExportError(f"--preset {name}: not in {file} (available: {available})")

    preset = presets[name]
    if not isinstance(preset, dict):
        raise ExportError(f"{file}: preset {name!r} must be an object")
    unknown = sorted(set(preset) - _PRESET_KEYS)
    if unknown:
        raise ExportError(f"{file}: preset {name!r} has unknown keys {unknown} (allowed: {sorted(_PRESET_KEYS)})")
    lists = {}
    for key in ("drop", "drop_device"):
        values = preset.get(key, [])
        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            raise ExportError(f"{file}: preset {name!r}: {key} must be a list of strings")
        lists[key] = values
    try:
        make_filter(drop=lists["drop"], drop_device=lists["drop_device"])
    except FilterError as e:
        raise ExportError(f"{file}: preset {name!r}: {e}")
    return Preset(name, file, lists["drop"], lists["drop_device"])


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
        folder = path.resolve().parent
        return ExportSource(name=folder.name, folder=folder, parts=[path], check_gaps=False)
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
    return ExportSource(name=folder.resolve().name, folder=folder, parts=parts, check_gaps=True)


def _export_to_stdout(source: ExportSource, options: ExportFilter, summary: bool) -> None:
    out = sys.stdout.buffer
    try:
        result = _export_session(source, out, options, summary)
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
    _print_warnings(source, result.report)
    if summary:
        _print_summary(source, result)


def _export_to_dir(jobs: list[tuple[ExportSource, ExportFilter]], outdir: Path, summary: bool) -> None:
    outdir.mkdir(parents=True, exist_ok=True)
    for source, options in jobs:
        out_path = outdir / f"{source.name}.log"
        with open(out_path, "wb") as out:
            result = _export_session(source, out, options, summary)
        print(f"{out_path}: {result.kept:,} of {result.read:,} lines", file=sys.stderr)
        _print_warnings(source, result.report)
        if summary:
            _print_summary(source, result)


def _export_session(source: ExportSource, out: BinaryIO, options: ExportFilter, summary: bool) -> ExportResult:
    result = ExportResult(report=SessionRead())
    if summary:
        result.tags = Counter()
    batch: list[bytes] = []
    for line in iter_session_lines(source.parts, result.report, check_gaps=source.check_gaps):
        result.read += 1
        if not keep_line(line, options):
            continue
        batch.append(line)
        if len(batch) >= _WRITE_BATCH_LINES:
            result.kept += len(batch)
            _write_lines(out, batch)
        if summary:
            _count_for_summary(result, line)
    result.kept += len(batch)
    _write_lines(out, batch)
    return result


def _count_for_summary(result: ExportResult, line: bytes) -> None:
    # A cut-off last line either still has its whole timestamp (the row did start then) or fails
    # line_timestamp()'s shape check - so no partial-line special case is needed here.
    ts = line_timestamp(line)
    if ts is not None:
        if result.first_ts is None:
            result.first_ts = ts
        result.last_ts = ts
    fields = line.split(b" ", 4)
    tag = fields[2] + b" " + fields[3].removesuffix(b":") if len(fields) >= 4 else b"(not a log line)"
    result.tags[tag] += 1


def _print_summary(source: ExportSource, result: ExportResult) -> None:
    def text(raw: bytes) -> str:
        return raw.decode("utf-8", errors="replace")

    lines = [f"summary: {source.name}"]
    lines.append(f"  lines     {result.kept:,} kept of {result.read:,} read")
    if result.first_ts is None:
        lines.append("  time      no timestamped lines kept")
    else:
        lines.append(f"  time      {text(result.first_ts)} .. {text(result.last_ts)} (UTC)")
    lines.append(f"  parts     {', '.join(_describe_part(part) for part in result.report.parts)}")
    lines.append(f"  dev mode  {_recorded_dev_mode(source.folder)}")
    if result.tags:
        lines.append(f"  top tags  (of {len(result.tags):,}, by kept lines)")
        for tag, count in result.tags.most_common(SUMMARY_TOP_TAGS):
            lines.append(f"    {count:>10,}  {text(tag)}")
    print("\n".join(lines), file=sys.stderr)


def _describe_part(part: PartReport) -> str:
    if part.path is None:
        return f"{part.listed.name} (vanished)"
    notes = [note for note, on in (("zst", part.compressed), ("warning", part.warning is not None)) if on]
    return part.path.name + (f" ({', '.join(notes)})" if notes else "")


def _recorded_dev_mode(folder: Path) -> str:
    """From metadata.json, for the summary only - filtering never depends on it (older sessions
    don't record it)."""
    environment = _read_metadata(folder).get("environment")
    if not isinstance(environment, dict) or "dev_mode" not in environment:
        return "not recorded"
    return "on" if environment["dev_mode"] else "off"


def _read_metadata(folder: Path) -> dict:
    """The session's metadata.json, or {} if it's missing or unreadable - a session folder copied
    by hand, or a part file passed on its own, may well have none."""
    try:
        meta = json.loads((folder / "metadata.json").read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return meta if isinstance(meta, dict) else {}


def _write_lines(out: BinaryIO, batch: list[bytes]) -> None:
    # Every line gets its b"\n" back, including a partial last line of a live/truncated part.
    if batch:
        out.write(b"\n".join(batch) + b"\n")
        batch.clear()


def _print_warnings(source: ExportSource, report: SessionRead) -> None:
    for warning in report.warning_lines():
        print(f"warning: {source.name}: {warning}", file=sys.stderr)

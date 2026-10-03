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

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import BinaryIO, Iterable, Iterator, Optional, Sequence

import zstandard

from blinkview.utils.session_lister import ARCHIVE_SUFFIX, existing_part, part_index

# Plain parts are read in chunks of this many bytes.
PLAIN_CHUNK_BYTES = 1 << 20
# Compressed bytes fed to the decompressor per call. decompressobj().decompress() has no output
# cap, so this bounds each burst of decompressed text (unified logs compress ~11x) and how much a
# corrupt block can take down with it - the call that hits the corruption returns nothing.
ZST_INPUT_CHUNK_BYTES = 1 << 16


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

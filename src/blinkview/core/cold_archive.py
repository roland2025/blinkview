# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""zstandard compression for persisted cold-storage segment files - see
plans/cold-storage-compression.md.

Segments are written uncompressed to `cold/` while a session is live (core/cold_segment.py's
write_cold_segment_file, driven by ColdStorageArchiver) - a live cold tier must stay directly
mmap-able for scrubback, so nothing in this module ever touches it while segments are still in
active use. Only once a session ends and its cold tier is being persisted for a later replay
(CentralStorage's cold_storage_persist_on_close) does compress_cold_storage_dir() shrink its
on-disk footprint: every raw `segment_*.blkseg` file gets zstd-compressed into a sibling
`cold-archive/` directory and deleted from `cold/`.

A later replay mounts straight from these compressed archives - decompress_cold_segment_archive()
decompresses one directly into an owned in-memory buffer (PooledLogBatch.from_compressed_archive,
core/numpy_batch_manager.py), skipping the write-decompressed-bytes-to-disk-then-mmap-them-back-in
round trip a naive "unpack" step would otherwise need (see plans/lazy-cold-segment-unpacking.md's
research into why - the write+reread was pure overhead once the decompressed content was going to
be fully read either way). It decompresses straight into a preallocated numpy buffer via zstd's
streaming `readinto()` rather than materializing an intermediate `bytes`/`bytearray` first and
copying that into the final buffer - see compress_cold_segment_file's `size=` argument, which is
what makes the decompressed size knowable up front without decompressing anything yet.

Scope: only the default `<session>/cold/` + `<session>/cold-archive/` layout - matches
cold_storage_persist_on_close's own documented scope (an overridden cold_storage_dir gets a fresh
uniquely-named subdirectory every run and is never reopened, so there'd be nothing to read back
from)."""

from pathlib import Path
from typing import Callable, Optional, Union

import numpy as np

from blinkview.core.zstd_file_compression import compress_file, decompress_file_to_buffer

_SEGMENT_GLOB = "segment_*.blkseg"
_ARCHIVE_SUFFIX = ".zst"


def count_cold_segments(cold_dir: Path) -> int:
    """Number of `segment_*.blkseg` files currently in `cold_dir` - lets a caller (Registry.stop's
    shutdown-compression progress reporting) know the total upfront, without duplicating
    compress_cold_storage_dir's glob pattern."""
    cold_dir = Path(cold_dir)
    if not cold_dir.exists():
        return 0
    return sum(1 for _ in cold_dir.glob(_SEGMENT_GLOB))


def _archive_dir_for(cold_dir: Path) -> Path:
    return cold_dir.parent / "cold-archive"


def compress_cold_segment_file(path: Path, archive_dir: Path) -> Path:
    """Compresses one already-closed segment file into `archive_dir` - thin cold-segment-shaped
    wrapper over core/zstd_file_compression.compress_file (same crash-safe `.tmp` + rename,
    embedded content size)."""
    archive_path = archive_dir / (path.name + _ARCHIVE_SUFFIX)
    compress_file(path, archive_path)
    return archive_path


def decompress_cold_segment_archive(archive_path: Union[str, Path]) -> np.ndarray:
    """Fully decompresses a cold segment archive straight into an owned, writable uint8 numpy
    buffer - thin wrapper over core/zstd_file_compression.decompress_file_to_buffer. Writable
    (an allocated ndarray, not a read-only view) so np.frombuffer views built over it
    (core/cold_segment.py's open_cold_segment_arrays_from_buffer) come back writable-typed rather
    than read-only, matching the mmap path's ACCESS_COPY choice and the same Numba
    double-specialization reasoning behind it (see _BufferRef's docstring) - nothing here ever
    actually writes through it either."""
    return decompress_file_to_buffer(archive_path)


def compress_cold_storage_dir(cold_dir: Path, logger=None, on_progress: Optional[Callable[[str], None]] = None) -> None:
    """Compresses every `segment_*.blkseg` file still sitting in `cold_dir` into
    `cold_dir.parent / "cold-archive"`, deleting each raw file once its compressed copy is
    confirmed written. Only safe to call after every mmap over these files has been closed (see
    CircularLogPool.release_all()/Registry.stop()) - reading/deleting them earlier risks a torn
    read against a still-open mapping and, on Windows, deletion would fail outright while any
    mapping is still open. Best-effort per file: a failure on one segment is logged and skipped
    rather than aborting the rest - a segment left uncompressed in `cold/` is still fully
    functional (just not space-saved), never silently lost.

    `on_progress(filename)`, if given, is called once per segment file (in a `finally`, so a
    failed/skipped file still advances progress) - this function only reports what it directly
    knows (one file just finished processing); aggregating that into an overall "i of N" count
    across cold storage and file-logger compression together is Registry.stop()'s job, not
    this module's."""
    cold_dir = Path(cold_dir)
    paths = sorted(cold_dir.glob(_SEGMENT_GLOB))
    if not paths:
        return

    archive_dir = _archive_dir_for(cold_dir)
    archive_dir.mkdir(parents=True, exist_ok=True)

    for path in paths:
        try:
            compress_cold_segment_file(path, archive_dir)
            path.unlink()
        except OSError as e:
            if logger:
                logger.warning(f"Failed to compress cold segment {path}: {e}")
        finally:
            if on_progress:
                on_progress(path.name)

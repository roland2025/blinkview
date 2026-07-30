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
from typing import Union

import numpy as np
import zstandard

_SEGMENT_GLOB = "segment_*.blkseg"
_ARCHIVE_SUFFIX = ".zst"

# get_frame_parameters() only needs the frame header, not the whole file - ZSTD_FRAMEHEADERSIZE_MAX
# is 18 bytes; reading a small, comfortably-larger prefix avoids relying on that exact constant.
_FRAME_HEADER_PROBE_SIZE = 32


def _archive_dir_for(cold_dir: Path) -> Path:
    return cold_dir.parent / "cold-archive"


def compress_cold_segment_file(path: Path, archive_dir: Path) -> Path:
    """Compresses one already-closed segment file into `archive_dir`. Writes to a `.tmp` sibling
    and renames into place, same crash-safety pattern as write_cold_segment_file - a reader
    (decompress_cold_segment_archive) can never observe a partially-written archive.

    Passes `size=` (the file's own on-disk size) so the frame embeds its decompressed content
    size - decompress_cold_segment_archive relies on being able to read that back cheaply (from
    just the frame header, not by decompressing anything) to preallocate an exactly-sized output
    buffer instead of growing one dynamically."""
    archive_path = archive_dir / (path.name + _ARCHIVE_SUFFIX)
    tmp_path = archive_path.with_name(archive_path.name + ".tmp")
    raw_size = path.stat().st_size
    cctx = zstandard.ZstdCompressor()
    with open(path, "rb") as src, open(tmp_path, "wb") as dst:
        cctx.copy_stream(src, dst, size=raw_size)
    tmp_path.replace(archive_path)
    return archive_path


def decompress_cold_segment_archive(archive_path: Union[str, Path]) -> np.ndarray:
    """Fully decompresses a cold segment archive straight into an owned, writable uint8 numpy
    buffer - no intermediate file, and (given the archive's frame embeds a content size - see
    compress_cold_segment_file) no intermediate `bytes`/`bytearray` copy either: the buffer is
    preallocated at its exact final size and zstd decompresses straight into it via
    `readinto()`. Writable (an allocated ndarray, not a read-only view) so np.frombuffer views
    built over it (core/cold_segment.py's open_cold_segment_arrays_from_buffer) come back
    writable-typed rather than read-only, matching the mmap path's ACCESS_COPY choice and the same
    Numba double-specialization reasoning behind it (see _BufferRef's docstring) - nothing here
    ever actually writes through it either.

    Falls back to a single grow-as-you-go `read()` (still zero-file-writes, just not
    zero-extra-copy) if the frame doesn't have a usable embedded content size - e.g. an archive
    from before this size-embedding change, or a foreign/corrupted file."""
    dctx = zstandard.ZstdDecompressor()
    with open(archive_path, "rb") as f:
        frame_params = zstandard.get_frame_parameters(f.read(_FRAME_HEADER_PROBE_SIZE))
        content_size = frame_params.content_size
        f.seek(0)

        # A frame compressed without size= (compress_cold_segment_file always passes it, but a
        # foreign tool or a pre-this-change archive might not) reports its content size as one of
        # these two unsigned 64-bit sentinels, not None/negative.
        if content_size in (zstandard.CONTENTSIZE_UNKNOWN, zstandard.CONTENTSIZE_ERROR):
            return np.frombuffer(dctx.stream_reader(f).read(), dtype=np.uint8).copy()

        buffer = np.empty(content_size, dtype=np.uint8)
        reader = dctx.stream_reader(f)
        view = memoryview(buffer)
        total = 0
        while total < content_size:
            n = reader.readinto(view[total:])
            if n == 0:
                break
            total += n

    if total != content_size:
        raise ValueError(
            f"Truncated cold segment archive: expected {content_size} decompressed bytes, got {total}: {archive_path}"
        )
    return buffer


def compress_cold_storage_dir(cold_dir: Path, logger=None) -> None:
    """Compresses every `segment_*.blkseg` file still sitting in `cold_dir` into
    `cold_dir.parent / "cold-archive"`, deleting each raw file once its compressed copy is
    confirmed written. Only safe to call after every mmap over these files has been closed (see
    CircularLogPool.release_all()/Registry.stop()) - reading/deleting them earlier risks a torn
    read against a still-open mapping and, on Windows, deletion would fail outright while any
    mapping is still open. Best-effort per file: a failure on one segment is logged and skipped
    rather than aborting the rest - a segment left uncompressed in `cold/` is still fully
    functional (just not space-saved), never silently lost."""
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

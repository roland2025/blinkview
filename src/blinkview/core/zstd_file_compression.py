# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Generic zstandard file compression helpers - extracted from core/cold_archive.py, which
originally had this logic inlined under cold-segment-specific names/docstrings despite being
plain path-in/path-out functions with no actual cold-segment coupling. Shared by cold_archive.py
(compress_cold_segment_file/decompress_cold_segment_archive, now thin wrappers) and
storage/log_file_archive.py (session/source raw log file compression) - see
plans/expressive-sauteeing-sun.md."""

import os
from pathlib import Path
from typing import Union

import numpy as np
import zstandard

# get_frame_parameters() only needs the frame header, not the whole file - ZSTD_FRAMEHEADERSIZE_MAX
# is 18 bytes; reading a small, comfortably-larger prefix avoids relying on that exact constant.
_FRAME_HEADER_PROBE_SIZE = 32


def _fsync_dir(dir_path: Path) -> None:
    """Makes a rename inside dir_path durable. POSIX only - Windows can't open a directory for
    fsync, and NTFS journals metadata operations in order anyway."""
    if os.name == "nt":
        return
    fd = os.open(dir_path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def compress_file(src_path: Union[str, Path], dst_path: Union[str, Path]) -> None:
    """Streams src_path through zstd into dst_path. Writes to a `.tmp` sibling of dst_path and
    renames into place, so a reader (decompress_file_to_buffer) can never observe a partially-
    written file. Passes `size=` (src_path's own on-disk size) so the frame embeds its
    decompressed content size - decompress_file_to_buffer relies on being able to read that back
    cheaply (from just the frame header, not by decompressing anything) to preallocate an
    exactly-sized output buffer instead of growing one dynamically.

    The `.tmp` is fsynced before the rename, and (on POSIX) the directory after it: every caller
    deletes src_path once this returns, so after a power loss the rename must never be on disk
    while the compressed bytes aren't - that would leave a truncated `.zst` as the only copy."""
    src_path = Path(src_path)
    dst_path = Path(dst_path)
    tmp_path = dst_path.with_name(dst_path.name + ".tmp")
    raw_size = src_path.stat().st_size
    cctx = zstandard.ZstdCompressor()
    with open(src_path, "rb") as src, open(tmp_path, "wb") as dst:
        cctx.copy_stream(src, dst, size=raw_size)
        dst.flush()
        os.fsync(dst.fileno())
    tmp_path.replace(dst_path)
    _fsync_dir(dst_path.parent)


def decompress_file_to_buffer(path: Union[str, Path]) -> np.ndarray:
    """Fully decompresses a zstd file straight into an owned, writable uint8 numpy buffer - no
    intermediate file, and (given the frame embeds a content size - see compress_file) no
    intermediate `bytes`/`bytearray` copy either: the buffer is preallocated at its exact final
    size and zstd decompresses straight into it via `readinto()`. Writable (an allocated ndarray,
    not a read-only view) so np.frombuffer views built over it come back writable-typed rather
    than read-only, matching the mmap path's ACCESS_COPY choice.

    Falls back to a single grow-as-you-go `read()` (still zero-file-writes, just not
    zero-extra-copy) if the frame doesn't have a usable embedded content size - e.g. a file
    compressed without `size=` by a foreign tool, or a pre-this-change archive."""
    path = Path(path)
    dctx = zstandard.ZstdDecompressor()
    with open(path, "rb") as f:
        frame_params = zstandard.get_frame_parameters(f.read(_FRAME_HEADER_PROBE_SIZE))
        content_size = frame_params.content_size
        f.seek(0)

        # A frame compressed without size= reports its content size as one of these two unsigned
        # 64-bit sentinels, not None/negative.
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
        raise ValueError(f"Truncated zstd file: expected {content_size} decompressed bytes, got {total}: {path}")
    return buffer

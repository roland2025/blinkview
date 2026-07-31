# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Coverage for the generic zstd compress/decompress helpers (core/zstd_file_compression.py),
extracted from core/cold_archive.py so storage/log_file_archive.py could reuse the same
crash-safe, no-extra-copy pattern instead of duplicating it - see
plans/expressive-sauteeing-sun.md. Cold-segment-specific behavior (archive dir/suffix
conventions, writable-buffer requirements) stays covered by tests/test_cold_archive.py."""

import numpy as np

from blinkview.core.zstd_file_compression import compress_file, decompress_file_to_buffer


def test_round_trip_is_byte_identical(tmp_path):
    src = tmp_path / "data.bin"
    original = (b"repeating payload text " * 500) + b"tail"
    src.write_bytes(original)

    dst = tmp_path / "data.bin.zst"
    compress_file(src, dst)
    assert dst.exists()
    assert dst.stat().st_size < len(original)  # actually compressed, not just copied

    buffer = decompress_file_to_buffer(dst)

    assert isinstance(buffer, np.ndarray)
    assert bytes(buffer) == original


def test_round_trip_empty_file(tmp_path):
    src = tmp_path / "empty.bin"
    src.write_bytes(b"")

    dst = tmp_path / "empty.bin.zst"
    compress_file(src, dst)

    buffer = decompress_file_to_buffer(dst)
    assert bytes(buffer) == b""


def test_compress_writes_via_tmp_sibling_then_renames(tmp_path):
    """No partially-written destination should ever be observable - compress_file's crash-safety
    contract (write to `.tmp`, then atomic rename)."""
    src = tmp_path / "data.bin"
    src.write_bytes(b"some content")
    dst = tmp_path / "data.bin.zst"

    compress_file(src, dst)

    assert dst.exists()
    assert not dst.with_name(dst.name + ".tmp").exists()


def test_decompressed_buffer_is_writable(tmp_path):
    """np.frombuffer's writeable flag mirrors the underlying buffer's - must be an allocated,
    writable array (not a read-only view), matching the reasoning in
    core/cold_archive.py's decompress_cold_segment_archive docstring."""
    src = tmp_path / "data.bin"
    src.write_bytes(b"writable check payload")
    dst = tmp_path / "data.bin.zst"
    compress_file(src, dst)

    buffer = decompress_file_to_buffer(dst)
    assert buffer.flags.writeable

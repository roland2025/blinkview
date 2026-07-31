# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Coverage for storage/log_file_archive.py - the FileLogger-facing wrapper around
core/zstd_file_compression.py used to compress session.NNNN.log / src_XXXX.NNNN.bin part files
in place. See tests/test_file_logger.py for the write-side trigger points (rotation/shutdown) and
tests/test_unified_log_replay.py for the read-side decompression."""

from blinkview.storage.log_file_archive import (
    ARCHIVE_SUFFIX,
    compress_log_part_file,
    decompress_log_part_to_buffer,
)


def test_compress_log_part_file_creates_zst_sibling_in_place(tmp_path):
    src = tmp_path / "session.0000.log"
    original = b"formatted log rows\n" * 200
    src.write_bytes(original)

    archive_path = compress_log_part_file(src)

    assert archive_path == tmp_path / ("session.0000.log" + ARCHIVE_SUFFIX)
    assert archive_path.exists()
    assert src.exists()  # compress_log_part_file itself doesn't delete the original
    assert archive_path.stat().st_size < len(original)


def test_round_trip_is_byte_identical(tmp_path):
    src = tmp_path / "src_abcd1234.0000.bin"
    original = bytes(range(256)) * 50
    src.write_bytes(original)

    archive_path = compress_log_part_file(src)
    buffer = decompress_log_part_to_buffer(archive_path)

    assert bytes(buffer) == original

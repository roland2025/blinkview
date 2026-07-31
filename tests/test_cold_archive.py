# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_archive import (
    compress_cold_segment_file,
    compress_cold_storage_dir,
    count_cold_segments,
    decompress_cold_segment_archive,
)
from blinkview.core.cold_segment import open_cold_segment_arrays_from_buffer, write_cold_segment_file
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.log_level import LogLevel


def make_real_segment_file(pool, path, row_count=200):
    """A real cold segment file (CircularLogPool's exact on-disk format) with enough repetitive
    row content that zstd has something real to compress, unlike a handful of bytes."""
    batch = pool.create(
        PooledLogBatch,
        req_capacity=row_count,
        buffer_bytes=row_count * 32,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
        has_pids=True,
        has_tids=True,
    )
    for i in range(row_count):
        assert batch.insert(
            100 + i, 100 + i, b"repeating log payload text", LogLevel.INFO.value, 1, 2, i, pid=10, tid=20
        )
    write_cold_segment_file(path, batch.bundle)
    batch.release()
    return path


@pytest.fixture
def global_pool():
    return NumpyArrayPool()


class TestCompressDecompressSegmentFile:
    def test_round_trip_is_byte_identical(self, global_pool, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()

        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        original_bytes = src.read_bytes()

        archive_path = compress_cold_segment_file(src, archive_dir)
        assert archive_path == archive_dir / "segment_0000000000.blkseg.zst"
        assert archive_path.exists()

        buffer = decompress_cold_segment_archive(archive_path)

        assert bytes(buffer) == original_bytes

    def test_decompressed_buffer_is_writable_not_read_only(self, global_pool, tmp_path):
        """np.frombuffer's writeable flag mirrors the underlying buffer's - a read-only buffer
        would force a second compiled Numba specialization the first time a kernel touches one
        (see _BufferRef's docstring). decompress_cold_segment_archive must return an allocated
        (writable), not a read-only view, so np.frombuffer views built over it come back
        writable too."""
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        archive_path = compress_cold_segment_file(src, archive_dir)

        buffer = decompress_cold_segment_archive(archive_path)

        assert isinstance(buffer, np.ndarray)
        assert buffer.dtype == np.uint8
        assert buffer.flags.writeable is True

    def test_decompress_preallocates_from_the_frames_embedded_content_size(self, global_pool, tmp_path):
        """compress_cold_segment_file passes size= so the frame embeds its decompressed content
        size - decompress_cold_segment_archive relies on reading that back cheaply to preallocate
        an exactly-sized buffer instead of growing one dynamically. Proven here by checking the
        returned buffer's length matches the original raw file's size exactly (not just its
        content being correct - a truncated-then-grown fallback buffer could still pass a pure
        content-equality check by accident if it happened to be read faithfully)."""
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        raw_size = src.stat().st_size
        archive_path = compress_cold_segment_file(src, archive_dir)

        buffer = decompress_cold_segment_archive(archive_path)

        assert len(buffer) == raw_size

    def test_unknown_content_size_falls_back_to_growable_read(self, global_pool, tmp_path):
        """An archive compressed without an embedded content size (e.g. by a foreign tool, or one
        written before this change) must still decompress correctly via the fallback path, not
        raise or silently truncate."""
        import zstandard

        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        original_bytes = src.read_bytes()

        # Compress without size= (bypassing compress_cold_segment_file on purpose) so the frame's
        # content size comes back unknown.
        archive_path = archive_dir / "segment_0000000000.blkseg.zst"
        cctx = zstandard.ZstdCompressor()
        with open(src, "rb") as f_in, open(archive_path, "wb") as f_out:
            cctx.copy_stream(f_in, f_out)

        buffer = decompress_cold_segment_archive(archive_path)

        assert isinstance(buffer, np.ndarray)
        assert buffer.flags.writeable is True
        assert bytes(buffer) == original_bytes

    def test_decompressed_buffer_parses_into_the_same_columns_as_the_original(self, global_pool, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg", row_count=50)
        archive_path = compress_cold_segment_file(src, archive_dir)

        buffer = decompress_cold_segment_archive(archive_path)
        header, handles = open_cold_segment_arrays_from_buffer(buffer)

        assert header.row_count == 50
        assert handles["timestamps"].array[0] == 100
        assert handles["timestamps"].array[49] == 149
        assert handles["sequences"].array.tolist() == list(range(50))

    def test_compressed_file_is_meaningfully_smaller_for_repetitive_content(self, global_pool, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()

        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg", row_count=2000)
        archive_path = compress_cold_segment_file(src, archive_dir)

        assert archive_path.stat().st_size < src.stat().st_size / 2


class TestCompressColdStorageDir:
    def test_moves_every_segment_into_a_sibling_archive_dir_and_deletes_raw_copies(self, global_pool, tmp_path):
        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)
        make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        make_real_segment_file(global_pool, cold_dir / "segment_0000000001.blkseg")
        (cold_dir / "id_registry.json").write_text("{}")  # must be left alone - not a segment file

        compress_cold_storage_dir(cold_dir)

        assert sorted(p.name for p in cold_dir.iterdir()) == ["id_registry.json"]
        archive_dir = tmp_path / "session" / "cold-archive"
        assert sorted(p.name for p in archive_dir.iterdir()) == [
            "segment_0000000000.blkseg.zst",
            "segment_0000000001.blkseg.zst",
        ]

    def test_empty_cold_dir_is_a_quiet_noop_and_creates_no_archive_dir(self, tmp_path):
        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)

        compress_cold_storage_dir(cold_dir)

        assert not (tmp_path / "session" / "cold-archive").exists()

    def test_a_failure_on_one_segment_does_not_abort_the_rest(self, global_pool, tmp_path, monkeypatch):
        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)
        make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        make_real_segment_file(global_pool, cold_dir / "segment_0000000001.blkseg")

        import blinkview.core.cold_archive as cold_archive_module

        real_compress = cold_archive_module.compress_cold_segment_file

        def flaky_compress(path, archive_dir):
            if path.name == "segment_0000000000.blkseg":
                raise OSError("simulated failure")
            return real_compress(path, archive_dir)

        monkeypatch.setattr(cold_archive_module, "compress_cold_segment_file", flaky_compress)

        class RecordingLogger:
            def __init__(self):
                self.warnings = []

            def warning(self, msg, *args):
                self.warnings.append(msg % args if args else msg)

        logger = RecordingLogger()
        compress_cold_storage_dir(cold_dir, logger=logger)

        # The failed segment is left alone (still raw, not lost); the other one still got archived.
        assert (cold_dir / "segment_0000000000.blkseg").exists()
        assert not (cold_dir / "segment_0000000001.blkseg").exists()
        assert (tmp_path / "session" / "cold-archive" / "segment_0000000001.blkseg.zst").exists()
        assert len(logger.warnings) == 1

    def test_on_progress_fires_once_per_segment_with_the_filename(self, global_pool, tmp_path):
        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)
        make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        make_real_segment_file(global_pool, cold_dir / "segment_0000000001.blkseg")

        calls = []
        compress_cold_storage_dir(cold_dir, on_progress=calls.append)

        assert calls == ["segment_0000000000.blkseg", "segment_0000000001.blkseg"]

    def test_on_progress_still_fires_for_a_segment_that_fails_to_compress(self, global_pool, tmp_path, monkeypatch):
        """A failed/skipped file must still advance progress - Registry.stop's aggregated "i of
        N" count would otherwise stall short of N if a file errors out."""
        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)
        make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        make_real_segment_file(global_pool, cold_dir / "segment_0000000001.blkseg")

        import blinkview.core.cold_archive as cold_archive_module

        real_compress = cold_archive_module.compress_cold_segment_file

        def flaky_compress(path, archive_dir):
            if path.name == "segment_0000000000.blkseg":
                raise OSError("simulated failure")
            return real_compress(path, archive_dir)

        monkeypatch.setattr(cold_archive_module, "compress_cold_segment_file", flaky_compress)

        calls = []
        compress_cold_storage_dir(cold_dir, on_progress=calls.append)

        assert calls == ["segment_0000000000.blkseg", "segment_0000000001.blkseg"]

    def test_empty_cold_dir_reports_no_progress(self, tmp_path):
        cold_dir = tmp_path / "session" / "cold"
        cold_dir.mkdir(parents=True)

        calls = []
        compress_cold_storage_dir(cold_dir, on_progress=calls.append)

        assert calls == []


class TestCountColdSegments:
    def test_counts_only_segment_files(self, global_pool, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg")
        make_real_segment_file(global_pool, cold_dir / "segment_0000000001.blkseg")
        (cold_dir / "id_registry.json").write_text("{}")

        assert count_cold_segments(cold_dir) == 2

    def test_missing_dir_counts_as_zero(self, tmp_path):
        assert count_cold_segments(tmp_path / "does-not-exist") == 0

    def test_empty_dir_counts_as_zero(self, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()

        assert count_cold_segments(cold_dir) == 0


class TestPooledLogBatchFromCompressedArchive:
    def test_row_content_matches_the_original_uncompressed_segment(self, global_pool, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg", row_count=50)
        archive_path = compress_cold_segment_file(src, archive_dir)

        segment = PooledLogBatch.from_compressed_archive(archive_path)
        try:
            assert segment.size == 50
            assert int(segment.bundle.timestamps[0]) == 100
            assert int(segment.bundle.timestamps[49]) == 149
            assert list(segment.bundle.sequences) == list(range(50))
        finally:
            segment.release()

        # Mounting from the archive must never write a decompressed copy back to disk.
        assert not (cold_dir / "segment_0000000000_restored.blkseg").exists()

    def test_default_metadata_is_built_from_the_archives_own_header(self, global_pool, tmp_path):
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg", row_count=10)
        archive_path = compress_cold_segment_file(src, archive_dir)

        segment = PooledLogBatch.from_compressed_archive(archive_path)
        try:
            # Points at the archive itself (the actual on-disk artifact to delete on eviction) -
            # a buffer-materialized segment never gets a raw cold/ file of its own.
            assert segment.metadata.path == str(archive_path)
            assert segment.metadata.first_seq == 0
            assert segment.metadata.last_seq == 9
        finally:
            segment.release()

    def test_cheap_properties_are_correct_without_touching_bundle_arrays(self, global_pool, tmp_path):
        """Mirrors test_cold_segment_format.py's equivalent for from_memmap - start_ts/end_ts/
        first_sequence_id/last_sequence_id must be populated from the header up front, not by
        indexing into (in this case, decompressed-buffer-backed) bundle arrays."""
        cold_dir = tmp_path / "cold"
        cold_dir.mkdir()
        archive_dir = tmp_path / "cold-archive"
        archive_dir.mkdir()
        src = make_real_segment_file(global_pool, cold_dir / "segment_0000000000.blkseg", row_count=5)
        archive_path = compress_cold_segment_file(src, archive_dir)

        segment = PooledLogBatch.from_compressed_archive(archive_path)
        try:
            assert segment.start_ts == 100
            assert segment.end_ts == 104
            assert segment.first_sequence_id == 0
            assert segment.last_sequence_id == 4
        finally:
            segment.release()

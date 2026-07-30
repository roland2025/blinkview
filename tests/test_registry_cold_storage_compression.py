# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Registry._compress_persisted_cold_storage - the Registry.stop() teardown step that shrinks a
persisted session's on-disk footprint by zstd-compressing its cold segment files (see
core/cold_archive.py, tested directly in tests/test_cold_archive.py). Tested the same way
tests/test_registry_cold_storage_id_persistence.py tests the sibling _dump_id_registry step: call
it directly against a manually-populated cold_dir, isolated from ConfigManager/factory wiring
concerns."""

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.cold_segment import write_cold_segment_file
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_registry


def make_real_segment_file(pool, path):
    batch = pool.create(
        PooledLogBatch,
        req_capacity=1,
        buffer_bytes=32,
        has_levels=True,
        has_modules=True,
        has_devices=True,
        has_sequences=True,
    )
    assert batch.insert(100, 100, b"hello", LogLevel.INFO.value, 0, 0, 0)
    write_cold_segment_file(path, batch.bundle)
    batch.release()
    return path


class TestCompressPersistedColdStorage:
    def test_compresses_segments_and_leaves_id_registry_json_alone(self, tmp_path):
        reg = make_real_registry(tmp_path, "compress_test")
        try:
            pool = NumpyArrayPool()
            cold_dir = tmp_path / "cold_out"
            cold_dir.mkdir()
            make_real_segment_file(pool, cold_dir / "segment_0000000000.blkseg")
            (cold_dir / "id_registry.json").write_text("{}")

            reg._compress_persisted_cold_storage(cold_dir)

            assert sorted(p.name for p in cold_dir.iterdir()) == ["id_registry.json"]
            archive_dir = tmp_path / "cold-archive"
            assert [p.name for p in archive_dir.iterdir()] == ["segment_0000000000.blkseg.zst"]
        finally:
            reg.stop()

    def test_empty_cold_dir_is_a_quiet_noop(self, tmp_path):
        reg = make_real_registry(tmp_path, "compress_empty_test")
        try:
            cold_dir = tmp_path / "cold_out_empty"
            cold_dir.mkdir()
            reg._compress_persisted_cold_storage(cold_dir)  # must not raise
            assert not (tmp_path / "cold-archive").exists()
        finally:
            reg.stop()

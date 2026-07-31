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

    def test_forwards_on_progress_to_compress_cold_storage_dir(self, tmp_path):
        reg = make_real_registry(tmp_path, "compress_progress_test")
        try:
            pool = NumpyArrayPool()
            cold_dir = tmp_path / "cold_out_progress"
            cold_dir.mkdir()
            make_real_segment_file(pool, cold_dir / "segment_0000000000.blkseg")
            make_real_segment_file(pool, cold_dir / "segment_0000000001.blkseg")

            calls = []
            reg._compress_persisted_cold_storage(cold_dir, on_progress=calls.append)

            assert calls == ["segment_0000000000.blkseg", "segment_0000000001.blkseg"]
        finally:
            reg.stop()


class TestRegistryStopProgressAggregation:
    """Registry.stop(on_progress=...) turns cold-storage's and each file logger's own
    per-file on_progress(label) callback into a single running (i, total, label) count - see
    plans/expressive-sauteeing-sun.md. Exercised against a real, running Registry (not just the
    cold-storage step in isolation above) since the total spans both compression stages."""

    def test_progress_reaches_i_equals_total_across_cold_storage_and_file_loggers(self, tmp_path):
        from blinkview.core.numpy_batch_manager import PooledLogBatch

        reg = make_real_registry(tmp_path, "stop_progress_test")
        reg.config.apply_patch("/central/cold_max_pieces", 4)
        reg.config.apply_patch("/central/max_pieces", 1)
        reg.start()

        device = reg.id_registry.get_device("progdev")
        module = device.get_module("chatty")
        array_pool = reg.system_ctx.array_pool

        base_ts = reg.now_ns()
        for b in range(6):
            batch = array_pool.create(
                PooledLogBatch, 500, 500 * 60, has_levels=True, has_modules=True, has_devices=True
            )
            with batch:
                for i in range(500):
                    ts = base_ts + (b * 500 + i) * 1000
                    batch.insert_any(
                        ts, ts, f"row {b}-{i}".encode("ascii"), level=0, module=module.id, device=device.id
                    )
                reg.central.put(batch.retain())

        import time

        time.sleep(1.0)

        calls = []
        reg.stop(on_progress=lambda i, total, label: calls.append((i, total, label)))

        assert len(calls) > 0, "expected at least the 'session' file logger to report progress"
        # Every call shares the same total, and the running index reaches it by the last call.
        totals = {c[1] for c in calls}
        assert len(totals) == 1
        assert calls[-1][0] == calls[-1][1]
        # Indices are a contiguous 1..total sequence, in order.
        assert [c[0] for c in calls] == list(range(1, calls[-1][1] + 1))

    def test_no_progress_callback_does_not_change_behavior(self, tmp_path):
        reg = make_real_registry(tmp_path, "stop_no_progress_test")
        reg.start()

        reg.stop()  # must not raise with on_progress left at its default (None)

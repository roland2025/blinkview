# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.central_storage import BaseCentralStorage, CentralFactory, CentralStorage
from blinkview.core.factory import BaseFactory
from blinkview.core.limits import CENTRAL_STORAGE_COLD_MAX_PIECES
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch


class FakeTasks:
    """Duck-typed stand-in for TaskManager's run_periodic/stop_periodic, avoiding the real
    TaskManager's background scheduler thread - same pattern as tests/test_uart_reader.py's
    FakeTasks. run_periodic does NOT actually invoke func on a timer here; HotTierMemoryGovernor
    tests that need real tick behavior call governor.tick()/._tick_safe() directly instead."""

    def __init__(self):
        self.started = []
        self.stopped = []
        self._next_id = 0

    def run_periodic(self, interval, func, *args, **kwargs):
        self._next_id += 1
        task_id = f"task-{self._next_id}"
        self.started.append((task_id, interval, func))
        return task_id

    def stop_periodic(self, task_id):
        self.stopped.append(task_id)


def make_storage(replay_source_dir=None, **config_overrides):
    storage = CentralStorage()
    storage.logger = PrintLogger("test.central_storage")
    # Cold storage is enabled by default now, and its default directory (when cold_storage_dir
    # isn't overridden) is resolved from the session's own FileManager.session_dir - fake one up
    # so tests that don't care about cold storage specifically don't have to plumb it through.
    # replay_source_dir=None matches a real (non-replay) FileManager's default; pass a Path to
    # simulate a run launched straight into replay (see CentralStorage._resolve_cold_storage_dir).
    session_dir = Path(tempfile.mkdtemp(prefix="test_central_storage_session_"))
    file_manager = SimpleNamespace(session_dir=session_dir, replay_source_dir=replay_source_dir)
    storage.shared = SimpleNamespace(
        array_pool=NumpyArrayPool(),
        registry=SimpleNamespace(file_manager=file_manager),
        tasks=FakeTasks(),
    )
    storage.apply_config(config_overrides)
    return storage


def make_batch(pool, msg=b"hello"):
    batch = pool.create(PooledLogBatch, 8, 256)
    batch.insert(100, 100, msg)
    return batch


class QueueParser:
    def __init__(self):
        self.queue: "queue.Queue[bytes]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                self.queue.put(bytes(msg))


class TestDefaults:
    def test_default_config_values(self):
        storage = make_storage()
        assert storage.maxlen > 0
        assert storage.max_pieces > 0
        assert storage.buffer_size_mb > 0

    def test_enabled_defaults_to_true_via_hydrate_config(self):
        storage = make_storage()

        hydrated = storage.hydrate_config({})
        try:
            storage.apply_config(hydrated)
            assert storage.enabled is True
        finally:
            storage.log_pool.release_all()

    def test_base_central_storage_is_a_base_daemon_subclass_with_factory(self):
        from blinkview.core.base_daemon import BaseDaemon

        assert issubclass(BaseCentralStorage, BaseDaemon)
        assert issubclass(CentralFactory, BaseFactory)
        assert CentralFactory.produces_type is BaseCentralStorage


class TestApplyConfig:
    def test_creates_log_pool_on_first_apply(self):
        storage = make_storage()
        assert storage.log_pool is not None

    def test_reapplying_config_updates_existing_log_pool_instead_of_recreating(self):
        storage = make_storage()
        pool = storage.log_pool

        storage.apply_config({"max_pieces": storage.max_pieces + 1})

        assert storage.log_pool is pool
        assert storage.log_pool.max_pieces == storage.max_pieces

    def test_cold_storage_enabled_by_default(self):
        storage = make_storage()
        try:
            assert storage.log_pool._archiver is not None
            assert storage.log_pool.cold_max_pieces == CENTRAL_STORAGE_COLD_MAX_PIECES
            # Default cold_storage_dir resolves to a "cold" subfolder of the session's own log
            # folder (see CentralStorage._resolve_cold_storage_dir), not an OS temp directory.
            assert storage.log_pool._archiver._dir == storage.shared.registry.file_manager.session_dir / "cold"
        finally:
            storage.log_pool.release_all()

    def test_cold_storage_can_be_disabled(self):
        storage = make_storage(cold_storage_enabled=False)
        assert storage.log_pool._archiver is None
        assert storage.log_pool.cold_max_pieces == 0

    def test_cold_storage_enabled_creates_archiver_and_temp_dir(self, tmp_path):
        storage = make_storage(cold_storage_enabled=True, cold_max_pieces=3, cold_storage_dir=str(tmp_path))

        try:
            assert storage.log_pool._archiver is not None
            assert storage.log_pool.cold_max_pieces == 3
            # A fresh, uniquely-named subdirectory is created under the configured dir - not the
            # configured dir itself (see CentralStorage._resolve_cold_storage_dir) - so an eventual
            # cleanup rmtree can never touch anything the caller already had in tmp_path.
            created_dirs = list(tmp_path.iterdir())
            assert len(created_dirs) == 1
            assert created_dirs[0].is_dir()
            assert storage.log_pool._archiver._dir == created_dirs[0]
        finally:
            storage.log_pool.release_all()

    def test_cold_storage_uses_replay_source_dir_when_launched_into_replay(self, tmp_path):
        """Regression test: a run launched straight into replay (ui/run.py's replay_mode/
        replay_session_info path) sets FileManager.replay_source_dir before Registry.
        configure_system() builds CentralStorage - cold storage must land under that *original*
        session's folder, not this run's own (lazily-created, otherwise-never-materialized -
        see FileManager.__init__'s `create=not replay_mode`) session_dir. Resolving against
        session_dir here used to `mkdir(parents=True)` that folder into existence purely as a
        side effect, which is exactly the "phantom session folder" a replay-only launch must
        avoid creating."""
        old_session_dir = tmp_path / "old_session"
        old_session_dir.mkdir()

        storage = make_storage(replay_source_dir=old_session_dir)
        try:
            assert storage.log_pool._archiver._dir == old_session_dir / "cold"
            # Nothing should have been created under this run's own live session_dir (the
            # fixture pre-creates session_dir itself via tempfile.mkdtemp as a stand-in for a
            # real FileManager's path - the regression is specifically about not creating a
            # "cold" subfolder under it, which is what used to happen unconditionally).
            assert not (storage.shared.registry.file_manager.session_dir / "cold").exists()
        finally:
            storage.log_pool.release_all()

    def test_mounts_a_compressed_cold_archive_from_a_previous_persisted_replay_source(self, tmp_path):
        """A previous run with cold_storage_persist_on_close + the compress-on-close step
        (Registry._compress_persisted_cold_storage - see core/cold_archive.py) leaves a session's
        cold segments as cold-archive/*.blkseg.zst, not raw cold/*.blkseg. Replaying that session
        must mount them straight from the compressed archives (CircularLogPool.
        _mount_existing_cold_segments -> PooledLogBatch.from_compressed_archive) without ever
        writing a decompressed copy back to cold/ - see CentralStorage._resolve_cold_storage_dir."""
        from blinkview.core.array_pool import NumpyArrayPool
        from blinkview.core.cold_archive import compress_cold_segment_file
        from blinkview.core.cold_segment import write_cold_segment_file
        from blinkview.core.numpy_batch_manager import PooledLogBatch
        from blinkview.utils.log_level import LogLevel

        old_session_dir = tmp_path / "old_session"
        raw_dir = tmp_path / "staging"  # scratch location, not itself part of old_session_dir
        raw_dir.mkdir()
        pool = NumpyArrayPool()
        batch = pool.create(
            PooledLogBatch, req_capacity=1, buffer_bytes=32, has_levels=True, has_modules=True, has_devices=True,
            has_sequences=True,
        )
        assert batch.insert(100, 100, b"hello", LogLevel.INFO.value, 0, 0, 0)
        raw_path = raw_dir / "segment_0000000000.blkseg"
        write_cold_segment_file(raw_path, batch.bundle)
        batch.release()

        archive_dir = old_session_dir / "cold-archive"
        archive_dir.mkdir(parents=True)
        compress_cold_segment_file(raw_path, archive_dir)

        storage = make_storage(replay_source_dir=old_session_dir)
        try:
            assert storage.log_pool.resumed_from_existing_cold_storage is True
            assert len(storage.log_pool.cold_segments) == 1
            assert int(storage.log_pool.cold_segments[0].bundle.sequences[0]) == 0
            # No decompressed copy is ever written back to cold/ - the whole point.
            assert not (old_session_dir / "cold" / "segment_0000000000.blkseg").exists()
        finally:
            storage.log_pool.release_all()

    def test_reapplying_config_can_grow_cold_max_pieces_live(self, tmp_path):
        storage = make_storage(cold_storage_enabled=True, cold_max_pieces=2, cold_storage_dir=str(tmp_path))

        try:
            storage.apply_config({"cold_storage_enabled": True, "cold_max_pieces": 5})
            assert storage.log_pool.cold_max_pieces == 5
        finally:
            storage.log_pool.release_all()


class TestMemoryGovernor:
    def test_disabled_by_default_no_governor_created(self):
        storage = make_storage()
        try:
            assert storage.memory_governor is None
        finally:
            storage.log_pool.release_all()

    def test_enabling_without_cold_storage_refuses_to_start_governor(self):
        """Regression test: shrinking the hot tier without cold storage enabled would silently
        drop evicted data instead of archiving it (see CircularLogPool._evict_hot_segment) - the
        opposite of what auto-memory-management is for. apply_config() must log a warning and
        leave the governor off rather than starting it."""
        storage = make_storage(auto_memory_management_enabled=True, cold_storage_enabled=False)
        try:
            assert storage.memory_governor is None
        finally:
            storage.log_pool.release_all()

    def test_enabling_with_cold_storage_starts_governor(self, tmp_path):
        storage = make_storage(
            auto_memory_management_enabled=True,
            cold_storage_enabled=True,
            cold_max_pieces=2,
            cold_storage_dir=str(tmp_path),
        )
        try:
            assert storage.memory_governor is not None
            assert storage.memory_governor._task_id is not None
            # Registered on the shared TaskManager, not a dedicated thread of its own.
            assert storage.shared.tasks.started == [
                (storage.memory_governor._task_id, storage.memory_poll_interval_sec, storage.memory_governor._tick_safe)
            ]
        finally:
            storage.stop()
            storage.log_pool.release_all()

    def test_disabling_after_enabled_stops_governor(self, tmp_path):
        storage = make_storage(
            auto_memory_management_enabled=True,
            cold_storage_enabled=True,
            cold_max_pieces=2,
            cold_storage_dir=str(tmp_path),
        )
        try:
            assert storage.memory_governor is not None
            storage.apply_config({"auto_memory_management_enabled": False})
            assert storage.memory_governor is None
        finally:
            storage.log_pool.release_all()

    def test_stop_tears_down_governor(self, tmp_path):
        storage = make_storage(
            auto_memory_management_enabled=True,
            cold_storage_enabled=True,
            cold_max_pieces=2,
            cold_storage_dir=str(tmp_path),
        )
        try:
            governor = storage.memory_governor
            assert governor is not None
            task_id = governor._task_id
            storage.stop()
            assert storage.memory_governor is None
            assert governor._task_id is None
            assert task_id in storage.shared.tasks.stopped
        finally:
            storage.log_pool.release_all()


class TestRun:
    def test_ingested_batches_are_appended_to_the_log_pool_and_distributed(self):
        storage = make_storage(maxlen=100, max_pieces=4, buffer_size_mb=1)
        storage.enabled = True

        subscriber = QueueParser()
        storage.subscribe(subscriber)

        batch = make_batch(storage.shared.array_pool, msg=b"payload")
        storage.put(batch)

        storage.start()
        try:
            deadline = time.time() + 5.0
            received = None
            while time.time() < deadline:
                try:
                    received = subscriber.queue.get(timeout=0.1)
                    break
                except queue.Empty:
                    continue
        finally:
            storage.stop()

        assert received == b"payload"

        total, _max_total, _seq = storage.log_pool.get_counts()
        assert total >= 1

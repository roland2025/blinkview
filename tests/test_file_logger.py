# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import pytest

from blinkview.core.logger import PrintLogger
from blinkview.storage.file_logger import BaseFileLogger, FileLogger, FileLoggerFactory


class FakeBatchProcessor:
    def __init__(self, data=b""):
        self.processed = []
        self._data = data

    def process(self, batch):
        self.processed.append(batch)

    def get_data(self):
        return memoryview(self._data)


class FakeFileManager:
    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.metadata = {"loggers": {}}
        self.added = []
        self.stats_calls = []
        self.write_metadata_calls = 0

    def add_file_logger(self, file_logger):
        self.added.append(file_logger)
        self.metadata["loggers"][file_logger.local.logging_id] = {"last_part": 0}

    def get_path_for_log(self, file_logger, part=0):
        return self.tmp_path / f"{file_logger.local.logging_id}_{part}.bin"

    def update_logger_stats(self, file_logger, bytes_written, absolute=False):
        self.stats_calls.append((bytes_written, absolute))

    def write_metadata(self):
        self.write_metadata_calls += 1


class FakeFactories:
    def __init__(self, processor):
        self._processor = processor
        self.build_calls = []

    def build(self, category, config, system_ctx=None, **kwargs):
        self.build_calls.append((category, config, system_ctx))
        return self._processor


class FakeTaskManager:
    """run_task executes synchronously (unlike the real TaskManager's thread pool) - fine for
    tests, which only care that rotation-time compression happened, not that it happened
    off-thread."""

    def __init__(self):
        self.run_task_calls = []

    def run_task(self, func, *args, **kwargs):
        self.run_task_calls.append((func, args, kwargs))
        func(*args, **kwargs)


@pytest.fixture
def file_manager(tmp_path):
    return FakeFileManager(tmp_path)


def make_file_logger(file_manager, processor=None, **config_overrides):
    processor = processor or FakeBatchProcessor()
    logger = FileLogger()
    logger.logger = PrintLogger("test.file_logger")
    logger.local = SimpleNamespace(logging_id="log-1")
    logger.shared = SimpleNamespace(
        factories=FakeFactories(processor),
        registry=SimpleNamespace(file_manager=file_manager),
        tasks=FakeTaskManager(),
    )
    logger.apply_config({"processor": {"type": "log_row"}, "name": "test-log", **config_overrides})
    return logger, processor


def test_apply_config_builds_batch_processor_via_factories(file_manager):
    logger, processor = make_file_logger(file_manager)

    assert logger.batch_processor is processor
    assert logger.process_batch == processor.process
    category, config, system_ctx = logger.shared.factories.build_calls[0]
    assert category == "logging_processor"
    assert config == {"type": "log_row"}
    assert system_ctx is logger.shared


def test_apply_config_registers_with_file_manager(file_manager):
    logger, _ = make_file_logger(file_manager)

    assert file_manager.added == [logger]


def test_open_file_creates_file_and_reports_zero_size_for_a_new_file(file_manager):
    logger, _ = make_file_logger(file_manager)

    size = logger.open_file()

    assert logger.file_path == file_manager.tmp_path / "log-1_0.bin"
    assert logger.file_path.exists()
    assert size == 0
    assert logger.file_handle is not None
    assert file_manager.stats_calls == [(0, True)]

    logger.file_handle.close()


def test_open_file_closes_the_previous_handle_before_reopening(file_manager):
    logger, _ = make_file_logger(file_manager)
    logger.open_file()
    first_handle = logger.file_handle

    logger.open_file()

    assert first_handle.closed
    assert logger.file_handle is not first_handle

    logger.file_handle.close()


def test_open_file_with_increment_part_index_advances_part_and_updates_metadata(file_manager):
    logger, _ = make_file_logger(file_manager)
    logger.open_file()
    logger.file_handle.close()

    logger.open_file(increment_part_index=True)

    assert logger.part_index == 1
    assert logger.file_path == file_manager.tmp_path / "log-1_1.bin"
    assert file_manager.metadata["loggers"]["log-1"]["last_part"] == 1
    assert file_manager.write_metadata_calls == 1

    logger.file_handle.close()


def test_flush_is_a_noop_when_there_is_no_open_file(file_manager):
    logger, _ = make_file_logger(file_manager)

    assert logger._flush() == 0
    assert file_manager.stats_calls == []


def test_flush_is_a_noop_when_the_processor_has_no_data(file_manager):
    logger, processor = make_file_logger(file_manager, processor=FakeBatchProcessor(data=b""))
    logger.open_file()

    assert logger._flush() == 0
    # only the open_file stats call, nothing from _flush
    assert file_manager.stats_calls == [(0, True)]

    logger.file_handle.close()


def test_flush_writes_processed_bytes_to_the_file_and_updates_stats(file_manager):
    logger, processor = make_file_logger(file_manager, processor=FakeBatchProcessor(data=b"hello"))
    logger.open_file()

    written = logger._flush()

    assert written == 5
    assert file_manager.stats_calls[-1] == (5, False)
    logger.file_handle.close()

    assert logger.file_path.read_bytes() == b"hello"


def test_set_batch_processor_updates_both_processor_and_process_batch(file_manager):
    logger, _ = make_file_logger(file_manager)
    new_processor = FakeBatchProcessor(data=b"x")

    logger.set_batch_processor(new_processor)

    assert logger.batch_processor is new_processor
    assert logger.process_batch == new_processor.process


def test_file_logger_is_a_base_file_logger():
    assert issubclass(FileLogger, BaseFileLogger)
    assert FileLoggerFactory.produces_type is BaseFileLogger


# ---------------------------------------------------------------------------
# zstandard compression - rotation and shutdown (see plans/expressive-sauteeing-sun.md)
# ---------------------------------------------------------------------------


def test_rotation_compresses_the_rotated_away_part_and_deletes_the_original(file_manager):
    logger, _ = make_file_logger(file_manager)
    logger.open_file()
    rotated_away_path = logger.file_path
    rotated_away_path.write_bytes(b"formatted log rows\n" * 200)
    logger.file_handle.close()

    logger.open_file(increment_part_index=True)
    logger.file_handle.close()

    archive_path = rotated_away_path.with_name(rotated_away_path.name + ".zst")
    assert archive_path.exists()
    assert not rotated_away_path.exists()

    from blinkview.core.zstd_file_compression import decompress_file_to_buffer

    assert bytes(decompress_file_to_buffer(archive_path)) == b"formatted log rows\n" * 200

    # Submitted through the (fake, synchronous) task manager - confirms this ran via the
    # background-offload path, not inline on the caller.
    assert len(logger.shared.tasks.run_task_calls) == 1


def test_rotation_of_an_empty_part_still_compresses_without_error(file_manager):
    """The very first open_file() -> immediate rotation case (no bytes ever written) - must not
    crash on a zero-byte source file."""
    logger, _ = make_file_logger(file_manager)
    logger.open_file()
    rotated_away_path = logger.file_path
    logger.file_handle.close()

    logger.open_file(increment_part_index=True)
    logger.file_handle.close()

    assert rotated_away_path.with_name(rotated_away_path.name + ".zst").exists()
    assert not rotated_away_path.exists()


class TestCloseAndCompressFinalPart:
    def test_compresses_the_final_part_and_advances_part_index(self, file_manager):
        logger, _ = make_file_logger(file_manager)
        logger.open_file()
        final_path = logger.file_path
        final_path.write_bytes(b"final stint content\n" * 50)

        logger._close_and_compress_final_part()

        archive_path = final_path.with_name(final_path.name + ".zst")
        assert archive_path.exists()
        assert not final_path.exists()
        assert logger.file_handle is None

        # part_index bumped so a subsequent restart's open_file() never reopens (and later
        # silently overwrites) this now-archived path.
        assert logger.part_index == 1
        assert file_manager.metadata["loggers"]["log-1"]["last_part"] == 1

    def test_restart_after_shutdown_compression_does_not_clobber_the_archive(self, file_manager):
        """Regression guard for the exact bug this part_index bump prevents: without it, a
        restart's open_file() would recreate a file at the same (now-deleted) path, and a later
        compression pass would silently overwrite the first stint's archive."""
        logger, _ = make_file_logger(file_manager)
        logger.open_file()
        first_stint_path = logger.file_path
        first_stint_content = b"first stint content\n" * 50
        first_stint_path.write_bytes(first_stint_content)

        logger._close_and_compress_final_part()
        archive_path = first_stint_path.with_name(first_stint_path.name + ".zst")

        # Simulate a restart: open_file() again (no explicit increment - mirrors run()'s own
        # bare self.open_file() at start), write new content, and compress again.
        logger.open_file()
        assert logger.file_path != first_stint_path  # a genuinely new part, not a reopen
        logger.file_path.write_bytes(b"second stint content\n" * 50)
        logger._close_and_compress_final_part()

        from blinkview.core.zstd_file_compression import decompress_file_to_buffer

        assert bytes(decompress_file_to_buffer(archive_path)) == first_stint_content

    def test_does_nothing_when_the_final_part_is_empty(self, file_manager):
        logger, _ = make_file_logger(file_manager)
        logger.open_file()
        empty_path = logger.file_path

        logger._close_and_compress_final_part()

        assert empty_path.exists()  # left as a plain empty file, not compressed
        assert not empty_path.with_name(empty_path.name + ".zst").exists()
        assert logger.part_index == 0  # never bumped - nothing was archived


def _wait_until(predicate, timeout=5.0):
    import time

    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        time.sleep(0.01)


def _archive_bytes(path):
    from blinkview.core.zstd_file_compression import decompress_file_to_buffer

    return bytes(decompress_file_to_buffer(path.with_name(path.name + ".zst")))


# ---------------------------------------------------------------------------
# End-to-end behaviour with the real batch processors
# ---------------------------------------------------------------------------


@pytest.fixture
def id_registry():
    from blinkview.core.array_pool import NumpyArrayPool
    from blinkview.core.id_registry.registry import IDRegistry

    return IDRegistry(NumpyArrayPool())


def _real_processor(kind, id_registry):
    """A real registered batch processor ("log_row" = the unified log's text format, "binary" =
    raw source logs), wired with just the shared context it reads."""
    from blinkview.core.array_pool import NumpyArrayPool
    from blinkview.storage.file_logger import BinaryBatchProcessor, LogRowBatchProcessor

    processor = {"log_row": LogRowBatchProcessor, "binary": BinaryBatchProcessor}[kind]()
    processor.shared = SimpleNamespace(array_pool=NumpyArrayPool(), id_registry=id_registry)
    return processor


def _row_batch(id_registry, marker: bytes, ts: int):
    """A one-row PooledLogBatch like the ones central storage / sources hand to a FileLogger."""
    from blinkview.core.array_pool import NumpyArrayPool
    from blinkview.core.numpy_batch_manager import PooledLogBatch

    module = id_registry.get_device("fldev").get_module("mod")
    batch = NumpyArrayPool().create(PooledLogBatch, 1, 256, has_levels=True, has_modules=True, has_devices=True)
    batch.insert_any(ts, ts, marker, level=0, module=module.id, device=module.device.id)
    return batch


def _assert_markers_in_order(data: bytes, markers):
    positions = [data.find(m) for m in markers]
    missing = [m for m, pos in zip(markers, positions) if pos < 0]
    assert not missing, f"missing from the log file: {missing}"
    assert positions == sorted(positions), "rows written out of order"


MARKERS = [f"marker-{i}".encode() for i in range(5)]
PROCESSOR_KINDS = ["log_row", "binary"]


@pytest.mark.parametrize("kind", PROCESSOR_KINDS)
class TestWithRealProcessors:
    """FileLogger's contract with a real batch processor: every row of every batch it's given
    reaches the file - regardless of how many batches get processed between two flushes
    (run() only flushes every max_batch rows or flush_interval seconds). Fakes elsewhere in this
    file accumulate on their own, so they can't catch a processor that loses data."""

    def test_every_batch_processed_before_a_flush_is_written(self, kind, file_manager, id_registry):
        logger, _ = make_file_logger(file_manager, _real_processor(kind, id_registry))
        logger.open_file()

        for i, marker in enumerate(MARKERS):
            with _row_batch(id_registry, marker, ts=1_700_000_000_000_000_000 + i) as batch:
                logger.process_batch(batch)
        logger._flush()
        logger.file_handle.close()

        _assert_markers_in_order(logger.file_path.read_bytes(), MARKERS)

    def test_flushing_between_batches_writes_each_batch_once(self, kind, file_manager, id_registry):
        logger, _ = make_file_logger(file_manager, _real_processor(kind, id_registry))
        logger.open_file()

        for i, marker in enumerate(MARKERS):
            with _row_batch(id_registry, marker, ts=1_700_000_000_000_000_000 + i) as batch:
                logger.process_batch(batch)
            logger._flush()
        logger.file_handle.close()

        data = logger.file_path.read_bytes()
        _assert_markers_in_order(data, MARKERS)
        assert all(data.count(m) == 1 for m in MARKERS), "a flushed batch was written again"

    def test_unflushed_bytes_survive_the_buffer_growing(self, kind, file_manager, id_registry):
        """Many large rows before one flush force the processor's buffer to grow (repeatedly)
        while it still holds unflushed rows - those must be carried over, not dropped."""
        processor = _real_processor(kind, id_registry)
        logger, _ = make_file_logger(file_manager, processor)
        logger.open_file()

        markers = [f"big-{i:02d}-".encode() + b"x" * 200 for i in range(40)]
        sizes = set()
        for i, marker in enumerate(markers):
            with _row_batch(id_registry, marker, ts=1_700_000_000_000_000_000 + i) as batch:
                logger.process_batch(batch)
            sizes.add(processor._buffer_size)
        logger._flush()
        logger.file_handle.close()

        assert len(sizes) > 1, "buffer never grew - test doesn't exercise the growth path"
        _assert_markers_in_order(logger.file_path.read_bytes(), markers)

    def test_running_logger_writes_every_queued_batch_by_stop(self, kind, file_manager, id_registry):
        """The real thread path: batches arrive faster than max_batch/flush_interval would ever
        trigger a flush, so everything is written by run()'s final flush at stop()."""
        logger, _ = make_file_logger(
            file_manager, _real_processor(kind, id_registry), enabled=True, flush_interval=3600
        )
        logger.start()
        try:
            for i, marker in enumerate(MARKERS):
                with _row_batch(id_registry, marker, ts=1_700_000_000_000_000_000 + i) as batch:
                    logger.put(batch)
            _wait_until(lambda: logger.input_queue.get_stats()["total"] == 0)
        finally:
            logger.stop()

        _assert_markers_in_order(_archive_bytes(logger.file_path.with_name("log-1_0.bin")), MARKERS)

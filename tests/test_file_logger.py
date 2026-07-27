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


@pytest.fixture
def file_manager(tmp_path):
    return FakeFileManager(tmp_path)


def make_file_logger(file_manager, processor=None, **config_overrides):
    processor = processor or FakeBatchProcessor()
    logger = FileLogger()
    logger.logger = PrintLogger("test.file_logger")
    logger.local = SimpleNamespace(logging_id="log-1")
    logger.shared = SimpleNamespace(
        factories=FakeFactories(processor), registry=SimpleNamespace(file_manager=file_manager)
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

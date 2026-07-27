# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.central_storage import BaseCentralStorage, CentralFactory, CentralStorage
from blinkview.core.factory import BaseFactory
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch


def make_storage(**config_overrides):
    storage = CentralStorage()
    storage.logger = PrintLogger("test.central_storage")
    storage.shared = SimpleNamespace(array_pool=NumpyArrayPool())
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
        storage = CentralStorage()
        storage.logger = PrintLogger("test.central_storage")
        storage.shared = SimpleNamespace(array_pool=NumpyArrayPool())

        hydrated = storage.hydrate_config({})
        storage.apply_config(hydrated)

        assert storage.enabled is True

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

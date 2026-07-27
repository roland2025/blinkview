# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.parsers.module_gen import ModuleGenParser


def make_parser(id_registry, **config_overrides):
    parser = ModuleGenParser()
    parser.logger = PrintLogger("test.module_gen_parser")
    parser.shared = SimpleNamespace(
        array_pool=NumpyArrayPool(),
        time_ns=time.time_ns,
        factories=SimpleNamespace(build=lambda *args, **kwargs: None),
    )
    parser.local = SimpleNamespace(device_id=id_registry.get_device("module_gen_test"))
    parser.apply_config(config_overrides)
    return parser


class QueueParser:
    """Minimal stand-in for a real downstream subscriber - the object BaseDaemon.distribute()
    calls .put(batch) on. Collects each row's module/level/msg for the test to inspect."""

    def __init__(self):
        self.queue: "queue.Queue[tuple]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, *_rest in batch:
                self.queue.put(bytes(msg))


def drain_until(q: "queue.Queue", count: int, timeout: float) -> list:
    collected = []
    deadline = time.time() + timeout
    while len(collected) < count:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        try:
            collected.append(q.get(timeout=remaining))
        except queue.Empty:
            break
    return collected


class TestDefaults:
    def test_default_modules_per_second(self, id_registry):
        parser = make_parser(id_registry)
        assert parser.modules_per_second == 200


class TestRun:
    def test_generates_and_distributes_synthetic_log_rows(self, id_registry):
        parser = make_parser(id_registry, modules_per_second=1000, delay=10, max_batch=50)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        parser.start()
        try:
            rows = drain_until(subscriber.queue, count=5, timeout=5.0)
        finally:
            parser.stop()

        assert len(rows) >= 5

    def test_module_counter_increments_across_runs(self, id_registry):
        parser = make_parser(id_registry, modules_per_second=1000, delay=10, max_batch=50)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        parser.start()
        try:
            drain_until(subscriber.queue, count=5, timeout=5.0)
        finally:
            parser.stop()

        assert parser.module_counter >= 5

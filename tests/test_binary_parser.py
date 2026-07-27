# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.factory_registry import FactoryRegistry
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.binary_parser import BinaryParser
from blinkview.parsers.frame_decoders import FrameDecoderFactory
from blinkview.parsers.frame_parsers import FrameParserFactory, FrameSectionParserFactory
from blinkview.utils.log_level import LogLevel


def make_shared(id_registry):
    registry = FactoryRegistry()
    registry.register("frame_decoder", FrameDecoderFactory)
    registry.register("frame_parser", FrameParserFactory)
    registry.register("frame_section_parser", FrameSectionParserFactory)
    return SimpleNamespace(
        array_pool=NumpyArrayPool(),
        time_ns=time.time_ns,
        factories=registry,
        id_registry=id_registry,
    )


def make_parser(id_registry, device_name="binary_parser_test", **config_overrides):
    parser = BinaryParser()
    parser.logger = PrintLogger("test.binary_parser")
    parser.shared = make_shared(id_registry)
    parser.local = SimpleNamespace(device_id=id_registry.get_device(device_name))
    config = {
        "frame_decoder": {"type": "line_decoder"},
        "frame_parser": {"type": "default", "steps": []},
        "delay": 20,
    }
    config.update(config_overrides)
    parser.apply_config(config)
    return parser


class QueueParser:
    def __init__(self):
        self.queue: "queue.Queue[bytes]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, _rx_ts, level, module, *_rest in batch:
                self.queue.put((bytes(msg), level, module))


def drain(q, count, timeout=5.0):
    items = []
    deadline = time.time() + timeout
    while len(items) < count and time.time() < deadline:
        try:
            items.append(q.get(timeout=max(0.0, deadline - time.time())))
        except queue.Empty:
            break
    return items


class TestApplyConfig:
    def test_builds_frame_codec_and_frame_parser_via_the_real_factories(self, id_registry):
        parser = make_parser(id_registry)

        assert parser._frame_codec is not None
        assert parser._frame_parser is not None

    def test_sync_state_is_created_once_and_kept_across_reapply(self, id_registry):
        parser = make_parser(id_registry)
        first_sync_state = parser.sync_state

        parser.apply_config(
            {"frame_decoder": {"type": "line_decoder"}, "frame_parser": {"type": "default", "steps": []}}
        )

        assert parser.sync_state is first_sync_state

    def test_apply_config_marks_thread_needs_restart(self, id_registry):
        parser = make_parser(id_registry)
        assert parser.thread_needs_restart is True


class TestNameChanged:
    def test_updates_the_device_identity_name(self, id_registry):
        parser = make_parser(id_registry)
        device = parser.local.device_id

        parser.apply_config(
            {
                "frame_decoder": {"type": "line_decoder"},
                "frame_parser": {"type": "default", "steps": []},
                "name": "renamed-device",
            }
        )

        assert device.name == "renamed-device"


class TestRunRealIngestion:
    """Runs BinaryParser.run() for real: real line_decoder framing, real nb_process_batch_kernel
    dispatch, real (empty) parser pipeline. A throwaway priming frame is sent first on every
    fresh parser - see tests/test_ops_dispatch.py / the memory note on nb_process_batch_kernel's
    first-frame-dropped bug - real frames of interest are sent after it."""

    def test_decoded_lines_are_distributed_with_default_level_and_module(self, id_registry):
        parser = make_parser(id_registry, delay=20)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 256)
        batch.insert(1000, 1000, b"__priming__\n")
        batch.insert(1000, 1000, b"hello world\n")
        batch.insert(1000, 1000, b"second line\n")
        parser.put(batch)

        parser.start()
        try:
            rows = drain(subscriber.queue, count=2)
        finally:
            parser.stop()

        assert [msg for msg, _level, _module in rows] == [b"hello world", b"second line"]
        for _msg, level, module in rows:
            assert level == LogLevel.INFO.value
            assert module == device.get_module("log").id

    def test_multiple_batches_after_priming_all_get_delivered(self, id_registry):
        parser = make_parser(id_registry, delay=20)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        pool = parser.shared.array_pool
        priming = pool.create(PooledLogBatch, 8, 256)
        priming.insert(1000, 1000, b"__priming__\n")
        priming.insert(1000, 1000, b"first batch line\n")
        parser.put(priming)

        parser.start()
        try:
            first_rows = drain(subscriber.queue, count=1)

            second_batch = pool.create(PooledLogBatch, 8, 256)
            second_batch.insert(1000, 1000, b"second batch line\n")
            parser.put(second_batch)

            second_rows = drain(subscriber.queue, count=1)
        finally:
            parser.stop()

        assert [msg for msg, *_r in first_rows] == [b"first batch line"]
        assert [msg for msg, *_r in second_rows] == [b"second batch line"]

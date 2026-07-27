# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
from types import SimpleNamespace

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.parsers.can_parser import CantoolsParser
from blinkview.utils.log_level import LogLevel

DBC_CONTENT = """VERSION ""

NS_ :

BS_:

BU_: Vector__XXX

BO_ 256 TestMsg: 8 Vector__XXX
 SG_ Speed : 0|16@1+ (1,0) [0|65535] "" Vector__XXX
 SG_ Flag : 16|8@1+ (1,0) [0|255] "" Vector__XXX
"""


@pytest.fixture
def dbc_path(tmp_path):
    path = tmp_path / "test.dbc"
    path.write_text(DBC_CONTENT)
    return path


def make_parser(id_registry, dbc_path, device_name="can_test", **config_overrides):
    parser = CantoolsParser()
    parser.logger = PrintLogger("test.can_parser")
    parser.shared = SimpleNamespace(
        array_pool=NumpyArrayPool(),
        time_ns=time.time_ns,
        factories=SimpleNamespace(build=lambda *a, **k: None),
    )
    parser.local = SimpleNamespace(device_id=id_registry.get_device(device_name))
    config = {"dbc_file": str(dbc_path), "strict": False, "ignore_unknown": False}
    config.update(config_overrides)
    parser.apply_config(config)
    return parser


def make_can_batch(pool, can_id, data, ts_ns=1000):
    batch = pool.create(PooledLogBatch, 8, 256, has_ext_u32_1=True)
    batch.insert(ts_ns, ts_ns, bytes(data), ext_u32_1=can_id)
    return batch


class QueueParser:
    def __init__(self):
        self.queue: "queue.Queue[tuple]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, _rx_ts, level, module, *_rest in batch:
                self.queue.put((bytes(msg), level, module))


def drain_one(q, timeout=5.0):
    try:
        return q.get(timeout=timeout)
    except queue.Empty:
        return None


class TestApplyConfig:
    def test_loads_dbc_and_builds_msg_info_map(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path)

        assert parser.db is not None
        assert 256 in parser._msg_info_map
        info = parser._msg_info_map[256]
        assert info.name == "TestMsg"
        assert set(info.signal_map.keys()) == {"Speed", "Flag"}

    def test_signal_map_resolves_module_ids_via_device_identity(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path)
        device = parser.local.device_id

        info = parser._msg_info_map[256]
        assert info.signal_map["Speed"] == device.get_module("Speed").id
        assert info.signal_map["Flag"] == device.get_module("Flag").id

    def test_invalid_dbc_path_leaves_db_none(self, id_registry, tmp_path):
        parser = make_parser(id_registry, tmp_path / "does_not_exist.dbc")

        assert parser.db is None
        assert parser._msg_info_map == {}


class TestRunRealDecoding:
    def test_known_can_id_decodes_signals_into_separate_rows(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = make_can_batch(parser.shared.array_pool, can_id=256, data=[10, 0, 5, 0, 0, 0, 0, 0])
        parser.put(batch)

        parser.start()
        try:
            row1 = drain_one(subscriber.queue)
            row2 = drain_one(subscriber.queue)
        finally:
            parser.stop()

        received = {row1, row2}
        expected_speed_module = device.get_module("Speed").id
        expected_flag_module = device.get_module("Flag").id
        assert (b"10", LogLevel.INFO.value, expected_speed_module) in received
        assert (b"5", LogLevel.INFO.value, expected_flag_module) in received

    def test_unmapped_id_emits_unmapped_row_by_default(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20, ignore_unknown=False)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = make_can_batch(parser.shared.array_pool, can_id=0x999, data=[1, 2, 3])
        parser.put(batch)

        parser.start()
        try:
            row = drain_one(subscriber.queue)
        finally:
            parser.stop()

        assert row is not None
        msg, level, module = row
        assert b"UNMAPPED" in msg
        assert level == LogLevel.INFO.value
        assert module == device.get_module("unknown").id

    def test_ignore_unknown_suppresses_unmapped_rows(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20, ignore_unknown=True)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        unknown_batch = make_can_batch(parser.shared.array_pool, can_id=0x999, data=[1, 2, 3])
        parser.put(unknown_batch)

        # Follow up with a known message so we have something to positively wait on - if the
        # unknown one had produced a row, it would show up first, before this one.
        known_batch = make_can_batch(parser.shared.array_pool, can_id=256, data=[10, 0, 5, 0, 0, 0, 0, 0])
        parser.put(known_batch)

        parser.start()
        try:
            row1 = drain_one(subscriber.queue)
            row2 = drain_one(subscriber.queue)
        finally:
            parser.stop()

        received_msgs = {row1[0], row2[0]}
        assert received_msgs == {b"10", b"5"}

    def test_decode_failure_emits_an_error_row(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        # Empty payload - msg_info.decode() will raise inside cantools since the DBC expects 8
        # bytes of signal data.
        batch = make_can_batch(parser.shared.array_pool, can_id=256, data=[])
        parser.put(batch)

        parser.start()
        try:
            row = drain_one(subscriber.queue)
        finally:
            parser.stop()

        assert row is not None
        msg, level, module = row
        assert b"Decoding error" in msg
        assert level == LogLevel.ERROR.value
        assert module == device.get_module("unknown").id

    def test_strict_mode_raises_but_thread_survives_and_keeps_processing(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20, strict=True)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        unknown_batch = make_can_batch(parser.shared.array_pool, can_id=0x999, data=[1, 2, 3])
        parser.put(unknown_batch)

        known_batch = make_can_batch(parser.shared.array_pool, can_id=256, data=[10, 0, 5, 0, 0, 0, 0, 0])
        parser.put(known_batch)

        parser.start()
        try:
            # The unknown-id batch raises ValueError internally (caught by the outer per-batch
            # handler) and produces no row - the known-id batch afterward should still be
            # processed normally, proving the run() thread didn't die.
            row1 = drain_one(subscriber.queue)
            row2 = drain_one(subscriber.queue)
        finally:
            parser.stop()

        received_msgs = {row1[0], row2[0]}
        assert received_msgs == {b"10", b"5"}

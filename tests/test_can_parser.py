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


class RecordingLogger(PrintLogger):
    """PrintLogger that also keeps every (level, formatted message) it logs."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.records = []
        inner = self.log  # PrintLogger binds log per instance

        def log(level, fmt, *log_args):
            self.records.append((level, fmt % log_args if log_args else fmt))
            inner(level, fmt, *log_args)

        self.log = log

    def child(self, name, enabled=None, essential=None):
        # stats_child() loggers stay plain; only the parser's own logger records
        return PrintLogger(f"{self.ctx}.{name}")


def make_parser(id_registry, dbc_path, device_name="can_test", **config_overrides):
    parser = CantoolsParser()
    parser.logger = RecordingLogger("test.can_parser")
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

    @pytest.mark.parametrize("strict", [False, True])
    def test_unknown_id_mid_batch_keeps_the_frames_after_it(self, id_registry, dbc_path, strict):
        # Regression: strict mode used to raise inside the per-frame loop, so every frame after the
        # unknown ID in the same input batch was lost.
        parser = make_parser(id_registry, dbc_path, delay=20, strict=strict, ignore_unknown=True)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 256, has_ext_u32_1=True)
        batch.insert(1000, 1000, bytes([1, 0, 2, 0, 0, 0, 0, 0]), ext_u32_1=256)
        batch.insert(2000, 2000, bytes([1, 2, 3]), ext_u32_1=0x999)
        batch.insert(3000, 3000, bytes([3, 0, 4, 0, 0, 0, 0, 0]), ext_u32_1=256)
        parser.put(batch)

        expected = 5 if strict else 4
        parser.start()
        try:
            rows = [drain_one(subscriber.queue) for _ in range(expected)]
        finally:
            parser.stop()

        assert None not in rows
        assert {r[0] for r in rows if r[2] != device.get_module("unknown").id} == {b"1", b"2", b"3", b"4"}

        unknown_rows = [r for r in rows if r[2] == device.get_module("unknown").id]
        if strict:
            # strict overrides ignore_unknown: the unknown ID becomes an ERROR row
            assert len(unknown_rows) == 1
            msg, level, _ = unknown_rows[0]
            assert b"UNKNOWN CAN ID" in msg
            assert level == LogLevel.ERROR.value
        else:
            assert unknown_rows == []

    def test_repeated_decode_failures_log_once_per_can_id_but_emit_every_row(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20)
        parser.enabled = True

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        # Empty payloads fail to decode against the 8-byte TestMsg
        batch = parser.shared.array_pool.create(PooledLogBatch, 8, 256, has_ext_u32_1=True)
        for i in range(3):
            batch.insert(1000 + i, 1000 + i, b"", ext_u32_1=256)
        parser.put(batch)

        parser.start()
        try:
            rows = [drain_one(subscriber.queue) for _ in range(3)]
        finally:
            parser.stop()

        assert None not in rows
        assert all(b"Decoding error" in msg and level == LogLevel.ERROR.value for msg, level, _ in rows)

        failure_logs = [m for _lvl, m in parser.logger.records if "Decoding failed for CAN ID 0100" in m]
        assert len(failure_logs) == 1
        assert "Traceback" not in failure_logs[0]

    def test_row_too_big_for_an_empty_batch_is_logged_and_the_rest_still_flows(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path, delay=20)
        parser.enabled = True
        device = parser.local.device_id

        subscriber = QueueParser()
        parser.subscribe(subscriber)

        # Make Speed decode to a value far bigger than any output batch the tuner will hand out
        info = parser._msg_info_map[256]
        info.decode = lambda data: {"Speed": "x" * 64_000_000, "Flag": 7}

        batch = make_can_batch(parser.shared.array_pool, can_id=256, data=[0] * 8)
        parser.put(batch)

        parser.start()
        try:
            row = drain_one(subscriber.queue)
            extra = drain_one(subscriber.queue, timeout=0.3)
        finally:
            parser.stop()

        assert row == (b"7", LogLevel.INFO.value, device.get_module("Flag").id)
        assert extra is None
        assert any(lvl == LogLevel.WARN and "does not fit an empty batch" in m for lvl, m in parser.logger.records)

    def test_steady_input_faster_than_delay_still_flushes_within_delay(self, id_registry, dbc_path):
        # Regression: delay was only the get() timeout, so input arriving more often than `delay`
        # never let it fire and rows waited until the 128-row default batch filled (~1.3 s here).
        parser = make_parser(id_registry, dbc_path, delay=100)
        parser.enabled = True

        first_out = queue.Queue()

        class FirstOutput:
            def put(self, batch):
                with batch:
                    first_out.put(time.perf_counter())

        parser.subscribe(FirstOutput())
        parser.start()
        try:
            t0 = time.perf_counter()
            deadline = t0 + 0.8
            while time.perf_counter() < deadline and first_out.empty():
                parser.put(make_can_batch(parser.shared.array_pool, can_id=256, data=[1, 0, 2, 0, 0, 0, 0, 0]))
                time.sleep(0.02)
            first = drain_one(first_out, timeout=1.0)
        finally:
            parser.stop()

        assert first is not None
        # 100 ms delay plus generous scheduling slack; the old behaviour took over a second
        assert first - t0 < 0.4

    def test_reconfigure_swaps_in_a_new_msg_info_map(self, id_registry, dbc_path):
        parser = make_parser(id_registry, dbc_path)
        old_map = parser._msg_info_map

        parser.apply_config({"dbc_file": str(dbc_path), "strict": False, "ignore_unknown": False})

        # run() may still be reading old_map; it must not have been cleared in place
        assert parser._msg_info_map is not old_map
        assert 256 in old_map
        assert 256 in parser._msg_info_map

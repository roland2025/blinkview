# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""End-to-end: frames sent on a python-can virtual bus -> real CANReader source -> real
CantoolsParser pipeline -> a real Registry's central storage.

The unit tests in test_can_parser.py build the parser's input batch by hand. This one checks the
seam between io/can_bus.py and parsers/can_parser.py (the CAN ID travels in ext_u32_1) and that
the decoded rows really land in central storage, with an unknown ID and a decode failure in the
middle of the stream."""

import json
import time
import uuid

import can
import pytest

from blinkview.utils.log_level import LogLevel
from tests.fakes.real_registry import make_real_registry

DBC_CONTENT = """VERSION ""

NS_ :

BS_:

BU_: Vector__XXX

BO_ 256 TestMsg: 8 Vector__XXX
 SG_ Speed : 0|16@1+ (1,0) [0|65535] "" Vector__XXX
 SG_ Flag : 16|8@1+ (1,0) [0|255] "" Vector__XXX
"""

PIPELINE_NAME = "canp"


def _write_profile(tmp_path, channel, strict):
    dbc_path = tmp_path / "test.dbc"
    dbc_path.write_text(DBC_CONTENT)
    (tmp_path / "test_config.json").write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {
                    "src_can": {
                        "enabled": True,
                        "type": "can",
                        "name": "can",
                        "interface": "virtual",
                        "channel": channel,
                        "delay": 20,
                    }
                },
                "pipelines": {
                    "pipe_can": {
                        "enabled": True,
                        "type": "cantools",
                        "name": PIPELINE_NAME,
                        "sources_": ["src_can"],
                        "dbc_file": str(dbc_path),
                        "strict": strict,
                        "ignore_unknown": False,
                        "delay": 20,
                    }
                },
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )


def _rows_for_device(log_pool, device_id):
    rows = []
    with log_pool.get_snapshot() as segments:
        for seg in segments:
            for ts, msg, _rx_ts, level, module, device, *_rest in seg:
                if device == device_id:
                    rows.append((ts, bytes(msg), level, module))
    return rows


def _poll_rows(log_pool, device_id, done, timeout=10.0, tick=None):
    deadline = time.time() + timeout
    rows = []
    while time.time() < deadline:
        if tick is not None:
            tick()
        rows = _rows_for_device(log_pool, device_id)
        if done(rows):
            return rows
        time.sleep(0.05)
    pytest.fail(f"timed out after {timeout}s; rows for device {device_id}: {rows}")


def _test_msg(speed, flag):
    return bytes([speed & 0xFF, speed >> 8, flag, 0, 0, 0, 0, 0])


@pytest.mark.parametrize("strict", [False, True])
def test_virtual_bus_frames_decode_into_central_storage(tmp_path, strict):
    # Unique channel so python-can's in-process virtual bus doesn't leak frames between tests
    channel = f"test_can_e2e_{uuid.uuid4().hex}"
    _write_profile(tmp_path, channel, strict)

    reg = make_real_registry(tmp_path, "can_e2e", start=True)
    sender = can.Bus(interface="virtual", channel=channel)
    try:
        device = reg.id_registry.get_device(PIPELINE_NAME)
        speed_mod = device.get_module("Speed").id
        flag_mod = device.get_module("Flag").id
        unknown_mod = device.get_module("unknown").id
        log_pool = reg.central.log_pool

        # A virtual bus only delivers to buses already open, so keep sending a priming frame until
        # the reader has opened its bus and the row comes out the other end.
        def send_priming():
            sender.send(can.Message(arbitration_id=256, data=_test_msg(999, 0), is_extended_id=False))

        _poll_rows(log_pool, device.id, lambda rows: any(r[1] == b"999" for r in rows), tick=send_priming)

        frames = [
            (256, _test_msg(1, 2)),
            (0x7FF, bytes([0xAA, 0xBB])),
            (256, _test_msg(3, 4)),
            (256, b""),  # too short for TestMsg: decode failure
            (256, _test_msg(5, 6)),
        ]
        for arb_id, data in frames:
            sender.send(can.Message(arbitration_id=arb_id, data=data, is_extended_id=False))

        def seen(rows):
            msgs = {r[1] for r in rows}
            return b"6" in msgs and any(b"aabb" in m for m in msgs) and any(b"Decoding error" in m for m in msgs)

        rows = _poll_rows(log_pool, device.id, seen)
    finally:
        sender.shutdown()
        reg.stop()

    # Drop the priming rows (one or more 999/0 pairs, sent before the sequence)
    last_priming_ts = max(r[0] for r in rows if r[1] == b"999")
    rows = sorted(r for r in rows if r[0] > last_priming_ts)

    unknown_level = LogLevel.ERROR.value if strict else LogLevel.INFO.value
    unknown_msg = b"[07FF] UNKNOWN CAN ID | aabb" if strict else b"[07FF] UNMAPPED | aabb"

    decoded = [(msg, level, module) for _ts, msg, level, module in rows]
    assert decoded[:2] in (
        [(b"1", LogLevel.INFO.value, speed_mod), (b"2", LogLevel.INFO.value, flag_mod)],
        [(b"2", LogLevel.INFO.value, flag_mod), (b"1", LogLevel.INFO.value, speed_mod)],
    )
    assert decoded[2] == (unknown_msg, unknown_level, unknown_mod)
    assert set(decoded[3:5]) == {(b"3", LogLevel.INFO.value, speed_mod), (b"4", LogLevel.INFO.value, flag_mod)}
    msg, level, module = decoded[5]
    assert msg.startswith(b"[0100] TestMsg |") and b"Decoding error" in msg
    assert (level, module) == (LogLevel.ERROR.value, unknown_mod)
    assert set(decoded[6:]) == {(b"5", LogLevel.INFO.value, speed_mod), (b"6", LogLevel.INFO.value, flag_mod)}

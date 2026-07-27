# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import queue
import time
import uuid
from types import SimpleNamespace

import can
import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.logger import PrintLogger
from blinkview.io.can_bus import CANReader


def make_reader(**config_overrides):
    reader = CANReader()
    reader.logger = PrintLogger("test.can_bus")
    reader.apply_config(config_overrides)
    return reader


class QueueParser:
    """Minimal stand-in for a real downstream parser - the object BaseDaemon.distribute() calls
    .put(batch) on. Collects each row's raw bytes and CAN-specific ext columns."""

    def __init__(self):
        self.queue: "queue.Queue[tuple]" = queue.Queue()

    def put(self, batch):
        with batch:
            for _ts, msg, *rest in batch:
                # Row shape after consuming (ts, msg): rx_ts, level, module, device, seq,
                # ext_u32_1, ext_u32_2, ext_u64_1, pid, tid - arb_id/flags land in
                # ext_u32_1/ext_u32_2, i.e. rest[5]/rest[6].
                ext_u32_1 = rest[5]
                ext_u32_2 = rest[6]
                self.queue.put((bytes(msg), ext_u32_1, ext_u32_2))


@pytest.fixture
def virtual_channel():
    # Unique per-test channel name so the python-can in-process virtual-bus registry doesn't
    # leak messages between tests running in the same process.
    return f"test_ch_{uuid.uuid4().hex}"


def _drain_one(q: "queue.Queue", timeout=5.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return q.get(timeout=max(0.0, deadline - time.time()))
        except queue.Empty:
            continue
    return None


class TestDefaults:
    def test_default_config_values(self, virtual_channel):
        reader = make_reader(interface="virtual", channel=virtual_channel)
        assert reader.bitrate == 250000
        assert reader.delay == 50
        assert reader.log_rx_tx is False


class TestRealLoopbackIngestion:
    """Runs CANReader.run() for real against python-can's in-process 'virtual' interface - no
    OS-level vcan/socketcan setup needed, so this works cross-platform including on Windows."""

    def test_receives_a_real_can_message_end_to_end(self, virtual_channel):
        reader = make_reader(interface="virtual", channel=virtual_channel, delay=20)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        subscriber = QueueParser()
        reader.subscribe(subscriber)

        reader.start()
        try:
            # Give the reader's bus.recv() loop a moment to actually open the bus before we
            # inject a message from a second peer on the same virtual channel.
            time.sleep(0.2)

            peer = can.Bus(interface="virtual", channel=virtual_channel, receive_own_messages=False)
            try:
                peer.send(can.Message(arbitration_id=0x123, data=[1, 2, 3, 4], is_extended_id=False))
            finally:
                peer.shutdown()

            received = _drain_one(subscriber.queue, timeout=5.0)
        finally:
            reader.stop()

        assert received is not None
        data, arb_id, flags = received
        assert data == bytes([1, 2, 3, 4])
        assert arb_id == 0x123
        assert flags & 0x10  # Bit 4: Rx flag set

    def test_extended_id_flag_is_captured(self, virtual_channel):
        reader = make_reader(interface="virtual", channel=virtual_channel, delay=20)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        subscriber = QueueParser()
        reader.subscribe(subscriber)

        reader.start()
        try:
            time.sleep(0.2)

            peer = can.Bus(interface="virtual", channel=virtual_channel, receive_own_messages=False)
            try:
                peer.send(can.Message(arbitration_id=0xABCDE, data=[9], is_extended_id=True))
            finally:
                peer.shutdown()

            received = _drain_one(subscriber.queue, timeout=5.0)
        finally:
            reader.stop()

        assert received is not None
        data, arb_id, flags = received
        assert arb_id == 0xABCDE
        assert flags & 0x01  # Bit 0: Extended ID flag set

    def test_multiple_messages_are_batched_and_all_delivered(self, virtual_channel):
        reader = make_reader(interface="virtual", channel=virtual_channel, delay=20)
        reader.enabled = True
        reader.shared = SimpleNamespace(array_pool=NumpyArrayPool(), time_ns=time.time_ns)

        subscriber = QueueParser()
        reader.subscribe(subscriber)

        reader.start()
        try:
            time.sleep(0.2)

            peer = can.Bus(interface="virtual", channel=virtual_channel, receive_own_messages=False)
            try:
                for i in range(5):
                    peer.send(can.Message(arbitration_id=0x100 + i, data=[i], is_extended_id=False))
            finally:
                peer.shutdown()

            received_ids = set()
            deadline = time.time() + 5.0
            while len(received_ids) < 5 and time.time() < deadline:
                item = _drain_one(subscriber.queue, timeout=max(0.1, deadline - time.time()))
                if item is None:
                    break
                _data, arb_id, _flags = item
                received_ids.add(arb_id)
        finally:
            reader.stop()

        assert received_ids == {0x100, 0x101, 0x102, 0x103, 0x104}

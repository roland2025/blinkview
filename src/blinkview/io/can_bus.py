# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep
from typing import TYPE_CHECKING, Any

from blinkview.core.configurable import configuration_property
from blinkview.core.limits import BATCH_MAXLEN
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

if TYPE_CHECKING:
    from can import Bus, CanError, Message


class CanLogBatch(PooledLogBatch):
    """
    Specialized batch for CAN data.
    Maps arbitration_id to ext_u32_1 and packed flags to ext_u32_2.
    """

    __slots__ = ()

    def __init__(
        self,
        pool: Any,
        req_capacity: int,
        buffer_bytes: int,
    ):
        # Hardcode the schema for CAN data, ignoring other optional columns
        super().__init__(
            pool=pool,
            req_capacity=req_capacity,
            buffer_bytes=buffer_bytes,
            has_ext_u32_1=True,  # CAN Address
            has_ext_u32_2=True,  # CAN Flags (Packed)
        )

    def insert_can(self, msg: "Message", offset_ns: int) -> bool:
        """
        Inserts one python-can Message: the arbitration id goes to ext_u32_1, the packed flags to
        ext_u32_2, and msg.timestamp (seconds on the interface's clock) is projected to epoch ns
        with offset_ns. Plain Python, not a kernel: python-can's recv() hands over one frame at a
        time, and a <=64-byte row costs far less to write than a kernel call costs to make (see
        plans/bundle-push-fast-path.md stage 4).
        """
        ts_ns = offset_ns + int(msg.timestamp * 1_000_000_000)
        # Stored layout - nothing decodes it yet, but append new flags rather than reorder.
        flags = (
            (0x01 if msg.is_extended_id else 0)  # Extended vs Standard id
            | (0x02 if msg.is_remote_frame else 0)  # Remote Frame
            | (0x04 if msg.is_error_frame else 0)  # Error Frame
            | (0x08 if msg.is_fd else 0)  # CAN FD Frame
            | (0x10 if msg.is_rx else 0)  # Rx vs Tx
            | (0x20 if msg.bitrate_switch else 0)  # Bit Rate Switch
            | (0x40 if msg.error_state_indicator else 0)  # Error State Indicator
        )
        data = msg.data
        # level, module, device, seq are 0 for raw CAN ingress
        return self._push(ts_ns, ts_ns, data, len(data), 0, 0, 0, 0, msg.arbitration_id, flags, 0, 0, 0)


@DeviceFactory.register("can")
@configuration_property(
    "interface",
    type="string",
    default="virtual",
    required=True,
    ui_order=10,
    enum=["virtual", "socketcan", "pcan", "kvaser", "ixxat", "slcan"],
    description="The python-can interface to use (e.g., 'socketcan' for Linux, 'pcan' for PEAK, 'kvaser' for Kvaser).",
)
@configuration_property(
    "channel",
    type="string",
    default="vcan0",
    required=True,
    ui_order=11,
    description="The CAN channel or port name (e.g., 'vcan0', 'PCAN_USBBUS1', 'can0').",
)
@configuration_property(
    "bitrate",
    type="integer",
    default=250000,
    required=True,
    ui_order=12,
    description="The CAN bus communication speed in bits per second. Usually 250000, 500000, or 1000000.",
)
@configuration_property(
    "maxlen",
    type="integer",
    default=BATCH_MAXLEN,
    description="The maximum number of CAN messages to batch before flushing downstream.",
)
@configuration_property(
    "delay",
    type="integer",
    default=50,
    description="The maximum time (in milliseconds) to hold messages before flushing a batch.",
)
@configuration_property(
    "log_rx_tx",
    type="boolean",
    default=False,
    description="When enabled, dumps raw CAN frames (ID, DLC, DATA) to the system log for low-level protocol debugging.",
)
class CANReader(BaseReader):
    __doc__ = """The primary ingestion source for Controller Area Network (CAN) bus data.

* Wraps the standard 'python-can' library for cross-platform compatibility.
* Supports physical hardware interfaces (PCAN, IXXAT, SocketCAN) and virtual buses.
* Batches raw can.Message objects with high-precision nanosecond timestamps.
* Leverages high-performance PooledLogBatch with hardware address and flags attached via extension columns.
* Gracefully handles bus errors and auto-reconnects if the physical adapter drops.
"""

    type: str
    interface: str
    channel: str
    bitrate: int
    delay: int

    def __init__(self):
        super().__init__()
        self.bus: "Bus" = None

    def run(self):
        from can import Bus, CanError

        # Localize method lookups for the tight loop
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)

        logger.info("Starting CAN Reader Thread (%s:%s @ %sbps)", self.interface, self.channel, self.bitrate)

        _ts_offset_ns = None

        # Tuner setup: Estimating an average CAN message payload of 8 bytes
        stats = Speedometer(logger=self.logger.stats_child("stats"))
        tuner = ThroughputAutoTuner(speedometer=stats, msg_size_bytes=8, logger=self.logger.stats_child("tuner"))

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            # Request ext_u32_1 for CAN Address and ext_u32_2 for CAN Flags
            return pool_create(CanLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch: CanLogBatch = None
        bus = None

        try:
            while not stop_is_set():
                if bus is None:
                    try:
                        bus = Bus(interface=self.interface, channel=self.channel, bitrate=self.bitrate)
                        logger.info("CAN bus connected successfully.")
                    except Exception as e:
                        logger.error("Failed to open CAN bus '%s'.", self.channel, exc=e)
                        sleep(1.0)
                        continue

                    _recv = bus.recv  # Localize for performance

                if batch is None:
                    batch = batch_acquire()

                try:
                    now = time_ns()

                    # Dynamic Timeout Calculation
                    if batch.size > 0:
                        # How much time until we MUST flush?
                        remaining_ns = (batch.start_ts + delay_ns) - now
                        timeout_s = max(0.0, min(0.2, remaining_ns / 1_000_000_000.0))
                    else:
                        # No batch pending, just wake up periodically to check stop flag
                        timeout_s = 0.2

                    msg = _recv(timeout=timeout_s)

                    # Update time AFTER the blocking call
                    now = time_ns()

                    if msg:
                        # 1. One-time Clock Sync Logic
                        if _ts_offset_ns is None:
                            import time

                            if msg.timestamp < 1_000_000_000:  # Uptime
                                _ts_offset_ns = time_ns() - int(time.monotonic() * 1e9)
                            else:  # Epoch
                                _ts_offset_ns = 0

                        # 2. Clean, high-level insertion
                        if not batch.insert_can(msg, _ts_offset_ns):
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)
                            batch = batch_acquire()
                            batch.insert_can(msg, _ts_offset_ns)

                    # Time-based flush check
                    if batch.size > 0 and (now - batch.start_ts >= delay_ns):
                        with batch:
                            self.distribute(batch)
                            tuner.update(batch.msg_cursor, batch.size, delay_s)
                        batch = None

                except CanError as e:
                    logger.exception("CAN Bus Error detected.", exc=e)
                    if bus:
                        bus.shutdown()
                    bus = None
                    sleep(1.0)

                except Exception as e:
                    logger.exception("Unexpected error in CAN read loop.", exc=e)
                    if bus:
                        bus.shutdown()
                    bus = None
                    sleep(1.0)

        except Exception as e:
            logger.exception("Fatal error in CAN Reader loop", exc=e)
        finally:
            # Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()

            if bus:
                bus.shutdown()
                logger.info("CAN bus shut down.")

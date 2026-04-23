# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import time
from time import sleep
from typing import TYPE_CHECKING, Any

import numpy as np

from ..core import dtypes
from ..core.configurable import configuration_property
from ..core.limits import BATCH_MAXLEN
from ..core.numba_config import app_njit
from ..core.numpy_batch_manager import PooledLogBatch
from ..core.types.empty import EMPTY_BYTES
from ..ops.segments import _nb_bundle_push
from ..utils.throughput import Speedometer, ThroughputAutoTuner
from .BaseReader import BaseReader, DeviceFactory

if TYPE_CHECKING:
    from can import Bus, CanError, Message


@app_njit()
def _nb_can_push(
    bundle,
    raw_timestamp: float,
    offset_ns: int,
    arb_id: int,
    data: np.ndarray,  # Expects a uint8 view
    is_ext: bool,
    is_rem: bool,
    is_err: bool,
    is_fd: bool,
    is_rx: bool,
    brs: bool,
    esi: bool,
) -> bool:
    # 1. Project Monotonic/Boot time to Epoch Nanoseconds
    # $$T_{ns} = T_{offset} + \text{int}(T_{raw} \times 10^9)$$
    ts_ns = offset_ns + int(raw_timestamp * 1_000_000_000)

    # 2. Fast Bit-Packing for ext_u32_2
    flags = 0
    if is_ext:
        flags |= 0x01  # Bit 0: Extended vs Standard
    if is_rem:
        flags |= 0x02  # Bit 1: Remote Frame
    if is_err:
        flags |= 0x04  # Bit 2: Error Frame
    if is_fd:
        flags |= 0x08  # Bit 3: CAN FD Frame
    if is_rx:
        flags |= 0x10  # Bit 4: Rx vs Tx
    if brs:
        flags |= 0x20  # Bit 5: Bit Rate Switch
    if esi:
        flags |= 0x40  # Bit 6: Error State Indicator

    # 3. Direct push into the bundle
    # level, module, device, seq are 0 for raw CAN ingress
    return _nb_bundle_push(bundle, ts_ns, data, 0, 0, 0, 0, arb_id, flags, 0)


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
        High-performance insertion of a python-can Message object.
        Handles zero-copy buffer views, flag packing, and clock sync.
        """
        if not (b := self.bundle):
            return False

        # 1. Zero-copy view of data (Fixes the Element Size ValueError)
        # Using np.uint8 explicitly
        d_view = np.frombuffer(msg.data, dtype=dtypes.BYTE) if msg.data else EMPTY_BYTES

        # 2. Extract hardware attributes safely
        # We use getattr for FD flags to support non-FD drivers/interfaces
        return _nb_can_push(
            b,
            msg.timestamp,
            offset_ns,
            msg.arbitration_id,
            d_view,
            msg.is_extended_id,
            msg.is_remote_frame,
            msg.is_error_frame,
            msg.is_fd,
            msg.is_rx,
            msg.bitrate_switch,
            msg.error_state_indicator,
        )


@DeviceFactory.register("can")
@configuration_property(
    "interface",
    type="string",
    default="virtual",
    required=True,
    ui_order=10,
    enum=["virtual", "socketcan", "pcan", "ixxat", "slcan"],
    description="The python-can interface to use (e.g., 'socketcan' for Linux, 'pcan' for Windows/Peak).",
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

        logger.info(f"Starting CAN Reader Thread ({self.interface}:{self.channel} @ {self.bitrate}bps)")

        _ts_offset_ns = None

        # Tuner setup: Estimating an average CAN message payload of 8 bytes
        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(speedometer=stats, msg_size_bytes=8, logger=self.logger.child("tuner"))

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            # Request ext_u32_1 for CAN Address and ext_u32_2 for CAN Flags
            return pool_create(CanLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch = None
        bus = None

        try:
            while not stop_is_set():
                if bus is None:
                    try:
                        bus = Bus(interface=self.interface, channel=self.channel, bitrate=self.bitrate)
                        logger.info("CAN bus connected successfully.")
                    except Exception as e:
                        logger.error(f"Failed to open CAN bus '{self.channel}'.", e)
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
                    logger.error("CAN Bus Error detected.", e)
                    if bus:
                        bus.shutdown()
                    bus = None
                    sleep(1.0)

                except Exception as e:
                    logger.error("Unexpected error in CAN read loop.", e)
                    if bus:
                        bus.shutdown()
                    bus = None
                    sleep(1.0)

        except Exception as e:
            logger.exception("Fatal error in CAN Reader loop", e)
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

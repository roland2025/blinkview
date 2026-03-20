# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep
from can import Bus, CanError

from .BaseReader import DeviceFactory, BaseReader
from ..core.base_configurable import configuration_property
from ..core.log_row import LogRow
from ..utils.level_map import LogLevel


@DeviceFactory.register("can")
@configuration_property("interface", type="string", default="virtual", required=True, ui_order=10,
                        enum=["virtual", "socketcan", "pcan", "ixxat", "slcan"],
                        description="The python-can interface to use (e.g., 'socketcan' for Linux, 'pcan' for Windows/Peak).")
@configuration_property("channel", type="string", default="vcan0", required=True, ui_order=11,
                        description="The CAN channel or port name (e.g., 'vcan0', 'PCAN_USBBUS1', 'can0').")
@configuration_property("bitrate", type="integer", default=250000, required=True, ui_order=12,
                        description="The CAN bus communication speed in bits per second. Usually 250000, 500000, or 1000000.")
@configuration_property("maxlen", type="integer", default=2000,
                        description="The maximum number of CAN messages to batch before flushing downstream.")
@configuration_property("delay", type="integer", default=50,
                        description="The maximum time (in milliseconds) to hold messages before flushing a batch.")
@configuration_property("log_rx_tx", type="boolean", default=False,
                        description="When enabled, dumps raw CAN frames (ID, DLC, DATA) to the system log for low-level protocol debugging.")
class CANReader(BaseReader):
    __doc__ = """The primary ingestion source for Controller Area Network (CAN) bus data.

* Wraps the standard 'python-can' library for cross-platform compatibility.
* Supports physical hardware interfaces (PCAN, IXXAT, SocketCAN) and virtual buses.
* Batches raw can.Message objects with high-precision nanosecond timestamps.
* Gracefully handles bus errors and auto-reconnects if the physical adapter drops.
"""

    type: str
    interface: str
    channel: str
    bitrate: int
    maxlen: int
    delay: int
    log_rx_tx: bool

    def __init__(self):
        super().__init__()

    def run(self):
        # Localize method lookups for the tight loop
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        delay_ns = int(self.delay * 1_000_000)
        maxlen = self.maxlen
        log_rx_tx = self.log_rx_tx

        logger.info(f"Starting CAN Reader Thread ({self.interface}:{self.channel} @ {self.bitrate}bps)")

        batch = []
        last_flush_time = time_ns()
        batch_size = 0

        if log_rx_tx:
            mod_rx = self.local.device_id.get_module('_reader.rx')
            batch_rx_log = []
            push_log = self.local.push_log

        bus = None

        def flush():
            nonlocal batch, batch_size, last_flush_time
            if log_rx_tx:
                nonlocal batch_rx_log

            if batch:
                last_flush_time = time_ns()

                if log_rx_tx and batch_rx_log:
                    push_log(batch_rx_log)
                    batch_rx_log = []

                self.distribute(batch)
                batch = []
                batch_size = 0

        while not stop_is_set():
            if bus is None:
                try:
                    bus = Bus(
                        interface=self.interface,
                        channel=self.channel,
                        bitrate=self.bitrate
                    )
                    logger.info("CAN bus connected successfully.")
                except Exception as e:
                    logger.error(f"Failed to open CAN bus '{self.channel}'.", e)
                    sleep(1.0)
                    continue

                _recv = bus.recv  # Localize for performance

            try:
                now = time_ns()

                # Dynamic Timeout Calculation
                if batch:
                    # How much time until we MUST flush?
                    remaining_ns = (last_flush_time + delay_ns) - now
                    # Convert to seconds.
                    # Max 0.05s so we still wake up frequently to check stop_is_set()
                    # Min 0.0s so we don't pass negative timeouts to python-can
                    timeout_s = max(0.0, min(0.05, remaining_ns / 1_000_000_000.0))
                else:
                    # No batch pending, just wake up periodically to check stop flag
                    timeout_s = 0.05

                msg = _recv(timeout=timeout_s)

                # Update time AFTER the blocking call
                now = time_ns()

                if msg:
                    # print(f"[CAN] {now} {msg}")
                    batch.append((now, msg))
                    batch_size += 1

                    if log_rx_tx:
                        hex_data = msg.data.hex().upper() if msg.data else ""
                        log_msg = f"ID: {msg.arbitration_id:04X} DLC: {msg.dlc} DATA: {hex_data}"
                        batch_rx_log.append(LogRow(now, LogLevel.TRACE, mod_rx, log_msg))

                    if batch_size >= maxlen:
                        flush()
                        continue

                # Time-based flush check
                # Because of our precise timeout calculation above, if msg is None,
                # we will wake up exactly when this condition evaluates to True.
                if batch and (now - last_flush_time >= delay_ns):
                    flush()

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

        # Cleanup on exit
        if bus:
            bus.shutdown()
            logger.info("CAN bus shut down.")
        flush()

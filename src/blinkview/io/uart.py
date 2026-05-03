# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep

from ..core.configurable import configuration_property, override_property
from ..core.numpy_batch_manager import PooledLogBatch
from ..utils.throughput import Speedometer, ThroughputAutoTuner
from .BaseReader import BaseReader, DeviceFactory


@DeviceFactory.register("serial")
@configuration_property(
    "url",
    type="string",
    default="",
    required=True,
    description="The device path or PySerial URL to connect to (e.g., 'COM3', '/dev/ttyUSB0', or 'socket://192.168.1.5:8080').",
)
@configuration_property(
    "baudrate",
    type="integer",
    default=115200,
    description="The communication speed in bits per second. Typically ignored for pure socket connections.",
)
# @configuration_property(
#     "maxlen",
#     type="integer",
#     default=1_000_000,
#     description="The maximum internal byte buffer size. Prevents memory exhaustion during massive data spikes or downstream pipeline stalls.",
# )
@configuration_property(
    "delay",
    type="integer",
    default=100,
    description="The maximum time (in milliseconds) to hold incoming bytes before flushing a batch downstream. Balances latency against throughput efficiency.",
)
# @configuration_property(
#     "log_rx_tx",
#     type="boolean",
#     default=False,
#     description="When enabled, dumps raw RX/TX hex data to the system log for low-level protocol debugging (WARNING: significantly impacts performance).",
# )
@override_property(
    "logging",
    hidden=False,
    required=True,
    default={"enabled": True, "processor": {"type": "binary"}},
    description="Enable logging of raw byte data. Uses a custom 'binary' processor that formats bytes as hex strings for readability.",
)
@configuration_property(
    "suppress_auto_reset",
    type="boolean",
    default=False,
    description="Prevents the device from resetting when the port opens by pulling DTR and RTS low. Essential for ESP32 and Arduino boards.",
)
class UARTReader(BaseReader):
    __doc__ = """The primary data ingestion source for serial and UART communication.

* Supports standard local hardware ports (e.g., COM3, /dev/ttyACM0)
* Includes 'suppress_auto_reset' to prevent ESP32/Arduino reboots on connection.
* Connects to raw TCP/IP sockets for network-based serial streams
* Supports RFC2217 remote serial port protocols
* Efficiently batches high-frequency incoming byte streams

Leverages PySerial's URL handler system under the hood, making it highly versatile for both direct hardware debugging and remote telemetry acquisition. Batches are accumulated based on the configured delay to minimize downstream processing overhead."""

    type: str
    url: str
    baudrate: int
    maxlen: int
    delay: int
    # log_rx_tx: bool
    suppress_auto_reset: bool

    def __init__(self):
        super().__init__()

        self.logging_type = "default"  # Use the default logging mechanism
        self.logging_processor = "binary"

        self.serial = None

    @classmethod
    def get_config_schema(cls) -> dict:
        # Grab the static, merged schema from BaseConfigurable
        schema = super().get_config_schema()
        from serial.tools.list_ports import comports

        # Dynamically fetch available hardware ports right now
        live_ports = comports()

        # Create arrays for your UI dropdown (enum and descriptions)
        port_names = [p.device for p in live_ports]
        port_descriptions = [p.description for p in live_ports]

        # Always good to add a fallback or let them type a custom socket URL
        port_names.append("socket://localhost:1234")
        port_descriptions.append("TCP Socket Connection")

        # Inject the dynamic data into the 'url' property
        if "url" in schema["properties"]:
            url = schema["properties"]["url"]
            url["enum"] = port_names
            url["enum_tooltips"] = port_descriptions
            url["_allow_custom"] = True

        # Return the newly enriched schema to your UI generator!
        return schema

    def run(self):
        # 1. Setup and Localize Lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Tuner configuration
        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)

        # 2. Stats and Auto-Tuning Setup
        # We set msg_size_bytes to 20 to maintain your ~50 chunks/KB density preference
        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(speedometer=stats, msg_size_bytes=20, logger=self.logger.child("tuner"))

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch = None
        ser = None
        self.serial = None

        try:
            while not stop_is_set():
                # 3. Serial Lifecycle Management
                if ser is None:
                    ser = self.open()
                    if ser is None:
                        sleep(1.0)
                        continue
                    _read = ser.read

                # 4. Acquire batch using current Tuner projections
                if batch is None:
                    batch = batch_acquire()

                try:
                    now = time_ns()

                    # 5. Read Lead Byte (Establish Arrival Timestamp)
                    first_byte = _read(1)
                    now = time_ns()
                    if first_byte:
                        # Start record
                        if not batch.insert(now, now, first_byte):
                            with batch:
                                self.distribute(batch)
                                # Update tuner with the results of the finished batch
                                tuner.update(batch.msg_cursor, batch.size, delay_s)

                            batch = batch_acquire()
                            batch.insert(now, now, first_byte)

                        # 6. Drain remaining burst and Append
                        waiting = ser.in_waiting or 1024
                        if waiting > 0:
                            rest = _read(waiting)
                            # print(f"{self.__class__.__name__}: read {time()} ... {first_byte + rest}")
                            if not batch.append(rest):
                                with batch:
                                    self.distribute(batch)
                                    tuner.update(batch.msg_cursor, batch.size, delay_s)

                                batch = batch_acquire()
                                batch.insert(now, now, rest)

                    if batch is not None and batch.start_ts > 0 and (now - batch.start_ts) >= delay_ns:
                        with batch:
                            self.distribute(batch)
                            tuner.update(batch.msg_cursor, batch.size, delay_s)
                        batch = None

                except Exception as e:
                    logger.error(f"Serial read error: {e}")
                    ser = None
                    self.serial = None
                    sleep(1.0)

        except Exception as e:
            logger.exception("Fatal error in Serial Reader loop", e)
        finally:
            # 8. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()

            if self.serial is not None:
                try:
                    self.serial.close()
                finally:
                    self.serial = None

    def open(self):
        try:
            from ..parsers.binary_parser import BinaryParser

            time_ns = self.shared.time_ns

            # sync_state = self._init_sync_engine(anchor_is_boot=True)

            target_parser = next((s for s in self.subscribers if isinstance(s, BinaryParser)), None)

            BUF_SIZE = 64 * 1024  # 64KB buffer for incoming serial data

            self.logger.info(f"Opening '{self.url}' at {self.baudrate} baud")

            from serial import serial_for_url

            # Use PySerial's URL handler (handles socket://, rfc2217://, hwgrep://, etc.)
            ser = serial_for_url(self.url, baudrate=self.baudrate, timeout=self.delay / 1000.0, inter_byte_timeout=0.01)
            self.serial = ser

            # --- ESP32 DTR/RTS Logic ---
            if self.suppress_auto_reset:
                self.logger.info("Setting DTR/RTS to 0")
                ser.dtr = False
                ser.rts = False
                sleep(0.1)
            # ---------------------------

            try:
                ser.set_buffer_size(rx_size=BUF_SIZE)
            except Exception as e:
                self.logger.error("Failed to set buffer size. This may not be supported on all platforms.", e)
                pass

            self.logger.info("Connected")

            return ser
        except Exception as e:
            self.logger.error("Failed to open serial port.", e)

    def send_data(self, data: str):
        if self.serial and self.serial.is_open:
            try:
                # print(f"{self.__class__.__name__}: send {time()} ... {data}")
                self.serial.write(data.encode())
            except Exception as e:
                self.logger.exception("Failed to send data", e)

    def reset_device(self):
        """
        Triggers a hardware reset by toggling the DTR/RTS lines.
        Matches the logic used by esptool.py for ESP32 boards.
        """
        if not self.serial or not self.serial.is_open:
            self.logger.warning("Cannot reset: Serial port is not open.")
            return

        try:
            self.logger.info("Triggering hardware reset sequence...")

            # 1. Pull EN (Reset) low
            # In most ESP32 circuits: RTS high + DTR low = EN low
            self.serial.dtr = False
            self.serial.rts = True
            sleep(0.1)

            # 2. Bring EN (Reset) back high to let the chip boot
            # RTS low + DTR low = EN high, GPIO0 high (Normal Run Mode)
            self.serial.rts = False
            self.serial.dtr = False

            self.logger.info("Reset signal sent.")
        except Exception as e:
            self.logger.error(f"Failed to perform hardware reset: {e}")

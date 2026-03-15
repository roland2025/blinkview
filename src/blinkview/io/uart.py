# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import sleep

from .BaseReader import DeviceFactory, BaseReader
from ..core.base_configurable import configuration_property, override_property
from ..core.log_row import LogRow
from ..utils.level_map import LogLevel


@DeviceFactory.register("serial")
@configuration_property("url", type="string", default="", required=True, description="The device path or PySerial URL to connect to (e.g., 'COM3', '/dev/ttyUSB0', or 'socket://192.168.1.5:8080').")
@configuration_property("baudrate", type="integer", default=115200, description="The communication speed in bits per second. Typically ignored for pure socket connections.")
@configuration_property("maxlen", type="integer", default=1_000_000, description="The maximum internal byte buffer size. Prevents memory exhaustion during massive data spikes or downstream pipeline stalls.")
@configuration_property("delay", type="integer", default=100, description="The maximum time (in milliseconds) to hold incoming bytes before flushing a batch downstream. Balances latency against throughput efficiency.")
@configuration_property("log_rx_tx", type="boolean", default=False, description="When enabled, dumps raw RX/TX hex data to the system log for low-level protocol debugging (WARNING: significantly impacts performance).")
@override_property("logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "binary"}}, description="Enable logging of raw byte data. Uses a custom 'binary' processor that formats bytes as hex strings for readability.")
class UARTReader(BaseReader):
    __doc__ = """The primary data ingestion source for serial and UART communication.

* Supports standard local hardware ports (e.g., COM3, /dev/ttyACM0)
* Connects to raw TCP/IP sockets for network-based serial streams
* Supports RFC2217 remote serial port protocols
* Efficiently batches high-frequency incoming byte streams

Leverages PySerial's URL handler system under the hood, making it highly versatile for both direct hardware debugging and remote telemetry acquisition. Batches are accumulated based on the configured delay to minimize downstream processing overhead."""

    type: str
    url: str
    baudrate: int
    maxlen: int
    delay: int
    log_rx_tx: bool

    def __init__(self):
        super().__init__()

        self.logging_type = "default"  # Use the default logging mechanism
        self.logging_processor = "binary"

    @classmethod
    def get_config_schema(cls) -> dict:
        # 1. Grab the static, merged schema from BaseConfigurable
        schema = super().get_config_schema()
        from serial.tools.list_ports import comports
        # 2. Dynamically fetch available hardware ports right now
        live_ports = comports()

        # 3. Create arrays for your UI dropdown (enum and descriptions)
        port_names = [p.device for p in live_ports]
        port_descriptions = [p.description for p in live_ports]

        # Always good to add a fallback or let them type a custom socket URL
        port_names.append("socket://localhost:1234")
        port_descriptions.append("TCP Socket Connection")

        # 4. Inject the dynamic data into the 'url' property
        if "url" in schema["properties"]:
            url = schema["properties"]["url"]
            url["enum"] = port_names
            url["enum_tooltips"] = port_descriptions
            url["_allow_custom"] = True

        # 5. Return the newly enriched schema to your UI generator!
        return schema

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns

        logger = self.logger

        delay_ns = int(self.delay * 1_000_000)  # Convert milliseconds to nanoseconds
        maxlen = self.maxlen
        log_rx_tx = self.log_rx_tx

        print("Starting Serial Reader Thread")
        logger.info(f"Starting Serial")

        batch = []
        last_flush_time = time_ns()

        if log_rx_tx:
            mod_rx = self.local.device_id.get_module('_reader.rx')
            batch_rx_log = []

        batch_bytes = 0

        push_log = self.local.push_log

        ser = None

        def flush():
            nonlocal batch, batch_bytes, last_flush_time, batch_rx_log
            if batch:
                last_flush_time = time_ns()
                # logger.log(f"Flush batch of {len(batch)} | {batch_bytes} bytes",
                #            LogLevel.WARN if batch_bytes >= maxlen else LogLevel.DEBUG)

                if log_rx_tx and batch_rx_log:
                    push_log(batch_rx_log)
                    batch_rx_log = []

                self.distribute(batch)
                batch = []
                batch_bytes = 0

        while not stop_is_set():
            if ser is None:
                ser = self.open()
                if ser is None:
                    sleep(1.0)
                    continue

                _read = ser.read  # Localize method lookup for performance

            try:
                first_byte = ser.read()
                if first_byte:
                    now = time_ns()
                    remaining = _read(ser.in_waiting)  # Read the rest of the available data
                    chunk = (now, first_byte + remaining)
                    chunk_len = len(chunk[1])
                    batch.append(chunk)
                    # self.log(f"Read {chunk_len} bytes")

                    if log_rx_tx:
                        batch_rx_log.append(LogRow(now, LogLevel.TRACE, mod_rx, chunk[1].hex()))

                    batch_bytes += chunk_len

                    if batch_bytes >= maxlen:
                        flush()
                        continue

                    # Timeout Check
                    if batch and (now - last_flush_time >= delay_ns):
                        flush()

            except Exception as e:
                logger.error("error", e)
                ser = None
                sleep(1.0)

        # Flush any remaining batch on exit
        flush()

    def open(self):
        try:
            BUF_SIZE = 64 * 1024  # 64KB buffer for incoming serial data

            self.logger.info(f"Opening '{self.url}' at {self.baudrate} baud")

            from serial import serial_for_url
            # Use PySerial's URL handler (handles socket://, rfc2217://, hwgrep://, etc.)
            ser = serial_for_url(
                self.url,
                baudrate=self.baudrate,
                timeout=0.01,
                inter_byte_timeout=0.01
            )
            try:
                ser.set_buffer_size(rx_size=BUF_SIZE)
            except Exception as e:
                self.logger.error("Failed to set buffer size. This may not be supported on all platforms.", e)
                pass

            self.logger.info(f"Connected")

            return ser
        except Exception as e:
            self.logger.error(f"Failed to open serial port.", e)

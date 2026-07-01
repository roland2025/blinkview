# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import socket
from time import sleep

from ..core.configurable import configuration_property, override_property
from ..core.numpy_batch_manager import PooledLogBatch
from ..utils.throughput import Speedometer, ThroughputAutoTuner
from .BaseReader import BaseReader, DeviceFactory


@DeviceFactory.register("udp")
@configuration_property(
    "host",
    type="string",
    default="0.0.0.0",
    description="The IP address or hostname to bind the UDP server to. Use 0.0.0.0 to listen on all available network interfaces.",
)
@configuration_property(
    "port",
    type="integer",
    default=5000,
    required=True,
    description="The local UDP port to listen on for incoming datagrams.",
)
@configuration_property(
    "buffer_size",
    type="integer",
    default=65535,
    description="Maximum expected UDP packet size in bytes. 65535 is the theoretical maximum for UDP over IPv4.",
)
@configuration_property(
    "delay",
    type="integer",
    default=100,
    description="The maximum time (in milliseconds) to hold incoming bytes before flushing a batch downstream. Balances latency against throughput efficiency.",
)
@override_property(
    "logging",
    hidden=False,
    required=True,
    default={"enabled": True, "processor": {"type": "binary"}},
    description="Enable logging of raw byte data. Uses a custom 'binary' processor that formats bytes as hex strings for readability.",
)
class UDPReader(BaseReader):
    __doc__ = """The primary data ingestion source for UDP datagram streams.

* Binds to a local port and listens for connectionless UDP traffic.
* Excellent for high-throughput, low-latency telemetry where occasional packet loss is acceptable.
* Efficiently batches high-frequency incoming datagrams using the pipeline's batch manager."""

    type: str
    host: str
    port: int
    buffer_size: int
    delay: int

    def __init__(self):
        super().__init__()

        self.logging_type = "default"
        self.logging_processor = "binary"

        self.sock = None
        self.target_address = None

    def open(self):
        try:
            self.logger_link.info(f"Binding UDP socket to {self.host}:{self.port}")

            # Initialize an IPv4 UDP Socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            # Allow port reuse to prevent "Address already in use" errors on restart
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            sock.bind((self.host, self.port))

            # Set a timeout equal to the delay.
            # This allows the recvfrom() call to unblock so we can flush batches and check the stop event.
            sock.settimeout(self.delay / 1000.0 / 2)

            self.sock = sock
            self.logger_link.info("Listening")

            self.logger_state_open.info("1")
            return sock

        except Exception as e:
            self.logger_link.error("Failed to bind UDP socket.", e)
            return None

    def run(self):
        # 1. Setup and Localize Lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Tuner configuration
        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)

        self.logger_state_open.info("0")

        # 2. Stats and Auto-Tuning Setup
        # Using a slightly larger baseline message size for UDP vs Serial
        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(speedometer=stats, msg_size_bytes=1024, logger=self.logger.child("tuner"))

        pool_create = self.shared.array_pool.create

        def batch_acquire() -> PooledLogBatch:
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch = None
        sock = None

        try:
            while not stop_is_set():
                # 3. Socket Lifecycle Management
                if sock is None:
                    sock = self.open()
                    if sock is None:
                        sleep(1.0)
                        continue

                # 4. Acquire batch using current Tuner projections
                if batch is None:
                    batch = batch_acquire()

                try:
                    # 5. Wait for incoming datagrams
                    # This will block until data arrives OR the socket timeout (self.delay) is reached
                    data, addr = sock.recvfrom(self.buffer_size)
                    now = time_ns()

                    # if self.target_address is None and data:
                    #     self.target_address = addr
                    #     self.logger.info(f"UDP Reader automatically bound return target to {addr[0]}:{addr[1]}")

                    if data:
                        # print(f"[UDPReader] data={data} add={addr}")
                        self.target_address = addr
                        # 6. Insert or Append data
                        if not batch.insert(now, now, data):
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)

                            batch = batch_acquire()
                            batch.insert(now, now, data)

                except socket.timeout:
                    # This is an expected condition. The timeout ensures we don't hang infinitely
                    # and gives us a chance to flush the current batch if it's sitting idle.
                    pass
                except Exception as e:
                    self.logger_link.error("Receive error", e)
                    sock = None
                    if self.sock:
                        self.sock.close()
                        self.sock = None

                        self.logger_state_open.error("0")
                    sleep(1.0)
                    continue

                # 7. Flush the batch if the delay window has elapsed
                now = time_ns()
                if batch is not None and batch.start_ts > 0 and (now - batch.start_ts) >= delay_ns:
                    with batch:
                        self.distribute(batch)
                        tuner.update(batch.msg_cursor, batch.size, delay_s)
                    batch = None

        except Exception as e:
            logger.exception("Fatal error in UDP Reader loop", e)
        finally:
            # 8. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()

            if self.sock is not None:
                try:
                    self.sock.close()
                finally:
                    self.logger_state_open.info("0")
                    self.logger_link.info("Closed")
                    self.sock = None

    def send_data(self, data: str):
        """
        Sends data back over UDP.
        Defaults to the automatically bound client address if none is provided.
        """
        # Fallback to autobound address if explicit address is omitted
        target = self.target_address

        if not target:
            self.logger.warning("Cannot send data: No remote client has sent a message yet to bind the target port.")
            return

        if self.sock:
            try:
                self.sock.sendto(data.encode(), target)
            except Exception as e:
                self.logger.exception(f"Failed to send UDP datagram to {target}", e)

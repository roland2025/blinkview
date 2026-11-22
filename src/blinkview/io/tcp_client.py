# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import socket
from time import sleep

import numpy as np

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property, override_property
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.io.batch_feeder import BatchFeeder


@DeviceFactory.register("tcp_client")
@configuration_property(
    "host",
    type="string",
    default="127.0.0.1",
    required=True,
    description="The remote server IP address or hostname to connect to.",
)
@configuration_property(
    "port",
    type="integer",
    default=5000,
    required=True,
    description="The remote TCP port to connect to.",
)
@configuration_property(
    "buffer_size",
    type="integer",
    default=65535,
    description="Chunk size in bytes extracted from the TCP socket buffer per read operation.",
)
@configuration_property(
    "delay",
    type="integer",
    default=100,
    description="The maximum time (in milliseconds) to hold incoming stream bytes before flushing a batch downstream.",
)
@configuration_property(
    "reconnect_interval",
    type="integer",
    default=5,
    description="Time in seconds to wait before attempting to reconnect if the connection drops.",
)
@override_property(
    "logging",
    hidden=False,
    required=True,
    default={"enabled": True, "processor": {"type": "binary"}},
    description="Record the incoming bytes to a session file. The file is not a plain byte dump and not text: each received chunk is stored as a timestamped header followed by its bytes.",
)
class TCPClientReader(BaseReader):
    __doc__ = """The data ingestion source for outbound TCP streaming sockets.

* Connects actively to a remote TCP server and pulls a continuous data stream.
* Automatically handles connection failures, remote dropouts, and reconnection loops.
* Efficiently batches stream fragments into the pipeline using the auto-tuning batch manager."""

    type: str
    host: str
    port: int
    buffer_size: int
    delay: int
    reconnect_interval: int

    def __init__(self):
        super().__init__()

        self.logging_type = "default"
        self.logging_processor = "binary"

        self.client_sock = None  # The active outbound connected socket

    def open(self):
        """Initializes the outbound connection to the remote TCP server."""
        try:
            self.logger_link.info("Connecting to: %s:%s", self.host, self.port)

            # Initialize an IPv4 TCP Streaming Socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Prevent connection hang ups during link setup
            sock.settimeout(self.delay / 1000.0)

            # Keepalive keeps connections alive across quiet telemetry periods
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            sock.connect((self.host, self.port))

            # Set data transfer timeout matching your latency window constraints
            sock.settimeout(self.delay / 1000.0 / 2)

            self.client_sock = sock
            self.logger_link.info("Connected to: %s:%s", self.host, self.port)
            self.logger_state_open.info("1")
            return sock

        except Exception as e:
            self.logger_link.error("Connection failed to: %s:%s", self.host, self.port, exc=e)
            self._close_client()
            return None

    def run(self):
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        self.logger_state_open.info("0")

        recv_buffer = bytearray(self.buffer_size)
        data_view = np.frombuffer(recv_buffer, dtype=dtypes.BYTE)

        feeder = None

        try:
            feeder = BatchFeeder(self, msg_size_bytes=1024, default_buffer_bytes=self.buffer_size)
            push_view = feeder.push_view
            maybe_flush = feeder.maybe_flush

            while not stop_is_set():
                # Active Client Socket Lifecycle Management
                if self.client_sock is None:
                    if self.open() is None:
                        # Wait for the user-configured interval before retrying connection
                        sleep(float(self.reconnect_interval))
                        continue

                    recv_into = self.client_sock.recv_into

                # Active Stream Data Read
                try:
                    bytes_read = recv_into(recv_buffer)
                    now = time_ns()

                    if bytes_read == 0:
                        # TCP Fin received. Remote server closed connection cleanly.
                        self.logger_link.warning("Remote server closed the connection.")
                        self._close_client()
                        continue

                    # Insert the stream chunk into the current batch
                    push_view(now, data_view, bytes_read)

                except socket.timeout:
                    # Expected. Allows timed evaluation of batch flushing windows
                    now = time_ns()
                except Exception as e:
                    self.logger_link.error("Stream receive error occurred on client connection", exc=e)
                    self._close_client()
                    sleep(0.5)
                    continue

                # Flush evaluation window (`now` is from the read, or from the timeout above)
                maybe_flush(now)

        except Exception as e:
            logger.exception("Fatal error in TCP Client Reader execution loop", exc=e)
        finally:
            # Final Cleanup
            if feeder is not None:
                feeder.close()
            self._close_client()

    def _close_client(self):
        """Safely tears down the active client data pipe."""
        if self.client_sock:
            try:
                self.client_sock.close()
            except Exception:
                pass
            finally:
                self.client_sock = None
                self.logger_state_open.info("0")
                self.logger_link.info("TCP Client Socket Closed")

    def send_data(self, data: str):
        """Sends bidirectional telemetry strings back up the established TCP pipe to the server."""
        if not self.client_sock:
            self.logger.warning("Cannot transmit command payload: No active TCP connection exists.")
            return

        try:
            self.client_sock.sendall(data.encode())
        except Exception as e:
            self.logger.exception("Failed to transmit data across client TCP socket", exc=e)
            self._close_client()

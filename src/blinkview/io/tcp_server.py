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
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner


@DeviceFactory.register("tcp_server")
@configuration_property(
    "host",
    type="string",
    default="0.0.0.0",
    description="The IP address or hostname to bind the TCP listening server to.",
)
@configuration_property(
    "port",
    type="integer",
    default=5000,
    required=True,
    description="The local TCP port to listen on for incoming streaming connections.",
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
@override_property(
    "logging",
    hidden=False,
    required=True,
    default={"enabled": True, "processor": {"type": "binary"}},
    description="Enable logging of raw byte data.",
)
class TCPReader(BaseReader):
    __doc__ = """The data ingestion source for continuous TCP streaming sockets.

* Binds to a local port as a listening server and manages a single active streaming client.
* Guarantees packet delivery and ordering via TCP flow-control windows.
* Efficiently batches stream fragments into the pipeline using the auto-tuning batch manager."""

    type: str
    host: str
    port: int
    buffer_size: int
    delay: int

    def __init__(self):
        super().__init__()

        self.logging_type = "default"
        self.logging_processor = "binary"

        self.server_sock = None  # The main listening server socket
        self.client_sock = None  # The active connected client data socket

    def open(self):
        """Initializes the listening TCP Master Server Socket."""
        try:
            self.logger_link.info("Binding TCP server socket to %s:%s", self.host, self.port)

            # Initialize an IPv4 TCP Streaming Socket
            server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            # Prevent "Address already in use" kernel lockouts on rapid server restarts
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

            # Keepalive keeps connections alive across quiet telemetry periods
            server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

            server_sock.bind((self.host, self.port))

            # Allow backlogging connections. 1 represents a strict single-producer design
            server_sock.listen(1)

            # Short timeout on the master accept socket so it can periodically check if the reader loop is stopping
            server_sock.settimeout(1.0)

            self.server_sock = server_sock
            self.logger_link.info("TCP Listening Server Ready")
            return server_sock

        except Exception as e:
            self.logger_link.error("Failed to bind TCP server socket.", exc=e)
            return None

    def run(self):
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)

        self.logger_state_open.info("0")

        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(
            speedometer=stats,
            default_buffer_bytes=self.buffer_size,
            msg_size_bytes=1024,
            logger=self.logger.child("tuner"),
        )
        pool_create = self.shared.array_pool.create

        def batch_acquire() -> PooledLogBatch:
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch = None

        recv_buffer = bytearray(self.buffer_size)
        data_view = np.frombuffer(recv_buffer, dtype=dtypes.BYTE)

        waiting_logged = False

        try:
            while not stop_is_set():
                # 1. Master Server Socket Lifecycle Management
                if self.server_sock is None:
                    if self.open() is None:
                        sleep(1.0)
                        continue

                # 2. Connection Management Loop
                if self.client_sock is None:
                    try:
                        if not waiting_logged:
                            self.logger_link.info("Waiting connection...")
                            waiting_logged = True

                        client_conn, client_addr = self.server_sock.accept()

                        # Set timeout on data transfer to prevent infinite blocks
                        # matching your exact latency window constraints
                        client_conn.settimeout(self.delay / 1000.0 / 2)

                        self.client_sock = client_conn

                        recv_into = self.client_sock.recv_into

                        self.logger_link.info("%s:%s connected", client_addr[0], client_addr[1])
                        self.logger_state_open.info("1")

                        waiting_logged = False
                    except socket.timeout:
                        continue  # Let the loop poll stop_is_set()
                    except Exception as e:
                        self.logger_link.error("Error accepting incoming client connection", exc=e)
                        waiting_logged = False
                        sleep(1.0)
                        continue

                if batch is None:
                    batch = batch_acquire()

                # 3. Active Stream Data Read
                try:
                    bytes_read = recv_into(recv_buffer)
                    now = time_ns()

                    if bytes_read == 0:
                        self._close_client()
                        continue

                    # 4. Insert or Append stream chunk into high-throughput layer
                    if not batch.insert_view(now, now, data_view, bytes_read):
                        with batch:
                            self.distribute(batch)
                            tuner.update(batch.msg_cursor + bytes_read, batch.size, delay_s)
                        batch = batch_acquire()
                        batch.insert_view(now, now, data_view, bytes_read)

                except socket.timeout:
                    # Expected. Allows timed evaluation of batch flushing windows
                    pass
                except Exception as e:
                    self.logger_link.error("Stream receive error occurred", exc=e)
                    self._close_client()
                    sleep(0.5)
                    continue

                # 5. Flush evaluation window
                now = time_ns()
                if batch is not None and batch.start_ts > 0 and (now - batch.start_ts) >= delay_ns:
                    with batch:
                        self.distribute(batch)
                        tuner.update(batch.msg_cursor, batch.size, delay_s)
                    batch = None

        except Exception as e:
            logger.exception("Fatal error in TCP Reader execution loop", exc=e)
        finally:
            # 6. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()
            self._close_client()
            self._close_server()

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

    def _close_server(self):
        """Safely tears down the master listening server."""
        if self.server_sock:
            try:
                self.server_sock.close()
            except Exception:
                pass
            finally:
                self.server_sock = None
                self.logger_link.info("TCP Listening Server Closed")

    def send_data(self, data: str):
        """Sends bidirectional telemetry strings back down the established TCP pipe."""
        if not self.client_sock:
            self.logger.warning("Cannot transmit command payload: No active TCP client connection exists.")
            return

        try:
            self.client_sock.sendall(data.encode())
        except Exception as e:
            self.logger.exception("Failed to transmit data across active client TCP socket", exc=e)
            self._close_client()

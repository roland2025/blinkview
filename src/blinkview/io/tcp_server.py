# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import socket
from time import sleep

from ..core.configurable import configuration_property
from .BaseReader import BaseReader, DeviceFactory


@DeviceFactory.register("tcp_server")
@configuration_property(
    "host",
    type="string",
    default="0.0.0.0",
    required=True,
    description="The IP interface to bind to ('0.0.0.0' for all interfaces, 'localhost' for local only).",
)
@configuration_property(
    "port",
    type="integer",
    default=9000,
    required=True,
    description="The TCP port to listen on for incoming connections.",
)
@configuration_property(
    "maxlen", type="integer", default=1_000_000, description="The maximum internal byte buffer size before flushing."
)
@configuration_property(
    "delay",
    type="integer",
    default=100,
    description="The maximum time (in milliseconds) to hold incoming bytes before flushing a batch downstream.",
)
class TcpServerSource(BaseReader):
    __doc__ = """A TCP Server ingestion source for networked telemetry.

* Acts as a TCP Server (Listens for incoming Client connections).
* Ideal for receiving logs from transient clients that may start or stop unpredictably.
* Reads continuous binary streams and passes raw byte chunks downstream.
* Handles client disconnections gracefully and automatically resumes listening.
"""

    type: str
    host: str
    port: int
    maxlen: int
    delay: int

    def __init__(self):
        super().__init__()

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        delay_ns = int(self.delay * 1_000_000)
        maxlen = self.maxlen

        logger.info(f"Starting TCP Server Source on {self.host}:{self.port}")

        batch = []
        last_flush_time = time_ns()
        batch_bytes = 0

        server_sock = None

        def flush():
            nonlocal batch, batch_bytes, last_flush_time
            if batch:
                last_flush_time = time_ns()
                self.distribute(batch)
                batch = []
                batch_bytes = 0

        while not stop_is_set():
            # Start the Server Socket if it isn't running
            if server_sock is None:
                try:
                    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    # SO_REUSEADDR prevents the "Address already in use" error if you restart quickly
                    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    server_sock.bind((self.host, self.port))
                    server_sock.listen(1)

                    # 1-second timeout so accept() doesn't block the thread from shutting down
                    server_sock.settimeout(1.0)
                    logger.info(f"🎧 Listening for incoming connections on port {self.port}...")
                except Exception as e:
                    logger.error(f"Failed to bind server socket", e)
                    if server_sock:
                        server_sock.close()
                    server_sock = None
                    sleep(1.0)
                    continue

            conn = None
            try:
                # Wait for a client (sender) to connect
                try:
                    conn, addr = server_sock.accept()
                    logger.info(f"✅ Client connected from {addr}")

                    # Short timeout for the active connection to allow flushing during idle times
                    conn.settimeout(0.1)
                except socket.timeout:
                    # No client connected yet; loop around and check stop_is_set()
                    continue

                # Read loop for the active connection
                while not stop_is_set():
                    try:
                        bytes_to_read = max(4096, maxlen - batch_bytes)
                        chunk = conn.recv(bytes_to_read)

                        # print(f"Received chunk of {len(chunk)} bytes from {addr}")

                        if not chunk:
                            logger.info(f"Client {addr} disconnected gracefully.")
                            break  # Break inner loop to go back to listening

                        now = time_ns()
                        batch.append((now, chunk))
                        batch_bytes += len(chunk)

                    except socket.timeout:
                        pass  # Normal timeout on an idle connection
                    except ConnectionResetError:
                        logger.warning(f"Client {addr} disconnected abruptly.")
                        break  # Break inner loop to go back to listening

                    # Flush Checks
                    now = time_ns()
                    if batch_bytes >= maxlen:
                        flush()
                        continue

                    if batch and (now - last_flush_time >= delay_ns):
                        flush()

            except Exception as e:
                logger.error(f"Server error", e)
                sleep(1.0)
            finally:
                # Flush whatever is left from that client and close their connection
                flush()
                if conn:
                    conn.close()

        # Final cleanup on thread exit
        flush()
        if server_sock:
            server_sock.close()

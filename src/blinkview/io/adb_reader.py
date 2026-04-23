# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import shutil
import subprocess
from pathlib import Path
from time import sleep

from ..core.configurable import configuration_property, override_property
from ..core.numpy_batch_manager import PooledLogBatch
from ..utils.throughput import Speedometer, ThroughputAutoTuner
from .BaseReader import BaseReader, DeviceFactory


@DeviceFactory.register("adb")
@configuration_property(
    "device_id",
    type="string",
    default="",
    description="Optional: The specific device serial number to connect to (equivalent to adb -s <device_id>). Leave empty to use the default/only connected device.",
)
@configuration_property(
    "filters",
    type="string",
    default="",
    description="Logcat filters, e.g., 'MyApp:D *:S' to see only Debug logs for MyApp and silence everything else.",
)
@configuration_property(
    "logcat_args",
    type="string",
    default="",
    description="Additional arguments for logcat formatting (e.g., '-v color', '-v long').",
)
@configuration_property(
    "clear_log",
    type="boolean",
    default=False,
    description="If true, executes 'adb logcat -c' to clear the device log buffer before streaming begins.",
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
    description="Enable logging.",
)
class AdbReader(BaseReader):
    __doc__ = """The primary data ingestion source for Android ADB logcat streams.

* Streams real-time text output from a connected Android device or emulator.
* Supports standard logcat filtering and formatting directly via properties.
* Capable of flushing device logs prior to connection for a clean slate.
* Safely manages the background ADB subprocess to prevent zombie processes.

This reader batches incoming standard output using unbuffered OS reads (`read1`), 
ensuring high throughput without pipeline stalls."""

    type: str
    device_id: str
    filters: str
    logcat_args: str
    clear_log: bool
    delay: int

    def __init__(self):
        super().__init__()

        self.logging_type = "default"
        self.logging_processor = "text"  # ADB is mostly text

        self._process = None

    def run(self):
        # 1. Setup and Localize Lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Tuner configuration
        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)

        # 2. Stats and Auto-Tuning Setup
        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(speedometer=stats, msg_size_bytes=20, logger=self.logger.child("tuner"))

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch = None

        try:
            while not stop_is_set():
                # 3. Subprocess Lifecycle Management
                if self._process is None:
                    self.open()
                    if self._process is None:
                        sleep(1.0)
                        continue

                # We localize the read method for performance
                # read1(size) blocks until *at least* 1 byte is available,
                # then returns whatever is buffered up to the size limit.
                _read1 = self._process.stdout.read1

                # 4. Acquire batch using current Tuner projections
                if batch is None:
                    batch = batch_acquire()

                try:
                    # 5. Read incoming chunk
                    # 65536 is a standard healthy OS buffer size
                    chunk = _read1(65536)

                    if chunk:
                        now = time_ns()

                        # 2. Attempt high-resolution insertion
                        # This returns False if we hit array capacity OR buffer capacity
                        if not batch.insert(now, chunk):
                            # 3. Batch is full: Flush and Acquire new
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)

                            batch = batch_acquire()

                            # Re-attempt the insert into the fresh batch
                            # (Assuming chunk < total batch capacity)
                            batch.insert(now, chunk)

                        # 4. Batching Window Check
                        # Now every chunk has a unique timestamp in the bundle,
                        # but we still flush based on the first arrival in this batch.
                        if (now - batch.start_ts) >= delay_ns:
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)
                            batch = None
                    else:
                        # If read1 returns empty bytes, the subprocess reached EOF (ADB died)
                        logger.warning("ADB process stream ended. Restarting...")
                        self._cleanup_process()
                        sleep(1.0)

                except Exception as e:
                    logger.error(f"ADB read error: {e}")
                    self._cleanup_process()
                    sleep(1.0)

        except Exception as e:
            logger.exception("Fatal error in ADB Reader loop", e)
        finally:
            # 7. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()

            self._cleanup_process()

    def _detect_adb_path(self) -> str:
        """
        Locates the ADB executable. Uses explicit string casting for
        compatibility with Python < 3.12 on Windows.
        """
        # 1. Check system PATH
        # Even if shutil.which("adb") works with a literal string,
        # we'll stay consistent.
        system_adb = shutil.which("adb")
        if system_adb:
            return str(system_adb)

        # 2. Check Windows Sdk location
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            # We build the Path object...
            sdk_adb = Path(local_app_data) / "Android" / "Sdk" / "platform-tools" / "adb.exe"

            # ...but we check existence and return as a STRING
            if sdk_adb.exists():
                return str(sdk_adb)

        # 3. Final fallback
        return "adb"

    def open(self):
        adb_bin = self._detect_adb_path()

        try:
            # Build the base command (e.g., ['adb'] or ['C:\\...\\adb.exe'])
            base_cmd = [adb_bin]
            if self.device_id:
                base_cmd.extend(["-s", self.device_id])

            # 1. Pre-clear logs if requested
            if self.clear_log or True:
                self.logger.info(f"Clearing device logs ({adb_bin} logcat -c)...")
                # capture_output=True prevents clear-log errors from polluting your stream
                subprocess.run([*base_cmd, "logcat", "-c"], check=False, capture_output=True)

            # 2. Build the main streaming command
            cmd = [*base_cmd, "logcat", "-v", "long", "-v", "year"]

            if self.logcat_args:
                cmd.extend(self.logcat_args.split())

            if self.filters:
                cmd.extend(self.filters.split())

            self.logger.info(f"Opening ADB stream: {' '.join(cmd)}")

            # 3. Start subprocess
            # stdout=PIPE is required for reading the stream
            # stderr=STDOUT merges errors (like 'device not found') into the stdout for easy handling
            # bufsize=0 ensures we get data in real-time (unbuffered)
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                bufsize=-1,
            )

            self.logger.info("ADB Connected")

            # Return the stdout handle so the 'run' loop can use its .read1 method
            return self._process.stdout

        except Exception as e:
            self.logger.error(f"Failed to start ADB process using: {adb_bin}", e)
            self._cleanup_process()
            return None

    def _cleanup_process(self):
        """Safely terminates the ADB subprocess."""
        if self._process is not None:
            self.logger.info("Terminating ADB subprocess...")
            try:
                self._process.terminate()
                self._process.wait(timeout=2.0)
            except Exception:
                try:
                    self._process.kill()
                except Exception as e:
                    self.logger.error(f"Failed to kill ADB process: {e}")
            finally:
                self._process = None

    def reset_device(self):
        """
        Triggers a soft reboot of the connected Android device via ADB.
        """
        try:
            self.logger.info("Triggering ADB reboot...")
            cmd = ["adb"]
            if self.device_id:
                cmd.extend(["-s", self.device_id])
            cmd.append("reboot")

            subprocess.run(cmd, check=True)
            self.logger.info("Reboot command sent.")
        except Exception as e:
            self.logger.error(f"Failed to reboot device: {e}")

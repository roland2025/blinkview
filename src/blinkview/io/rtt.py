# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import ctypes
from time import sleep

import numpy as np

from ..core.configurable import configuration_property, override_property
from ..core.log_row import LogRow
from ..core.numpy_batch_manager import PooledLogBatch
from ..utils.log_level import LogLevel
from ..utils.throughput import Speedometer, ThroughputAutoTuner
from .BaseReader import BaseReader, DeviceFactory

# Values in kHz as required by jlink.connect()
SWD_SPEEDS = [
    100,  # Low speed (Safe / Recovery)
    400,  # Low speed (Standard startup)
    1000,  # 1 MHz (Common default)
    2000,  # 2 MHz
    4000,  # 4 MHz (J-Link Default - highly stable)
    8000,  # 8 MHz (High-performance debugging)
    12000,  # 12 MHz (Typical max for Base models)
    15000,  # 15 MHz
    20000,  # 20 MHz (Requires J-Link Ultra+ / Pro)
    30000,  # 30 MHz
    50000,  # 50 MHz (Maximum for high-end hardware)
]

# Recommended UI Tooltips
SWD_SPEED_DESCRIPTIONS = [
    "100 kHz - Safe/Recovery (Very long cables)",
    "400 kHz - Stable (Standard startup)",
    "1 MHz - Standard Default",
    "2 MHz - Reliable High Speed",
    "4 MHz - J-Link Recommended Default",
    "8 MHz - High-speed RTT",
    "12 MHz - Production (Short traces)",
    "15 MHz - Fast Flashing",
    "20 MHz - Ultra High Speed (Ultra+ hardware)",
    "30 MHz - Pro Grade",
    "50 MHz - Extreme (High-end targets only)",
]


@DeviceFactory.register("jlink_rtt")
@configuration_property(
    "target_device",
    type="string",
    default="NRF52840_XXAA",
    required=True,
    ui_order=5,
    description="The target microcontroller device name (e.g., 'STM32F407VG', 'NRF52840_XXAA').",
)
@configuration_property(
    "serial_number",
    type="string",
    default="",
    ui_order=10,
    description="Specific J-Link serial number to connect to. Leave empty to connect to the first available J-Link.",
)
@configuration_property(
    "channel",
    type="integer",
    default=0,
    ui_order=12,
    description="The RTT channel to read from. Defaults to 0 (the standard terminal channel).",
)
@configuration_property(
    "interface",
    type="string",
    default="swd",
    enum=["swd", "jtag"],
    ui_order=14,
    description="Target interface to use: 'swd' or 'jtag'.",
)
@configuration_property(
    "speed",
    type="integer",
    default=4000,
    description="The target communication speed.",
    enum=SWD_SPEEDS,
    enum_descriptions=SWD_SPEED_DESCRIPTIONS,
    ui_order=16,
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
@configuration_property(
    "target_rtt_buffer_size",
    type="integer",
    default=8192,
    ui_order=22,
    description="The physical size of the RTT Up-Buffer in the target's RAM. "
    "Used to precisely flush stale data on startup without discarding live telemetry.",
)
class JLinkRTTReader(BaseReader):
    __doc__ = """The primary data ingestion source for Segger J-Link RTT (Real-Time Transfer).

* Provides high-speed, non-intrusive background telemetry acquisition
* Supports specific device targeting via J-Link serial numbers
* Reads continuously from the specified RTT Up-Buffer (Channel)
* Efficiently batches high-frequency incoming byte streams

Leverages the `pylink-square` library under the hood. Batches are accumulated based on the configured delay to minimize downstream processing overhead without dropping high-throughput streams."""

    type: str
    target_device: str
    serial_number: str
    channel: int
    interface: str
    speed: int
    delay: int

    def __init__(self):
        super().__init__()

        self.logging_type = "default"
        self.logging_processor = "binary"
        self.jlink = None

    @classmethod
    def get_config_schema(cls) -> dict:
        schema = super().get_config_schema()

        try:
            import pylink

            jlink = pylink.JLink()
            # Dynamically fetch connected J-Link emulators
            emulators = jlink.connected_emulators()
            serials = [str(emu.SerialNumber) for emu in emulators]
            descriptions = [f"J-Link {sn}" for sn in serials]

            if "serial_number" in schema["properties"]:
                sn_prop = schema["properties"]["serial_number"]
                sn_prop["enum"] = [""] + serials
                sn_prop["enum_tooltips"] = ["First available device"] + descriptions
                sn_prop["_allow_custom"] = True

        except ImportError:
            # If pylink isn't available during schema generation, fail gracefully
            pass
        except Exception:
            # Ignore connection issues during schema fetch
            pass

        return schema

    def run(self):
        # 1. Setup and Localize Lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Tuner configuration
        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)
        channel = self.channel

        read_size = 64 * 1024

        c_buf = (ctypes.c_ubyte * read_size)()
        np_buf = np.frombuffer(c_buf, dtype=np.uint8)

        # 2. Stats and Auto-Tuning Setup
        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(
            speedometer=stats, default_buffer_bytes=read_size, msg_size_bytes=20, logger=self.logger.child("tuner")
        )

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch = None

        try:
            while not stop_is_set():
                # 3. J-Link Lifecycle Management
                if self.jlink is None:
                    self.jlink = self.open()
                    if self.jlink is None:
                        sleep(1.0)
                        continue
                    _dll = self.jlink._dll

                    _dll_rtterminal_read = _dll.JLINK_RTTERMINAL_Read
                    _dll_is_open = _dll.JLINKARM_IsOpen
                    _dll_is_connected = _dll.JLINKARM_EMU_IsConnected

                # 4. Acquire batch using current Tuner projections
                if batch is None:
                    batch = batch_acquire()

                try:
                    now = time_ns()

                    # --- FAST HARDWARE CHECK ---
                    # Calling the C-pointers directly.
                    # If they return 0, they are falsy, which triggers the exception.
                    if not _dll_is_open() or not _dll_is_connected():
                        raise Exception("J-Link connection lost.")

                    bytes_read = _dll_rtterminal_read(channel, c_buf, read_size)

                    if bytes_read > 0:
                        # Zero-copy slice of the pre-allocated Numpy memory view
                        chunk_view = np_buf[:bytes_read]

                        # 5. Insert Chunk
                        if not batch.insert(now, now, chunk_view):
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)

                            batch = batch_acquire()
                            batch.insert(now, now, chunk_view)

                    elif bytes_read == 0:
                        # No data? Sleep 1ms to yield the CPU

                        sleep(0.001)

                    else:
                        # JLINK_RTTERMINAL_Read returns < 0 on error

                        raise Exception(f"RTT Read failed with error code: {bytes_read}")

                    # 6. Maintenance: Check for time-based flush
                    if batch is not None and batch.start_ts > 0 and (now - batch.start_ts) >= delay_ns:
                        with batch:
                            self.distribute(batch)
                            tuner.update(batch.msg_cursor, batch.size, delay_s)
                        batch = None

                except Exception as e:
                    logger.error("J-Link RTT Runtime Error", e)
                    self.cleanup_jlink()
                    sleep(1.0)

        except Exception as e:
            logger.exception("Fatal error in J-Link RTT Reader loop", e)
        finally:
            # 7. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()

            self.cleanup_jlink()

    def cleanup_jlink(self):
        """Safely shuts down the J-Link session."""
        if self.jlink:
            # Step 1: Try to stop RTT
            try:
                self.jlink.rtt_stop()
            except Exception:
                pass  # Expected if the device was unexpectedly unplugged

            # Step 2: ALWAYS ensure the connection is closed
            try:
                self.jlink.close()
            except Exception:
                pass

            self.jlink = None

    def _drain_stale_data(self, jl):
        """
        Drains stale data from the target RAM.
        Stops as soon as the buffer is empty OR we have drained
        one full 'target_rtt_buffer_size', ensuring zero-loss of live data.
        """
        horizon = self.target_rtt_buffer_size
        self.logger.info(f"Draining RTT (Target Buffer: {horizon} bytes)...")

        total_drained = 0
        poll_attempts = 100  # 1.0s timeout

        while True:
            # Always read in 4k chunks for efficiency during drain
            junk = jl.rtt_read(self.channel, 4096)

            if junk:
                total_drained += len(junk)

                # --- THE HORIZON CHECK ---
                if total_drained >= horizon:
                    self.logger.info(f"Drain: Horizon reached ({total_drained} bytes). Handing off to main loop.")
                    break
            else:
                # If we've seen data and it suddenly stops, we're dry.
                if total_drained > 0:
                    self.logger.debug(f"Drain: Buffer dry after {total_drained} bytes.")
                    break

                # If we haven't seen anything yet, wait for the DLL to sync
                poll_attempts -= 1
                if poll_attempts <= 0:
                    self.logger.debug("Drain: No data found in 1s window.")
                    break

            sleep(0.01)

    def open(self):
        jl = None
        try:
            import pylink

            self.logger.info(f"Connecting to J-Link: {self.target_device}")
            jl = pylink.JLink()
            jl.exec_command("SuppressGUI")

            if self.serial_number:
                jl.open(serial_no=int(self.serial_number))
            else:
                jl.open()

            tif = (
                pylink.enums.JLinkInterfaces.SWD
                if self.interface.lower() == "swd"
                else pylink.enums.JLinkInterfaces.JTAG
            )
            jl.set_tif(tif)
            jl.connect(self.target_device, speed=self.speed)
            jl.rtt_start()

            self._drain_stale_data(jl)

            self.logger.info("Connected")
            return jl
        except Exception as e:
            self.logger.error("Failed to open J-Link.", e)
            if jl is not None:
                try:
                    jl.close()
                except Exception:
                    pass
            return None

    def send_data(self, data: str, channel: int = 0):
        """
        Sends data to the target's RTT Down-buffer.
        Can be called from other threads or downstream logic.
        """
        if self.jlink and self.jlink.opened():
            try:
                self.logger.info(f"Sending data to J-Link: {self.target_device}")
                # rtt_write returns the number of bytes actually written
                return self.jlink.rtt_write(channel, data.encode())
            except Exception as e:
                self.logger.error("RTT Write failed", e)
        return 0

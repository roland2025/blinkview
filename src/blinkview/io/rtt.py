# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import ctypes
from contextlib import contextmanager
from threading import Lock, RLock
from time import sleep
from typing import TYPE_CHECKING, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.io.jlink_debug import JLinkDebug, TargetDebugError
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner

if TYPE_CHECKING:
    import pylink

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


# How long a call from another thread (UI, agent) waits for the probe. The reader thread holds it
# for the length of every DLL call, and one that does not return must not freeze the caller too.
JLINK_LOCK_TIMEOUT_S = 2.0

# A read normally returns at once, with or without data.
SLOW_READ_NS = 1_000_000_000

_enumerator_lock = Lock()
_enumerator_jlink: Optional["pylink.JLink"] = None

_reader_jlinks_lock = Lock()
_reader_jlinks: dict[str, "pylink.JLink"] = {}


def _reader_jlink(serial_number: str) -> "pylink.JLink":
    """The JLink a reader opens its probe with, one per serial number for the life of the
    process and reused on every reconnect.

    JLINKARM_OpenEx() keeps the log/error ctypes callbacks of the JLink that called it, and a
    failed open leaves them registered (pylink only calls JLINKARM_Close() after a successful
    one). A JLink dropped after "Cannot connect to J-Link" so left the DLL holding freed
    callbacks - and while anything else keeps the DLL loaded (the _enumerator_jlink does),
    the next JLink() faulted on its first DLL call (an access violation in JLINKARM_IsOpen
    inside JLink.__init__), on every retry. A JLink that is never dropped keeps them valid.
    """
    with _reader_jlinks_lock:
        jl = _reader_jlinks.get(serial_number)
        if jl is None:
            import pylink

            jl = _reader_jlinks[serial_number] = pylink.JLink()
        return jl


def _list_jlink_serials() -> list[str]:
    """Serial numbers of the J-Links currently attached over USB.

    Uses one long-lived, lock-guarded JLink instance. Every pylink.JLink() in the
    process shares the same J-Link DLL, and destroying an instance calls into it
    and FreeLibrary()s it, so a throwaway instance per call (schema fetches run on
    worker threads) crashes the DLL under a reader thread that is opening its probe.
    """
    global _enumerator_jlink

    with _enumerator_lock:
        if _enumerator_jlink is None:
            import pylink

            _enumerator_jlink = pylink.JLink()
        return [str(emu.SerialNumber) for emu in _enumerator_jlink.connected_emulators()]


@DeviceFactory.register("jlink_rtt")
@configuration_property(
    "target_device",
    type="string",
    default="NRF52840_XXAA",
    required=True,
    ui_order=5,
    description="The target microcontroller device name, exactly as in SEGGER's J-Link device list "
    "(e.g., 'STM32F407VG', 'NRF52840_XXAA'). A family name such as 'NRF52' does not connect.",
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
    description="Record the incoming bytes to a session file. The file is not a plain byte dump and not text: each received chunk is stored as a timestamped header followed by its bytes.",
)
@configuration_property(
    "target_rtt_buffer_size",
    type="integer",
    default=8192,
    ui_order=22,
    description="The physical size of the RTT Up-Buffer in the target's RAM. "
    "Used to precisely flush stale data on startup without discarding live telemetry.",
)
@override_property("stall_warn_s", default=30)
@override_property("stall_reconnect_s", default=60)
class JLinkRTTReader(BaseReader):
    __doc__ = """The primary data ingestion source for Segger J-Link RTT (Real-Time Transfer).

* Provides high-speed, non-intrusive background telemetry acquisition
* Supports specific device targeting via J-Link serial numbers
* Reads continuously from the specified RTT Up-Buffer (Channel)
* Efficiently batches high-frequency incoming byte streams

Leverages the `pylink-square` library under the hood. Batches are accumulated based on the configured delay to minimize downstream processing overhead without dropping high-throughput streams."""

    supports_reconnect = True
    stall_hint = (
        "Check the J-Link USB hubs and controller: when every board goes quiet at once, power-cycling "
        "the hubs has fixed it."
    )

    type: str
    target_device: str
    serial_number: str
    channel: int
    interface: str
    speed: int
    delay: int
    target_rtt_buffer_size: int

    def __init__(self):
        super().__init__()

        self.logging_type = "default"
        self.logging_processor = "binary"
        self.jlink: Optional[pylink.JLink] = None
        self._jlink_lock = RLock()
        self.debug = JLinkDebug(self)

        self.logger_send = None

    def apply_config(self, config: dict):
        # 1. Apply configuration via the parent framework
        changed = super().apply_config(config)

        self.logger_send = self.logger_send or self.logger.child("send")

        return changed

    @classmethod
    def get_config_schema(cls) -> dict:
        schema = super().get_config_schema()

        try:
            # Dynamically fetch connected J-Link emulators
            serials = _list_jlink_serials()
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
        stop_wait = self._stop_event.wait
        time_ns = self.shared.time_ns
        logger = self.logger
        jlink_lock = self._jlink_lock

        # Tuner configuration
        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)
        channel = self.channel

        read_size = 64 * 1024

        self.logger_state_open.info("0")

        c_buf = (ctypes.c_ubyte * read_size)()
        np_buf = np.frombuffer(c_buf, dtype=dtypes.BYTE)

        # 2. Stats and Auto-Tuning Setup
        stats = Speedometer(logger=self.logger.stats_child("stats"))
        tuner = ThroughputAutoTuner(
            speedometer=stats,
            default_buffer_bytes=read_size,
            msg_size_bytes=20,
            logger=self.logger.stats_child("tuner"),
        )

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch: PooledLogBatch = None

        try:
            while not stop_is_set():
                # self.open() handles its own internal locking safely
                if self.jlink is None:
                    created_jl = self.open()
                    if created_jl is None:
                        stop_wait(1.0)
                        continue

                    # Safely bind the localized function pointers under lock assignment
                    with jlink_lock:
                        self.jlink = created_jl
                        _dll = self.jlink._dll  # noqa
                        _dll_rtterminal_read = _dll.JLINK_RTTERMINAL_Read
                        _dll_is_open = _dll.JLINKARM_IsOpen
                        _dll_is_connected = _dll.JLINKARM_EMU_IsConnected
                        self._mark_ready()

                # Acquire batch using current Tuner projections
                if batch is None:
                    batch = batch_acquire()

                try:
                    with jlink_lock:
                        if self.jlink is None:
                            continue  # closed by another thread (rtt_restart) while we waited for the lock

                        # --- FAST HARDWARE CHECK ---
                        # Calling the C-pointers directly.
                        # If they return 0, they are falsy, which triggers the exception.
                        if not _dll_is_open() or not _dll_is_connected():
                            raise Exception("J-Link connection lost.")

                        now = time_ns()
                        bytes_read = _dll_rtterminal_read(channel, c_buf, read_size)

                    read_ns = time_ns() - now
                    if read_ns > SLOW_READ_NS:
                        self.logger_link.warn("RTT read blocked for %.1f s.", read_ns / 1e9)

                    if bytes_read > 0:
                        # 5. Insert Chunk
                        if not batch.insert_view(now, now, np_buf, bytes_read):
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor + bytes_read, batch.size, delay_s)
                            batch = batch_acquire()
                            batch.insert_view(now, now, np_buf, bytes_read)

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
                    self.logger_link.error("Problem.", exc=e)
                    self.cleanup_jlink()
                    stop_wait(1.0)

        except Exception as e:
            logger.exception("Fatal error in J-Link RTT Reader loop", exc=e)
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
        with self._jlink_lock:
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

                self.logger_state_open.warn("0")
                self.jlink = None

    def _drain_stale_data(self, jl):
        """
        Drains stale data from the target RAM.
        Stops as soon as the buffer is empty OR we have drained
        one full 'target_rtt_buffer_size', ensuring zero-loss of live data.
        """
        with self._jlink_lock:
            horizon = self.target_rtt_buffer_size
            self.logger.info("Draining RTT (Target Buffer: %s bytes)...", horizon)

            total_drained = 0

            time_ns = self.shared.time_ns
            start_time = time_ns()
            timeout_ns = 1_500_000_000  # 1.5 seconds in nanoseconds

            while True:
                # Absolute Timeout
                if (time_ns() - start_time) > timeout_ns:
                    self.logger.warn("Drain: Timed out after 1.5s. Handing off.")
                    break

                # Always read in 4k chunks for efficiency during drain
                junk = jl.rtt_read(self.channel, 4096)

                if junk:
                    total_drained += len(junk)

                    # --- THE HORIZON CHECK ---
                    if total_drained >= horizon:
                        self.logger.info("Drain: Horizon reached (%s bytes). Handing off.", total_drained)
                        break

                    # Fast-Drain: DO NOT SLEEP if data was found
                    continue

                else:
                    # If we've seen data and it suddenly stops, we're dry.
                    if total_drained > 0:
                        self.logger.debug("Drain: Buffer dry after %s bytes.", total_drained)
                        break

                # Wait State: Sleep ONLY if no data was found yet
                sleep(0.01)

    def open(self):
        with self._jlink_lock:
            jl = None
            try:
                import pylink

                self.logger_link.info("Connecting to J-Link: %s", self.serial_number)
                jl = _reader_jlink(self.serial_number)
                jl.exec_command(
                    "SuppressGUI = 1"
                )  # bare "SuppressGUI" makes the DLL print an "Info: No parameter" line per open

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
                try:
                    jl.connect(self.target_device, speed=self.speed)
                except (ValueError, pylink.errors.JLinkException) as e:
                    # What pylink says for a name that is not in the J-Link device list.
                    if str(e) in ("Invalid index.", "Unsupported device selected."):
                        raise ValueError(
                            f"J-Link does not know the target device '{self.target_device}': it needs the exact "
                            "name from SEGGER's device list (e.g. 'NRF52840_XXAA', not 'NRF52')"
                        ) from None
                    raise
                jl.rtt_start()

                self._drain_stale_data(jl)

                self.logger_link.info("Connected")

                self.logger_state_open.info("1")
                return jl
            except Exception as e:
                self.logger_link.error(str(e))

                self.logger_state_open.error("0")
                if jl is not None:
                    try:
                        jl.close()
                    except Exception:
                        pass
                return None

    @property
    def is_connected(self) -> bool:
        return self.jlink is not None

    def reconnect(self) -> bool:
        """Closes the probe; the reader loop sees self.jlink is None and opens it again. A full
        reconnect, not rtt_stop() + rtt_start() on the open session: that left a stalled RTT (reads
        returning 0 bytes) stalled, while closing the probe and opening it again - what disabling and
        enabling the source does - brought data back. Used by the rtt_restart command and the stall
        watchdog; False when the probe stays busy."""
        if not self._jlink_lock.acquire(timeout=JLINK_LOCK_TIMEOUT_S):
            self.logger_link.warn("Reconnect dropped: the J-Link is busy (a DLL call has not returned).")
            return False
        try:
            self.cleanup_jlink()
            return True
        finally:
            self._jlink_lock.release()

    def describe_stall(self) -> Optional[str]:
        """The DLL's RTT status and whether the probe can reach the target, for the stall warning."""
        if not self._jlink_lock.acquire(timeout=JLINK_LOCK_TIMEOUT_S):
            return "J-Link busy (a DLL call has not returned)"
        try:
            jl = self.jlink
            if jl is None:
                return "no open J-Link session"
            parts = []
            try:
                st = jl.rtt_get_status()
                parts.append(
                    f"RTT running={st.IsRunning} up_buffers={st.NumUpBuffers} "
                    f"bytes_transferred={st.NumBytesTransferred} bytes_read={st.NumBytesRead} "
                    f"host_overflows={st.HostOverflowCount}"
                )
            except Exception as e:
                parts.append(f"RTT status failed: {e}")
            try:
                parts.append("target halted" if jl.halted() else "target running")
            except Exception as e:
                parts.append(f"target not reachable: {e}")
            return "; ".join(parts)
        finally:
            self._jlink_lock.release()

    @contextmanager
    def probe(self):
        """The open JLink, held for the length of the block. For calls from other threads: waits
        JLINK_LOCK_TIMEOUT_S for the probe, and raises TargetDebugError when it stays busy or no
        session is open."""
        if not self._jlink_lock.acquire(timeout=JLINK_LOCK_TIMEOUT_S):
            raise TargetDebugError("the J-Link is busy: a call into its DLL has not returned")
        try:
            if self.jlink is None:
                raise TargetDebugError("no open J-Link session: the source is not connected")
            try:
                yield self.jlink
            except TargetDebugError:
                raise
            except Exception as e:
                raise TargetDebugError(str(e) or type(e).__name__) from e
        finally:
            self._jlink_lock.release()

    def send_data(self, data: str, channel: int = 0):
        """
        Sends data to the target's RTT Down-buffer.
        Can be called from other threads or downstream logic.
        """
        if not self._jlink_lock.acquire(timeout=JLINK_LOCK_TIMEOUT_S):
            self.logger_link.error("RTT Write dropped: the J-Link is busy.")
            return 0
        try:
            if self.jlink and self.jlink.opened():
                try:
                    encoded = data.encode()
                    self.logger_send.debug(str(encoded))
                    # rtt_write returns the number of bytes actually written
                    return self.jlink.rtt_write(channel, encoded)
                except Exception as e:
                    self.logger_link.error("RTT Write failed", exc=e)
            return 0
        finally:
            self._jlink_lock.release()

    def get_commands(self) -> list[tuple[str, str]]:
        """Exposes expanded J-Link RTT runtime capabilities to GUI/CLI layers."""
        return [
            ("reset", "Reset MCU"),
            ("halt", "Halt"),
            ("restart", "Resume"),
            ("rtt_restart", "Restart RTT"),
        ]

    def send_command(self, command: str):
        """Processes incoming command strings routed from the UI/CLI layers."""
        command = command.strip()

        self.logger.debug("send_command: %s", command)

        if not self._jlink_lock.acquire(timeout=JLINK_LOCK_TIMEOUT_S):
            self.logger.warn("Command dropped: the J-Link is busy.")
            return
        try:
            if not self.jlink:
                self.logger.warn("Command dropped: No active J-Link session initialized.")
                return

            match command:
                case "rtt_restart":
                    self.logger.info("RTT restart: reconnecting the J-Link.")
                    self.reconnect()

                case "reset":
                    self.logger.info("Initiating hardware reset on target MCU: %s", self.target_device)
                    try:
                        self.jlink.reset(halt=False)
                        self.logger.info("Target MCU successfully reset.")
                    except Exception as e:
                        self.logger.error("Hardware reset failed: %s", e)

                case "halt":
                    try:
                        success = self.jlink.halt()
                        if success:
                            self.logger.info("Target core execution halted (paused).")
                        else:
                            self.logger.error("Target was not halted")

                    except Exception as e:
                        self.logger.error("Failed to halt core: %s", e)

                case "restart":
                    try:
                        if self.jlink.restart():
                            self.logger.info("Target core execution resumed.")
                        else:
                            self.logger.warn("Target was not halted")
                    except Exception as e:
                        self.logger.error("Failed to resume core: %s", e)

                case _:
                    # Fall back to raw string transmission over RTT Down-buffer channel 0
                    self.send_data(command)
        finally:
            self._jlink_lock.release()

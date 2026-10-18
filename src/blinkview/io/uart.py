# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from time import sleep

from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner


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
    description="Record the incoming bytes to a session file. The file is not a plain byte dump and not text: each received chunk is stored as a timestamped header followed by its bytes.",
)
@configuration_property(
    "suppress_auto_reset",
    type="boolean",
    default=False,
    description="Prevents the device from resetting when the port opens by pulling DTR and RTS low. Essential for ESP32 and Arduino boards.",
)
@configuration_property(
    "serial_exclusive",
    type="boolean",
    default=True,
    description="Open the port so no other program can open it at the same time. On Windows a COM port is always "
    "exclusive; on Linux/macOS this stops a flasher (or a second terminal) from silently sharing the port with "
    "blink, which garbles both. Turn off only to share a port on purpose.",
)
@configuration_property(
    "allow_borrow",
    type="boolean",
    default=False,
    description="Lend the port to an external tool on request: `blink borrow COM5 -- pio run -t upload` closes the "
    "port while the command runs and reopens it afterwards. For flashing through the same port the logs come from "
    "(ESP32, Arduino).",
)
@configuration_property(
    "borrow_name",
    type="string",
    default="",
    description="The name a tool borrows this port by. Empty: the port's own name (COM5, ttyUSB0). A path is used as "
    "the borrow file itself.",
)
@configuration_property(
    "borrow_timeout_s",
    type="integer",
    default=120,
    minimum=0,
    description="Take the port back when a borrow has not been returned after this many seconds. 0: never.",
)
@configuration_property(
    "reset_after_return",
    type="boolean",
    default=False,
    description="Reset the device (DTR/RTS) right after the port comes back from a borrow, so its boot log is "
    "captured. Pair it with esptool's `--after no_reset` on the flashing side.",
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

    serial_exclusive: bool
    allow_borrow: bool
    borrow_name: str
    borrow_timeout_s: int
    reset_after_return: bool

    supports_pause = True

    def __init__(self):
        super().__init__()

        self.logging_type = "default"  # Use the default logging mechanism
        self.logging_processor = "binary"

        self._serial_lock = Lock()

        self.serial = None

        self.serial_broken = False

        # Set by the reader thread once it has closed the port for request_pause().
        self._paused = False

        self.logger_send = None

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

    def apply_config(self, config: dict):
        # 1. Apply configuration via the parent framework
        changed = super().apply_config(config)

        self.logger_send = self.logger_send or self.logger.child("send")

        return changed

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
        # We set msg_size_bytes to 20 to maintain your ~50 chunks/KB density preference
        stats = Speedometer(logger=self.logger.stats_child("stats"))
        tuner = ThroughputAutoTuner(speedometer=stats, msg_size_bytes=20, logger=self.logger.stats_child("tuner"))

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch: PooledLogBatch = None
        ser = None
        self.serial = None

        try:
            while not stop_is_set():
                # 3. Serial Lifecycle Management
                if self.serial_broken:
                    with self._serial_lock:
                        if self.serial is not None:
                            try:
                                self.serial.close()
                            finally:
                                self.serial = None
                                ser = None
                                self.logger_link.warn("Broken, port closed.")
                                self.logger_state_open.warning("0")

                # Paused (the port is lent to an external tool, io/source_borrow.py): keep it
                # closed until request_resume().
                if self._pause_requested:
                    if ser is not None:
                        # Hand over what was read so far, it is not part of what the tool sends.
                        if batch is not None:
                            if len(batch) > 0:
                                with batch:
                                    self.distribute(batch)
                            else:
                                batch.release()
                            batch = None
                        with self._serial_lock:
                            try:
                                ser.close()
                            finally:
                                ser = None
                                self.serial = None
                                self.logger_state_open.info("0")
                    if not self._paused:
                        self._paused = True
                        self.logger_link.info("Port closed, lent out.")
                        self.logger_state_lent.info("1")
                    sleep(0.05)
                    continue

                if self._paused:
                    self._paused = False
                    self.logger_state_lent.info("0")
                    ser = self.open()
                    if ser is None:
                        sleep(1.0)
                        continue
                    _read = ser.read
                    if self.reset_after_return:
                        self.reset_device()

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
                    self.logger_link.error("Closed, read error", exc=e)
                    ser = None
                    self.serial = None
                    self.logger_state_open.warning("0")
                    sleep(1.0)

        except Exception as e:
            logger.exception("Fatal error in Serial Reader loop", exc=e)
        finally:
            # 8. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()
            with self._serial_lock:
                if self.serial is not None:
                    try:
                        self.serial.close()
                    finally:
                        self.serial = None
                        self.logger_link.info("Port closed.")
                        self.logger_state_open.info("0")

    def open(self):
        try:
            from blinkview.parsers.binary_parser import BinaryParser

            time_ns = self.shared.time_ns

            # sync_state = self._init_sync_engine(anchor_is_boot=True)

            target_parser = next((s for s in self.subscribers if isinstance(s, BinaryParser)), None)

            BUF_SIZE = 64 * 1024  # 64KB buffer for incoming serial data

            self.logger_link.info("Opening '%s' at %s baud", self.url, self.baudrate)

            from serial import serial_for_url

            # Use PySerial's URL handler (handles socket://, rfc2217://, hwgrep://, etc.)
            ser = serial_for_url(
                self.url,
                baudrate=self.baudrate,
                timeout=self.delay / 1000.0,
                inter_byte_timeout=0.01,
                # pyserial's own lock (flock on POSIX) only keeps out tools that also take it.
                exclusive=True if self.serial_exclusive else None,
            )
            if self.serial_exclusive:
                self._lock_tty_exclusive(ser)

            with self._serial_lock:
                self.serial = ser
                self.serial_broken = False

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
                self.logger.error("Failed to set buffer size. This may not be supported on all platforms.", exc=e)
                pass

            self.logger_link.info("Connected")
            self.logger_state_open.info("1")
            self._mark_ready()

            return ser
        except Exception as e:
            self.logger_link.error("Failed to open serial port.", exc=e)

    def _lock_tty_exclusive(self, ser):
        """TIOCEXCL on a real tty (Linux/macOS): the kernel then refuses every other non-root open
        with EBUSY, also from tools that don't take pyserial's advisory flock - a flasher run
        without `blink borrow` fails cleanly with "port busy" instead of sharing the port. Windows
        COM ports are exclusive already."""
        fd = getattr(ser, "fd", None)
        if fd is None or "://" in self.url:
            return
        try:
            import fcntl
            import termios

            fcntl.ioctl(fd, termios.TIOCEXCL)
        except (ImportError, AttributeError):
            pass
        except OSError as e:
            self.logger_link.warn("Could not lock the port for exclusive use: %s", e)

    def send_data(self, data: str):
        with self._serial_lock:
            if self.serial and self.serial.is_open:
                try:
                    encoded = data.encode()
                    self.logger_send.debug(str(encoded))
                    self.serial.write(encoded)
                except Exception as e:
                    self.logger_link.exception("Failed to send data", exc=e)
                    self.serial_broken = True

    def reset_device(self):
        """
        Triggers a hardware reset by toggling the DTR/RTS lines.
        Matches the logic used by esptool.py for ESP32 boards.
        """
        with self._serial_lock:
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
                self.logger.error("Failed to perform hardware reset", exc=e)
                self.serial_broken = True

    @property
    def is_connected(self) -> bool:
        if self._paused:
            return False
        with self._serial_lock:
            try:
                return self.serial is not None and self.serial.is_open
            except Exception as e:
                self.logger.error("Failed to check serial connection", exc=e)
                self.serial_broken = True
                return False

    @property
    def is_paused(self) -> bool:
        return self._paused

    def default_borrow_name(self) -> str:
        from blinkview.borrow import name_from_port

        return name_from_port(self.url)

    def get_commands(self) -> list[tuple[str, str]]:
        """Exposes UART runtime capabilities to GUI/CLI layers."""
        return [
            ("reset", "Reset MCU"),
        ]

    def send_command(self, command: str):
        """Processes incoming command strings routed from the UI/CLI layers."""
        command = command.strip()

        self.logger.debug("send_command: %s", command)

        match command:
            case "reset":
                self.logger_link.info("Initiating hardware reset via DTR/RTS toggle.")
                self.reset_device()

            case _:
                # Fall back to raw string transmission over serial
                self.send_data(command)

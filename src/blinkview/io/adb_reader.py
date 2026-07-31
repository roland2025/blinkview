# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import datetime
import os
import statistics
import subprocess
from threading import Lock
from time import perf_counter, sleep

from blinkview.core.configurable import configuration_property, override_property
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.time_sync_engine import TimeSyncEngine
from blinkview.core.types.parsing import SyncState
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.parsers.binary_parser import BinaryParser
from blinkview.utils.adb import detect_adb_path
from blinkview.utils.throughput import Speedometer, ThroughputAutoTuner


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
# @configuration_property(
#     "clear_log",
#     type="boolean",
#     default=False,
#     description="If true, executes 'adb logcat -c' to clear the device log buffer before streaming begins.",
# )
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

        self._shell = None  # The persistent command shell
        self._shell_lock = Lock()

        self._syncer = None

        self.sleep_offset_ns = 0

        self._last_error_log_ns = 0

        self._process_ids: dict[str, int] = {}
        self._last_polled_pids: set[int] = set()
        self._pid_poll_task_id = None

    def run(self):
        # 1. Setup and Localize Lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Backoff configuration (All in Nanoseconds)
        current_backoff_ns = 1_000_000_000  # Start at 1s
        max_backoff_ns = 5_000_000_000  # Cap at Xs
        next_reconnect_ns = 0

        # Tuner configuration
        delay_s = self.delay / 1000.0
        delay_ns = int(self.delay * 1_000_000)

        # 2. Stats and Auto-Tuning Setup
        stats = Speedometer(logger=self.logger.child("stats"))
        tuner = ThroughputAutoTuner(
            speedometer=stats, default_buffer_bytes=65536, msg_size_bytes=64, logger=self.logger.child("tuner")
        )

        pool_create = self.shared.array_pool.create

        def batch_acquire():
            # Dynamically pull configuration from the tuner's latest projections
            return pool_create(PooledLogBatch, tuner.estimated_capacity, tuner.estimated_buffer_bytes)

        batch: PooledLogBatch = None

        try:
            while not stop_is_set():
                # 3. Subprocess Lifecycle Management
                if self._process is None:
                    now = time_ns()

                    if now >= next_reconnect_ns:
                        self.open()

                        # IF OPEN FAILED: We MUST continue to avoid the crash below
                        if self._process is None:
                            next_reconnect_ns = now + current_backoff_ns

                            current_backoff_ns = min(int(current_backoff_ns * 1.5), max_backoff_ns)
                            # Only log as INFO every once in a while to keep the terminal clean
                            if current_backoff_ns == max_backoff_ns:
                                logger.debug("Device not found. Polling at max frequency...")
                            else:
                                logger.info("Device not found. Retrying in %.1fs...", current_backoff_ns / 1e9)

                            # Stay responsive to stop_is_set while waiting for next attempt
                            sleep(0.2)
                            continue
                        else:
                            # Connection Successful! Reset backoff
                            current_backoff_ns = 1_000_000_000
                    else:
                        # We are in the backoff wait period
                        sleep(0.2)
                        continue

                    # We localize the read method for performance
                    # read1(size) blocks until *at least* 1 byte is available,
                    # then returns whatever is buffered up to the size limit.
                    _read1 = self._process.stdout.read1
                    fd = self._process.stdout.fileno()
                    os_read = os.read

                # 4. Acquire batch using current Tuner projections
                if batch is None:
                    batch = batch_acquire()

                try:
                    # 5. Read incoming chunk
                    # 65536 is a standard healthy OS buffer size
                    # chunk = _read1(65536)
                    chunk = os_read(fd, 65536)

                    if chunk:
                        now = time_ns()

                        # 2. Attempt high-resolution insertion
                        # This returns False if we hit array capacity OR buffer capacity
                        if not batch.insert(now, now, chunk):
                            # 3. Batch is full: Flush and Acquire new
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)

                            batch = batch_acquire()

                            # Re-attempt the insert into the fresh batch
                            # (Assuming chunk < total batch capacity)
                            batch.insert(now, now, chunk)

                        # 4. Batching Window Check
                        # Now every chunk has a unique timestamp in the bundle,
                        # but we still flush based on the first arrival in this batch.
                        if (now - batch.start_ts) >= delay_ns:
                            with batch:
                                self.distribute(batch)
                                tuner.update(batch.msg_cursor, batch.size, delay_s)
                            batch = None

                        # if len(chunk) < 2048:
                        # sleep(0.016)

                    else:
                        # If read1 returns empty bytes, the subprocess reached EOF (ADB died)
                        logger.warning("ADB process stream ended. Restarting...")
                        self._cleanup_process()
                        next_reconnect_ns = time_ns() + current_backoff_ns

                except Exception as e:
                    logger.error("ADB read error: %s", e)
                    self._cleanup_process()
                    next_reconnect_ns = time_ns() + current_backoff_ns

        except Exception as e:
            logger.exception("Fatal error in ADB Reader loop", exc=e)
        finally:
            # 7. Final Cleanup
            if batch is not None:
                if len(batch) > 0:
                    with batch:
                        self.distribute(batch)
                else:
                    batch.release()

            self._cleanup_process()

    def open(self):
        adb_bin = detect_adb_path()
        time_ns = self.shared.time_ns
        now_ns = time_ns()

        try:
            base_cmd = [adb_bin]
            if self.device_id:
                base_cmd.extend(["-s", self.device_id])

            # 1. Start Persistent Shell FIRST (Necessary for the Anchor)
            self.logger.debug("Opening ADB shell...")
            self._shell = subprocess.Popen(
                [*base_cmd, "shell"],
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=False,
                bufsize=-1,
            )

            # Give it a moment to fail
            sleep(0.1)
            if self._shell.poll() is not None:
                # If we're here, the shell died immediately
                _, err = self._shell.communicate()
                err_msg = err.strip() if err else "No devices found"
                raise ConnectionError(err_msg)

            # 5. Clear logs if requested (Now safe to do since shell is open)
            # if self.clear_log or True:
            #     self.logger.info("Clearing device logs...")
            #     subprocess.run([*base_cmd, "logcat", "-c"], check=False, capture_output=True)

            # We use 'monotonic' to get hardware-level crystal time
            cmd = [*base_cmd, "logcat", "-v", "long", "-v", "monotonic", "-v", "nsec", "-T", "1"]

            if self.logcat_args:
                cmd.extend(self.logcat_args.split())
            if self.filters:
                cmd.extend(self.filters.split())

            self.logger.debug("Opening Monotonic ADB stream: %s", " ".join(cmd))
            self._process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                bufsize=-1,
            )

            # 7. Finalize Environment
            self._refresh_process_ids()
            self._pid_poll_task_id = self.shared.tasks.run_periodic(15.0, self._refresh_process_ids)

            self.logger.info("ADB Connected")
            self._last_error_log_ns = 0  # Reset so the next failure is 'Red' immediately
            return self._process.stdout

        except Exception as e:
            # --- SUPPRESSION LOGIC ---
            # We only show the ERROR level every 30 seconds
            if (now_ns - self._last_error_log_ns) > 30_000_000_000:
                self.logger.error("ADB Connection failed: %s", e)
                self._last_error_log_ns = now_ns
            else:
                # Keep it quiet in the meantime
                self.logger.debug("Retrying ADB... (%s)", e)

            self._cleanup_process()
            return None

    def is_connected(self):
        return self.enabled and self._shell and self._process

    def _cleanup_process(self):
        """
        Safely terminates both the Logcat stream and the persistent
        command shell subprocesses.
        """
        # 1. Handle the Logcat Stream
        if self._process is not None:
            self.logger.debug("Terminating ADB Logcat subprocess...")
            self._finalize_subprocess(self._process)
            self._process = None

        # 2. Handle the Persistent Shell
        if self._shell is not None:
            self.logger.debug("Terminating persistent ADB shell...")
            # Try to be polite and exit the shell first
            try:
                if self._shell.stdin:
                    self._shell.stdin.write("exit\n")
                    self._shell.stdin.flush()
            except Exception:
                pass

            self._finalize_subprocess(self._shell)
            self._shell = None

        # if self._syncer_task_id is not None:
        #     self.shared.tasks.stop_periodic(self._syncer_task_id)

        if self._pid_poll_task_id is not None:
            self.shared.tasks.stop_periodic(self._pid_poll_task_id)
            self._pid_poll_task_id = None

    def _finalize_subprocess(self, proc: subprocess.Popen):
        """Helper to terminate, wait, and eventually kill a process."""
        try:
            proc.terminate()
            try:
                proc.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                self.logger.warning("Process %s didn't stop in time. Killing...", proc.pid)
                proc.kill()
        except Exception as e:
            self.logger.error("Error while finalizing process %s: %s", proc.pid, e)

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
            self.logger.error("Failed to reboot device: %s", e)

    def send_data(self, data: str):
        """
        Sends a command string to the persistent ADB shell and waits for completion.
        This prevents 'Pipe Pollution' by ensuring all output is consumed.
        """
        # Simply delegate to query and ignore the return list.
        # The sentinel inside query() acts as a synchronization barrier.
        _ = self.query(data)

    def query(self, cmd: str) -> list[str]:
        if self._shell is None:
            return []

        # Use bytes for the sentinel
        sentinel = b"__BlinkSync_Done__"
        results = []

        with self._shell_lock:
            # Encode command to bytes
            self._shell.stdin.write(f"{cmd}; echo {sentinel.decode()}\n".encode())
            self._shell.stdin.flush()

            output = b""
            while True:
                # .buffer is not needed because the stream is already binary
                chunk = self._shell.stdout.read1(4096)
                if not chunk:
                    break
                output += chunk
                if sentinel in output:
                    break

            # Decode once at the end
            decoded_output = output.replace(sentinel, b"").decode(errors="ignore")
            return [line.strip() for line in decoded_output.splitlines() if line.strip()]

    def _refresh_process_ids(self):
        """Uses the persistent shell to map all process names to PIDs. Also feeds the shared,
        Numba-backed IdHistory (self.shared.pid_history) so that a PID's process name can later
        be resolved as of any past timestamp, not just "whatever currently holds it" - Android
        recycles PIDs, so the two are not the same thing over the life of a session."""
        # Use ps -A -o NAME,PID for modern Android compatibility
        lines = self.query("ps -A -o NAME,PID")

        logger = self.logger

        pids: dict[str, int] = {}

        # We skip the header 'NAME PID' if it exists in the output
        for line in lines:
            parts = line.split()
            if len(parts) >= 2:
                name = parts[0]
                try:
                    # The PID is usually the second column
                    pid = int(parts[1])
                    pids[name] = pid
                    logger.debug("pid=%s name=%s", pid, name)
                except ValueError:
                    continue

        self._process_ids = pids
        logger.info("Captured %s application PIDs via shell.", len(self._process_ids))

        now_ns = self.shared.time_ns()
        history = self.shared.pid_history
        current_pids = set(pids.values())
        device_ids = self._owning_pipeline_device_ids()

        for device_id in device_ids:
            for name, pid in pids.items():
                history.update(self._pid_history_key(device_id, pid), name, now_ns)

            # Any PID we saw last poll but not this one is gone (process died) - close its
            # interval rather than leaving it open forever.
            for pid in self._last_polled_pids - current_pids:
                history.close(self._pid_history_key(device_id, pid), now_ns)

        self._last_polled_pids = current_pids

    def _owning_pipeline_device_ids(self) -> list[int]:
        """Finds every pipeline (parser) configured to consume from this source and returns each
        one's registry device id. This raw io source has no device identity of its own - device
        ids are resolved at the pipeline/parser layer (core/pipeline_manager.py: `device_id =
        self.shared.id_registry.get_device(name)`), and a source's own config name is a separate,
        unrelated string that's never registered as a DeviceIdentity. PID history has to be keyed
        by the SAME device id LogBundle rows end up tagged with, so this reader must look it up
        rather than guess at its own. A single source can legitimately feed more than one
        differently-named pipeline (e.g. two frame_parser configs decoding the same raw bytes
        two different ways), so this returns a list, not a single id."""
        pipelines = getattr(self.shared.registry.pipelines, "pipelines", None)
        if not pipelines:
            return []

        device_ids = []
        for pipeline in pipelines.values():
            sources = getattr(pipeline, "sources_", None)
            if isinstance(sources, str):
                sources = [sources]
            if sources and self.reference_id in sources:
                device_ids.append(pipeline.local.device_id.id)
        return device_ids

    @staticmethod
    def _pid_history_key(device_id: int, pid: int) -> int:
        """Bit-packs a pipeline's registry device id with a locally-scoped PID into the single
        opaque uint64 key IdHistory expects, so PIDs from different simultaneously-connected ADB
        devices never collide (both halves comfortably fit 32 bits)."""
        return (device_id << 32) | pid

    def resolve_process_name(self, pid: int, ts_ns: int) -> str | None:
        """Convenience wrapper so callers (UI, future logfmt filtering) don't need to know the
        key-packing convention above. Checks every pipeline fed by this source, since PID history
        is recorded per-pipeline-device-id (see _owning_pipeline_device_ids)."""
        history = self.shared.pid_history
        for device_id in self._owning_pipeline_device_ids():
            name = history.resolve(self._pid_history_key(device_id, pid), ts_ns)
            if name:
                return name
        return None

    def get_name_from_pid(self, pid: int) -> str | None:
        """
        Reads /proc/[pid]/cmdline to find the package name.
        """
        # cmdline contains the name followed by a null byte (\0)
        # 'tr' or 'cat' works, but cat is standard.
        res = self.query(f"cat /proc/{pid}/cmdline")

        if res:
            # Android processes often have the name as the first null-terminated string.
            # We strip any trailing null characters.
            name = res[0].strip("\x00")
            if name:
                self._process_ids[name] = pid  # Sync our internal map
                return name
        return None

    def get_pid_from_name(self, package_name: str) -> int | None:
        """
        Uses 'ps' to find the PID of a specific package.
        """
        # -A: all processes
        # -o PID,NAME: only return the columns we care about
        # grep: find our specific package
        lines = self.query(f"ps -A -o PID,NAME | grep {package_name}")

        for line in lines:
            parts = line.split()
            if len(parts) >= 2:
                try:
                    # In 'PID NAME' format, PID is index 0
                    pid = int(parts[0])
                    name = parts[1]

                    # Ensure it's an exact match, not just a substring
                    if name == package_name:
                        self._process_ids[name] = pid
                        return pid
                except ValueError:
                    continue
        return None

    def _get_best_coarse_anchor(self, num_tries: int = 3) -> tuple[int, int, int]:
        time_ns = self.shared.time_ns
        best_rtt = 2**63 - 1
        best_phone_ns = 0
        best_pc_ns = 0

        self.logger.info("Synchronizing coarse monotonic baseline (tries=%s)...", num_tries)

        for i in range(num_tries):
            t0 = time_ns()
            # /proc/uptime returns: "1733138.75 3456.78" (uptime_seconds idle_seconds)
            res = self.query("cat /proc/uptime")
            t1 = time_ns()

            if res:
                parts = res[0].split()
                if parts:
                    try:
                        # Parse the float and convert to nanoseconds
                        phone_time_sec = float(parts[0])
                        phone_time = int(phone_time_sec * 1_000_000_000)
                        rtt = t1 - t0

                        if rtt < best_rtt:
                            best_rtt = rtt
                            best_phone_ns = phone_time
                            best_pc_ns = t0 + (rtt // 2)
                    except ValueError:
                        continue

        if best_phone_ns == 0:
            self.logger.warning("Coarse anchor failed. Falling back to PC-only identity.")
            now = time_ns()
            return now, now, 0

        return best_phone_ns, best_pc_ns, best_rtt

    def _calculate_sleep_offset_ns(self, num_samples: int = 7) -> int:
        """
        Calculates the sleep offset by taking multiple samples using a clean,
        silent chained shell command.
        """
        offsets = []

        # Chained command that silences stderr and filters for only the numbers
        cmd = (
            'BOOT=$(cat /proc/uptime 2>/dev/null | cut -d" " -f1); '
            r'MONO=$(logcat -v monotonic -v nsec -t 1 2>/dev/null | grep -oE "[0-9]+\.[0-9]{9}" | head -n 1); '
            'echo "$BOOT $MONO"'
        )

        self.logger.info("Synchronizing sleep offset (samples=%s)...", num_samples)

        for i in range(num_samples):
            res = self.query(cmd)

            if res and len(res[0].split()) == 2:
                try:
                    boot_s, mono_s = res[0].split()
                    boot_ns = int(float(boot_s) * 1_000_000_000)
                    mono_ns = int(float(mono_s) * 1_000_000_000)

                    offsets.append(boot_ns - mono_ns)
                except (ValueError, IndexError):
                    continue

        if not offsets:
            self.logger.error("Failed to capture any valid sleep offset samples.")
            return 0

        # Median kills the outliers if the shell was slow on one specific sample
        stable_offset = int(statistics.median(offsets))

        # Optional: Log the jitter to see how 'clean' the phone is behaving
        jitter_ms = (max(offsets) - min(offsets)) / 1_000_000.0
        self.logger.info("Offset synced: %.6fs (Jitter: %.3fms)", stable_offset / 1e9, jitter_ms)

        return stable_offset

    def _shell_sync_handler(self):
        """
        Fallback syncer that uses the ADB shell to 'ping' the device.
        Lacks microsecond precision but prevents long-term drift.
        """
        # 1. Capture PC Transmit Time
        t_tx = self.shared.time_ns()

        # 2. Get Phone time via Shell (This is the 'Pong')
        # We fetch BOOT and MONO in one shot to minimize internal jitter
        res = self.query(
            'B=$(cat /proc/uptime 2>/dev/null | cut -d" " -f1); '
            r'M=$(logcat -v monotonic -v nsec -t 1 2>/dev/null | grep -oE "[0-9]+\.[0-9]{9}" | head -n 1); '
            'echo "$B $M"'
        )

        # 3. Capture PC Receive Time
        t_rx = self.shared.time_ns()

        if res and len(res[0].split()) == 2:
            try:
                boot_s, mono_s = res[0].split()
                phone_boot = int(float(boot_s) * 1_000_000_000)
                phone_mono = int(float(mono_s) * 1_000_000_000)

                # 4. Feed the engine as if it were a pong
                # The 'pc_rx' is our t_rx, but we treat the phone_mono
                # as occurring exactly at the midpoint of t_tx and t_rx.
                self._syncer_engine.feed(pc_tx=t_tx, phone_mono=phone_mono, phone_boot=phone_boot, pc_rx=t_rx)
            except (ValueError, IndexError):
                pass

    # --- LEGACY / REFERENCE METHODS (DO NOT DELETE) ---

    def _perform_legacy_coarse_sync(self, sync_state_obj):
        """
        Original logic to guess the clock offset before the first ping.
        Keeping this here for reference or for boot-date calculation.
        """
        try:
            phone_boot_ns, pc_ns, rtt = self._get_best_coarse_anchor(num_tries=5)
            sleep_offset = self._calculate_sleep_offset_ns(num_samples=7)
            phone_mono_ns = phone_boot_ns - sleep_offset

            # Forensic Boot Date Calculation
            boot_time_sec = (pc_ns - phone_boot_ns) / 1e9
            dt = datetime.datetime.fromtimestamp(boot_time_sec).astimezone()
            self.logger.info("Legacy Anchor Found. Approx Phone Boot: %s", dt.strftime("%Y-%m-%d %H:%M:%S"))

            # Prime the bridge
            sync_state_obj.ref_time[:] = [phone_mono_ns, phone_mono_ns]
            sync_state_obj.offset[:] = [pc_ns, pc_ns]
            sync_state_obj.drift_m[:] = [10**9, 10**9]
            sync_state_obj.drift_d[:] = [10**9, 10**9]
            sync_state_obj.enabled[0] = 1
        except Exception:
            self.logger.debug("Legacy coarse sync skipped.")

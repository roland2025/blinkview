# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import re
import shlex
import subprocess
from threading import Event, Lock, Timer
from time import sleep

from blinkview.core.configurable import configuration_property, override_property
from blinkview.io.BaseReader import BaseReader, DeviceFactory
from blinkview.io.batch_feeder import BatchFeeder
from blinkview.utils.adb import detect_adb_path, list_adb_devices

# Ends every command's output on the persistent shell, followed by the command's exit code.
SENTINEL = b"__BlinkSync_Done__"
# Needs the digits: a shell that echoes its input would otherwise match on "echo ... $?".
_SENTINEL_RE = re.compile(re.escape(SENTINEL) + rb" (\d+)\r?\n")
# How long one command may run before the shell is killed (ps -A on a phone takes ~0.1 s).
QUERY_TIMEOUT_S = 10.0


class AdbShellError(RuntimeError):
    """A command could not be run on the device's shell; the message says why."""


_PS_ELAPSED_RE = re.compile(r"^(?:(?:(\d+)-)?(\d+):)?(\d+):(\d+)$")


def parse_ps_elapsed_s(text: str) -> int | None:
    """Seconds from toybox ps's ELAPSED column (`-o ETIME`): `[[dd-]hh:]mm:ss`. None when it
    doesn't look like one."""
    match = _PS_ELAPSED_RE.match(text)
    if match is None:
        return None
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return ((days * 24 + hours) * 60 + minutes) * 60 + seconds


def _shorten(text: str, limit: int = 80) -> str:
    text = text.strip()
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _kill_quietly(proc: subprocess.Popen):
    try:
        proc.kill()
    except Exception:
        pass


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
    description="Record the incoming bytes to a session file. The file is not a plain byte dump and not text: each received chunk is stored as a timestamped header followed by its bytes.",
)
class AdbReader(BaseReader):
    __doc__ = """The primary data ingestion source for Android ADB logcat streams.

* Streams real-time text output from a connected Android device or emulator.
* Supports standard logcat filtering and formatting directly via properties.
* Capable of flushing device logs prior to connection for a clean slate.
* Safely manages the background ADB subprocess to prevent zombie processes.

This reader batches incoming standard output using unbuffered OS reads (`read1`), 
ensuring high throughput without pipeline stalls."""

    time_syncer_type = "adb_time_syncer"

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
        self._base_cmd: list[str] | None = None  # adb [-s serial], to restart the shell with

        self._syncer = None

        self.sleep_offset_ns = 0

        self._last_error_log_ns = 0

        self._process_ids: dict[str, int] = {}
        self._last_polled_pids: set[int] = set()
        self._pid_poll_task_id = None
        self._ps_has_elapsed = True  # cleared once a device's ps rejects `-o ETIME`

    @classmethod
    def get_config_schema(cls) -> dict:
        schema = super().get_config_schema()

        try:
            devices = list_adb_devices()
        except Exception:
            # adb missing, server slow to start, etc.: leave device_id as free text
            return schema

        if "device_id" in schema["properties"]:
            prop = schema["properties"]["device_id"]
            prop["enum"] = [""] + [serial for serial, _ in devices]
            prop["enum_tooltips"] = ["Default / only connected device"] + [desc for _, desc in devices]
            prop["_allow_custom"] = True

        return schema

    def run(self):
        # Setup and Localize Lookups
        stop_is_set = self._stop_event.is_set
        time_ns = self.shared.time_ns
        logger = self.logger

        # Backoff configuration (All in Nanoseconds)
        current_backoff_ns = 1_000_000_000  # Start at 1s
        max_backoff_ns = 5_000_000_000  # Cap at Xs
        next_reconnect_ns = 0

        feeder = None

        try:
            feeder = BatchFeeder(self, msg_size_bytes=64, default_buffer_bytes=65536)
            push = feeder.push
            maybe_flush = feeder.maybe_flush

            while not stop_is_set():
                # Subprocess Lifecycle Management
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

                try:
                    # Read incoming chunk
                    # 65536 is a standard healthy OS buffer size
                    # chunk = _read1(65536)
                    chunk = os_read(fd, 65536)

                    if chunk:
                        now = time_ns()

                        # One row per chunk, each with its own timestamp
                        push(now, chunk)

                        # Batching Window Check: flushes on the first arrival in this batch.
                        maybe_flush(now)

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
            # Final Cleanup
            if feeder is not None:
                feeder.close()

            self._cleanup_process()

    def open(self):
        adb_bin = detect_adb_path()
        time_ns = self.shared.time_ns
        now_ns = time_ns()

        try:
            base_cmd = [adb_bin]
            if self.device_id:
                base_cmd.extend(["-s", self.device_id])

            self._base_cmd = base_cmd

            # Start the persistent shell first (necessary for the anchor)
            self._shell = self._start_shell(base_cmd)

            # Clear logs if requested (now safe to do since shell is open)
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

            # Finalize environment
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

    def _start_shell(self, base_cmd: list[str]) -> subprocess.Popen:
        """Starts the persistent `adb shell` that send_data() and query() write their commands to.

        stderr goes into stdout: an undrained stderr pipe would fill up over a long session and
        block the shell, and an error then arrives in the reply instead of being lost."""
        self.logger.debug("Opening ADB shell...")
        shell = subprocess.Popen(
            [*base_cmd, "shell"],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=False,
            bufsize=-1,
        )

        # Give it a moment to fail
        sleep(0.1)
        if shell.poll() is not None:
            out, _ = shell.communicate()
            raise ConnectionError(out.decode(errors="replace").strip() if out else "No devices found")
        return shell

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
            # Try to be polite and exit the shell first (stdin is a binary pipe, so bytes).
            # Give it a brief chance to close the remote session before force-terminating.
            try:
                if self._shell.stdin:
                    self._shell.stdin.write(b"exit\n")
                    self._shell.stdin.flush()
                    self._shell.stdin.close()
                self._shell.wait(timeout=0.5)
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
        Runs `data` as a shell command on the device (like `adb shell <data>`) and waits for it
        to finish, so all its output is consumed before the next command.

        Raises AdbShellError, after logging why, when the command could not be delivered or
        exited non-zero.
        """
        rc, lines = self._run(data)
        if rc != 0:
            detail = lines[-1] if lines else "no output"
            self.logger.warning("send: %r exited with %s: %s", _shorten(data), rc, detail)
            raise AdbShellError(f"command exited with {rc}: {detail}")

    def query(self, cmd: str) -> list[str]:
        """The output lines of `cmd`, for BlinkView's own lookups (ps, /proc): [] when not
        connected or the shell failed, which is logged. Never raises."""
        try:
            return self._run(cmd)[1]
        except AdbShellError as e:
            self.logger.debug("query %r failed: %s", _shorten(cmd), e)
            return []

    def _run(self, cmd: str, timeout_s: float = QUERY_TIMEOUT_S) -> tuple[int, list[str]]:
        """Runs `cmd` on the persistent shell and returns (exit code, output lines).

        The command runs in its own `sh -c` with stdin from /dev/null, so nothing in it reaches
        the persistent shell's parser or input: a trailing newline (the MCP `send` and watch
        buttons add one), a syntax error or `exit` would otherwise make the non-interactive
        shell exit, and a command reading stdin would eat the sentinel. A shell that died
        anyway is restarted; when the write itself failed the command never ran and is retried
        once. Raises AdbShellError, after logging it, when the command can't be run."""
        wire = f"sh -c {shlex.quote(cmd.rstrip())} </dev/null 2>&1; echo {SENTINEL.decode()} $?\n".encode()

        with self._shell_lock:
            for attempt in range(2):
                shell = self._ensure_shell()
                try:
                    shell.stdin.write(wire)
                    shell.stdin.flush()
                    break
                except (OSError, ValueError) as e:
                    # The pipe is closed: the shell is gone, the command never left this PC
                    self.logger.warning("ADB shell pipe is closed (%s); restarting the shell", e)
                    self._drop_shell(shell)
            else:
                raise self._fail("could not write to a fresh ADB shell")

            # A shell that stops answering is killed, which ends the read below with EOF
            timed_out = Event()

            def on_timeout():
                timed_out.set()
                _kill_quietly(shell)

            timer = Timer(timeout_s, on_timeout)
            timer.daemon = True
            timer.start()
            output = b""
            try:
                while True:
                    match = _SENTINEL_RE.search(output)
                    if match:
                        break
                    chunk = shell.stdout.read1(4096)
                    if not chunk:
                        break
                    output += chunk
            except (OSError, ValueError):
                match = None
            finally:
                timer.cancel()

            if match is None:
                self._drop_shell(shell)
                if timed_out.is_set():
                    raise self._fail(f"no reply to {_shorten(cmd)!r} within {timeout_s:.0f}s; the ADB shell was killed")
                tail = output.decode(errors="replace").strip().splitlines()
                detail = tail[-1] if tail else "no output"
                raise self._fail(f"ADB shell ended before {_shorten(cmd)!r} finished ({detail})")

            body = output[: match.start()].decode(errors="ignore")
            lines = [line.strip() for line in body.splitlines() if line.strip()]
            return int(match.group(1)), lines

    def _ensure_shell(self) -> subprocess.Popen:
        """The live persistent shell, restarted if it exited. Call with _shell_lock held."""
        shell = self._shell
        if shell is not None and shell.poll() is None:
            return shell
        if shell is not None:
            self.logger.warning("ADB shell had exited (code %s); restarting it", shell.returncode)
            self._drop_shell(shell)
        if self._process is None or self._base_cmd is None:
            raise self._fail("ADB source is not connected")
        try:
            self._shell = self._start_shell(self._base_cmd)
        except Exception as e:
            raise self._fail(f"could not restart the ADB shell: {e}")
        self.logger.info("ADB shell restarted")
        return self._shell

    def _drop_shell(self, shell: subprocess.Popen):
        if self._shell is shell:
            self._shell = None
        _kill_quietly(shell)

    def _fail(self, message: str) -> "AdbShellError":
        self.logger.error("ADB send failed: %s", message)
        return AdbShellError(message)

    def _run_ps(self) -> list[str]:
        """`ps` lines with each process's elapsed run time as a third column when the device's ps
        supports it (toybox: `-o ETIME`, header ELAPSED), else NAME,PID only. A ps that rejects
        the field says so on its output, not its exit code; then NAME,PID is used from then on."""
        if self._ps_has_elapsed:
            lines = self._run("ps -A -o NAME,PID,ETIME")[1]
            if not any(line.startswith("ps:") for line in lines[:2]):
                return lines
            self._ps_has_elapsed = False
            self.logger.info("ps has no ETIME column; process start times fall back to poll times")
        return self._run("ps -A -o NAME,PID")[1]

    def _refresh_process_ids(self):
        """Uses the persistent shell to map all process names to PIDs. Also feeds the shared,
        Numba-backed IdHistory (self.shared.pid_history) so that a PID's process name can later
        be resolved as of any past timestamp, not just "whatever currently holds it" - Android
        recycles PIDs, so the two are not the same thing over the life of a session.

        Each new PID's interval starts at the process's start time (now minus ps's elapsed time,
        rounded down by ps's 1 s resolution), not at this poll - the poll runs every 15 s, so
        otherwise an app's startup logs would never resolve to its name."""
        # A failed run (already logged) skips this poll: an empty list would close every PID's
        # history interval.
        try:
            lines = self._run_ps()
        except AdbShellError:
            return

        now_ns = self.shared.time_ns()
        logger = self.logger

        # Every (pid, name, started_ns), including several PIDs sharing a name (sh, app helper
        # processes). The header line has no integer PID and is skipped.
        processes: list[tuple[int, str, int | None]] = []
        for line in lines:
            parts = line.split()
            if len(parts) < 2:
                continue
            try:
                pid = int(parts[1])
            except ValueError:
                continue
            elapsed_s = parse_ps_elapsed_s(parts[2]) if len(parts) >= 3 else None
            started_ns = None if elapsed_s is None else now_ns - (elapsed_s + 1) * 1_000_000_000
            processes.append((pid, parts[0], started_ns))
            logger.debug("pid=%s name=%s elapsed_s=%s", pid, parts[0], elapsed_s)

        self._process_ids = {name: pid for pid, name, _ in processes}
        logger.info("Captured %s application PIDs via shell.", len(processes))

        history = self.shared.pid_history
        current_pids = {pid for pid, _, _ in processes}
        device_ids = self._owning_pipeline_device_ids()

        for device_id in device_ids:
            for pid, name, started_ns in processes:
                history.update(self._pid_history_key(device_id, pid), name, now_ns, started_ns=started_ns)

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

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Checks that no Numba kernel is compiled (or first loaded from cache) after BlinkView reports
"System ready", while an agent runs every MCP tool. Exits non-zero if one is.

Two real processes: `blink gui` on a scratch profile (a tcp_client source this script plays the
device for, a default parser pipeline and an `mcp_server` pipeline), and `blink mcp` driving it
over stdio. The GUI starts with an EMPTY Numba cache directory, so every kernel has to compile
during the startup warmup - one that is missing from the warmup (core/warmup_registry.py's
@register_warmup) compiles on its first MCP call instead and is reported here by name.

Detection uses Numba's own cache messages (NUMBA_DEBUG_CACHE, which ui/run.py always sets):
"[cache] ... saved" is a compile, "[cache] ... loaded" a first use of an already cached kernel.
Both count, in two windows after ready: a quiet baseline with device data flowing but no MCP
call (anything there is not the MCP tools' doing), and the MCP calls themselves.

    uv run scripts/mcp_cold_cache_check.py            # cold: ~1.5 min of compiling, then the calls
    uv run scripts/mcp_cold_cache_check.py -v         # also list every tool call and its time
    uv run scripts/mcp_cold_cache_check.py --cache-dir DIR   # reuse DIR (fast when already warm)

Isolated like tests/test_blink_gui_subprocess.py (fake home, BLINK_PROJECT_ROOT, offscreen Qt);
nothing in the real profile, logs or ~/.blinkview is touched. Not for Linux without a display:
ui/run.py forces QT_QPA_PLATFORM=xcb there.

When adding an MCP tool or argument, add a call for it to run_all_tools() below."""

import argparse
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
NL = b"\n"

READY_MARKER = "show: System ready"
FAILED_MARKER = "Compilation failed"
BASELINE_SECONDS = 15


class McpSession:
    """`blink mcp` as a subprocess, plus the socket playing the device on the other side."""

    def __init__(self, cwd: Path, env: dict, device: socket.socket, verbose: bool):
        self.proc = subprocess.Popen(
            [sys.executable, "-m", "blinkview", "mcp"],
            cwd=str(cwd),
            env=env,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.device = device
        self.verbose = verbose
        self.calls = 0
        self.unexpected = []
        self._next_id = 0

    def rpc(self, method: str, params: dict = None) -> dict:
        self._next_id += 1
        request = {"jsonrpc": "2.0", "id": self._next_id, "method": method, "params": params or {}}
        self.proc.stdin.write((json.dumps(request) + "\n").encode())
        self.proc.stdin.flush()
        response = json.loads(self.proc.stdout.readline())
        assert response["id"] == self._next_id, response
        return response["result"]

    def tool(self, tool_name: str, expect_error: bool = False, **arguments) -> dict:
        """Calls a tool; returns its JSON result with the log-text block under "_text"."""
        self.calls += 1
        started = time.monotonic()
        result = self.rpc("tools/call", {"name": tool_name, "arguments": arguments})
        took_ms = (time.monotonic() - started) * 1000
        meta = result["content"][0]["text"]
        if self.verbose:
            print(f"  {'ERR' if result['isError'] else 'ok '} {took_ms:7.0f} ms  {tool_name}({arguments})")
        if result["isError"] != expect_error:
            self.unexpected.append(f"{tool_name}({arguments}) -> {meta[:300]}")
        try:
            parsed = json.loads(meta)
        except ValueError:
            parsed = {}
        parsed["_text"] = result["content"][1]["text"] if len(result["content"]) > 1 else ""
        return parsed

    def say(self, *lines: str):
        """The device prints these lines."""
        self.device.sendall(b"".join(line.encode() + NL for line in lines))

    def firmware(self, expect: bytes, *replies: str) -> threading.Thread:
        """The device waits for `expect` from BlinkView, then prints `replies`."""

        def work():
            got = self.device.recv(200)
            assert got == expect, got
            self.say(*replies)

        thread = threading.Thread(target=work, daemon=True)
        thread.start()
        return thread

    def close(self):
        self.proc.stdin.close()
        try:
            self.proc.wait(10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
        stderr = self.proc.stderr.read().decode(errors="replace").strip()
        if stderr:
            print("blink mcp stderr:", stderr[-800:])


def run_all_tools(mcp: McpSession):
    """Every MCP tool, in every mode and with every argument at least once."""
    tool, say = mcp.tool, mcp.say

    mcp.rpc("initialize", {"protocolVersion": "2025-06-18"})
    mcp.rpc("tools/list")
    seq0 = tool("status")["instances"][0]["seq"]

    # --- send: plain, with a reply, and with every wait/filter/format argument ---
    waiting = mcp.firmware(b"plain" + NL)
    tool("send", text="plain")
    waiting.join(5)
    waiting = mcp.firmware(b"ver" + NL, "version 1.2 state=ok", "OK")
    sent = tool("send", text="ver", wait_s=10, until="OK", device="board")
    waiting.join(5)
    waiting = mcp.firmware(b"ver2" + NL, "version 1.3 state=ok", "DONE")
    tool(
        "send",
        text="ver2",
        source="board",
        wait_s=10,
        quiet_ms=300,
        device="board",
        module="log",
        min_level="INFO",
        contains="version",
        kv="state=ok",
        show_date=False,
        show_dev=False,
        ts_precision=6,
        show_rx_ts=True,
        utc=True,
        max_lines=50,
    )
    waiting.join(5)
    waiting = mcp.firmware(b"raw")
    tool("send", text="raw", newline=False, wait_s=1)
    waiting.join(5)
    tool("send", text="x", source="nope", expect_error=True)

    # --- command: the scratch profile's tcp_client source offers none, so only the refusal ---
    tool("command", command="reset", expect_error=True)

    # --- read_logs: newest lines, each filter, each formatter option ---
    tool("read_logs")
    tool("read_logs", last_n=5, device="board")
    tool("read_logs", device="board", module="log", min_level="WARN")
    tool("read_logs", contains="boot line 1")
    tool("read_logs", kv="state=ok id=3")
    tool("read_logs", include_system=True, last_n=20)
    tool("read_logs", device="system", contains="mcp")
    tool("read_logs", show_ts=False, show_dev=False, show_lvl=False, show_mod=False)
    tool("read_logs", show_date=False, ts_precision=0)
    tool("read_logs", ts_precision=9, show_rx_ts=True, utc=True)

    # --- read_logs: cursor, and more new lines than max_lines (the forward-paging path) ---
    page = tool("read_logs", after_seq=seq0, device="board")
    tool("read_logs", after_seq=page["next_seq"], device="board")
    say(*[f"burst {i:02d} state=run" for i in range(40)])
    time.sleep(1.0)
    cursor = sent["seq"]
    for _page in range(4):
        cursor = tool("read_logs", after_seq=cursor, device="board", max_lines=15)["next_seq"]
    tool("read_logs", after_seq=cursor, include_system=True, kv="state=run", contains="burst")

    # --- read_logs: waiting (timeout, first lines, quiet) ---
    tool("read_logs", wait_s=1, until="never printed", device="board")
    threading.Timer(0.5, say, ("late line",)).start()
    tool("read_logs", wait_s=10, device="board")
    threading.Timer(0.5, say, ("late a", "late b")).start()
    tool("read_logs", wait_s=10, quiet_ms=400, device="board", module="log")

    # --- read_logs: around a time ---
    line = tool("read_logs", device="board", contains="burst 20", ts_precision=6)["_text"].strip()
    stamp = " ".join(line.split(" ")[:2])
    tool("read_logs", around=stamp, before=5, after=5, device="board")
    tool("read_logs", around=stamp.split(" ")[1], before=0, after=3)
    tool(
        "read_logs", around=stamp, before=3, after=0, include_system=True, kv="state=run", module="log", device="board"
    )
    tool("read_logs", around="yesterday", expect_error=True)
    tool("read_logs", device="nope", expect_error=True)

    # --- list_modules ---
    tool("list_modules")
    tool("list_modules", include_system=True, contains="parser", max_modules=3)
    tool("list_modules", device="board")

    # --- telemetry: latest values, then a one-channel and a three-channel series ---
    say(*[f"{3.5 + i / 100:.2f}" for i in range(30)])
    time.sleep(1.0)
    latest = tool("telemetry_latest")
    tool("telemetry_latest", device="board", module="log", contains="log", utc=True, max_values=5)
    tool("telemetry_latest", include_system=True, changed_after_seq=latest["next_seq"])
    tool("telemetry_series", device="board", module="log")
    tool("telemetry_series", device="board", module="log", since_s=60, max_points=5, utc=True)
    say(*[f"{i}.0 {i}.5 {i}.25" for i in range(10)])
    time.sleep(1.0)
    tool("telemetry_series", device="board", module="log", max_points=2000)
    tool("telemetry_series", device="board", module="nope", expect_error=True)

    # --- rotate_session, then the read paths again on the new session ---
    tool("rotate_session", name="second")
    tool("status")
    say("after rotate 1", "7.5")
    time.sleep(1.0)
    tool("read_logs", device="board")
    tool("read_logs", after_seq=cursor, device="board", wait_s=1, quiet_ms=200)
    tool("telemetry_latest", device="board")
    tool("telemetry_series", device="board", module="log")
    tool("list_modules")
    tool("rotate_session")


def write_profile(path: Path, device_port: int):
    path.write_text(
        json.dumps(
            {
                "version": "0.2",
                "sources": {
                    "src_board": {
                        "enabled": True,
                        "type": "tcp_client",
                        "name": "board",
                        "host": "127.0.0.1",
                        "port": device_port,
                    }
                },
                "pipelines": {
                    "pipe_board": {"enabled": True, "type": "default", "name": "board", "sources_": ["src_board"]},
                    "pipe_mcp": {"enabled": True, "type": "mcp_server", "name": "mcp"},
                },
                "plugins": {},
                "reorder": {"enabled": True, "type": "default"},
                "central": {"enabled": True, "type": "default"},
            }
        )
    )


def cache_events(log_text: str) -> list:
    """Kernel cache files named in "[cache] ... saved/loaded" lines, as (what, kernel) pairs."""
    events = []
    for line in log_text.splitlines():
        if "[cache]" not in line:
            continue
        what = "compiled" if " saved " in line else "loaded from cache" if " loaded " in line else None
        if what is None:
            continue
        file_name = line.rstrip("'\" ").replace("\\\\", "\\").replace("\\", "/").rsplit("/", 1)[-1]
        events.append((what, file_name.split(".py", 1)[0]))  # "telemetry.nb_foo-117", without the version/ext tail
    return events


def report(label: str, log_text: str) -> int:
    events = cache_events(log_text)
    kernels = sorted(set(events))
    print(f"\n{label}: {len(kernels)} kernel(s)")
    for what, kernel in kernels:
        print(f"  {what}: {kernel}")
    return len(kernels)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--cache-dir",
        type=Path,
        help="Numba cache directory to use and keep (default: a new empty one, deleted afterwards). "
        "Already warm, the run is fast and a kernel missing from the warmup shows as 'loaded from cache'.",
    )
    parser.add_argument("--timeout", type=int, default=1800, help="Seconds to wait for 'System ready' (default 1800)")
    parser.add_argument("--keep", action="store_true", help="Keep the scratch directory (gui.log, sessions)")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print every tool call and its duration")
    args = parser.parse_args()

    scratch = Path(tempfile.mkdtemp(prefix="blink_mcp_cold_"))
    home = scratch / "home"
    (home / ".blinkview").mkdir(parents=True)
    # A valid update.path keeps UpdateWidget.ensure_update_path() from opening a blocking dialog.
    (home / ".blinkview" / "settings.json").write_text(json.dumps({"update": {"path": str(REPO_ROOT)}}))
    cache_dir = args.cache_dir.resolve() if args.cache_dir else scratch / "numba_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    cold = not any(cache_dir.iterdir())

    listener = socket.socket()
    listener.bind(("127.0.0.1", 0))
    listener.listen(1)
    listener.settimeout(60)
    config_path = scratch / "config.json"
    write_profile(config_path, listener.getsockname()[1])

    env = dict(os.environ)
    env.update(
        QT_QPA_PLATFORM="offscreen",
        QT_API="pyside6",
        HOME=str(home),
        USERPROFILE=str(home),
        BLINK_PROJECT_ROOT=str(scratch / "no_project"),
        PYTHONUNBUFFERED="1",
        NUMBA_CACHE_DIR=str(cache_dir),
    )

    gui_log_path = scratch / "gui.log"

    def log_text() -> str:
        return gui_log_path.read_text(encoding="utf-8", errors="replace")

    print(f"Starting blink with a {'cold' if cold else 'pre-filled'} Numba cache: {cache_dir}")
    started = time.monotonic()
    device = None
    mcp = None
    failed = True
    with open(gui_log_path, "w", encoding="utf-8", errors="replace") as gui_log:
        gui = subprocess.Popen(
            [sys.executable, "-u", "-m", "blinkview", "gui", "-s", "cold", "-l", str(scratch / "logs")]
            + ["-c", str(config_path)],
            cwd=str(REPO_ROOT),
            env=env,
            stdout=gui_log,
            stderr=subprocess.STDOUT,
        )
        try:
            while True:
                text = log_text()
                if READY_MARKER in text:
                    break
                if FAILED_MARKER in text or gui.poll() is not None or time.monotonic() - started > args.timeout:
                    print(f"blink did not become ready. Last output:\n{text[-3000:]}")
                    return 1
                time.sleep(1)
            ready_offset = len(text)
            startup = cache_events(text)
            print(
                f"Ready after {time.monotonic() - started:.0f} s; startup compiled "
                f"{len({k for what, k in startup if what == 'compiled'})} kernels, "
                f"loaded {len({k for what, k in startup if what != 'compiled'})} from cache."
            )

            # Baseline: device data through the real source and parser, no MCP call yet.
            device, _addr = listener.accept()
            device.settimeout(30)
            # The first line only syncs the parser's framing (FrameDecoder.frame_resync_on_start).
            device.sendall(
                b"priming line" + NL + b"".join(f"boot line {i} state=ok id={i}".encode() + NL for i in range(20))
            )
            run_dir = home / ".blinkview" / "run"
            deadline = time.monotonic() + 30
            while not list(run_dir.glob("*.json")):
                if time.monotonic() > deadline:
                    print("The mcp_server pipeline never wrote its run file.")
                    return 1
                time.sleep(0.2)
            print(f"Baseline: {BASELINE_SECONDS} s with device data, no MCP calls...")
            time.sleep(BASELINE_SECONDS)
            mcp_offset = len(log_text())

            print("Running every MCP tool...")
            mcp = McpSession(scratch, env, device, args.verbose)
            run_all_tools(mcp)
            time.sleep(3)  # late cache messages from the last calls

            text = log_text()
            in_baseline = report("After ready, before any MCP call", text[ready_offset:mcp_offset])
            in_mcp = report(f"During {mcp.calls} MCP tool calls", text[mcp_offset:])
            if mcp.unexpected:
                print(f"\n{len(mcp.unexpected)} tool call(s) did not behave as expected:")
                for line in mcp.unexpected:
                    print("  " + line)

            failed = bool(in_baseline or in_mcp or mcp.unexpected)
            print("\nFAILED" if failed else "\nOK: nothing compiled or first-loaded after ready.")
            if in_mcp or in_baseline:
                print("Add the kernels above to a @register_warmup callback (see the numba-njit skill).")
            return 1 if failed else 0
        finally:
            if mcp is not None:
                mcp.close()
            if device is not None:
                device.close()
            listener.close()
            gui.terminate()
            try:
                gui.wait(15)
            except subprocess.TimeoutExpired:
                gui.kill()
                gui.wait(15)
            gui_log.flush()
            if args.keep or failed:
                print(f"Scratch directory kept: {scratch}")
            else:
                gui_log.close()
                shutil.rmtree(scratch, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Port borrowing: an external tool (a flasher) borrows a source's port from a running blink and
gives it back when it is done.

The protocol is two files in one directory (`borrow_dir()`), named after a borrow name:

* `<name>.borrow` - written by the tool: "I want the port". JSON `{"pid", "ts", "by"}`, where
  pid is the process whose exit ends the borrow.
* `<name>.lent` - written by blink once the port is closed: "go ahead".

The tool deletes its `.borrow` when it is done; blink then deletes `.lent` and reopens the port.

Both sides import this module - the source's watcher (io/source_borrow.py) and the CLI
(`blink borrow` / `blink return`) - so they always agree on the paths. It imports nothing from
blinkview, which keeps `blink borrow` fast to start (no registry, Qt or Numba).
"""

import json
import os
import re
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

BORROW_SUFFIX = ".borrow"
LENT_SUFFIX = ".lent"
BORROW_DIR_ENV = "BLINKVIEW_BORROW_DIR"

# How long `blink borrow` waits for blink's `.lent` by default. A source checks for borrows once
# a second, so this leaves room for a busy machine without making a flash wait long when no blink
# is running.
DEFAULT_WAIT_S = 3.0

_UNSAFE_CHARS = re.compile(r"[^A-Za-z0-9_.-]+")
_RESERVED_NAMES = {"CON", "PRN", "AUX", "NUL", *(f"COM{i}" for i in range(10)), *(f"LPT{i}" for i in range(10))}


def borrow_dir() -> Path:
    """~/.blinkview/borrow (the same home as utils/global_settings.get_blink_home(), not imported
    from there to keep this module free of blinkview imports), or $BLINKVIEW_BORROW_DIR."""
    override = os.environ.get(BORROW_DIR_ENV)
    if override:
        return Path(override).expanduser()
    return Path.home() / ".blinkview" / "borrow"


def name_from_port(url: str) -> str:
    """The default borrow name of a serial source: its port's own name. `COM5` -> `COM5`,
    `/dev/ttyUSB0` -> `ttyUSB0`, `\\\\.\\COM10` -> `COM10`, `socket://host:1234` ->
    `socket_host_1234`. A flasher knows its upload port, so in the common case both sides arrive
    at the same name without anyone configuring it."""
    url = (url or "").strip()
    if "://" in url:
        url = url.replace("://", "_")
    else:
        url = url.replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
    return _UNSAFE_CHARS.sub("_", url).strip("._")


@dataclass(frozen=True)
class BorrowPaths:
    borrow: Path
    lent: Path


def resolve(name: str, directory: Optional[Path] = None) -> BorrowPaths:
    """The two files of a borrow name.

    * A plain name (`COM5`, `esp32-main`) lives in borrow_dir(); a trailing `.borrow` is allowed.
    * Anything with a path separator is the borrow file itself, wherever it is (for a tool that
      already writes a lock at a fixed place); the ack is next to it with a `.lent` suffix.
    """
    name = (name or "").strip()
    if not name:
        raise ValueError("A borrow name is empty")

    if "/" in name or "\\" in name:
        borrow = Path(name).expanduser().absolute()
        return BorrowPaths(borrow, borrow.with_suffix(LENT_SUFFIX))

    if name.endswith(BORROW_SUFFIX):
        name = name[: -len(BORROW_SUFFIX)]
    safe = _UNSAFE_CHARS.sub("_", name).strip("._")
    if not safe:
        raise ValueError(f"A borrow name has no usable characters: {name!r}")

    # Windows maps CON, NUL, COM1-COM9, LPT1-LPT9... to devices whatever the extension:
    # `COM5.borrow` is the serial port itself, not a file. Escaped the same on every OS, so the
    # files don't depend on where they were written.
    base = safe.split(".", 1)[0]
    if base.upper() in _RESERVED_NAMES:
        safe = f"{base}_{safe[len(base) :]}"

    directory = directory or borrow_dir()
    return BorrowPaths(directory / f"{safe}{BORROW_SUFFIX}", directory / f"{safe}{LENT_SUFFIX}")


def atomic_write(path: Path, text: str):
    """Writes through a temp file + os.replace, so a reader polling for the file never sees it
    half-written."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def remove(path: Path) -> bool:
    """Deletes a file; False if it was not there."""
    try:
        path.unlink()
        return True
    except FileNotFoundError:
        return False


def write_borrow(paths: BorrowPaths, pid: int, by: str = "") -> dict:
    info = {"pid": pid, "ts": time.time(), "by": by}
    atomic_write(paths.borrow, json.dumps(info))
    return info


def read_borrow(paths: BorrowPaths) -> Optional[dict]:
    """The borrow's JSON, None when there is no borrow. A file that is not valid JSON (a tool that
    just touches the file) is still a borrow, with no owner to check: {}."""
    try:
        text = paths.borrow.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except OSError:
        # Exists but can't be read right now (Windows sharing violation mid-write): still a borrow.
        return {}
    try:
        info = json.loads(text)
    except ValueError:
        return {}
    return info if isinstance(info, dict) else {}


def write_lent(paths: BorrowPaths, info: dict):
    atomic_write(paths.lent, json.dumps(info))


def is_lent(paths: BorrowPaths) -> bool:
    return paths.lent.exists()


def pid_alive(pid) -> bool:
    """True when process `pid` still runs. Anything that is not a positive int counts as alive:
    without a usable pid the borrow can only end by its file being removed (or the source's
    timeout)."""
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return True
    try:
        import psutil

        return psutil.pid_exists(pid)
    except ImportError:
        pass

    if sys.platform == "win32":
        # os.kill(pid, 0) would terminate the process on Windows.
        import ctypes

        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        STILL_ACTIVE = 259
        ERROR_ACCESS_DENIED = 5
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
        if not handle:
            # Access denied means it exists; anything else (invalid parameter) means it doesn't.
            return ctypes.get_last_error() == ERROR_ACCESS_DENIED
        try:
            code = ctypes.c_ulong()
            if not kernel32.GetExitCodeProcess(handle, ctypes.byref(code)):
                return True
            return code.value == STILL_ACTIVE
        finally:
            kernel32.CloseHandle(handle)

    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


# Process names that are only a console-script launcher for this very command: on Windows pip
# installs `blink.exe`, which starts python.exe and waits for it. Its pid dies together with ours,
# so it is never the right owner of a borrow.
_LAUNCHER_NAMES = {"blink", "blink.exe", "blinkview", "blinkview.exe"}


def owner_pid() -> int:
    """The pid whose exit should end a borrow taken by `blink borrow <name>` (without a command):
    the caller - a shell, make, or PlatformIO's SCons process - skipping a console-script
    launcher in between. `blink borrow` itself exits before the flash starts."""
    pid = os.getppid()
    try:
        import psutil
    except ImportError:
        return pid
    try:
        process = psutil.Process(pid)
        while process is not None and process.name().lower() in _LAUNCHER_NAMES:
            process = process.parent()
        return process.pid if process is not None else pid
    except (psutil.Error, OSError):
        return pid


def wait_for_lent(paths: BorrowPaths, timeout_s: float, poll_s: float = 0.05) -> bool:
    """Waits for blink's ack. False on timeout - no blink is running or lending this port."""
    deadline = time.monotonic() + max(timeout_s, 0.0)
    while True:
        if is_lent(paths):
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(poll_s)

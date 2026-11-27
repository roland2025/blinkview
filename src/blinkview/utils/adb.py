# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import shutil
from pathlib import Path


def detect_adb_path() -> str:
    """
    Locates the ADB executable with cross-version Windows compatibility.
    Prioritizes: System PATH -> ANDROID_HOME -> LOCALAPPDATA (Windows)
    """
    # 1. Check System PATH first (shutil.which returns str or None)
    # Using a literal string "adb" is safe on all Python versions.
    system_adb = shutil.which("adb")  # noqa
    if system_adb:
        return system_adb

    # 2. Check standard Environment Variables
    # ANDROID_HOME is the modern standard; SDK_ROOT is the legacy backup.
    for var in ("ANDROID_HOME", "ANDROID_SDK_ROOT"):
        sdk_root = os.getenv(var)
        if sdk_root:
            candidate = Path(sdk_root) / "platform-tools" / "adb.exe"
            if candidate.exists():
                return str(candidate)

    # 3. Windows-Specific Fallback (LOCALAPPDATA)
    if os.name == "nt":
        local_app_data = os.getenv("LOCALAPPDATA")
        if local_app_data:
            sdk_adb = Path(local_app_data) / "Android" / "Sdk" / "platform-tools" / "adb.exe"
            if sdk_adb.exists():
                return str(sdk_adb)

    # 4. Final fallback: hope it's in the path anyway
    return "adb"


def list_adb_devices(timeout_s: float = 3.0) -> list[tuple[str, str]]:
    """
    Returns the devices `adb devices -l` reports, as (serial, description) pairs.

    The description holds the model (or product) and the state when the device is not
    ready ("unauthorized", "offline"). Starting the adb server on the first call can take a
    second or two, hence the timeout. Raises on a missing adb binary or a timeout.
    """
    import subprocess

    creationflags = subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0
    result = subprocess.run(
        [detect_adb_path(), "devices", "-l"],
        capture_output=True,
        text=True,
        timeout=timeout_s,
        stdin=subprocess.DEVNULL,
        creationflags=creationflags,
    )
    return parse_adb_devices(result.stdout)


def parse_adb_devices(output: str) -> list[tuple[str, str]]:
    """Parses the text of `adb devices -l` into (serial, description) pairs."""
    devices = []
    for line in output.splitlines():
        line = line.strip()
        # Skip the header and daemon start-up chatter ("* daemon started successfully")
        if not line or line.startswith("List of devices") or line.startswith("*"):
            continue
        parts = line.split()
        if len(parts) < 2:
            continue
        serial, state = parts[0], parts[1]
        fields = dict(p.split(":", 1) for p in parts[2:] if ":" in p)
        name = fields.get("model") or fields.get("product") or fields.get("device") or serial
        name = name.replace("_", " ")
        description = name if state == "device" else f"{name} ({state})"
        devices.append((serial, description))
    return devices

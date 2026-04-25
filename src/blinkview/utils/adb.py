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

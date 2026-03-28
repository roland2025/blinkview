# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from pathlib import Path
from datetime import datetime, timezone
import os
import platform


def get_git_revision_hash() -> str:
    try:

        import subprocess
        # Returns the short hash (e.g., 8a2f3c1)
        return subprocess.check_output(['git', 'rev-parse', '--short', 'HEAD']).decode('ascii').strip()
    except Exception:
        return "unknown"


def create_session_metadata(session_path: Path, session_name: str, extra_meta: dict = None):
    metadata = {
        "session_name": session_name,
        "start_time_utc": datetime.now(timezone.utc).isoformat() + "Z",
        "git_hash": get_git_revision_hash(),
        "platform": f"{os.name} {platform.system()} {platform.release()}",
        "python_version": platform.python_version(),
    }

    if extra_meta:
        metadata.update(extra_meta)

    # Write to the folder
    meta_file = session_path / "metadata.json"
    with meta_file.open("w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=4)

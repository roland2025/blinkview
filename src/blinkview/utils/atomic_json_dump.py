# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import os
import time
import uuid
from pathlib import Path

# os.replace onto a file another process has open (Windows) fails with PermissionError -
# typically another BlinkView instance re-reading the same profile at that moment.
REPLACE_ATTEMPTS = 5
REPLACE_RETRY_DELAY_S = 0.02


def atomic_json_dump(data: dict, target_path: str | Path, indent: int = 4):
    """
    Safely writes a dictionary to a JSON file using an atomic swap.
    """
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Hidden temp file in the same directory, unique per write: several BlinkView instances may
    # save the same profile at once, and a shared temp name would let them write into one file.
    temp_file = target.parent / f".{target.name}.{os.getpid()}.{uuid.uuid4().hex[:8]}.tmp"

    try:
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent)
            f.flush()
            # Ensure data is physically on the disk before renaming
            os.fsync(f.fileno())

        # Atomic swap (overwrites target if it exists)
        for attempt in range(REPLACE_ATTEMPTS):
            try:
                temp_file.replace(target)
                break
            except PermissionError:
                if attempt == REPLACE_ATTEMPTS - 1:
                    raise
                time.sleep(REPLACE_RETRY_DELAY_S)

    except (IOError, OSError) as e:
        if temp_file.exists():
            temp_file.unlink()
        raise e  # Re-raise to let the manager handle the specific error

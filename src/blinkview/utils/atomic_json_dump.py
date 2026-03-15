# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import json
from pathlib import Path


def atomic_json_dump(data: dict, target_path: str | Path, indent: int = 4):
    """
    Safely writes a dictionary to a JSON file using an atomic swap.
    """
    target = Path(target_path)
    target.parent.mkdir(parents=True, exist_ok=True)

    # Create a hidden temp file in the same directory
    temp_file = target.parent / f".{target.name}.tmp"

    try:
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent)
            f.flush()
            # Ensure data is physically on the disk before renaming
            os.fsync(f.fileno())

        # Atomic swap (overwrites target if it exists)
        temp_file.replace(target)

    except (IOError, OSError) as e:
        if temp_file.exists():
            temp_file.unlink()
        raise e  # Re-raise to let the manager handle the specific error

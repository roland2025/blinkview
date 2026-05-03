# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
from pathlib import Path

from blinkview import __version__

IS_CACHE_FRESH = False


def export_numba_cache(settings):
    """
    Calculates the versioned cache path and exports it to the environment.
    Sets a global flag indicating if the directory was empty upon initialization.
    """
    global IS_CACHE_FRESH

    # 1. Determine base path
    repo_path = Path(settings.get("update.path", "."))

    # 2. Define the versioned structure
    cache_root = repo_path / ".numba_cache"
    versioned_dir = cache_root / __version__

    # 3. Check if empty BEFORE creating/writing to it
    # We check if the directory exists and contains any files
    if not versioned_dir.exists():
        IS_CACHE_FRESH = True
        versioned_dir.mkdir(parents=True, exist_ok=True)
    else:
        # Check if directory contains any files (excluding hidden system files if necessary)
        is_empty = not any(versioned_dir.iterdir())
        IS_CACHE_FRESH = is_empty

    # 4. EXPORT: Set the environment variable for the Numba JIT compiler
    os.environ["NUMBA_CACHE_DIR"] = str(versioned_dir.resolve())

    return versioned_dir

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
from pathlib import Path

from blinkview import __version__

IS_CACHE_WARM = False


def export_numba_cache(settings):
    """
    Calculates the versioned cache path and exports it to the environment.
    Sets a global flag indicating if the directory was empty upon initialization.
    """
    global IS_CACHE_WARM

    # 1. Determine base path
    repo_path = Path(settings.get("update.path", "."))

    # 2. Define the versioned structure
    cache_root = repo_path / ".numba_cache"
    versioned_dir = cache_root / __version__

    # 3. Check if populated BEFORE creating/writing to it.
    # IS_CACHE_WARM means "a warm cache from a previous run is already there", so the UI can
    # skip the compiling-shaders toast/delays - not "the directory was just freshly created".
    if not versioned_dir.exists():
        IS_CACHE_WARM = False
        versioned_dir.mkdir(parents=True, exist_ok=True)
    else:
        # Check if directory contains any files (excluding hidden system files if necessary)
        IS_CACHE_WARM = any(versioned_dir.iterdir())

    # Check if the variable is already defined in the environment
    if "NUMBA_CACHE_DIR" not in os.environ:
        # Ensure the directory exists before pointing Numba to it
        versioned_dir.mkdir(parents=True, exist_ok=True)

        # Resolve and set the environment variable
        os.environ["NUMBA_CACHE_DIR"] = str(versioned_dir.resolve())
        print(f"DEBUG: Numba cache redirected to: {os.environ['NUMBA_CACHE_DIR']}")
    else:
        versioned_dir = Path(os.environ["NUMBA_CACHE_DIR"])
        print(f"DEBUG: Using pre-existing NUMBA_CACHE_DIR: {versioned_dir}")

    return versioned_dir

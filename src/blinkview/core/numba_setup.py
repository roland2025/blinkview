# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
from pathlib import Path

from blinkview import __version__


def export_numba_cache(settings):
    """
    Calculates the versioned cache path and exports it to the environment.
    Returns the path for the updater to use later.
    """
    # 1. Determine base path (Default to repo path if set, otherwise current dir)
    repo_path = Path(settings.get("update.path", "."))

    # 2. Define the versioned structure
    # This prevents cross-version bytecode contamination
    cache_root = repo_path / ".numba_cache"
    versioned_dir = cache_root / __version__

    # 3. Ensure directory exists
    versioned_dir.mkdir(parents=True, exist_ok=True)

    # 4. EXPORT: This is the critical step for Numba
    os.environ["NUMBA_CACHE_DIR"] = str(versioned_dir.resolve())

    return versioned_dir

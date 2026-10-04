# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import shutil
import threading
import time
from pathlib import Path

from blinkview import __version__

IS_CACHE_WARM = False

# Touched on every launch in the running version's cache dir; prune_old_caches() reads it.
LAST_USED_MARKER = ".last_used"
PRUNE_AFTER_DAYS = 7


def numba_cache_root(settings, update_source=None) -> Path:
    """Source checkouts keep the cache next to the repo (<update.path>/.numba_cache), as before;
    package installs have no repo, so theirs goes under ~/.blinkview/numba_cache."""
    from blinkview.utils.install_info import UpdateSource, resolve_update_source

    if update_source is None:
        update_source = resolve_update_source(settings)

    if update_source == UpdateSource.GIT:
        return Path(settings.get("update.path", ".")) / ".numba_cache"

    from blinkview.utils.global_settings import get_blink_home

    return get_blink_home() / "numba_cache"


def export_numba_cache(settings, update_source=None):
    """
    Calculates the versioned cache path and exports it to the environment.
    Sets a global flag indicating if the directory was empty upon initialization.
    """
    global IS_CACHE_WARM

    cache_root = numba_cache_root(settings, update_source)
    versioned_dir = cache_root / __version__

    # Check if populated BEFORE creating/writing to it.
    # IS_CACHE_WARM means "a warm cache from a previous run is already there", so the UI can
    # skip the compiling-shaders toast/delays - not "the directory was just freshly created".
    # Dotfiles (the .last_used marker) don't count as cache contents.
    if not versioned_dir.exists():
        IS_CACHE_WARM = False
        versioned_dir.mkdir(parents=True, exist_ok=True)
    else:
        IS_CACHE_WARM = any(not p.name.startswith(".") for p in versioned_dir.iterdir())

    # Check if the variable is already defined in the environment
    if "NUMBA_CACHE_DIR" not in os.environ:
        # Ensure the directory exists before pointing Numba to it
        versioned_dir.mkdir(parents=True, exist_ok=True)

        # Resolve and set the environment variable
        os.environ["NUMBA_CACHE_DIR"] = str(versioned_dir.resolve())
        print(f"DEBUG: Numba cache redirected to: {os.environ['NUMBA_CACHE_DIR']}")

        _touch_last_used(versioned_dir)
        # Background: rmtree of a 100+ MB dir shouldn't delay startup.
        threading.Thread(
            target=prune_old_caches, args=(cache_root, __version__), name="numba-cache-prune", daemon=True
        ).start()
    else:
        # Someone else (tests, the user) chose the cache dir - don't touch anything around it.
        versioned_dir = Path(os.environ["NUMBA_CACHE_DIR"])
        print(f"DEBUG: Using pre-existing NUMBA_CACHE_DIR: {versioned_dir}")

    return versioned_dir


def prune_old_caches(cache_root: Path, current_version: str, max_age_days: float = PRUNE_AFTER_DAYS, now=None):
    """Deletes cache dirs of versions older than current_version that haven't been launched for
    max_age_days. Newer versions are always kept (a rollback then re-upgrade reuses them), and the
    grace period keeps switching between source branches with different versions warm. Names that
    don't parse as versions are never touched. Best-effort: returns the dirs it removed."""
    from packaging.version import InvalidVersion, Version

    now = time.time() if now is None else now
    cutoff = now - max_age_days * 86400

    try:
        current = Version(current_version)
        entries = list(Path(cache_root).iterdir())
    except (InvalidVersion, OSError):
        return []

    removed = []
    for entry in entries:
        if not entry.is_dir():
            continue
        try:
            if Version(entry.name) >= current:
                continue
        except InvalidVersion:
            continue

        if _last_used(entry) > cutoff:
            continue

        shutil.rmtree(entry, ignore_errors=True)
        if not entry.exists():
            removed.append(entry)
    return removed


def _touch_last_used(versioned_dir: Path):
    try:
        (versioned_dir / LAST_USED_MARKER).touch()
    except OSError:
        pass


def _last_used(versioned_dir: Path) -> float:
    for p in (versioned_dir / LAST_USED_MARKER, versioned_dir):
        try:
            return p.stat().st_mtime
        except OSError:
            continue
    return 0.0

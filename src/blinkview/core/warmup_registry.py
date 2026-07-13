# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING, Callable, List

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

_WARMUP_CALLBACKS: List[Callable[["NumbaWarmupHelper"], None]] = []


def register_warmup(func: Callable[["NumbaWarmupHelper"], None]) -> Callable[["NumbaWarmupHelper"], None]:
    """Registers a callable to run as part of NumbaWarmupHelper.run_all(), passed the helper
    instance so it can reuse its dummy pool/registry/log_pool instead of building its own.
    Callers only need to import the module the callback lives in before start() runs (module
    load order, not registration order, is what matters) for it to be picked up.

    Lives in its own module (not warmup.py) so that core infrastructure classes (CircularLogPool,
    TimeSyncEngine, etc.) can decorate their own warmup() with it without importing warmup.py
    itself - warmup.py imports many of those same modules to build its dummy environment, so a
    two-way import through warmup.py would be circular."""
    _WARMUP_CALLBACKS.append(func)
    return func

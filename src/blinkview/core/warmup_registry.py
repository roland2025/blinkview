# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING, Callable, List, Optional, Tuple

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

# (priority, callback) pairs. Higher priority runs first - NumbaWarmupHelper.run_all() sorts
# this before executing, so callers no longer have to rely on import order to make sure
# foundational state (e.g. CircularLogPool's dummy rows) exists before other callbacks that need
# it run.
_WARMUP_CALLBACKS: List[Tuple[int, Callable[["NumbaWarmupHelper"], None]]] = []

DEFAULT_PRIORITY = 0


def register_warmup(func: Optional[Callable] = None, *, priority: int = DEFAULT_PRIORITY):
    """Registers a callable to run as part of NumbaWarmupHelper.run_all(), passed the helper
    instance so it can reuse its dummy pool/registry/log_pool instead of building its own.
    Callers only need to import the module the callback lives in before start() runs (module
    load order, not registration order, is what matters) for it to be picked up.

    Usable bare (`@register_warmup`, priority=0) or with an explicit priority
    (`@register_warmup(priority=100)`) for callbacks that other warmup callbacks depend on -
    run_all() executes callbacks in descending priority order (ties keep registration order).

    Lives in its own module (not warmup.py) so that core infrastructure classes (CircularLogPool,
    TimeSyncEngine, etc.) can decorate their own warmup() with it without importing warmup.py
    itself - warmup.py imports many of those same modules to build its dummy environment, so a
    two-way import through warmup.py would be circular."""

    def decorator(f: Callable) -> Callable:
        _WARMUP_CALLBACKS.append((priority, f))
        return f

    if func is not None:
        return decorator(func)
    return decorator

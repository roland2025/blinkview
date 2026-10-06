# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Event
from typing import Callable, List, Optional

from blinkview.core.warmup_registry import _WARMUP_CALLBACKS, _WARMUP_WEIGHTS, DEFAULT_WEIGHT, register_warmup

__all__ = ["NumbaWarmupHelper", "WarmupCancelled", "register_warmup"]

# on_progress(fraction, label): fraction in [0, 1] of the whole warmup, label = the callback
# being compiled ("" once finished).
ProgressCallback = Callable[[float, str], None]


class WarmupCancelled(Exception):
    """Raised by run_all() when its cancel event is set. Checked between callbacks and at every
    report_substep() - a Numba compile that's already running can't be interrupted."""


def warmup_label(callback: Callable) -> str:
    """'BinaryParser.warmup' -> 'BinaryParser', the same name the callbacks' own log lines use."""
    qualname = getattr(callback, "__qualname__", repr(callback))
    owner, _, name = qualname.rpartition(".")
    if owner and name == "warmup":
        return owner.rpartition(".")[2]  # the class name, also for nested/<locals> classes
    return qualname


class _WarmupProgress:
    """Turns "callback i of N, sub-step j of M" into one fraction, weighting each callback by its
    registered cold-cache cost so the fraction moves at roughly constant speed."""

    def __init__(self, weights: List[float], on_progress: Optional[ProgressCallback], cancel: Optional[Event]):
        self._weights = weights
        self._total = sum(weights) or 1.0
        self._on_progress = on_progress
        self._cancel = cancel
        self._done_weight = 0.0
        self._index = -1
        self._label = ""

    def begin(self, index: int, label: str):
        self._check_cancel()
        if self._index >= 0:
            self._done_weight += self._weights[self._index]
        self._index = index
        self._label = label
        self._emit(0.0)

    def substep(self, done: int, total: int):
        self._check_cancel()
        if total > 0:
            self._emit(min(1.0, max(0.0, done / total)))

    def finish(self):
        if self._on_progress is not None:
            self._on_progress(1.0, "")

    def _emit(self, fraction_of_current: float):
        if self._on_progress is not None:
            current = self._weights[self._index] * fraction_of_current
            self._on_progress((self._done_weight + current) / self._total, self._label)

    def _check_cancel(self):
        if self._cancel is not None and self._cancel.is_set():
            raise WarmupCancelled()


class NumbaWarmupHelper:
    """
    Encapsulates a dummy environment to trigger Numba JIT compilation
    for logging, telemetry, and registry kernels.
    """

    def __init__(self, shared: "SystemContext"):

        from blinkview.core.system_context import SystemContext

        self.array_pool = shared.array_pool
        self.time_ns = shared.time_ns

        from blinkview.core.logger import PrintLogger

        self.logger = PrintLogger("warmup")

        # Set by run_all() for the duration of a run; report_substep() is a no-op without it.
        self._progress: Optional[_WarmupProgress] = None

        from blinkview.core.id_history import IdHistory
        from blinkview.core.id_registry import IDRegistry
        from blinkview.core.numpy_log import CircularLogPool

        # 1. Initialize dummy infrastructure
        self.registry = IDRegistry(self.array_pool)
        self.log_pool = CircularLogPool(self.array_pool, 4, 1024 * 16)
        self.pid_history = IdHistory()

        # Constructed by LatestModuleValueTracker.warmup() (a registered warmup callback), not
        # here.
        self.tracker = None

        # 2. Pre-resolve modules to ensure ID system kernels are warm
        self.warmup_mod = self.registry.resolve_module("numba.warmup")
        self.floats_mod = self.registry.resolve_module("tool.floats")

        self.shared = SystemContext(
            time_ns=self.time_ns,
            registry=None,
            id_registry=self.registry,
            factories=shared.factories,
            tasks=shared.tasks,
            settings=shared.settings,
            array_pool=shared.array_pool,
            pid_history=self.pid_history,
        )

    def report_substep(self, done: int, total: int):
        """For callbacks with a long loop of independent compiles (e.g. BinaryParser's 30 decoder
        and section configs): call before each item so progress moves within the callback and a
        cancel takes effect between items instead of only after the whole callback. A no-op when
        the callback is run outside run_all() (unit tests)."""
        progress = getattr(self, "_progress", None)
        if progress is not None:
            progress.substep(done, total)

    def run_all(self, on_progress: Optional[ProgressCallback] = None, cancel: Optional[Event] = None):
        """Execute the full warmup suite, highest-priority callbacks first (stable sort - equal
        priorities keep registration/import order).

        `on_progress(fraction, label)` is called as each callback starts, at every
        report_substep(), and once with (1.0, "") at the end - from the calling thread, so a GUI
        caller running this off the main thread must marshal it (e.g. through a Qt signal).
        Setting `cancel` raises WarmupCancelled at the next callback boundary or sub-step."""
        callbacks = sorted(_WARMUP_CALLBACKS, key=lambda item: item[0], reverse=True)
        weights = [_WARMUP_WEIGHTS.get(callback, DEFAULT_WEIGHT) for _priority, callback in callbacks]
        progress = _WarmupProgress(weights, on_progress, cancel)
        self._progress = progress
        try:
            for index, (_priority, callback) in enumerate(callbacks):
                progress.begin(index, warmup_label(callback))
                callback(self)
            progress.finish()
        finally:
            self._progress = None
            # Clean up dummy data
            self.log_pool.release_all()
            _WARMUP_CALLBACKS.clear()
            _WARMUP_WEIGHTS.clear()

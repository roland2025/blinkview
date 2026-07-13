# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.warmup_registry import _WARMUP_CALLBACKS, register_warmup

__all__ = ["NumbaWarmupHelper", "register_warmup"]


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

        from blinkview.core.id_registry import IDRegistry
        from blinkview.core.numpy_log import CircularLogPool

        # 1. Initialize dummy infrastructure
        self.registry = IDRegistry(self.array_pool)
        self.log_pool = CircularLogPool(self.array_pool, 4, 1024 * 16)

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
        )

    def run_all(self):
        """Execute the full warmup suite."""
        try:
            for callback in _WARMUP_CALLBACKS:
                callback(self)
        finally:
            # Clean up dummy data
            self.log_pool.release_all()
            _WARMUP_CALLBACKS.clear()

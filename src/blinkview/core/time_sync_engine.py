# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import numpy as np

from blinkview.core.types.parsing import SyncState
from blinkview.ops.timesync import (
    IDX_ARRAY_LENGTH,
    IDX_BEST_RTT,
    IDX_LAST_MEAN,
    IDX_LAST_STD,
    IDX_RTT_PTR,
    IDX_SAMPLE_COUNT,
    IDX_SKIPS,
    IDX_TOTAL_COUNT,
    EngineState,
    nb_sync_kernel,
)


class TimeSyncEngine:
    __slots__ = ("sync", "engine", "logger", "logger_sync", "anchor_is_boot")

    def __init__(self, sync_state: SyncState, anchor_is_boot: bool = False, logger=None):
        self.sync = sync_state
        self.anchor_is_boot = anchor_is_boot
        self.logger = logger
        # Create child logger only if parent exists
        self.logger_sync = logger.child("sync") if logger else None

        scalars = np.zeros(IDX_ARRAY_LENGTH, dtype=np.int64)
        scalars[IDX_BEST_RTT] = np.iinfo(np.int64).max

        self.engine = EngineState(
            scalars=scalars, ppb_hist=np.zeros(15, dtype=np.int64), rtt_hist=np.zeros(50, dtype=np.uint64)
        )

    def feed(self, pc_tx: int, phone_mono: int, phone_boot: int, pc_rx: int) -> bool:
        # Pass phone_boot to the kernel
        success, quality, mean_ms, stddev_ms = nb_sync_kernel(
            pc_tx, phone_mono, phone_boot, pc_rx, self.engine, self.sync, self.anchor_is_boot
        )

        if success:
            if log_s := self.logger_sync:
                idx = self.sync.active_idx[0]
                rtt_ms = (pc_rx - pc_tx) / 1e6
                drift = self.sync.drift_m[idx] / self.sync.drift_d[idx]
                log_s.info(
                    f"rtt={rtt_ms:.2f}ms drift={drift:.9f} q={quality:.3f} mean={mean_ms:.3f}ms, std={stddev_ms:.3f}ms"
                )
        else:
            if log := self.logger:
                log.debug(
                    f"Skipped jittery pong. rtt={(pc_rx - pc_tx) / 1e6} q={quality:.3f} (mean={mean_ms:.3f}ms, std={stddev_ms:.3f}ms)"
                )

        return success

    def soft_reset(self):
        """Clears network jitter history but RETAINS hardware clock drift (PPB) memory."""
        sc = self.engine.scalars
        sc[IDX_SAMPLE_COUNT] = 0
        sc[IDX_BEST_RTT] = np.iinfo(np.int64).max
        sc[IDX_TOTAL_COUNT] = 0
        sc[IDX_RTT_PTR] = 0
        sc[IDX_SKIPS] = 0
        sc[IDX_LAST_MEAN] = 0
        sc[IDX_LAST_STD] = 0

        self.sync.enabled[0] = 0

        if log := self.logger:
            log.info("Network RTT history cleared for warm-start. Clock anchors retained.")

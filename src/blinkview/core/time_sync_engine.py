# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING

import numpy as np

from blinkview.core import dtypes
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.types.parsing import SyncState, create_default_sync
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.segments import nb_find_next_module_index, nb_find_next_module_match
from blinkview.ops.timesync import (
    IDX_ARRAY_LENGTH,
    IDX_ASYM_RATIO,
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

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper


class TimeSyncEngine:
    __slots__ = ("sync", "engine", "logger", "logger_ref", "logger_sync", "anchor_is_boot")

    def __init__(self, sync_state: SyncState, anchor_is_boot: bool = False, logger=None):
        self.sync = sync_state
        self.anchor_is_boot = anchor_is_boot
        self.logger = logger
        # Create child logger only if parent exists
        self.logger_sync = logger.child("sync") if logger else None

        # self.logger_ref = logger.child("ref") if logger else None

        scalars = np.zeros(IDX_ARRAY_LENGTH, dtype=np.int64)
        scalars[IDX_BEST_RTT] = np.iinfo(np.int64).max

        self.engine = EngineState(
            scalars=scalars, ppb_hist=np.zeros(15, dtype=np.int64), rtt_hist=np.zeros(50, dtype=np.uint64)
        )

    def set_asymmetry(self, val):
        self.engine.scalars[IDX_ASYM_RATIO] = val

    def feed(self, pc_tx: int, phone_mono: int, phone_boot: int, pc_rx: int) -> bool:
        # Pass phone_boot to the kernel
        sync = self.sync
        success, quality, mean_ms, stddev_ms = nb_sync_kernel(
            pc_tx, phone_mono, phone_boot, pc_rx, self.engine, sync, self.anchor_is_boot
        )

        if success:
            if log_s := self.logger_sync:
                idx = self.sync.active_idx[0]
                rtt_ms = (pc_rx - pc_tx) / 1e6
                drift = sync.drift_m[idx] / sync.drift_d[idx]

                log_s.debug(
                    "rtt=%.3fms drift=%.9f q=%.3f mean=%.3fms, std=%.3fms",
                    rtt_ms,
                    drift,
                    quality,
                    mean_ms,
                    stddev_ms,
                )

                # ref_time = sync.ref_time[idx]
                # offset = sync.offset[idx]
                # self.logger_ref.info(f"ref={ref_time} offset={offset}")
        else:
            if log := self.logger:
                log.debug(
                    "Skipped jittery pong. rtt=%s q=%.3f (mean=%.3fms, std=%.3fms)",
                    (pc_rx - pc_tx) / 1e6,
                    quality,
                    mean_ms,
                    stddev_ms,
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

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Triggers compilation for the TimeSyncEngine (nb_sync_kernel via feed/soft_reset) and
        the module-lookup kernels used alongside timesync projection. Requires data in the pool,
        provided by NumbaWarmupHelper.exercise_logging_kernels()."""
        print("[Warmup] TimeSyncEngine ...")

        now_ns = helper.time_ns()
        sync_state = create_default_sync(now_ns, start_enabled=True)
        engine = TimeSyncEngine(sync_state)

        mock_pc_tx = now_ns
        mock_phone_mono = 1_000_000_000  # 1 second uptime
        mock_pc_rx = now_ns + 30_000_000  # 30ms RTT

        # Ping 1: Initial anchor
        engine.feed(mock_pc_tx, mock_phone_mono, mock_phone_mono, mock_pc_rx)

        # Ping 2: Jitter check and drift accumulation
        engine.feed(
            mock_pc_tx + 1_000_000_000,
            mock_phone_mono + 1_000_000_000,
            mock_phone_mono + 1_000_000_000,
            mock_pc_rx + 1_000_000_000,
        )

        engine.soft_reset()

        with helper.log_pool.get_snapshot() as segments:
            for segment in segments:
                b = segment.bundle
                nb_find_next_module_match(b, dtypes.ID_TYPE(helper.warmup_mod.id), SEQ_NONE)
                nb_find_next_module_index(b, dtypes.ID_TYPE(helper.warmup_mod.id), dtypes.SEQ_TYPE(SEQ_NONE))
                break

        print("[Warmup] TimeSyncEngine ... done")

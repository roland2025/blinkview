# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import math
from typing import TYPE_CHECKING, Callable, Optional

import psutil

from blinkview.core.limits import MAX_SEGMENTS_EVICTED_PER_TICK

if TYPE_CHECKING:
    from blinkview.core.task_manager import TaskManager


def get_available_memory_bytes() -> int:
    """Cross-platform "available" memory (not "free" - see plan doc's "Memory query" section for
    why "free" is misleading on Linux, where most physical RAM sits in reclaimable page cache)."""
    return psutil.virtual_memory().available


def compute_target_pieces(
    available_bytes: int,
    current_pieces: int,
    segment_bytes: int,
    min_hot_pieces: int,
    max_hot_pieces: Optional[int],
    target_free_bytes: int,
) -> int:
    """Pure policy decision, no I/O/threading - see plan doc's "Policy (runs once per poll tick)"
    and "Hysteresis" sections. `segment_bytes` doubles as the hysteresis margin (shrink triggers
    below `target_free_bytes`, grow only once comfortably above it by one segment's worth) so
    there's no separate hysteresis config knob."""
    segment_bytes = max(segment_bytes, 1)
    slack = available_bytes - target_free_bytes

    if slack < 0:
        step = min(math.ceil(-slack / segment_bytes), MAX_SEGMENTS_EVICTED_PER_TICK)
        target = current_pieces - step
    elif slack >= segment_bytes:
        target = current_pieces + 1
    else:
        target = current_pieces

    target = max(target, min_hot_pieces)
    if max_hot_pieces:
        target = min(target, max_hot_pieces)
    return target


class HotTierMemoryGovernor:
    """Periodically watches system free memory and adjusts CircularLogPool's hot-tier ceiling
    (`update_max_pieces`) to match - grows it while memory is abundant, shrinks it (evicting
    oldest segments to cold storage) the moment free memory gets tight. Just a policy object
    scheduled on the registry's existing `TaskManager` (`shared.tasks.run_periodic`/
    `stop_periodic` - see io/adb_reader.py, io/uart.py, io/source_handshake.py for the same
    pattern) rather than spinning up its own OS thread - this is exactly the "periodic small task"
    case that infrastructure already exists for."""

    def __init__(
        self,
        log_pool,
        task_manager: "TaskManager",
        get_available_bytes: Callable[[], int],
        min_hot_pieces: int,
        max_hot_pieces: Optional[int],
        target_free_bytes: int,
        poll_interval_sec: float = 3.0,
        logger=None,
    ):
        self._log_pool = log_pool
        self._task_manager = task_manager
        self._get_available_bytes = get_available_bytes
        self.min_hot_pieces = min_hot_pieces
        self.max_hot_pieces = max_hot_pieces
        self.target_free_bytes = target_free_bytes
        self.poll_interval_sec = poll_interval_sec
        self._logger = logger
        self._task_id: Optional[str] = None

    def start(self) -> None:
        if self._task_id is not None:
            return
        self._task_id = self._task_manager.run_periodic(self.poll_interval_sec, self._tick_safe)

    def stop(self) -> None:
        if self._task_id is None:
            return
        self._task_manager.stop_periodic(self._task_id)
        self._task_id = None

    def update_policy(
        self,
        min_hot_pieces: int,
        max_hot_pieces: Optional[int],
        target_free_bytes: int,
        poll_interval_sec: float,
    ) -> None:
        """Applies live config changes. A changed poll interval needs the periodic task
        re-registered (TaskManager.run_periodic bakes the interval in at registration time), so
        this restarts it when that's the only thing to do - the others are just plain attribute
        reads on the next tick."""
        self.min_hot_pieces = min_hot_pieces
        self.max_hot_pieces = max_hot_pieces
        self.target_free_bytes = target_free_bytes
        if poll_interval_sec != self.poll_interval_sec:
            self.poll_interval_sec = poll_interval_sec
            if self._task_id is not None:
                self._task_manager.stop_periodic(self._task_id)
                self._task_id = self._task_manager.run_periodic(self.poll_interval_sec, self._tick_safe)

    def _tick_safe(self) -> None:
        """TaskManager runs periodic callbacks on its own thread pool with no caller watching for
        exceptions - swallow and log rather than letting one bad tick silently kill future ones."""
        try:
            self.tick()
        except Exception:
            if self._logger:
                self._logger.exception("HotTierMemoryGovernor tick failed")

    def tick(self) -> None:
        """One policy evaluation - split out from `_run` so tests can drive it synchronously
        without threading/timing flakiness."""
        available = self._get_available_bytes()
        current_pieces = self._log_pool.max_pieces
        segment_bytes = self._log_pool.final_buffer_bytes

        target = compute_target_pieces(
            available_bytes=available,
            current_pieces=current_pieces,
            segment_bytes=segment_bytes,
            min_hot_pieces=self.min_hot_pieces,
            max_hot_pieces=self.max_hot_pieces,
            target_free_bytes=self.target_free_bytes,
        )

        if target == current_pieces:
            return

        if self._logger:
            direction = "shrinking" if target < current_pieces else "growing"
            self._logger.info(
                f"HotTierMemoryGovernor {direction} hot tier: {current_pieces} -> {target} pieces "
                f"(available={available // (1024 * 1024)}MB, target_free={self.target_free_bytes // (1024 * 1024)}MB)"
            )
        self._log_pool.update_max_pieces(target)

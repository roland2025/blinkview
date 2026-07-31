# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import tempfile
from pathlib import Path
from threading import Event
from typing import Optional

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import configuration_factory, configuration_property, override_property
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.hot_tier_memory_governor import HotTierMemoryGovernor, get_available_memory_bytes
from blinkview.core.limits import (
    CENTRAL_STORAGE_AUTO_MEMORY_MANAGEMENT_ENABLED,
    CENTRAL_STORAGE_BUFFER_SIZE_MB,
    CENTRAL_STORAGE_COLD_MAX_PIECES,
    CENTRAL_STORAGE_COLD_STORAGE_ENABLED,
    CENTRAL_STORAGE_MAX_HOT_PIECES,
    CENTRAL_STORAGE_MAX_PIECES,
    CENTRAL_STORAGE_MAXLEN,
    CENTRAL_STORAGE_MEMORY_POLL_INTERVAL_SEC,
    CENTRAL_STORAGE_MIN_HOT_PIECES,
    CENTRAL_STORAGE_TARGET_FREE_MEMORY_MB,
)
from blinkview.core.numpy_log import (
    CircularLogPool,
)
from blinkview.utils.paths import resolve_config_path
from blinkview.utils.throughput import Speedometer


@configuration_factory(FactoryCategory.CENTRAL)
@override_property("enabled", default=True, hidden=True)
class BaseCentralStorage(BaseDaemon):
    def __init__(self):
        super().__init__()


@register_factory_category(FactoryCategory.CENTRAL)
class CentralFactory(BaseFactory[BaseCentralStorage]):
    pass


@CentralFactory.register("default")
@configuration_property(
    "maxlen",
    type="integer",
    default=CENTRAL_STORAGE_MAXLEN,
    description="Maximum number of log entries to keep in memory",
    ui_order=10,
)
@configuration_property(
    "max_pieces",
    type="integer",
    default=CENTRAL_STORAGE_MAX_PIECES,
    description="Maximum number of pieces in the circular log pool",
    ui_order=11,
)
@configuration_property(
    "buffer_size_mb",
    type="integer",
    default=CENTRAL_STORAGE_BUFFER_SIZE_MB,
    description="Total in memory = max_pieces * buffer_size_mb",
    ui_order=12,
)
@configuration_property(
    "cold_storage_enabled",
    type="boolean",
    default=CENTRAL_STORAGE_COLD_STORAGE_ENABLED,
    description="Extend scrollback beyond RAM by archiving evicted segments to disk (memmap-backed) "
    "instead of dropping them. See plans/mmap-coldstore.md.",
    ui_order=13,
)
@configuration_property(
    "cold_max_pieces",
    type="integer",
    default=CENTRAL_STORAGE_COLD_MAX_PIECES,
    description="Maximum number of additional pieces kept on disk once cold storage is enabled.",
    ui_order=14,
)
@configuration_property(
    "cold_storage_dir",
    type="string",
    default="",
    ui_type="file",
    description="Directory for cold-storage segment files (ideally a fast local NVMe path). "
    "Empty = use this session's own log folder (<session_dir>/cold/).",
    ui_order=15,
)
@configuration_property(
    "cold_storage_persist_on_close",
    type="boolean",
    default=True,
    description="Keep cold-storage segment files on disk when the app closes, instead of deleting "
    "them, and flush the hot (RAM) tier to disk too so the whole session is archived - not just "
    "what already got evicted. Reopening the same session (live or replay) later remounts these "
    "files directly instead of re-parsing/re-ingesting from scratch. Only applies to the default "
    "cold_storage_dir (a fixed <session>/cold/ folder) - an overridden cold_storage_dir always "
    "gets a fresh uniquely-named subdirectory per run, so there's nothing stable to reopen.",
    ui_order=16,
)
@configuration_property(
    "auto_memory_management_enabled",
    type="boolean",
    default=CENTRAL_STORAGE_AUTO_MEMORY_MANAGEMENT_ENABLED,
    description="Let the hot tier grow to use most of whatever system RAM is free, and shrink "
    "(evicting oldest segments to cold storage) the moment free memory gets tight, instead of a "
    "static max_pieces ceiling. Requires cold_storage_enabled - otherwise shrinking would delete "
    "data instead of archiving it, so this stays off in that case regardless of this setting. See "
    "plans/auto-hot-cold-memory-management.md.",
    ui_order=17,
)
@configuration_property(
    "min_hot_pieces",
    type="integer",
    default=CENTRAL_STORAGE_MIN_HOT_PIECES,
    description="Floor on the hot tier's piece count when auto_memory_management_enabled - recent "
    "scrollback never becomes disk-latency-bound no matter how much memory pressure there is.",
    ui_order=18,
)
@configuration_property(
    "max_hot_pieces",
    type="integer",
    default=CENTRAL_STORAGE_MAX_HOT_PIECES,
    description="Optional ceiling on the hot tier's piece count when auto_memory_management_enabled, "
    "even under abundant free memory. 0 = unbounded except by memory pressure itself.",
    ui_order=19,
)
@configuration_property(
    "target_free_memory_mb",
    type="integer",
    default=CENTRAL_STORAGE_TARGET_FREE_MEMORY_MB,
    description="System free-memory floor (MB) auto_memory_management_enabled tries to maintain - "
    "the hot tier shrinks once available memory drops below this.",
    ui_order=20,
)
@configuration_property(
    "memory_poll_interval_sec",
    type="number",
    default=CENTRAL_STORAGE_MEMORY_POLL_INTERVAL_SEC,
    description="How often (seconds) auto_memory_management_enabled re-checks system free memory.",
    ui_order=21,
)
@override_property(
    "logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "log_row"}}, ui_order=30
)
class CentralStorage(BaseCentralStorage):
    maxlen: int
    max_pieces: int
    buffer_size_mb: int
    cold_storage_enabled: bool
    cold_max_pieces: int
    cold_storage_dir: str
    cold_storage_persist_on_close: bool
    auto_memory_management_enabled: bool
    min_hot_pieces: int
    max_hot_pieces: int
    target_free_memory_mb: int
    memory_poll_interval_sec: float

    def __init__(self):
        super().__init__()
        self.name = "central"
        self.input_queue = BatchQueue()  # messages that have not yet been pushed to subscribers

        self.put = self.input_queue.put

        self.log_pool: Optional[CircularLogPool] = None
        self.memory_governor: Optional[HotTierMemoryGovernor] = None

        # Set (open) by default - cleared while a replay session's historical backfill is being
        # bulk-loaded directly into log_pool (see UnifiedLogReplay), so anything already queued
        # here (typically this process's own live SystemLogger messages) waits rather than
        # interleaving with - and potentially getting lower sequence numbers than - historical
        # rows still being loaded. See CircularLogPool.freeze_cold_storage_from_now.
        self._ingest_gate = Event()
        self._ingest_gate.set()

    def pause_ingest(self):
        self._ingest_gate.clear()

    def resume_ingest(self):
        self._ingest_gate.set()

    def _resolve_cold_storage_dir(self) -> Path:
        """Always creates and returns a fresh directory the cold-storage archiver can rmtree
        wholesale on cleanup (see ColdStorageArchiver.cleanup) without risking files unrelated to
        this session.

        Default (cold_storage_dir unset): a `cold/` subfolder under the session whose data is
        actually being ingested right now - `FileManager.replay_source_dir` (the *original*
        session being replayed) if this run was launched straight into replay
        (ui/run.py's replay_mode/replay_session_info path - see Registry.__init__), otherwise
        this run's own `FileManager.session_dir`. Using `session_dir` unconditionally here used
        to materialize a brand-new (otherwise lazily-created, see FileManager.__init__'s
        `create=not replay_mode`) live session folder purely as a `mkdir(parents=True)` side
        effect of resolving the cold dir - exactly the "phantom session folder" a replay-only
        launch is supposed to avoid creating at all. It lives and dies with whichever session
        owns it, and gets cleared out at program shutdown via the archiver's atexit hook.
        `cold_storage_dir`, if the user sets it (e.g. to point at a faster NVMe mount than
        wherever `logs/` lives), is instead used as *where* to create a uniquely-named temp
        subdirectory - it must not be reused as-is since it may already contain unrelated files."""
        if self.cold_storage_dir:
            base = resolve_config_path(self.cold_storage_dir)
            return Path(tempfile.mkdtemp(prefix="blinkview_coldstore_", dir=base))
        file_manager = self.shared.registry.file_manager
        owner_dir = file_manager.replay_source_dir or file_manager.session_dir
        cold_dir = owner_dir / "cold"
        cold_dir.mkdir(parents=True, exist_ok=True)
        # A previous run of this same session may have shrunk this dir's footprint by moving
        # segments into a sibling cold-archive/ directory as zstd-compressed files (see
        # _compress_persisted_cold_storage in Registry.stop()) - CircularLogPool's
        # _mount_existing_cold_segments (below, via the constructor) mounts straight from those
        # archives directly into memory when a raw copy isn't present here, so nothing further
        # needs to happen at this point - see core/cold_archive.py.
        return cold_dir

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        if self.log_pool is None:
            buffer_bytes = self.buffer_size_mb * 1024 * 1024

            cold_max_pieces = self.cold_max_pieces if self.cold_storage_enabled else 0
            cold_storage_dir = self._resolve_cold_storage_dir() if cold_max_pieces > 0 else None

            self.log_pool = CircularLogPool(
                self.shared.array_pool,
                max_pieces=self.max_pieces,
                final_buffer_bytes=buffer_bytes,
                cold_max_pieces=cold_max_pieces,
                cold_storage_dir=cold_storage_dir,
                persist_cold_storage=self.cold_storage_persist_on_close,
                logger=self.logger,
            )
        else:
            # Runtime dynamic updates
            self.log_pool.update_max_pieces(self.max_pieces)

            new_bytes = self.buffer_size_mb * 1024 * 1024
            self.log_pool.update_final_buffer_bytes(new_bytes)

            # Cold storage's enabled/dir can only take effect on (re)creation - see
            # CircularLogPool.__init__ - but the piece-count ceiling can still be adjusted live.
            if self.cold_storage_enabled:
                self.log_pool.update_cold_max_pieces(self.cold_max_pieces)

        self._apply_memory_governor_config()

        return changed

    def _apply_memory_governor_config(self):
        """Starts/stops/reconfigures HotTierMemoryGovernor to match current config. Refuses to run
        without cold storage enabled - see plans/auto-hot-cold-memory-management.md's
        "Precondition" section: update_max_pieces' shrink path only archives to cold storage when
        an archiver is configured, otherwise it silently drops the evicted data, which would turn
        "evict under memory pressure" into "delete data under memory pressure"."""
        if not self.auto_memory_management_enabled or not self.cold_storage_enabled:
            if self.auto_memory_management_enabled and not self.cold_storage_enabled and self.logger:
                self.logger.warning(
                    "auto_memory_management_enabled requires cold_storage_enabled - leaving hot-tier "
                    "auto-sizing off so shrinking the hot tier can't silently delete data."
                )
            if self.memory_governor is not None:
                self.memory_governor.stop()
                self.memory_governor = None
            return

        target_free_bytes = self.target_free_memory_mb * 1024 * 1024
        max_hot_pieces = self.max_hot_pieces or None

        if self.memory_governor is None:
            self.memory_governor = HotTierMemoryGovernor(
                log_pool=self.log_pool,
                task_manager=self.shared.tasks,
                get_available_bytes=get_available_memory_bytes,
                min_hot_pieces=self.min_hot_pieces,
                max_hot_pieces=max_hot_pieces,
                target_free_bytes=target_free_bytes,
                poll_interval_sec=self.memory_poll_interval_sec,
                logger=self.logger,
            )
            self.memory_governor.start()
        else:
            self.memory_governor.update_policy(
                min_hot_pieces=self.min_hot_pieces,
                max_hot_pieces=max_hot_pieces,
                target_free_bytes=target_free_bytes,
                poll_interval_sec=self.memory_poll_interval_sec,
            )

    def stop(self, timeout: float = 5.0) -> None:
        # Make sure run()'s loop can wake up and observe _stop_event even if it's currently
        # parked waiting on a paused ingest gate.
        self._ingest_gate.set()
        super().stop(timeout)
        if self.memory_governor is not None:
            self.memory_governor.stop()
            self.memory_governor = None

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        get = self.input_queue.get

        speedometer = Speedometer(logger=self.logger.child("stats"))

        while not stop_is_set():
            # we need to push messages to subscribers here, but for now we just keep them in the log

            # Blocks here (not while a batch is mid-flight below) while a replay session's
            # historical backfill is being loaded directly into log_pool - see pause_ingest.
            self._ingest_gate.wait()
            if stop_is_set():
                break

            try:
                batch = get(timeout=120)
                if batch is None:
                    continue

                with batch:
                    # print(f"[CENTRAL] Received batch of {len(entry)} entries.")
                    # print(f"[Central] batch={batch}")
                    # print(f"[central] batch={batch}")

                    # log_batch(self, batch, "IN")

                    speedometer.batch(batch)

                    self.log_pool.batch_append(batch)

                    self.distribute(batch)

            except Exception as e:
                self.logger.exception("fcked", exc=e)

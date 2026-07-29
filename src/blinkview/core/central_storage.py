# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import tempfile
from pathlib import Path
from typing import Optional

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import configuration_factory, configuration_property, override_property
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.limits import (
    CENTRAL_STORAGE_BUFFER_SIZE_MB,
    CENTRAL_STORAGE_COLD_MAX_PIECES,
    CENTRAL_STORAGE_COLD_STORAGE_ENABLED,
    CENTRAL_STORAGE_MAX_PIECES,
    CENTRAL_STORAGE_MAXLEN,
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
@override_property(
    "logging", hidden=False, required=True, default={"enabled": True, "processor": {"type": "log_row"}}, ui_order=20
)
class CentralStorage(BaseCentralStorage):
    maxlen: int
    max_pieces: int
    buffer_size_mb: int
    cold_storage_enabled: bool
    cold_max_pieces: int
    cold_storage_dir: str

    def __init__(self):
        super().__init__()
        self.name = "central"
        self.input_queue = BatchQueue()  # messages that have not yet been pushed to subscribers

        self.put = self.input_queue.put

        self.log_pool: Optional[CircularLogPool] = None

    def _resolve_cold_storage_dir(self) -> Path:
        """Always creates and returns a fresh directory the cold-storage archiver can rmtree
        wholesale on cleanup (see ColdStorageArchiver.cleanup) without risking files unrelated to
        this session.

        Default (cold_storage_dir unset): a `cold/` subfolder directly under this session's own
        log folder (`FileManager.session_dir`) - it lives and dies with the session, next to
        `metadata.json`/`gui/`/etc., and gets cleared out at program shutdown via the archiver's
        atexit hook. `cold_storage_dir`, if the user sets it (e.g. to point at a faster NVMe mount
        than wherever `logs/` lives), is instead used as *where* to create a uniquely-named temp
        subdirectory - it must not be reused as-is since it may already contain unrelated files."""
        if self.cold_storage_dir:
            base = resolve_config_path(self.cold_storage_dir)
            return Path(tempfile.mkdtemp(prefix="blinkview_coldstore_", dir=base))
        cold_dir = self.shared.registry.file_manager.session_dir / "cold"
        cold_dir.mkdir(parents=True, exist_ok=True)
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

        return changed

    def run(self):
        # Localize method lookups
        stop_is_set = self._stop_event.is_set
        get = self.input_queue.get

        speedometer = Speedometer(logger=self.logger.child("stats"))

        while not stop_is_set():
            # we need to push messages to subscribers here, but for now we just keep them in the log

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
                self.logger.exception("fcked", e)

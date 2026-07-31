# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Background writer that serializes evicted, already-frozen CircularLogPool segments to disk -
see plans/mmap-coldstore.md. Never runs on the ingestion thread: CircularLogPool._rotate_segment()
hands off its own reference to a segment it just popped, and this archiver either writes it in the
background or, if it can't keep up, drops it immediately - ingestion throughput must never depend
on disk throughput."""

import atexit
import queue
import shutil
import threading
from pathlib import Path
from typing import Callable, Optional, Union

from blinkview.core.cold_segment import ColdSegmentMeta, write_cold_segment_file
from blinkview.core.numpy_batch_manager import PooledLogBatch


class ColdStorageArchiver:
    def __init__(
        self,
        storage_dir: Union[str, Path],
        on_archived: Callable[[PooledLogBatch], None],
        queue_depth: int = 4,
        logger=None,
        persist: bool = False,
    ):
        self._dir = Path(storage_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._on_archived = on_archived
        self._logger = logger
        self._persist = persist
        self._queue: "queue.Queue[PooledLogBatch]" = queue.Queue(maxsize=queue_depth)
        # Continue numbering after whatever's already in storage_dir (e.g. persisted segment
        # files from a previous run being resumed - see CircularLogPool's mount-existing-segments
        # step) instead of always starting at 0, which would silently overwrite them the moment
        # this run's first new segment gets archived.
        self._counter = self._next_counter_after_existing_files()
        self._counter_lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = threading.Thread(target=self._run, name="ColdStorageArchiver", daemon=True)
        self._thread.start()

        # Best-effort safety net: if nobody explicitly calls CircularLogPool.release_all() before
        # the process exits (today's CentralStorage.stop() intentionally does not - see its
        # docstring/plan doc - "stop" means "stop the ingestion thread," not "wipe scrollback"),
        # this still cleans up the session-scoped temp directory rather than leaking it forever.
        # cleanup() is idempotent (shutil.rmtree(ignore_errors=True)), so this is harmless if
        # release_all() already ran it explicitly. Skipped entirely when persist=True - the whole
        # point of persisting is that this directory survives process exit.
        atexit.register(self.cleanup)

    def _next_counter_after_existing_files(self) -> int:
        """Scans both possible on-disk forms of a previously-persisted segment - raw
        `segment_*.blkseg` files here in storage_dir, and zstd-compressed
        `segment_*.blkseg.zst` archives in the sibling `cold-archive/` directory
        (core/cold_archive.py) - since CircularLogPool._mount_existing_cold_segments prefers the
        compressed form once a raw file has been compressed-and-deleted. Missing the archive
        directory here would reset the counter to 0 the moment every raw segment from a previous
        run has been fully compressed (storage_dir then looks empty), reusing already-taken
        segment indices - which breaks _mount_existing_cold_segments's lexical-filename-order-is-
        chronological-order assumption the next time this session is resumed, since the reused,
        freshly-written index now sorts before (but is timestamped after) the real segments that
        index used to belong to."""
        highest = -1
        for path in self._dir.glob("segment_*.blkseg"):
            try:
                index = int(path.stem.split("_", 1)[1])
            except (IndexError, ValueError):
                continue
            highest = max(highest, index)

        archive_dir = self._dir.parent / "cold-archive"
        if archive_dir.is_dir():
            for path in archive_dir.glob("segment_*.blkseg.zst"):
                try:
                    index = int(path.name.split("_", 1)[1].split(".", 1)[0])
                except (IndexError, ValueError):
                    continue
                highest = max(highest, index)

        return highest + 1

    def archive(self, segment: PooledLogBatch) -> bool:
        """Takes ownership of `segment`'s caller-held reference (the caller must not touch it
        again after calling this). Returns True if it was queued for background writing, False if
        the queue was full and the segment was dropped (released) immediately instead - same
        end-user-visible behavior as today's unconditional release-on-evict, just usually
        deferred to disk first."""
        try:
            self._queue.put_nowait(segment)
            return True
        except queue.Full:
            if self._logger:
                self._logger.warning("Cold storage archiver backlogged - dropping segment without archiving")
            segment.release()
            return False

    def _next_path(self) -> Path:
        with self._counter_lock:
            n = self._counter
            self._counter += 1
        return self._dir / f"segment_{n:010d}.blkseg"

    def _run(self):
        while True:
            try:
                segment = self._queue.get(timeout=0.2)
            except queue.Empty:
                if self._stop_event.is_set():
                    return
                continue

            try:
                path = self._next_path()
                header = write_cold_segment_file(path, segment.bundle)
                meta = ColdSegmentMeta(
                    str(path), header.earliest_ts, header.latest_ts, header.first_seq, header.last_seq
                )
                cold_segment = PooledLogBatch.from_memmap(path, metadata=meta)
                self._on_archived(cold_segment)
            except Exception:
                if self._logger:
                    self._logger.exception("Failed to archive cold segment")
            finally:
                segment.release()

    def stop(self, timeout: Optional[float] = 5.0) -> None:
        self._stop_event.set()
        self._thread.join(timeout=timeout)

    def cleanup(self) -> None:
        """Deletes the cold-store directory tree. Only safe to call after stop() and after every
        MmapArrayHandle/PooledLogBatch.from_memmap referencing a file under it has been
        released - an open mapping will keep the file (and on Windows, the whole delete) blocked.

        No-op when persist=True (cold_storage_persist_on_close) - the files are left on disk so a
        later run can remount them (see CircularLogPool's mount-existing-segments step) instead of
        re-parsing/re-ingesting the session from scratch."""
        if self._persist:
            return
        shutil.rmtree(self._dir, ignore_errors=True)

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from typing import TYPE_CHECKING, Iterator, Tuple

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.module_snapshot import (
    MAX_MSG_BYTES,
    ModuleSnapshotParams,
    nb_build_snapshot_as_of,
    nb_copy_snapshot_state,
    nb_update_master_arrays_reverse,
)

if TYPE_CHECKING:
    from blinkview.core.id_registry.tables import IndexedStringTable
    from blinkview.core.numpy_log import CircularLogPool
    from blinkview.core.warmup import NumbaWarmupHelper


class ModuleSnapshot:
    """
    A ref-counted, point-in-time view of module statuses backed by pooled arrays.
    Consumers should use this as a context manager to ensure release.
    """

    __slots__ = (
        "ts_h",
        "seq_h",
        "lvl_h",
        "lens_h",
        "buf_h",
        "_bundle",
        "last_known_seq",
        "_ref_count",
        "_lock",
    )

    def __init__(self, ts_h, seq_h, lvl_h, lens_h, buf_h, count: int, last_known_seq: int):
        self.ts_h = ts_h
        self.seq_h = seq_h
        self.lvl_h = lvl_h
        self.lens_h = lens_h
        self.buf_h = buf_h

        self._bundle = ModuleSnapshotParams(
            timestamps=ts_h.array,
            sequence_ids=seq_h.array,
            levels=lvl_h.array,
            lengths=lens_h.array,
            buffer=buf_h.array,
            count=count,
            capacity=len(ts_h.array),
        )
        self.last_known_seq = last_known_seq

        self._ref_count = 1  # Initially held by the tracker
        self._lock = Lock()

    def bundle(self) -> ModuleSnapshotParams:
        """Returns the Numba-compatible NamedTuple for kernel ingestion."""
        return self._bundle

    def retain(self):
        """Increments reference count for safe cross-thread consumption."""
        with self._lock:
            if self._ref_count <= 0:
                raise RuntimeError("Cannot retain a ModuleSnapshot that has already been released to the pool.")
            self._ref_count += 1
        return self

    def release(self):
        """Decrements reference count. If 0, returns all underlying arrays to the global pool."""
        with self._lock:
            self._ref_count -= 1
            if self._ref_count > 0:
                return

        self._bundle = None

        for h in (self.ts_h, self.seq_h, self.lvl_h, self.lens_h, self.buf_h):
            if h is not None:
                h.release()

        self.ts_h = self.seq_h = self.lvl_h = self.lens_h = self.buf_h = None

    def get_message(self, module_id: int) -> str:
        """Decodes the message for a module using the stored length."""
        b = self._bundle
        if module_id >= b.count or b.sequence_ids[module_id] == 0:
            return ""

        length = b.lengths[module_id]
        off = module_id * MAX_MSG_BYTES
        return b.buffer[off : off + length].tobytes().decode("utf-8", errors="replace")

    def get_level(self, module_id: int) -> int:
        """Safely retrieves the level integer for a given module."""
        b = self._bundle
        if module_id >= b.count:
            return 0
        return b.levels[module_id]

    def get_sequence(self, module_id: int) -> int:
        """Safely retrieves the sequence ID for a given module. Returns 0 if empty or out of bounds."""
        b = self._bundle
        if module_id >= b.count:
            return 0
        return b.sequence_ids[module_id]

    def __iter__(self) -> Iterator[Tuple[int, int, str]]:
        """Yields (timestamp, sequence, message) for modules that have actual data."""
        b = self._bundle
        for i in range(b.count):
            seq = b.sequence_ids[i]

            # If seq is 0, this module has no data yet.
            # Skip or yield empty to avoid printing "ghost" bytes from the pool.
            if seq == 0:
                yield b.timestamps[i], 0, ""
                continue

            length = b.lengths[i]
            off = i * MAX_MSG_BYTES
            msg = b.buffer[off : off + length].tobytes().decode("utf-8", errors="replace")
            yield b.timestamps[i], seq, msg

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    def debug_print(self, table: "IndexedStringTable" = None):
        """Prints the contents of this specific snapshot."""
        b = self._bundle
        print(f"--- ModuleSnapshot Dump | Last Known Seq: {self.last_known_seq} ---")
        print(f"Capacity: {b.capacity} | Module Count: {b.count}")

        for i, (ts, seq, msg) in enumerate(self):
            if seq == 0:
                continue  # Skip modules that haven't received data

            if table is not None:
                mod_name = table.get_string(i)
                print(f"  i={i:<4} mod={mod_name:<32} ts={ts:<18} seq={seq:<10} msg='{msg}'")
            else:
                print(f"  i={i:<4} ts={ts:<18} seq={seq:<10} msg='{msg}'")

        print("--- End Snapshot Dump ---")


class LatestModuleValueTracker:
    __slots__ = (
        "_log_pool",
        "_array_pool",
        "_module_table",
        "time_ns",
        "_initialized",
        "last_known_seq",
        "_current_snapshot",
        "_update_lock",
    )

    def __init__(
        self, log_pool: "CircularLogPool", modules_table: "IndexedStringTable", array_pool: "NumpyArrayPool", time_ns
    ):
        self._log_pool: "CircularLogPool" = log_pool
        self._array_pool: "NumpyArrayPool" = array_pool
        self._module_table: "IndexedStringTable" = modules_table

        self.time_ns = time_ns

        self._initialized = False

        self.last_known_seq = dtypes.SEQ_TYPE(0)

        self._update_lock = Lock()

        m_bundle = self._module_table.bundle()
        initial_capacity = max(1024, m_bundle.count)

        self._current_snapshot = self._allocate_snapshot(initial_capacity, m_bundle.count, 0)
        self._current_snapshot.bundle().sequence_ids[:] = 0

    @staticmethod
    @register_warmup
    def warmup(helper: "NumbaWarmupHelper"):
        """Builds the helper's dummy tracker and triggers compilation for Module Snapshot
        tracking and state copying (nb_copy_snapshot_state, nb_update_master_arrays_reverse,
        and the playback-scrub nb_build_snapshot_as_of).
        Requires data in the pool, provided by NumbaWarmupHelper.exercise_logging_kernels().
        Assigns the tracker onto helper.tracker so other warmup callbacks (e.g.
        TelemetryTableModel.warmup) can reuse it."""

        print("[Warmup] LatestModuleValueTracker ...")

        tracker = LatestModuleValueTracker(
            helper.log_pool, helper.registry.modules_table, helper.array_pool, helper.time_ns
        )
        tracker.update()

        # Optionally exercise the string decoding/iterator logic
        with tracker.get_snapshot() as snap:
            for _ in snap:
                break

        # Compile the playback-scrub "rebuild as of a past ts" path
        with tracker.build_snapshot_as_of(helper.time_ns()) as replay_snap:
            for _ in replay_snap:
                break

        print("[Warmup] LatestModuleValueTracker ... done")

    def _allocate_snapshot(self, capacity: int, count: int, last_known_seq: int) -> ModuleSnapshot:
        """Acquires pool arrays and constructs a snapshot."""
        ts_h = self._array_pool.acquire(capacity, dtype=dtypes.TS_TYPE)
        seq_h = self._array_pool.acquire(capacity, dtype=dtypes.SEQ_TYPE)
        lvl_h = self._array_pool.acquire(capacity, dtype=dtypes.LEVEL_TYPE)
        lens_h = self._array_pool.acquire(capacity, dtype=dtypes.LEN_TYPE)
        buf_h = self._array_pool.acquire(capacity * MAX_MSG_BYTES, dtype=dtypes.BYTE)

        return ModuleSnapshot(ts_h, seq_h, lvl_h, lens_h, buf_h, count, last_known_seq)

    def update(self):
        with self._update_lock:
            start = self.time_ns()

            # 1. Localize frequently accessed attributes
            lks = self.last_known_seq  # Localize high-water mark
            initialized = self._initialized

            m_bundle = self._module_table.bundle()
            current_count = m_bundle.count

            old_snap = self._current_snapshot
            old_b = old_snap.bundle()

            capacity = old_b.capacity
            if current_count > capacity:
                capacity = max(current_count, capacity * 2)

            # 2. Allocate a fresh snapshot from the pool
            # Using localized 'lks'
            new_snap = self._allocate_snapshot(capacity, current_count, lks)
            new_b = new_snap.bundle()

            # 3. Copy state from the old snapshot via Numba
            nb_copy_snapshot_state(old_b, new_b)

            # Use local 'lks' as the baseline for the new burst
            new_high_water = lks

            # 4. Process logs into the newly acquired arrays
            with self._log_pool.get_reversed_snapshot() as segments:
                for segment in segments:
                    if segment.size == 0:
                        continue

                    # Check the segment header before diving into the kernel
                    seg_last_seq = segment.last_sequence_id
                    if seg_last_seq <= lks:
                        break

                    if seg_last_seq > new_high_water:
                        new_high_water = seg_last_seq

                    seg_b = segment.bundle

                    # Kernel uses the localized baseline
                    hit_boundary = nb_update_master_arrays_reverse(
                        seg_b,
                        new_b,
                        current_count,
                        lks,
                        initialized,
                    )

                    if hit_boundary:
                        break

            # 5. Finalize State
            new_snap.last_known_seq = new_high_water
            self._initialized = True

            # 6. Atomic Swap
            self._current_snapshot = new_snap

            # Publicly announce the new high-water mark
            self.last_known_seq = new_high_water

            old_snap.release()

            end = self.time_ns()
            duration = (end - start) / 1e6
            # print(f"LatestModuleValueTracker: Reverse update completed in {duration:.4f} ms")

    def get_snapshot(self) -> ModuleSnapshot:
        # 6. Lock-free retry loop to prevent the read-side RuntimeError race condition
        while True:
            try:
                return self._current_snapshot.retain()
            except RuntimeError:
                # The background thread swapped and released this snapshot
                # a microsecond before we called retain(). Try again.
                continue

    def build_snapshot_as_of(self, ts_ns: int) -> ModuleSnapshot:
        """Playback-scrub counterpart to get_snapshot(): rebuilds a one-shot ModuleSnapshot
        holding the latest-per-module message as of an arbitrary past `ts_ns`, instead of
        the tracker's incrementally-maintained "latest ever" snapshot. Callers (widgets
        following registry.playback_clock in REPLAY) are expected to call this once per
        follow tick and NOT retain it across ticks - unlike the live snapshot, there's no
        watermark to resume from, so every call does a fresh bounded backward scan.
        """
        m_bundle = self._module_table.bundle()
        count = m_bundle.count

        snap = self._allocate_snapshot(max(1024, count), count, 0)
        b = snap.bundle()
        b.sequence_ids[:] = 0

        if count == 0:
            return snap

        found_mask = np.zeros(count, dtype=np.bool_)
        remaining = count

        with self._log_pool.get_reversed_snapshot() as segments:
            for segment in segments:
                if remaining <= 0:
                    break
                if segment.size == 0:
                    continue

                all_found, remaining = nb_build_snapshot_as_of(segment.bundle, b, count, ts_ns, found_mask, remaining)
                if all_found:
                    break

        return snap

    def debug_print(self):
        """Helper to print the current active snapshot."""
        with self.get_snapshot() as snap:
            # Pass our table into the snapshot's print logic
            snap.debug_print(self._module_table)

    def update_and_print(self):
        self.update()
        # self.debug_print()

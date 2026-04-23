# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Iterator, NamedTuple, Tuple

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.numba_config import app_njit
from blinkview.core.types.log_batch import LogBundle

if TYPE_CHECKING:
    from blinkview.core.id_registry.tables import IndexedStringTable
    from blinkview.core.numpy_log import CircularLogPool


class ModuleSnapshotParams(NamedTuple):
    """Numba-compatible view of a module snapshot's memory."""

    timestamps: np.ndarray  # dtypes.TS_TYPE
    sequence_ids: np.ndarray  # dtypes.SEQ_TYPE
    levels: np.ndarray  # dtypes.LEVEL_TYPE
    lengths: np.ndarray  # dtypes.LEN_TYPE
    buffer: np.ndarray  # dtypes.BYTE

    count: int
    capacity: int


@app_njit()
def _copy_snapshot_state(
    old_b: ModuleSnapshotParams,
    new_b: ModuleSnapshotParams,
):
    """Fast compiled memory copy from the old snapshot to the new one."""
    old_cnt = old_b.count

    new_b.timestamps[:old_cnt] = old_b.timestamps[:old_cnt]
    new_b.levels[:old_cnt] = old_b.levels[:old_cnt]
    new_b.lengths[:old_cnt] = old_b.lengths[:old_cnt]
    new_b.sequence_ids[:old_cnt] = old_b.sequence_ids[:old_cnt]

    bytes_to_copy = old_cnt * 512
    new_b.buffer[:bytes_to_copy] = old_b.buffer[:bytes_to_copy]

    # CLEANSE THE TAIL:
    if new_b.capacity > old_cnt:
        new_b.sequence_ids[old_cnt:] = 0
        new_b.lengths[old_cnt:] = 0
        new_b.timestamps[old_cnt:] = 0
        new_b.levels[old_cnt:] = 0


@app_njit()
def _update_master_arrays_reverse(
    seg_b: LogBundle,
    snap_b: ModuleSnapshotParams,
    module_count: int,
    last_known_seq: int,
    is_initialized: bool,
) -> bool:
    """
    Scans a segment from back-to-front.
    Returns True if we hit the 'last_known_seq' (time to stop everything).
    """
    row_count = seg_b.size[0]
    seg_b_modules = seg_b.modules
    seg_b_timestamps = seg_b.timestamps
    seg_b_levels = seg_b.levels
    seg_b_lengths = seg_b.lengths
    seg_b_offsets = seg_b.offsets
    seg_b_buffer = seg_b.buffer
    seg_b_sequences = seg_b.sequences

    snap_b_timestamps = snap_b.timestamps
    snap_b_sequence_ids = snap_b.sequence_ids
    snap_b_levels = snap_b.levels
    snap_b_lengths = snap_b.lengths
    snap_b_buffer = snap_b.buffer

    for i in range(row_count - 1, -1, -1):
        seq = seg_b_sequences[i]

        if is_initialized and seq <= last_known_seq:
            return True

        mod_id = seg_b_modules[i]
        # Protect against out-of-bounds or newly registered modules
        # not yet accounted for in this update cycle
        if mod_id >= module_count:
            continue

        if seq > snap_b_sequence_ids[mod_id]:
            snap_b_timestamps[mod_id] = seg_b_timestamps[i]
            snap_b_sequence_ids[mod_id] = seq
            snap_b_levels[mod_id] = seg_b_levels[i]

            m_len = seg_b_lengths[i]
            # Cap at 511 to guarantee room for the 0-terminator
            if m_len > 511:
                m_len = 511

            s_off = seg_b_offsets[i]
            m_off = mod_id * 512  # Computed on the fly

            # Copy the message payload
            snap_b_buffer[m_off : m_off + m_len] = seg_b_buffer[s_off : s_off + m_len]
            snap_b_lengths[mod_id] = m_len

            # Always 0-terminate the string, regardless of original length
            snap_b_buffer[m_off + m_len] = 0

    return False


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
        off = module_id * 512
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
            off = i * 512
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

    def _allocate_snapshot(self, capacity: int, count: int, last_known_seq: int) -> ModuleSnapshot:
        """Acquires pool arrays and constructs a snapshot."""
        ts_h = self._array_pool.acquire(capacity, dtype=dtypes.TS_TYPE)
        seq_h = self._array_pool.acquire(capacity, dtype=dtypes.SEQ_TYPE)
        lvl_h = self._array_pool.acquire(capacity, dtype=dtypes.LEVEL_TYPE)
        lens_h = self._array_pool.acquire(capacity, dtype=dtypes.LEN_TYPE)
        buf_h = self._array_pool.acquire(capacity * 512, dtype=dtypes.BYTE)

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
            _copy_snapshot_state(old_b, new_b)

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
                    hit_boundary = _update_master_arrays_reverse(
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

    def debug_print(self):
        """Helper to print the current active snapshot."""
        with self.get_snapshot() as snap:
            # Pass our table into the snapshot's print logic
            snap.debug_print(self._module_table)

    def update_and_print(self):
        self.update()
        # self.debug_print()

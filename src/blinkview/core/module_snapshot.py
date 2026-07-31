# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from typing import TYPE_CHECKING, Iterator, Optional, Tuple

import numpy as np

from blinkview.core import dtypes
from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.module_snapshot import (
    MAX_MSG_BYTES,
    ModuleSnapshotParams,
    nb_build_snapshot_as_of,
    nb_copy_live_valid_modules,
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
        "_scrub_cache",
        "_scrub_cache_ts_ns",
        "_first_seen_ts",
        "_first_seen_seq",
        "_first_seen_coverage_ts",
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

        self._scrub_cache: Optional[ModuleSnapshot] = None
        self._scrub_cache_ts_ns: Optional[int] = None

        # Persistent, tracker-lifetime "first occurrence per module" table - see update()'s
        # docstring for why piggybacking on its existing scan is sufficient (no separate
        # prescan). 0 in _first_seen_seq means "not yet confirmed one way or the other" (matches
        # SEQ_NONE); _first_seen_coverage_ts gates trusting a 0 entry as genuinely "confirmed
        # absent" vs. "haven't looked far enough yet".
        self._first_seen_ts = np.zeros(initial_capacity, dtype=dtypes.TS_TYPE)
        self._first_seen_seq = np.zeros(initial_capacity, dtype=dtypes.SEQ_TYPE)
        self._first_seen_coverage_ts = dtypes.TS_UNSPECIFIED

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

        # Compile the playback-scrub "rebuild as of a past ts" path - once for the full/cold-scan
        # branch, once more at a later ts_ns to exercise the forward-incremental (cache-hit)
        # branch, and once at an earlier ts_ns to exercise the backward-jump (pre-seeded
        # found_mask) branch - all three are the same compiled kernel/types, but this confirms
        # the Python-side branching logic itself runs cleanly, not just that it compiles.
        now_ns = helper.time_ns()
        with tracker.build_snapshot_as_of(now_ns) as replay_snap:
            for _ in replay_snap:
                break
        with tracker.build_snapshot_as_of(now_ns + 1) as replay_snap:
            for _ in replay_snap:
                break
        with tracker.build_snapshot_as_of(now_ns - 1) as replay_snap:
            for _ in replay_snap:
                break

        # And once at ts=0 (before any real data existed) to compile+exercise the
        # confirmed-absent first-occurrence pre-seeding branch specifically.
        with tracker.build_snapshot_as_of(0) as replay_snap:
            for _ in replay_snap:
                break

        # Exercise the background-thread-facing wrappers too (update_replay/get_replay_snapshot)
        # - same compiled kernel as above, just confirms this thin API surface runs cleanly.
        tracker.update_replay(now_ns)
        with tracker.get_replay_snapshot() as replay_snap:
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

            # Grow the persistent first-occurrence table alongside the snapshot capacity - plain
            # numpy arrays, not pool-acquired (this is tracker-lifetime bookkeeping, not a
            # per-call ref-counted handout like ModuleSnapshot). np.resize on growth *tiles* the
            # old contents to fill the new shape rather than zero-filling, so the newly added
            # tail must be zeroed explicitly afterward - matching nb_copy_snapshot_state's
            # "cleanse the tail" behavior for new_b.
            old_capacity = self._first_seen_seq.shape[0]
            if current_count > old_capacity:
                new_capacity = max(current_count, old_capacity * 2)
                self._first_seen_ts = np.resize(self._first_seen_ts, new_capacity)
                self._first_seen_ts[old_capacity:] = 0
                self._first_seen_seq = np.resize(self._first_seen_seq, new_capacity)
                self._first_seen_seq[old_capacity:] = 0

            # Use local 'lks' as the baseline for the new burst
            new_high_water = lks

            # Captured *before* scanning (not after) so a race with data arriving mid-scan can
            # only under-report coverage, never over-report it - see LatestModuleValueTracker's
            # docstring on why an over-reported coverage watermark would be unsafe.
            pool_newest_at_scan_start = self._log_pool.get_time_bounds()[0]

            # 4. Process logs into the newly acquired arrays. get_reversed_snapshot_since (not
            # get_reversed_snapshot) since this is an incremental "what's new since last tick"
            # query - already-consumed cold segments can never contribute a row newer than lks
            # again, so retaining them every 60Hz tick was pure overhead (see
            # CircularLogPool.get_reversed_snapshot_since's docstring).
            with self._log_pool.get_reversed_snapshot_since(lks) as segments:
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
                        self._first_seen_ts,
                        self._first_seen_seq,
                    )

                    if hit_boundary:
                        break

            # 5. Finalize State
            new_snap.last_known_seq = new_high_water
            self._initialized = True
            self._first_seen_coverage_ts = pool_newest_at_scan_start

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
        """Playback-scrub counterpart to get_snapshot(): rebuilds a ModuleSnapshot holding the
        latest-per-module message as of an arbitrary past `ts_ns`, instead of the tracker's
        incrementally-maintained "latest ever" snapshot. Callers (widgets following
        registry.playback_clock in REPLAY) are expected to call this once per follow tick.

        Bidirectional incremental cache: unlike the old always-rescan-everything version, this
        keeps the last-built snapshot (`_scrub_cache`) - which already doubles as "the latest
        known occurrence per module we've resolved so far" - and the ts_ns it represents
        (`_scrub_cache_ts_ns`).

        Key fact this relies on: because every scan visits segments newest-to-oldest and only
        ever records a module's *first* sighting (skipping rows for already-found modules), a
        resolved module's cached (ts, seq) is guaranteed to be its true latest occurrence at or
        before `_scrub_cache_ts_ns`, and - just as importantly - the gap between `cached_ts` and
        `_scrub_cache_ts_ns` is *proven empty* for that module (nothing in there, or it would
        have been found first). A cached "no data" entry (seq == 0) is the same fact taken to
        its limit: no occurrence exists anywhere at or before `_scrub_cache_ts_ns`.

        The two directions exploit this differently:
        - **Forward** (`ts_ns >= _scrub_cache_ts_ns`): moves the anchor *past* the previously
          proven-empty range into genuinely new, never-scanned territory - a newer row could
          exist there for any module, so `found_mask` starts all-False and every cached value is
          only a fallback default (already seeded into `b`) used if nothing newer turns up. Only
          the segments covering the new `(cached_ts, ts_ns]` window need scanning at all.
        - **Backward** (`ts_ns < _scrub_cache_ts_ns`): as long as a module's `cached_ts <= ts_ns`,
          the new anchor still falls inside that same already-proven-empty range - nothing can
          possibly be found for it there, so it's pre-seeded straight into `found_mask` as found,
          skipping it entirely without touching a single row (this is the "reduced search space"
          that keeps a small backward step cheap). A confirmed "no data" entry stays pre-seeded
          too, for the same reason. Only modules whose cached occurrence is now excluded
          (`cached_ts > ts_ns`) are reset to "not found" and re-resolved via an unbounded scan
          (no lower bound to stop at, since how far back their next-older occurrence sits isn't
          known in advance).

        Two further pre-seeding passes below (before any segment scanning) close the gap the
        cache above can't: the very first call on a fresh tracker, and a module registered after
        the cache was last built. Both are populated by update() (the always-running LIVE
        tracker) rather than by a prior call to this method:
        - `_first_seen_seq[mod] == 0` (persistent, tracker-lifetime) means genuinely zero data
          exists anywhere at or before `_first_seen_coverage_ts` - modules that never log at all
          resolve instantly, permanently, with zero segment access.
        - `_current_snapshot` (update()'s continuously-maintained "latest ever per module") is
          used directly for any module whose latest-ever occurrence is already at or before
          ts_ns - correct because nothing newer can exist for it anywhere. One caveat: during
          *live* ingestion (not REPLAY, where the pool is already fully static) this is only as
          fresh as update()'s last tick (60Hz) - a row that arrived in the last ~16ms and hasn't
          been folded into `_current_snapshot` yet could theoretically be missed. Accepted as
          equivalent to the LIVE view's own existing staleness bound (get_snapshot() has the
          identical property), not a new risk this introduces.
        """
        with self._update_lock:
            m_bundle = self._module_table.bundle()
            count = m_bundle.count

            cache = self._scrub_cache
            if cache is not None and ts_ns == self._scrub_cache_ts_ns:
                return cache.retain()

            snap = self._allocate_snapshot(max(1024, count), count, 0)
            b = snap.bundle()

            if cache is not None:
                cache_b = cache.bundle()
                cache_count = cache_b.count
                nb_copy_snapshot_state(cache_b, b)

                found_mask = np.zeros(count, dtype=np.bool_)

                if ts_ns >= self._scrub_cache_ts_ns:
                    # Forward: every cached module's occurrence might still be superseded by a
                    # *newer* row somewhere in the new (cached_ts, ts_ns] window, so found_mask
                    # must stay False for all of them - the kernel needs to actually look at that
                    # window and overwrite where it finds something newer. If it finds nothing,
                    # the copied-forward cached value (already in `b`) is correctly left as-is.
                    min_ts_ns_exclusive = self._scrub_cache_ts_ns
                else:
                    # Backward: there is no "newer row in between" to look for - a resolved
                    # module's cached occurrence is by construction its latest at or before the
                    # *old* (larger) anchor, so if cached_ts <= ts_ns it's already exactly the
                    # answer for ts_ns too (see docstring fact) and can be pre-seeded as found,
                    # skipping it entirely. Only modules whose cached occurrence now falls after
                    # ts_ns (or genuinely have none, which stays true for any smaller anchor) get
                    # re-resolved by scanning for their next-older occurrence.
                    still_valid = (cache_b.sequence_ids[:cache_count] == 0) | (
                        cache_b.timestamps[:cache_count] <= ts_ns
                    )
                    found_mask[:cache_count] = still_valid

                    invalidated = np.nonzero(~still_valid)[0]
                    if invalidated.size:
                        # Clear the now-inapplicable (too-new) cached value so an unresolved
                        # module reads as "no data" rather than leaking it if nothing older is
                        # found.
                        b.sequence_ids[invalidated] = 0

                    # Invalidated modules may need to look arbitrarily far back for their
                    # next-older occurrence - no lower bound to stop at.
                    min_ts_ns_exclusive = dtypes.TS_UNSPECIFIED

                remaining = int(count - np.count_nonzero(found_mask))
            else:
                b.sequence_ids[:] = 0
                found_mask = np.zeros(count, dtype=np.bool_)
                remaining = count
                min_ts_ns_exclusive = dtypes.TS_UNSPECIFIED

            # Two more pre-seeding passes, independent of whether a scrub cache exists at all -
            # both close the "first call on a fresh tracker" gap the cache-based pre-seeding
            # above can't help with, since they're populated by update() (the always-running
            # LIVE tracker) rather than by a prior build_snapshot_as_of call.
            if remaining > 0:
                # 1. Confirmed-absent modules (persistent first-occurrence table): a module with
                # no recorded first occurrence, where update() has scanned far enough (coverage)
                # to vouch for that, genuinely has zero data anywhere at or before ts_ns - no
                # need to touch a single segment for it, ever. Cheap vectorized check, no kernel
                # call, so it runs before the live-valid kernel pass below.
                if ts_ns <= self._first_seen_coverage_ts:
                    fs_limit = min(count, self._first_seen_seq.shape[0])
                    confirmed_absent = (~found_mask[:fs_limit]) & (self._first_seen_seq[:fs_limit] == 0)
                    newly_confirmed = int(np.count_nonzero(confirmed_absent))
                    if newly_confirmed:
                        found_mask[:fs_limit] |= confirmed_absent
                        remaining -= newly_confirmed

            if remaining > 0:
                # 2. Live-valid modules: _current_snapshot (update()'s continuously-maintained
                # "latest ever per module") already holds the correct answer for any module whose
                # latest-ever occurrence is at or before ts_ns - nothing newer than it exists
                # anywhere, live or otherwise. Resolves the extremely common "anchor at/near the
                # live edge" case for free.
                remaining = nb_copy_live_valid_modules(
                    self._current_snapshot.bundle(), b, count, ts_ns, found_mask, remaining
                )

            if remaining > 0:
                with self._log_pool.get_reversed_snapshot() as segments:
                    for segment in segments:
                        if remaining <= 0:
                            break
                        if segment.size == 0:
                            continue

                        # A segment entirely newer than ts_ns can't contribute anything (every
                        # row would just be skipped by the kernel's own `ts > max_ts_ns` check
                        # anyway) - skip the whole (row-by-row linear scan) kernel call using its
                        # cached start_ts, same as fetch_telemetry_window/scan_history_window -
                        # see plans/fetch-telemetry-window-cold-segment-perf.md.
                        if segment.start_ts > ts_ns:
                            continue

                        # Segments are visited newest-to-oldest here - once one is entirely at or
                        # before min_ts_ns_exclusive, every remaining (older) segment is too, so
                        # it's safe to stop entirely rather than just skipping this one. Only
                        # fires on the forward path (min_ts_ns_exclusive = TS_UNSPECIFIED on the
                        # first-ever call and on any backward jump).
                        if segment.end_ts <= min_ts_ns_exclusive:
                            break

                        all_found, remaining = nb_build_snapshot_as_of(
                            segment.bundle, b, count, ts_ns, min_ts_ns_exclusive, found_mask, remaining
                        )
                        if all_found:
                            break

            if cache is not None:
                cache.release()
            self._scrub_cache = snap.retain()
            self._scrub_cache_ts_ns = ts_ns
            return snap

    def update_replay(self, ts_ns: int) -> None:
        """Background-thread counterpart to update(): recomputes and stores the replay scrub
        cache (build_snapshot_as_of's own _scrub_cache), discarding the returned handle - callers
        read the result cheaply via get_replay_snapshot() instead of consuming this call's return
        value directly. Intended to be driven by a periodic background task (see
        Registry._tick_replay_snapshot) rather than called from the UI thread."""
        self.build_snapshot_as_of(ts_ns).release()

    def get_replay_snapshot(self) -> ModuleSnapshot:
        """Cheap read-side counterpart to get_snapshot(), for the REPLAY-follow case: retains
        whatever update_replay() last computed instead of computing inline on the calling
        (typically UI) thread. Falls back to get_snapshot() (the LIVE "latest ever" state) for
        the brief window before the first update_replay() call has run after entering REPLAY -
        a reasonable placeholder, not a special empty-allocation path."""
        with self._update_lock:
            cache = self._scrub_cache
            if cache is not None:
                return cache.retain()
        return self.get_snapshot()

    def debug_print(self):
        """Helper to print the current active snapshot."""
        with self.get_snapshot() as snap:
            # Pass our table into the snapshot's print logic
            snap.debug_print(self._module_table)

    def update_and_print(self):
        self.update()
        # self.debug_print()

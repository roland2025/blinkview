# Extending scrollback beyond RAM with mmap-backed cold storage

## Status: implemented (2026-07-27), on-by-default (2026-07-28)

**2026-07-28 update:** cold storage is now enabled by default
(`CENTRAL_STORAGE_COLD_STORAGE_ENABLED = True`) with `CENTRAL_STORAGE_COLD_MAX_PIECES = 128`
(128 x 32MB = ~4GB of extra on-disk scrollback per session, `core/limits.py`). The default
`cold_storage_dir` also changed: instead of a fresh OS-temp directory, it's now a `cold/`
subfolder directly under the current session's own log folder (`FileManager.session_dir`) - see
`CentralStorage._resolve_cold_storage_dir`. It's created on first use and deleted wholesale by
`ColdStorageArchiver`'s `atexit` cleanup hook, same mechanism as before, just pointed at a
session-local path instead of the OS temp root. Explicitly setting `cold_storage_dir` (e.g. to a
faster NVMe mount than wherever `logs/` lives) still creates a uniquely-named temp subdirectory
under that path, unchanged from the original design.

All of the design below is implemented and tested (1884 tests passing, no regressions):

- `core/cold_segment.py` - the on-disk format (header + column offset/length table), write/read,
  `_MmapFileRef`/`MmapArrayHandle` (shared single mmap per file, refcounted release).
- `core/numpy_batch_manager.py`'s `PooledLogBatch.from_memmap()` - the alternate constructor
  (took the "shim" option from section 2 below, not a new class).
- `core/cold_storage_archiver.py`'s `ColdStorageArchiver` - the background writer, bounded
  queue-full-drops-immediately backpressure, `atexit`-registered best-effort cleanup.
- `core/numpy_log.py`'s `CircularLogPool` - `cold_segments` deque, `_handle_archived`/
  `_evict_hot_segment`/`_evict_cold_segment`, snapshot chaining, `get_time_bounds()` reading the
  cached `ColdSegmentMeta.earliest_ts`/`latest_ts` instead of touching mmap'd pages,
  `update_cold_max_pieces()`.
- `core/central_storage.py` - `cold_storage_enabled`/`cold_max_pieces`/`cold_storage_dir` config
  properties; always creates a fresh unique subdirectory under the configured/temp dir rather than
  using it directly (a rmtree of a caller-supplied directory would be too destructive otherwise).
- Tests: `tests/test_cold_segment_format.py` (format round-trip + real-kernel equivalence),
  `tests/test_cold_storage_archiver.py` (writes, distinct files, deterministic backpressure-drop),
  `tests/test_numpy_log_cold_tier.py` (real hot->cold rotation/eviction/file-deletion, and a real
  `LogSegmentScanner.scan_tail` fetch through `segment_filter_reversed` against a live memmap'd
  cold segment - not a mock), `tests/test_central_storage.py` additions for config wiring.

One deliberate deviation from the original design: `CentralStorage.stop()` does **not** call
`CircularLogPool.release_all()` (which would tear down the archiver and delete its temp
directory) - `stop()` means "stop the ingestion thread," and an existing test
(`test_ingested_batches_are_appended_to_the_log_pool_and_distributed`) already asserts log data
survives a `.stop()` call, including at real `Registry.stop()` teardown (same code path). Instead,
the temp cold-storage directory is cleaned up via a best-effort `atexit` hook registered in
`ColdStorageArchiver.__init__`; `release_all()` remains available for any caller that does want a
full, explicit teardown (as `core/warmup.py` already does for its own dummy pool).

## Goal

`CircularLogPool` (`core/numpy_log.py`) currently holds `X` pieces ("segments") entirely in RAM
and permanently drops the oldest one the moment a new one is needed beyond that ceiling. This plan
adds a second, disk-backed tier of `Y` additional segments (targeted at local NVMe) so scrollback
can extend far past what fits in RAM, without touching the Numba kernels that already consume
segments, and without slowing down live ingestion.

This is a design document - no code has been written yet. It's scoped to *extending the queryable
in-app scrollback window*, not to durable session logging - `io/logging.py` + each reader's
`logging_processor` config already write a permanent raw/text record of everything ingested to
`logs/<session>/`, and that's an unrelated, already-solved concern.

## Current architecture (what we're extending)

- **`CentralStorage`** (`core/central_storage.py`) owns one `CircularLogPool` and feeds it via
  `batch_append()` on every incoming `PooledLogBatch`. Config: `max_pieces` (default
  `CENTRAL_STORAGE_MAX_PIECES=16`, `core/limits.py`) and `buffer_size_mb` (default
  `CENTRAL_STORAGE_BUFFER_SIZE_MB=32`) - so the default RAM budget is `16 x 32MB = 512MB` of log
  buffer space (plus per-row fixed-width columns on top).
- **`CircularLogPool`** (`core/numpy_log.py`) holds `self.segments: deque[PooledLogBatch]`. Each
  segment is a fixed-capacity, struct-of-arrays chunk (`PooledLogBatch.bundle` is a `LogBundle`
  NamedTuple of real numpy arrays: `timestamps`, `rx_timestamps`, `offsets`, `lengths`, `buffer`,
  `levels`, `modules`, `devices`, `sequences`, `pids`, `tids` - see `core/types/log_batch.py` for
  exact dtypes). Once a segment stops being `active_segent` (replaced by `_rotate_segment()`), it
  is **frozen** - nothing ever writes to it again. `_rotate_segment()` is exactly where data is
  currently destroyed:

  ```python
  if len(self.segments) >= self.max_pieces:
      oldest = self.segments.popleft()
      oldest.release()   # <- returns arrays to NumpyArrayPool, contents gone
  ```

- **`NumpyArrayPool`** (`core/array_pool.py`) hands out power-of-two "slab" numpy arrays and
  recycles them via `PooledArrayHandle.release()`. `PooledLogBatch` (`core/numpy_batch_manager.py`)
  is just a bundle of these handles plus the `LogBundle` view over them.
- **Every reader of segment data** - `LogSegmentScanner.scan_tail`/`scan_history_window`
  (`core/log_fetch.py`), `fetch_telemetry_arrays`/`fetch_telemetry_window`/`get_telemetry_anchor`
  (`core/numpy_log.py`) - only ever touches `log_pool.get_snapshot()` /
  `get_reversed_snapshot()` (both return a `SegmentSnapshot` wrapping `retain()`'d
  `PooledLogBatch`-shaped objects) and then calls Numba kernels (`segment_filter`,
  `nb_extract_telemetry_segment_*`, `nb_segment_format`, ...) directly against `segment.bundle`.
  **None of this code cares how a segment's arrays were allocated** - it only needs real numpy
  arrays with the right dtype/shape and a `PooledLogBatch`-shaped wrapper
  (`.bundle`, `.size`, `.capacity`, `.last_sequence_id`, `.first_sequence_id`, `.retain()`,
  `.release()`, `.metadata`).

## The key enabling fact

`np.memmap` **is** an `np.ndarray` subclass - it satisfies the buffer protocol and dtype/shape
contract identically to a normal array. Numba's `@app_njit` kernels accept it transparently; no
kernel in `ops/segments.py`, `ops/telemetry.py`, `ops/formatting.py` etc. needs to change at all.
This means a "cold" segment can be represented as the *exact same* `LogBundle`/`PooledLogBatch`
shape the rest of the codebase already knows how to consume - the only new code is (a) how the
arrays get backed (mmap'd file instead of pool slab) and (b) the eviction/promotion bookkeeping in
`CircularLogPool`. `LogSegmentScanner`, the telemetry fetchers, and every Qt widget built on top of
them need **zero changes**.

## Proposed design

### 1. On-disk segment format

One file per archived segment (e.g. `segment_<counter>.blkseg` under a session-scoped cold-store
directory). Because a frozen segment never grows again, write it at its *actual* used size
(`size` rows, `msg_cursor` bytes) rather than its padded pool-slab capacity - smaller files, and
`capacity == size` on reload (kernels only ever read up to `bundle.size[0]`, so this is safe).

Fixed binary layout, page-aligned (4096-byte) header so each column can start on a clean mmap
boundary. The header carries an explicit **(offset, length_bytes) table, one entry per column**,
rather than the reader re-deriving offsets by adding up fixed column sizes in a hardcoded order -
that keeps `MmapLogSegment.open()` a dumb "read the table, mmap each slice at its recorded offset"
loop instead of encoding the on-disk layout twice (once implicitly via write order, once via
whatever the reader assumes), and leaves room to add/reorder columns later (e.g. `ext_u32_1`) by
extending the table without touching the fixed part of the header:

```text
[fixed header: magic, version, row_count, buffer_len, first_seq, last_seq, earliest_ts, latest_ts]
[column table, 11 x (offset: uint64, length_bytes: uint64):
    timestamps, rx_timestamps, offsets, lengths, levels, modules, devices,
    sequences, pids, tids, buffer]
[pad to 4096]
[column data blocks, each at its table-recorded offset/length - order on disk need not match
 table order, only each entry's own (offset, length_bytes) needs to be correct]
```

Schema (which columns exist, and their dtypes) is still hardcoded to `CircularLogPool`'s known
segment shape (`has_levels/modules/devices/sequences/pids/tids=True`, no `ext_u32/u64` -
`_rotate_segment()` never sets those) rather than a fully generic serializer - there's exactly one
producer of these files, so inferring dtypes/column identity from data alone buys nothing. The
table only removes the *offset arithmetic* from being implicit; it doesn't need to describe dtype
or column count generically. `first_seq`/`last_seq`/`earliest_ts`/`latest_ts` are cached in the
fixed header (not the table) so `get_time_bounds()` and the seq-bound early-exit checks in
`scan_tail`/`scan_history_window` don't have to fault in any pages just to decide whether a cold
segment is in range.

### 2. `MmapLogSegment` - the cold-tier `PooledLogBatch` stand-in

A small class satisfying the same duck-typed contract (`.bundle`, `.size`, `.capacity`,
`.last_sequence_id`, `.first_sequence_id`, `.metadata`, `.retain()`/`.release()`, context manager).
`.bundle` is a `LogBundle` built from `np.memmap(path, dtype=X, mode='r', offset=O, shape=(N,))`
slices per column, with `O` (and derived `N = length_bytes / dtype.itemsize`) read straight from
the header's column table rather than recomputed - read-only, since a cold segment is by
definition already frozen.
`retain()`/`release()` just do refcounting around closing the mmap (no pool hand-back). This can
either be a genuinely new class, or `PooledLogBatch` gains an alternate construction path
(`PooledLogBatch.from_memmap(...)`) using a `MmapArrayHandle` shim with the same
`.array`/`.release()` interface as `PooledArrayHandle` - reusing `PooledLogBatch.release()`'s
existing per-handle teardown loop as-is. Leaning towards the shim approach: less surface area, and
every other `PooledLogBatch` method (`__iter__`, `__getitem__`, `start_ts`, ...) keeps working
unmodified.

### 3. Archiving: background writer, never on the ingestion path

`_rotate_segment()` runs under `CircularLogPool._lock`, on the `CentralStorage` ingestion thread.
Disk writes (tens of MB) must never happen there - that would stall live ingestion.

Add a bounded-queue background `ColdStorageArchiver` thread:

- `_rotate_segment()`'s eviction becomes: pop the oldest RAM segment, `retain()` it, hand it to the
  archiver's queue (non-blocking `put_nowait`), return immediately.
- If the queue is full (disk can't keep up with ingestion rate), **skip archiving that segment and
  release it immediately instead** - same behavior as today, just rate-limited-logged. Ingestion
  speed must never depend on disk throughput; losing scrollback depth under sustained
  faster-than-disk throughput is an acceptable, explicit degradation.
- The archiver thread: pops a retained RAM segment, writes it to a new `.blkseg` file (plain
  sequential `file.write()`, not mmap - simpler, and NVMe sequential write is not the bottleneck),
  opens it back as an `MmapLogSegment`, appends it to `CircularLogPool.cold_segments` (another
  `deque`, capped at `cold_max_pieces`, evicting-and-deleting the oldest file when exceeded, same
  shape as the existing `max_pieces` eviction), then releases the RAM segment.

### 4. `CircularLogPool` changes

- New `cold_segments: deque[MmapLogSegment]`, guarded by the same `self._lock` (mutated rarely -
  only on rotation - so no contention concern from sharing the hot-path lock).
- `get_snapshot()` / `get_reversed_snapshot()` chain hot + cold in the right order: `get_snapshot()`
  (oldest-to-newest) yields `cold_segments` then `segments`; `get_reversed_snapshot()`
  (newest-to-oldest) yields `segments` then `reversed(cold_segments)`.
- `get_time_bounds()` extends its `earliest`/`latest` lookup to the oldest cold segment when
  present (cheap - header-only, no page fault).
- `get_counts()` / any "how much history do I have" UI surface should report hot vs. cold extent
  separately, since read latency differs.

### 5. Config

New `CentralStorage` properties: `cold_storage_enabled` (bool, default off until proven out),
`cold_max_pieces` (the `Y` from the prompt), `cold_storage_dir` (path, so the user can point it at
an actual NVMe mount rather than assuming the OS temp dir lives on one). New defaults in
`core/limits.py` alongside the existing `CENTRAL_STORAGE_*` constants.

### 6. Lifecycle

- v1 scope: **session-only**. Cold-store directory lives under a per-run temp/session location and
  is deleted on clean shutdown (and swept on next startup in case of a crash) - this is scrollback
  depth, not an archive. Cross-session resume is a plausible v2, not v1.
- Graceful degradation: if the configured `cold_storage_dir` isn't writable, or disk fills up mid-
  session, log once and fall back to today's RAM-only behavior (drop-on-evict) rather than raising
  into the ingestion path.

## Performance characterization (why NVMe specifically matters here)

- **Write side**: NVMe sequential write throughput (multiple GB/s) comfortably absorbs archiving a
  32MB segment well within the time it takes to fill the next one at any realistic log rate - this
  is why the design doesn't need to be clever about write batching/compaction for v1.
- **Read side**: `mode='r'` mmap means the OS page cache does the work for us - a cold segment that
  was just archived (or is being re-scanned repeatedly, e.g. scrubbing back and forth in playback)
  stays effectively RAM-speed until memory pressure evicts its pages; a segment nobody has touched
  in a while pays real (but NVMe-cheap, ~10s of microseconds) random-read latency on first touch
  per page. This graceful hot/cold degradation is exactly the intended behavior and needs no
  explicit caching layer on top - the OS already provides one.

## Testing strategy

Per this project's established habit of verifying pipeline changes through a real chained
script/test rather than only per-stage unit tests (a new `LogBundle`-adjacent storage tier is
exactly the kind of change that can silently drop data at a seam), this needs:

1. **Format round-trip**: write a `PooledLogBatch` to a `.blkseg` file, reopen as
   `MmapLogSegment`, assert every column matches byte-for-byte and `segment_filter`/
   `nb_segment_format` produce identical output against the mmap'd version vs. the original.
2. **`CircularLogPool` integration**: push enough real batches through `batch_append()` to force
   several rotations past `max_pieces`, verify evicted segments land in `cold_segments`, and that
   `LogSegmentScanner.scan_history_window` (real kernel path, not mocked) can fetch rows that only
   exist in the cold tier - proving the "kernels don't care" claim above rather than assuming it.
3. **Backpressure**: saturate the archiver's queue and confirm ingestion throughput is unaffected
   (segments silently drop back to today's release-only behavior instead of blocking).
4. **Fresh-process check** is not needed here (no decorator/registry involved), but a real-disk
   (`tmp_path`) round trip is, including on Windows where `np.memmap` goes through `mmap.mmap`'s
   Windows path - don't assume POSIX-only behavior works unmodified.

## Explicitly out of scope for v1

- Cross-session resume / treating cold storage as a permanent archive (that's `io/logging.py`'s
  job already).
- Compaction or merging of many small cold segment files.
- Any change to the telemetry/log Numba kernels themselves - the whole point of this design is that
  none are needed.
- Remote/network-backed storage tiers.

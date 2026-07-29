# CircularLogPool fetch functions scaling with cold segment count

## Status: fixed (2026-07-28)

## Context

User suspected one of the log-pool "fetch" functions used by the plotter/log window/watch was
poorly optimized for a large number of cold (mmap-backed, disk-archived) segments - specifically
around 128 segments. Asked for unit tests to check fetch speed at that scale before knowing which
function was at fault. After the first bug was found and fixed, asked to check every other
log_pool access location for the same shape of issue - found two more real instances (below).

## Investigation

Traced every plausible fetch path (`plans/mmap-coldstore.md` has the cold-tier design background):

- **`fetch_telemetry_window`** (`src/blinkview/core/numpy_log.py`) - used by `TelemetryPlotter`'s
  REPLAY-follow/scrub path (`plotter.py`'s `apply_updates`, ticked up to 10 Hz per visible
  module). **Confirmed the bug.** Its before/after segment loops called the
  `nb_extract_telemetry_segment_window_backward/forward` kernel unconditionally on every segment
  in the snapshot, never consulting the cached `ColdSegmentMeta.earliest_ts/latest_ts` header
  field that exists specifically so a caller can answer "is this segment in range" in O(1) without
  touching (page-faulting in) any of the segment's mmap'd column data. Cost scaled with total
  segment count, not with rows actually relevant to the query.
- `fetch_telemetry_arrays` (live-forward path) - fine, breaks early via a segment's O(1)
  `last_sequence_id` check.
- `CircularLogPool.get_time_bounds()` - fine, true O(1), no iteration at all.
- `LogSegmentScanner.scan_tail` (backs the log viewer/table's LIVE fetch) - fine, breaks early via
  a segment's cheap `last_sequence_id` check.
- `LatestModuleValueTracker.update()` (the live "latest value per module" path
  TelemetryWatch/TelemetryTableModel/console read from) - fine, same watermark early-break.
- `get_telemetry_anchor()` - fine, breaks via the same cheap `first_sequence_id`/
  `last_sequence_id` checks.

A follow-up "check every other log_pool access location" pass found two more real instances of
the exact same bug shape, both on the HISTORY-direction (playback-scrub/scroll-back) side rather
than the live-forward side:

- **`LogSegmentScanner.scan_history_window`** (`src/blinkview/core/log_fetch.py`) - backs the log
  viewer/table's HISTORY window (scrolling back through old rows, or a REPLAY playback-clock
  anchor). Its before/after loops called `segment_filter_reversed`/`segment_filter` (each doing
  1-2 binary searches) unconditionally on every segment until the row quota was filled, never
  skipping a segment provably outside the anchor's ts/seq range first. **Confirmed the second
  bug**: ~14.5x slower going from 4 to 128 cold segments.
- **`LatestModuleValueTracker.build_snapshot_as_of`** (`src/blinkview/core/module_snapshot.py`) -
  the actual "watch" path: `TelemetryWatch`/`TelemetryTableModel`'s REPLAY-follow tick rebuilds
  "latest value per module as of a past ts" from scratch every call (per its own docstring:
  "expected to call this once per follow tick"). Its segment loop never skipped a segment whose
  data entirely postdates the query ts before calling `nb_build_snapshot_as_of` - which does a
  plain **linear scan of every row** in the segment (no binary search at all, unlike the other two
  bugs), making this the most expensive per-irrelevant-segment cost of the three, and the one
  actually reachable from "watch" in the user's original guess (the widget itself doesn't touch
  `log_pool` directly - it goes through this tracker). **Confirmed the third bug**: ~21.5x slower
  going from 4 to 128 cold segments.

The remaining call sites that touch `log_pool.get_snapshot()`/`get_reversed_snapshot()`
(`core/log_fetch.py`'s and `core/time_sync_engine.py`'s `@register_warmup` functions,
`log_table_viewer.py`'s warmup) are one-shot warmup code (`break` after the first segment) - not
real fetch paths, not candidates.

## Measured

Built via `tests/test_log_pool_fetch_perf.py`: real one-row `.blkseg` cold segment files (via
`write_cold_segment_file`/`PooledLogBatch.from_memmap`, bypassing the archiver thread entirely for
speed/determinism), appended directly to `log_pool.cold_segments`, spaced 10s apart so only one
segment is ever relevant to a narrow query window regardless of total segment count.

- Before fix: `fetch_telemetry_window` median call time went from **0.068ms at 4 segments to
  1.42ms at 128 segments (~21x)** for the exact same single relevant row.
- After fix: **0.033ms → 0.178ms (~5.4x** scaling factor, but only **0.178ms absolute** - an 8x
  wall-clock speedup at 128 segments).

## Fix

`src/blinkview/core/numpy_log.py`'s `fetch_telemetry_window`: before calling the kernel on a
segment, check `isinstance(segment.metadata, ColdSegmentMeta)` and whether its cached
`earliest_ts`/`latest_ts` overlaps the query window. Skip the kernel call (a `continue`) if it
doesn't overlap **and** the segment isn't still needed for the `plus_one` edge-row capture
(`before_edge_remaining`/`after_edge_remaining` still > 0) - preserves the exact existing
edge-capture semantics (a segment entirely outside the window can still be the source of the
single nearest-outside-the-window sample) while skipping every other irrelevant segment's
mmap-touching binary searches. Hot segments (metadata is a plain int, not `ColdSegmentMeta`) are
unaffected - always call the kernel as before, since hot data is already RAM-resident and bounded
by `max_pieces` (small).

The remaining ~5x isn't zero because the Python-level loop still iterates every segment (just with
a cheap `isinstance` + two int comparisons instead of a full njit kernel call) - going fully flat
would need skipping the loop iteration itself (e.g. a separate index structure), judged not worth
the added complexity given the 8x absolute speedup already achieved and the accepted <8x threshold.

Regression test: `tests/test_log_pool_fetch_perf.py` - asserts the 4-segment-to-128-segment
scaling ratio stays under 8x, plus two flat-scaling control tests for `fetch_telemetry_arrays` and
`get_time_bounds`.

## Fix (scan_history_window and build_snapshot_as_of)

Same shape of fix, applied to the two newly-found bugs:

- `LogSegmentScanner.scan_history_window`'s before loop skips a segment (via `continue`) before
  calling `segment_filter_reversed` if `has_seq_anchor and segment.first_sequence_id >
  anchor_seq - 1` (seq case, using the already-cheap `first_sequence_id` property - no new caching
  needed) or `has_ts_anchor and isinstance(segment.metadata, ColdSegmentMeta) and
  segment.metadata.earliest_ts > anchor_ts - 1` (ts case, cold segments only). The after loop
  mirrors this with `last_sequence_id`/`latest_ts`.
- `LatestModuleValueTracker.build_snapshot_as_of` skips a segment before calling
  `nb_build_snapshot_as_of` if `isinstance(segment.metadata, ColdSegmentMeta) and
  segment.metadata.earliest_ts > ts_ns` (the segment entirely postdates the query - every one of
  its rows would just be skipped by the kernel's own per-row `ts > max_ts_ns` check anyway).

Both fixes only special-case cold segments (`isinstance(..., ColdSegmentMeta)`) for the ts-bound
checks, matching `fetch_telemetry_window`'s fix - hot segments are few (bounded by `max_pieces`)
and already RAM-resident, so they were never the actual scaling problem.

Measured: `scan_history_window` went from ~14.5x to ~4.2x (4→128 segments); `build_snapshot_as_of`
went from ~21.5x to ~4.0x. Regression tests: `tests/test_log_pool_fetch_perf.py`'s
`TestScanHistoryWindowScalesWithSegmentCount`/`TestBuildSnapshotAsOfScalesWithSegmentCount`.

Incidental fix: `tests/fakes/log_pool.py::FakeSegment` was missing `first_sequence_id`/`metadata`
attributes that every real `PooledLogBatch` has - added both (existing `test_log_fetch.py` tests
construct `FakeSegment` directly and hit an `AttributeError` once `scan_history_window` started
reading `segment.first_sequence_id`).

## Follow-up: the cheap per-segment properties themselves weren't actually cheap for cold segments

All three fixes above lean on `PooledLogBatch.first_sequence_id`/`last_sequence_id`/`start_ts`
being O(1) "just read a cached header field" checks - but for a cold (memmap-backed) segment they
were still indexing into `b.sequences[0]`/`b.sequences[sz-1]`/`b.timestamps[0]`, i.e. touching the
mmap'd array (a potential page-fault) on every single poll, even though `ColdSegmentMeta` already
caches `earliest_ts`/`latest_ts` and the on-disk `ColdSegmentHeader` already computes
`first_seq`/`last_seq` at archive time.

Fixed by:

- Adding `first_seq`/`last_seq` fields to `ColdSegmentMeta` (`core/cold_segment.py`), threaded
  through from `ColdSegmentHeader` at the two real construction sites
  (`core/cold_storage_archiver.py`, plus the perf test's own segment builder).
- `PooledLogBatch.start_ts`/`first_sequence_id`/`last_sequence_id`
  (`core/numpy_batch_manager.py`) now check `isinstance(self.metadata, ColdSegmentMeta)` first and
  return the cached field directly, only falling back to indexing the bundle arrays for hot
  segments (which have no such cache and are RAM-resident anyway, so it's cheap either way).

Regression test: `tests/test_cold_segment_format.py::
TestColdSegmentCheapPropertiesReadFromCacheNotArrays` - deliberately constructs a `ColdSegmentMeta`
whose cached values *disagree* with the real underlying row data, so the test only passes if the
properties genuinely trust the cache rather than reading the array (confirmed it fails against the
pre-fix code via `git stash`).

Measured (isolated microbenchmark, 5000-row cold segment so `sequences[sz-1]`/`timestamps[0]` sit
in different pages, not just index 0 like the tiny 1-row segments used in the skip-logic fixes'
perf tests; OS page cache already warm in both cases, so this isolates pure CPU overhead, not
page-fault avoidance): `start_ts` 244ns → 80ns (**3.06x**), `first_sequence_id` 248ns → 82ns
(**3.03x**), `last_sequence_id` 296ns → 84ns (**3.52x**) - from skipping a numpy scalar index into
a memmap'd view in favor of a plain tuple-field read. On a genuinely cold page (file not recently
touched, evicted from OS cache) the old array-indexing path would be considerably slower still due
to the page fault itself, which a same-process microbenchmark can't safely simulate. Since all
three properties are polled on every segment visited by every scan (`scan_tail`,
`scan_history_window`, `build_snapshot_as_of`, `get_telemetry_anchor`), this compounds across the
hundreds of segments a 128-cold-segment scan touches per call, on top of (not a replacement for)
the skip-logic fixes above.

## Follow-up: extending the same cache to HOT segments (plain-int, updated on write)

Asked what speedup extending this same idea to HOT (RAM/pool-backed) segments would give - i.e.
maintaining `first_seq`/`last_seq`/`first_ts` as plain Python int attributes updated incrementally
at insert time, instead of reading `b.sequences[0]`/`b.sequences[sz-1]`/`b.timestamps[0]` on every
call. A hypothetical microbenchmark (bare slotted-attribute read vs. hot-segment array indexing)
suggested ~9-10x; implemented for real, the actual number is lower (**~3.4-4.2x** - see "Measured"
below) because the real property still pays for the unavoidable `isinstance(self.metadata,
ColdSegmentMeta)` branch the hypothetical benchmark didn't include. Insert-side cost of maintaining
the cache: measured at **+4.2ns/insert (0.09% slower)** - effectively free.

### Implementation

- `PooledLogBatch` (`core/numpy_batch_manager.py`) gained three new `__slots__`:
  `_cached_first_seq`, `_cached_first_ts`, `_cached_last_seq` - initialized empty
  (`None`/`None`/`SEQ_NONE`) in `__init__`, `from_memmap` (unused there, but the slots must hold
  something), and reset by `clear()`.
- `insert()`/`insert_any()`/`insert_view()` all call a new `_note_inserted_row(ts_ns, seq)` helper
  after a successful push - cheap, since the exact values just inserted are already in hand, no
  array read-back needed.
- `start_ts`/`first_sequence_id`/`last_sequence_id` now read from this cache for non-cold segments
  (cold segments still take the `ColdSegmentMeta` branch from the earlier fix, unchanged).

### The hard part: finding every write path that bypasses insert()/insert_any()/insert_view()

The properties are only correct if the cache is updated on *every* path that adds a real row to a
segment's arrays - two more had to be found and hooked, or the cache would silently go stale
(reporting an "empty" segment's sentinel values for segments that actually have data):

1. **`CircularLogPool.batch_append`** (`core/numpy_log.py`) - the actual high-throughput path all
   real ingested data takes. It calls `nb_copy_batch_to_segment` directly on `self.active_segment.
   bundle`, bypassing `insert()` entirely. Fixed by adding `PooledLogBatch.note_appended_rows
   (new_row_count)`, called right after each successful `nb_copy_batch_to_segment` inside
   `batch_append`'s copy loop - this one *does* read back the just-written tail of the arrays
   (`b.sequences[last_idx]` etc.), but only once per `batch_append` call (already amortized over
   however many rows it copied), not once per later read - and those exact array locations were
   just written, so they're still hot in cache regardless.

2. **`CanLogBatch.insert_can`** (`io/can_bus.py`) - a `PooledLogBatch` subclass for raw CAN
   ingestion that calls a *third*, CAN-specific kernel (`nb_can_push`) directly, also bypassing
   `insert()`/`insert_any()`/`insert_view()`. **This one caused a real, confirmed regression**:
   without a fix, `batch.start_ts` (read by `can_bus.py`'s time-based flush check,
   `if batch.size > 0 and (now - batch.start_ts >= delay_ns):`) stayed stuck at the "empty"
   sentinel (max int64) forever after real inserts, so `now - batch.start_ts` was always a huge
   negative number and the time-based flush never fired - a batch only ever got distributed to
   subscribers once its buffer physically filled up. `tests/test_can_bus_reader.py`'s three real
   virtual-CAN-bus loopback tests (which send only a handful of small messages, never enough to
   fill a buffer) went from passing to hanging/timing out with zero messages delivered the moment
   this change landed - caught immediately by running the full suite, not by any targeted
   correctness check. Fixed by adding the same `_note_inserted_row(...)` call inside
   `insert_can()`, computing `ts_ns`/`seq` identically to how `nb_can_push` derives them internally
   (`seq` is always 0 for raw CAN ingress) so no extra array read is needed there either.

Both `PooledLogBatch` subclasses/direct-kernel-write call sites were found by grepping for every
`class ...(PooledLogBatch)` and every direct `nb_bundle_push`/`nb_bundle_push_len`/
`nb_copy_batch_to_segment`/`.size[0] =`-style write in `src/`, then checking whether the same
object's `.start_ts`/`.first_sequence_id`/`.last_sequence_id` is ever read afterward. Confirmed
(via an Explore-agent audit) that all eight other `io/*.py` readers using `.start_ts` for
send-pacing go through the three patched methods only - `can_bus.py` was the sole other bypass.

### Regression tests

- `tests/test_can_bus_reader.py::TestCanLogBatchStartTsTracksRealInserts` - direct unit test of
  `CanLogBatch.insert_can()` + `.start_ts`, isolated from the (slower, more flake-prone) real
  virtual-bus loopback tests. Confirmed it fails against the pre-fix `can_bus.py` via `git stash`.
- The three existing `TestRealLoopbackIngestion` tests in the same file now also serve as an
  end-to-end regression guard for this exact bug (they went from passing → hanging → passing
  again across the break/fix cycle).

### Measured (hot-segment cache)

Apples-to-apples microbenchmark (5000-row hot segment, reconstructing the exact pre-fix property
bodies including the `isinstance(metadata, ColdSegmentMeta)` branch, vs. calling the real
post-fix properties): `start_ts` 301ns → 88ns (**3.42x**), `first_sequence_id` 298ns → 86ns
(**3.47x**), `last_sequence_id` 341ns → 82ns (**4.16x**) - close to the cold-segment numbers above,
since most of the win in both cases is avoiding numpy scalar-index/boxing overhead, not
mmap-specific page-fault avoidance.

## Cleanup: unifying hot/cold into one cache, removing every `isinstance(metadata, ColdSegmentMeta)` check

After the hot-segment extension landed, every read site (the three properties in
`numpy_batch_manager.py`, plus the three skip-logic fixes in `numpy_log.py`/`log_fetch.py`/
`module_snapshot.py`) had its own `isinstance(segment.metadata, ColdSegmentMeta)` branch to decide
whether to read the cold header or the hot plain-int cache - six near-identical branches doing the
same "cold vs hot" dispatch. Asked to get rid of this.

**Fix:** instead of branching per-read, populate the *same* plain-int cache fields for cold
segments too, once, up front, in `from_memmap()` - straight from the `ColdSegmentHeader` already
being read there (`header.first_seq`/`last_seq`/`earliest_ts`/`latest_ts`), rather than deferring
to a live `isinstance` check on `self.metadata` every time a property is read. `self.metadata`
itself is untouched (still a real `ColdSegmentMeta`, still used by `_evict_cold_segment`'s
`.path` and nowhere else for reads anymore).

- `PooledLogBatch` (`numpy_batch_manager.py`): added a fourth cache field, `_cached_last_ts`
  (mirroring `_cached_first_ts`), and a new `end_ts` property (mirroring `start_ts`) so every
  "is this segment relevant" check downstream can use `start_ts`/`end_ts` uniformly instead of
  reaching into `.metadata` at all. `start_ts`/`end_ts`/`first_sequence_id`/`last_sequence_id` are
  now unconditional one-line cache reads - no branching, no `ColdSegmentMeta` import in this file
  at all anymore.
- `fetch_telemetry_window` (`numpy_log.py`), `scan_history_window` (`log_fetch.py`), and
  `build_snapshot_as_of` (`module_snapshot.py`): the skip-logic checks now read
  `segment.start_ts`/`segment.end_ts` directly instead of branching on `isinstance(segment.
  metadata, ColdSegmentMeta)` first - as a bonus, the ts-bound skip now applies to **hot**
  segments too (previously only cold ones got it), for free, since the cache makes the check
  equally cheap either way. `ColdSegmentMeta` import removed from all three files.
- `CircularLogPool.get_time_bounds()`: also simplified to read `.start_ts`/`.end_ts` off the
  boundary segments uniformly instead of branching between `.metadata.earliest_ts` (cold) and
  `.bundle.timestamps[0]` (hot). Had to preserve one existing edge case by hand: the original code
  explicitly returned `(0, 0)` when the only segment was hot and still completely empty, whereas
  the cached properties return sentinel values (`±maxint64`) for an empty segment - re-added the
  `segment.size` guard so a fresh, empty pool still reports `(0, 0)` instead of the sentinels
  (already covered by `tests/test_numpy_log.py::test_get_time_bounds_empty_pool_is_zero`).
- `tests/fakes/log_pool.py::FakeSegment` gained `start_ts`/`end_ts` attributes (mirroring the real
  `PooledLogBatch` properties) since `scan_history_window`'s ts-anchor tests exercise them via this
  fake.
- `tests/test_cold_segment_format.py`'s cache regression test had to be redesigned: it used to pass
  a `ColdSegmentMeta` with values deliberately disagreeing with the real row data and check the
  mismatched values won through - that mechanism no longer applies now that the cache is populated
  from the on-disk header, not from whatever `metadata` object happens to be passed in. Replaced
  with a test that corrupts the underlying mmap'd arrays *after* construction and confirms the
  properties keep returning what was cached at construction time regardless.

All perf numbers held after the refactor (4-128 cold segments): `fetch_telemetry_window` 5.3x,
`fetch_telemetry_arrays` 3.4x, `get_time_bounds` 0.9x, `scan_history_window` 3.9x,
`build_snapshot_as_of` 3.9x - all comfortably under the 8x regression threshold.

## Real-world speedup and remaining bottleneck

Measured the actual before/after for `fetch_telemetry_window` at the real default of 128 cold
segments by stashing this session's changes and re-running the identical benchmark against the
original (pre-session) code, then restoring:

- Query anchored at the **newest** segment: 1.407ms (original) → 0.185ms (current) - **~7.6x**.
- Query anchored at the **oldest** segment: 1.418ms (original) → 0.201ms (current) - **~7.1x**.

Symmetric across the pool, as expected - the original code had no skip logic at all, so it paid
the same O(128-segments) cost regardless of query position; the fix's cached `start_ts`/`end_ts`
skip works the same in either scan direction.

Profiled the *fixed* code with `cProfile` (1000 calls, n=128) plus a scaling sweep (n=1..256) to
see what's left. Result: **~47% of the remaining time is `SegmentSnapshot.retain()`/`.release()`**
(`numpy_log.py`'s `SegmentSnapshot.__init__`/`__exit__`) - one Python `threading.Lock` acquisition
per segment, per snapshot (`get_reversed_snapshot()`/`get_snapshot()` each retain *every* segment
to keep it alive for the scan, released again on `__exit__`), regardless of whether the ts-bound
skip later decides that segment is irrelevant. This is a cost that scales with segment *count*,
not data volume - it's what's left of the "isn't quite linear" shape: a genuine fixed cost
(~25-26us: JIT dispatch, `array_pool.get()` output-buffer allocation, context-manager setup) plus
a shrinking-per-segment linear cost (~1.3-1.4us/segment at scale, dominated by the retain/release
lock pair) that's cheap per unit but still paid for every segment in the snapshot, not just the
relevant ones.

### Mitigation chosen: fewer, bigger segments (config default change, not further code changes)

Rather than re-engineering `PooledLogBatch`'s ref-counting to avoid the per-segment lock (a
deeper, riskier change), the user opted for the simpler lever: since total retained cold-storage
volume is just `cold_max_pieces * buffer_size_mb` (confirmed in `central_storage.py`'s own config
description), reshaping the same ~4GB budget into 4x fewer, 4x bigger pieces cuts the
count-scaling overhead by roughly the same factor for free.

Changed in `src/blinkview/core/limits.py`:

- `CENTRAL_STORAGE_BUFFER_SIZE_MB`: `32` → `128`.
- `CENTRAL_STORAGE_COLD_MAX_PIECES`: `128` → `32`.
- `CENTRAL_STORAGE_MAX_PIECES` (hot tier) left at `2` - already at a practical floor; hot-tier RAM
  (`max_pieces * buffer_size_mb`) grows from 64MB to 256MB as an accepted side effect.

No code changes needed elsewhere: `CentralStorage.apply_config()` already derives
`final_buffer_bytes`/`cold_max_pieces` from these two config defaults, and `CircularLogPool.
_apply_real_world_heuristics()` already re-derives `segment_capacity` (rows) from
`final_buffer_bytes` (bytes) at runtime from the observed average message size.

**Open item, not yet verified**: `_apply_real_world_heuristics()` clamps `segment_capacity` to
`[1000, 500_000]` rows, independent of `final_buffer_bytes`. If real-world average message size is
small enough that `128MB / avg_bytes_per_msg` exceeds 500,000, that clamp becomes the binding
constraint instead of the byte budget, and the intended 4x segment-size increase wouldn't fully
materialize for row-count purposes (byte-volume math still holds regardless). Should be checked
empirically in a real running session (`registry.central.log_pool.segment_capacity`) rather than
the clamp being pre-emptively raised.

Full test suite re-run after the default change: 2123 passed, no regressions (no existing test
hardcodes these two default values as literals).

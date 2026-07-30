# Lazy unpacking of compressed cold-storage segments

**Status**: not pursued. After this research, the simpler fix (decompress straight into an owned
in-memory buffer instead of round-tripping through disk - see `plans/cold-storage-compression.md`)
was implemented instead, eagerly at mount time same as before. That already removes the
write-then-reread I/O this doc's "Problem" section was really about; the remaining win laziness
would add (skipping segments scrubback never visits) was judged not worth this doc's added
complexity (a new `PooledLogBatch` lazy mode, a materialization lock, three changed cached-property
implementations) for a first pass. Kept here in case that tradeoff is revisited later - the header-
only-read verification below still holds and would still be the starting point.

## Problem

`unpack_cold_archive_dir()` (`core/cold_archive.py`, see `plans/cold-storage-compression.md`)
currently decompresses **every** `cold-archive/*.blkseg.zst` file the moment a persisted session
is replayed, before `CircularLogPool` mounts anything. For a long session with many archived
segments, this eagerly pays full decompression cost for segments the user may never actually
scrub into - the exact same "pay for it whether or not it's used" problem that motivated
compressing the files in the first place, just moved from disk space to CPU/latency at
replay-open time.

## Verified: header reads are cheap regardless of file size

Tested `zstandard`'s `ZstdDecompressor().stream_reader(f).read(4096)` against 200MB compressed
payloads (both a highly-repetitive one and a high-entropy/incompressible one, so the result isn't
an artifact of trivial compression): reading just the first 4096 bytes of decompressed output
took ~0.0002s vs. ~0.15s for a full decompress - roughly **800x** faster, confirming the streaming
reader genuinely does incremental block-by-block decoding rather than buffering/decompressing the
whole frame internally. This is the load-bearing assumption for everything below: a segment's
fixed header + column table (`HEADER_SIZE = 4096` in `core/cold_segment.py`) can be read from a
`.zst` archive almost for free, independent of how large the full segment is.

## Design

### Mounting: build a lazy placeholder instead of eagerly unpacking

`CircularLogPool._mount_existing_cold_segments` currently only looks at raw `cold/*.blkseg`
files. Change the mount step (called from `_resolve_cold_storage_dir`/`CircularLogPool.__init__`)
to also enumerate `cold-archive/*.blkseg.zst` files that don't already have a raw counterpart in
`cold/`, and for each one:

1. Partially decompress just the fixed header + column table (4096 bytes) via `stream_reader`,
   the same struct-unpack `read_cold_segment_header` already does, just fed compressed-then-
   streamed bytes instead of a plain file.
2. Build the segment's `ColdSegmentMeta` (path, earliest_ts, latest_ts, first_seq, last_seq) from
   that header, exactly as today.
3. Construct a `PooledLogBatch` in a new **lazy** mode: metadata and header-derived counts are
   set immediately (see below); the actual mmap-backed column arrays are not opened, and the
   `.zst` file is not fully decompressed, until something actually needs row data.

This replaces `unpack_cold_archive_dir()`'s current eager pass entirely - unpacking becomes a
mounting-time detail rather than a separate up-front step.

### `PooledLogBatch`: making `.bundle` lazy without breaking existing call sites

`.bundle` is currently a plain `__slots__` attribute, populated eagerly by `_allocate` (hot
segments) or `from_memmap` (cold/mmap segments). Dozens of call sites across the fetch/scan/kernel
code do `seg.bundle.timestamps[...]` etc. directly, assuming it's already populated - changing all
of them isn't practical, so the plan is to keep `.bundle` fully "just works" from every existing
caller's point of view:

- Rename the slot to `_bundle`; add a `bundle` `@property` that, for a segment still in lazy mode
  (a new `_lazy_archive_path` slot is set and `_bundle` is still `None`), synchronously fully
  decompresses the archive (`decompress_cold_segment_file`, writing into `cold/` exactly like
  today's eager path did), mmaps it (reusing `open_cold_segment_arrays`/the same handle-population
  code `from_memmap` already has), populates `_bundle`, clears `_lazy_archive_path`, and returns
  it. Every existing `seg.bundle.foo` call site keeps working unmodified - it just may pay a
  one-time decompression cost the first time it runs against a given segment, on whatever thread
  happens to touch it first.
- A per-instance lock around this check-then-materialize sequence (a new dedicated lock, not the
  existing refcount `_lock`, to avoid entangling materialization with `retain()`/`release()`'s own
  locking) - two threads (e.g. a GUI fetch and a background scan tick) touching the same
  never-before-read segment at once must not race into decompressing/renaming the same destination
  file twice.

**Critical seam**: `.size`/`.capacity`/`.msg_cursor` currently read `self.bundle.size[0]` etc.
directly (`numpy_batch_manager.py:302-324`) - if left as-is, they'd force materialization just to
answer "how many rows does this segment have," which is checked constantly (every snapshot
iteration does `if seg.size == 0: continue`), defeating the entire point. These three properties
need to source from new cached plain-int fields (`_cached_size`/`_cached_msg_cursor`/
`_cached_capacity`, populated from the header at lazy-construction time) whenever the segment is
frozen (cold), falling back to today's `self.bundle.size[0]`-style read only for a live/hot
segment. This mirrors the existing `_cached_first_seq`/`_cached_last_seq`/`start_ts`/`end_ts`
pattern (`plans/fetch-telemetry-window-cold-segment-perf.md`) exactly - it's the same "don't touch
possibly-mmap'd/possibly-not-yet-materialized data for a cheap, frequently-polled property" idea,
just extended to counts instead of just seq/ts.

Nothing else needs to change: `start_ts`/`end_ts`/`first_sequence_id`/`last_sequence_id` are
already fully independent of `.bundle` today (populated once from the header), so they're
naturally free for a lazy segment with no further work.

### Eviction of a never-materialized segment

`_evict_cold_segment` (`numpy_log.py:214`) already only touches `meta.path` and calls
`.release()` - both already safe against a segment that was never materialized (`.release()`'s
per-handle loop already guards on `if self._ts_h:` etc., all `None` for an unmaterialized
segment, so nothing to close; `_try_delete_cold_file`'s `unlink(missing_ok=True)` already tolerates
`meta.path` not existing on disk). The one gap: eviction must **also** delete the segment's
`cold-archive/*.blkseg.zst` file, not just its (possibly never-created) raw `cold/*.blkseg` path -
today's compression feature never needed this because eviction only ever ran against already-
materialized, disk-resident-only-in-`cold/` segments. A segment evicted from the cold tier while
still lazy should be dropped without ever paying to decompress it.

### Mounting priority when both a raw file and an archive exist

If a previous run (this session or an earlier replay of it) already lazily materialized a segment,
`cold/segment_N.blkseg` exists alongside `cold-archive/segment_N.blkseg.zst`. Mount from the raw
file directly (today's existing `_mount_existing_cold_segments` path, unchanged) in that case -
no reason to re-decompress something already sitting there unpacked.

## Testing strategy

- Unit tests for the header-only partial read against a real compressed segment (byte-for-byte
  match against `read_cold_segment_header`'s result on the uncompressed original).
- `PooledLogBatch` lazy-mode tests: `.size`/`.capacity`/`.msg_cursor`/`.start_ts`/`.end_ts`/
  `.first_sequence_id`/`.last_sequence_id` all correct *before* first `.bundle` touch, with
  `cold/segment_N.blkseg` asserted to **not** exist yet at that point; `.bundle` access (directly,
  or via `__iter__`/`__getitem__`) triggers exactly one materialization (assert the raw file now
  exists, and that row content matches the pre-compression original).
- Concurrency test: many threads touching `.bundle` on the same lazy segment simultaneously,
  asserting the decompression work happened effectively once (no corruption, no crash) - matching
  this project's general bias toward exercising real threading rather than trusting a lock exists.
- Eviction test: a lazy (never-touched) segment aged out by `cold_max_pieces` never gets
  materialized (raw file never appears in `cold/`) and its `.zst` archive is deleted.
- End-to-end: extend the existing full-cycle test
  (`tests/test_cold_archive_replay_round_trip.py`) so the remount step asserts segments are lazy
  immediately after mounting, and only become materialized after a query actually touches them.

## Open questions / tradeoffs to flag before implementing

1. **Is this worth the complexity for the common case?** The win is largest for long sessions
   where a user only scrubs into a small window of history. For a session short enough (or a
   replay where the user does scroll through most of it) that most segments get touched anyway,
   this adds real complexity (a new `PooledLogBatch` mode, a new lock, three changed property
   implementations) for a small or negative net win (materializing lazily, one segment at a time
   as scrubbing reaches it, could even be a worse user experience than one up-front pause at
   session load if the user scrubs briskly through history the eager approach would've already
   finished preparing).
2. **Per-segment, not per-column, granularity.** The on-disk format already table-indexes each
   column's offset - a further refinement could decompress only the columns actually accessed
   (e.g. `timestamps` for a bisection search, without pulling in the `buffer` text column) rather
   than the whole segment at once. Meaningfully more complexity (partial-materialization state per
   column) for a secondary win; not part of this plan's initial scope, worth a future look if
   segment-level laziness proves worthwhile first.
3. **First-touch latency spike.** Unlike an mmap page fault (reads only the specific 4KB page
   touched), first `.bundle` access on a lazy segment decompresses the *entire* segment
   synchronously on whatever thread touched it - for a 128MB default segment, that's the
   ~0.15s-per-200MB order of magnitude measured above, on (likely) a GUI-adjacent thread. Probably
   fine (still much better than blocking session-open on every segment), but worth deciding
   up front whether that call belongs on a background thread with the caller getting placeholder/
   loading-state data in the meantime, or is acceptable inline. Inline is simpler and is what this
   plan assumes; flagging the alternative in case scrubbing-into-cold-history is expected to be a
   frequent enough interaction that a stall there matters more than it does for a one-time
   session-open cost.

## Non-goals

- Not changing anything about the live (hot-tier eviction) cold-write path - segments are still
  written uncompressed to `cold/` while a session is running, exactly as today.
- Not implementing per-column lazy materialization (see open question 2).
- Not backgrounding the materialization decompression call (see open question 3) - synchronous,
  same as the rest of this codebase's cold-segment reads.

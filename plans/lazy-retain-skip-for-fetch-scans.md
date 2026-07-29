# Skip retain()/release() for segments a fetch scan never actually reads

**Status**: implemented for the two watermark-based follow paths (2026-07-29); the ts-windowed
ones (fetch_telemetry_window's plus_one edge case, build_snapshot_as_of) are still open - see
"What shipped" below.

## What shipped

User reported real-world symptom this predicted: with ~25M messages in hot+cold storage, replay
mode got laggy specifically near the newest/live edge while browsing to a fixed old point stayed
fast. Root cause confirmed by investigation: the *history/browse* fetch functions this doc's
sibling plan (`fetch-telemetry-window-cold-segment-perf.md`) fixed are symmetric (equally fast
for an old or new anchor) - the asymmetry instead comes from the *follow* paths
(`LatestModuleValueTracker.update()` at 60Hz unconditional/global, `LogSegmentScanner.scan_tail`
at 10Hz) paying this exact retain/release-per-segment cost **continuously**, tick after tick,
instead of once per user action - the same fixed cost, multiplied by tick rate instead of by 1.

Implemented `CircularLogPool.get_reversed_snapshot_since(last_known_seq)` (`core/numpy_log.py`)
and wired it into both `LatestModuleValueTracker.update()` (`core/module_snapshot.py`) and
`LogSegmentScanner.scan_tail` (`core/log_fetch.py`) in place of `get_reversed_snapshot()` - both
already had a per-segment `last_sequence_id <= last_known_seq: break` early-exit for the
*kernel*-level work, but `SegmentSnapshot` was still retaining *every* segment in both tiers
before that loop ever ran. The new method skips retain() entirely for any cold segment whose
`segment.metadata.last_seq` (the immutable `ColdSegmentMeta` field, never touched by
`release()`/`clear()`) is already `<= last_known_seq` - once a cold segment is fully consumed by
an incremental "what's new" query, it can never become relevant to that same query again, since
nothing is ever appended to an already-archived cold segment.

**The race this doc worried about (open question / "the race retain() already has a pattern
for") turned out not to apply**: the original concern was reading segment metadata *before*
`retain()` without protection against a concurrent eviction. But `get_snapshot()`/
`get_reversed_snapshot()` already run their entire retain loop inside `self._lock` - the exact
same lock every eviction path (`_rotate_segment`/`_evict_hot_segment`/`_evict_cold_segment`/
`_handle_archived`) already holds before mutating `self.segments`/`self.cold_segments`. Keeping
the new method's relevance check *and* retain loop inside that same lock (matching the existing
methods' shape) means no eviction can happen concurrently in the first place - no
`RuntimeError`-catching retry dance needed, unlike the speculative design below.

Resolved open question 1 (hot segments) per the doc's own lean: hot segments are always
retained regardless of relevance - only `max_pieces` of them exist (small, bounded), not worth
a branch. Resolved open question 2 (where the skip logic lives): a new pool-level method,
keeping eviction-race reasoning centralized in `numpy_log.py`.

**Not yet done** (the trickier remaining follow-path costs, deliberately left out of this pass to
avoid rushing the edge-case-heavy ones):
- `TelemetryPlotter`'s REPLAY-follow tick (`fetch_telemetry_window`, per-visible-module, 10Hz) -
  needs the `plus_one` before/after edge-row-capture semantics preserved, which doesn't reduce to
  a simple watermark check the way the two shipped fixes did.
- `LatestModuleValueTracker.build_snapshot_as_of()` (REPLAY-follow for `TelemetryWatch`/
  `telemetry_table`, per-widget, 10Hz) - a from-scratch "latest value as of ts X" query with no
  incremental watermark; scrubbing near a large recording's tail may need to touch many segments
  regardless of retain/release cost, which could be a separate, deeper issue worth profiling on
  its own before assuming retain-skip alone would fix it.

Regression tests: `tests/test_log_pool_fetch_perf.py`'s
`TestGetReversedSnapshotSinceDoesNotScaleWithSegmentCount` (flat-scaling perf test, 4→128 cold
segments, plus two correctness tests: only non-stale segments are included, hot segments always
included regardless of `last_known_seq`).

## Original design sketch (context for the above)

## Context

`plans/fetch-telemetry-window-cold-segment-perf.md` fixed three `CircularLogPool` fetch functions
(`fetch_telemetry_window`, `scan_history_window`, `build_snapshot_as_of`) that scaled with total
segment count instead of relevant-row count, then measured the real-world effect (~7-7.6x at 128
cold segments) and profiled what's left. Result: **~47% of the remaining cost is
`SegmentSnapshot.retain()`/`.release()`** - one `threading.Lock` acquisition per segment, per
snapshot (`get_reversed_snapshot()`/`get_snapshot()` retain *every* segment up front so it can't be
evicted mid-scan), paid regardless of whether the ts-bound skip logic then decides that segment is
irrelevant and never touches its `.bundle` at all.

Mitigated for now by a config default change (4x bigger/4x fewer cold segments - same plan doc),
which cuts this cost proportionally without any further code risk. This doc captures the next,
more surgical lever for later: only retain the segments a scan actually reads.

## Idea

`retain()`/`release()` exist to protect `segment.bundle` (the actual array data - pool-backed for
hot, mmap'd for cold) from being freed while something is reading it. A segment's `.metadata`
(the `ColdSegmentMeta` NamedTuple for cold segments) is a *separate*, immutable object that
`release()`/`clear()` never reassigns or clears - only the plain-int `_cached_*` cache fields on
`PooledLogBatch` get reset back to their empty sentinels. So reading `segment.metadata.
earliest_ts`/`.latest_ts` (or, for hot segments, `.start_ts`/`.end_ts` - see caveat below) never
touches `.bundle` and stays valid even if the segment gets fully evicted a moment later.

That means the ts-bound relevance check (already present in all three fixed functions) could run
*before* `retain()` instead of after, and only relevant segments would ever get retained - cutting
lock acquisitions from O(total segments in the snapshot) down to O(relevant segments), which for a
narrow query window is close to O(1) regardless of how many total segments exist.

Sequence per segment, corrected from the naive "just read metadata, no lock needed at all" framing:

1. Read the cheap relevance signal (cold: `segment.metadata.earliest_ts/.latest_ts`, immutable,
   safe without retain; hot: see caveat below) - decide overlap with the query window.
2. If irrelevant (and not needed for `plus_one`/edge-row capture): skip entirely. `.bundle` is
   never touched, so no protection was ever needed for this segment.
3. If relevant: `retain()` *now* (same as today), read `.bundle` for the kernel call, `release()`
   after.

## The race retain() already has a pattern for

Between step 1 and step 3, a concurrent eviction could in principle free that exact segment's
resources. `PooledLogBatch.retain()` already handles "this was already fully released" by raising
`RuntimeError` if `_ref_count <= 0` - not a new problem this design introduces. There's an existing
precedent for handling it: `LatestModuleValueTracker.get_snapshot()`'s retry loop
(`core/module_snapshot.py`):
```python
while True:
    try:
        return self._current_snapshot.retain()
    except RuntimeError:
        continue
```
For this use case the handling should be simpler - not a retry loop, just "catch it, treat this
segment as evicted-out-from-under-us, skip it" (equivalent to it having aged out normally between
the relevance check and the retain call - not a correctness regression, just a rare timing miss).

## Open questions to resolve before implementing (why this needs more thought first)

1. **Hot segments have no immutable-metadata equivalent.** `PooledLogBatch._cached_first_ts`/
   `_cached_last_seq`/etc. (the plain-int cache added in the earlier fix) *do* get reset by
   `clear()` on release - unlike cold's `ColdSegmentMeta`, they're not safe to read without a
   retain under a race. Options: (a) always retain hot segments regardless (there are only
   `max_pieces` of them - currently 2, cheap either way, so this asymmetry may just be fine and not
   worth solving), or (b) find/add a similarly-immutable hot-segment signal. Leaning (a) - hot
   segment count is small and bounded, so the win is entirely on the cold side, which is also
   where the segment counts (32-256+) actually get large.

2. **Where should the skip-before-retain logic live?** Two shapes:
   - Inside `CircularLogPool.get_snapshot()`/`get_reversed_snapshot()` themselves, e.g. a new
     variant taking a `relevance_predicate(segment) -> bool` callback, evaluated per-segment while
     still holding the pool's own `self._lock` (the same critical section that already does the
     retains today) - keeps the pool's internal lock as the single place reasoning about
     eviction-vs-snapshot races has to happen, but means the predicate has to be pool-API shaped
     (probably just `(start_ts, end_ts) -> bool` or similar) rather than each caller's own
     bespoke overlap math.
   - Inside each of the three fetch functions' own loops, calling `segment.retain()`/`.release()`
     explicitly instead of relying on `SegmentSnapshot` to do it upfront - more flexible (each
     caller's overlap logic differs slightly: ts-only, seq-only, or ts-with-edge-capture) but means
     duplicating the "catch RuntimeError from a raced retain" handling in three places instead of
     one, and changes `get_snapshot()`'s contract for these three callers only (they'd stop calling
     the current retain-everything version).

   Current lean: a new pool-level method (or an optional predicate parameter on the existing ones)
   so the eviction-race reasoning stays centralized in `numpy_log.py`, not spread across three
   different fetch functions each independently deciding when it's safe to skip a retain.

3. **`scan_tail`/`LatestModuleValueTracker.update()`/`get_telemetry_anchor` don't need this at
   all** - they already break out of their loops via a cheap watermark check (`last_sequence_id`)
   rather than scanning every segment in the first place, so they never retain segments outside
   the relevant range to begin with. This change is specific to the three ts-windowed "history"/
   "scrub" functions that scan every segment in the snapshot looking for an overlap.

4. **Benchmark before/after using the existing infrastructure**
   (`tests/test_log_pool_fetch_perf.py`) rather than guessing at the improvement - same
   methodology as the earlier fixes (4 vs 128 cold segments, median of repeated calls after a
   warmup call). Expect the retain/release share of `cProfile`'s cumulative time (currently ~47%
   at n=128) to shrink roughly in proportion to (relevant segments / total segments) for a narrow
   query window.

5. **Correctness test to add**: a test that deliberately makes `retain()` raise mid-scan (e.g. by
   having a background thread evict/release a segment right after the relevance check passes but
   before `retain()` is called) and confirms the fetch function degrades gracefully (skips that
   segment, doesn't crash, doesn't return corrupted data) - this is the one behavior this whole
   design change actually introduces that doesn't exist today (today, retain happens for every
   segment atomically under the pool lock before any of them can be evicted).

## Non-goals

- Not touching the underlying `threading.Lock` primitive itself (that was evaluated separately -
  see the conversation this doc was extracted from - and judged a bigger, more architecturally
  invasive change than warranted right now: a pool-level "readers active" counter instead of
  per-segment locks would need eviction to defer resource release until no scan is in flight,
  which is a real behavior change, not just an optimization).
- Not applicable to `scan_tail` et al. (see open question 3).

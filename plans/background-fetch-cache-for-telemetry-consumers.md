# Background-computed fetch cache for TelemetryPlotter/TelemetryWatch-style consumers

## Status: design sketch, not yet implemented - needs more thought before starting

## Context

Follow-on from `plans/fetch-telemetry-window-cold-segment-perf.md` and
`plans/lazy-retain-skip-for-fetch-scans.md`. Those cut the *per-call* cost of
`fetch_telemetry_window`/`scan_history_window`/`build_snapshot_as_of`, but with 40+ simultaneous
consumers (visible plots/watches) each polling at 10Hz+ on what's likely a single shared Qt timer,
the *aggregate* cost still lands as one lump of blocking work in a single UI tick (measured:
~2.6ms/tick for 40 consumers @ 10Hz with the current implementation, ~1.4ms with lazy retain - see
that plan doc's numbers).

Confirmed while discussing this that every `@app_njit` kernel already runs with `nogil=True` by
default (`core/numba_config.py:67-68`) - the numba-heavy parts of fetch/filter/format already
release the GIL. So moving that work off the UI thread onto a background worker isn't just
"moving where the cost lands" - it's genuine parallelism for the kernel portions, on top of taking
the Python glue (loop iteration, retain/release, array_pool allocation) off the UI thread entirely.

The pieces to build this already exist in the codebase and don't need inventing:

- **`TaskManager`** (`core/task_manager.py`) - wraps a `ThreadPoolExecutor`, `run_periodic()`
  schedules a task at an interval, and its `is_running` flag (set in `_scheduler_loop`, cleared by
  the future's done-callback) already prevents a task from being re-dispatched while a previous run
  is still in flight - the debounce/coalesce behavior this design needs, for free.
- **`PooledArrayHandle`**/`PooledLogBatch`-style retain/release (`core/array_pool.py`,
  `core/numpy_batch_manager.py`) - already the safe mechanism for handing a buffer computed on one
  thread to a reader on another, with `RuntimeError` on a raced retain of an already-freed handle.
- **`LatestModuleValueTracker`** (`core/module_snapshot.py`) already implements the exact shape
  this design needs, just for module snapshots instead of telemetry/log fetch results: a
  background-updated "current result" reference, atomically swapped, with readers doing
  `while True: try: return self._current_snapshot.retain() except RuntimeError: continue` to
  safely grab it without a full lock held across the read. This plan generalizes that pattern
  rather than inventing a new one.

## Design

A generic cache class, one instance per consumer (or per module, if a widget tracks several):

```python
class BackgroundFetchCache:
    """Runs a fetch callback periodically on TaskManager's thread pool, atomically swaps in the
    latest completed result, and lets the UI thread's own timer grab the current result safely -
    mirrors LatestModuleValueTracker's retain/retry pattern, generalized to any retain()/release()
    -capable result object."""

    def __init__(self, task_manager, fetch_fn, interval_seconds):
        self._fetch_fn = fetch_fn  # () -> a retain()/release()-capable result, or None
        self._current = None
        self._lock = threading.Lock()
        self._task_id = task_manager.run_periodic(interval_seconds, self._run_once)

    def _run_once(self):
        new_result = self._fetch_fn()  # runs on a worker thread - nogil kernels overlap with UI
        with self._lock:
            old, self._current = self._current, new_result
        if old is not None:
            old.release()

    def get_latest(self):
        """Called from the UI thread's own render tick. Returns a retained result the caller
        must release when done, or None if nothing's been computed yet."""
        while True:
            with self._lock:
                result = self._current
            if result is None:
                return None
            try:
                return result.retain()
            except RuntimeError:
                continue  # swapped out between the lock release and our retain - retry

    def stop(self, task_manager):
        task_manager.stop_periodic(self._task_id)
        with self._lock:
            if self._current is not None:
                self._current.release()
                self._current = None
```

Each consumer (e.g. `TelemetryPlotter`) gets one `BackgroundFetchCache` per module it's tracking,
constructed with `fetch_fn` wrapping the same `fetch_telemetry_window`/`fetch_telemetry_arrays`
call `apply_updates` makes today. The refactored `apply_updates` (now on the UI's own faster
timer, e.g. 120Hz) becomes: `result = cache.get_latest(); if result: render(result); result.
release()` - no fetch/filter/format work happens on the UI thread's tick at all, just a retain,
a read, a release.

Background compute cadence (e.g. 10-30Hz) and UI render cadence (e.g. 120Hz) are now decoupled -
the UI can redraw at whatever rate feels smooth using whatever was last computed, without
recomputing every frame; this is a hold/last-value pattern, not interpolation.

## Expected effect on UI smoothness

Because each consumer's background compute runs independently on `TaskManager`'s thread pool at
its own cadence (not all synchronized to fire together), completions land staggered across
whichever real-world moment they finish at, not bunched into whichever single UI tick happens to
be "due." So no single UI tick has to absorb N consumers' full fetch+filter+format cost at once -
each tick's UI-thread work becomes N x (retain + render/downsample + release), which is far
cheaper per-consumer than fetch+filter+format was, rather than N x (fetch+filter+format). This is
the direct answer to "will this make ticks more uniform, not just cheaper on average": yes, by
construction, since the expensive part is no longer synchronized to tick boundaries at all.

Two things worth being precise about, not fixing, just knowing going in:

- **This removes the *fetch* cost from the UI tick, not all of it.** The tick still does a real
  render/paint/downsample per visible widget - this design doesn't touch that cost, only the
  fetch+filter+format portion that currently runs inline before it.
- **Cross-widget staleness becomes eventually-consistent, not tick-synchronized.** Today, all 40
  widgets recompute from the exact same instant's data on the same tick. With independent
  background cadences, one widget's displayed data might be ~30ms old while another's is ~80ms
  old at the same visual moment, since their background computations aren't phase-locked to each
  other or to the render tick. **Reviewed and judged acceptable**: a few ms to low tens-of-ms of
  phase drift between independent telemetry/log widgets is below the threshold of human-perceptible
  asynchrony - this isn't a frame-sync-critical display (e.g. stereo video, audio/video lipsync)
  where sub-frame drift across channels would actually be noticeable. Doubly so for the actual
  usage pattern this targets (10+ separate widget windows spread across a monitor/desktop): a
  human eye/attention shift between two windows (a saccade plus fixation-stabilization) already
  takes longer than the phase drift itself, so widgets are never perceived simultaneously in the
  first place - the comparison a viewer could actually make (glance at widget A, then glance at
  widget B) is inherently sequential and far slower than the drift between them. Not a blocking
  concern for this design; no further mitigation planned for it.

## What needs resolving before implementing

1. **`fetch_telemetry_window`'s current lifetime model doesn't fit.** It's a `@contextmanager`
   that owns an `ExitStack` releasing its output `array_pool` handles the instant the `with` block
   exits - i.e. today, the caller is expected to fully consume the batch within one synchronous
   call. For a background cache, the result has to *outlive* the call that produced it (until the
   *next* background run replaces it, possibly several ticks later) and be released later by
   whichever thread swaps it out, not by the function that created it. Needs either: (a) a
   non-contextmanager variant that returns a retain()/release()-capable wrapper object owning
   those handles directly (probably a small wrapper NamedTuple/class around `TelemetryBatch` +
   the three `array_pool` handles, with its own `retain()`/`release()`), or (b) reworking
   `TelemetryBatch` itself to carry that responsibility. Same issue applies to any other fetch
   function that gets cached this way (`scan_history_window`'s consume-callback shape is
   different again and needs its own look).
2. **Per-widget vs per-module caching granularity** - a plot showing 5 channels from 1 module is
   one fetch; a plot showing channels from 5 different modules might want 5 independent caches (so
   one slow/stale module doesn't block the others) or one combined fetch (fewer background tasks,
   but a straggler module holds up the whole result). Needs a decision once real usage patterns
   (how many modules/widgets typically co-exist) are better understood.
3. **Interval tuning** - background compute interval doesn't have to match "10Hz" universally;
   REPLAY-follow vs LIVE-forward paths may want different cadences, and a not-currently-visible
   tab/widget arguably shouldn't be scheduled on `TaskManager` at all (pause when hidden, resume
   on tab activation) - needs wiring into whatever visibility-change signal already exists for
   tabs/docks.
4. **Error handling in the periodic task** - `TaskManager.run_periodic`'s dispatched future doesn't
   appear to have any built-in exception surfacing (a failed fetch would just silently never swap
   in a new result, per `_scheduler_loop`'s done-callback only clearing `is_running`) - worth
   checking whether that's sufficient (stale-but-not-crashing is probably fine) or whether errors
   need to be logged/surfaced somewhere.
5. **Shutdown/teardown ordering** - each cache needs `.stop()` called before the owning widget is
   destroyed (to stop_periodic + release the last held result), and before `TaskManager.shutdown()`
   tears down the executor - needs to hook into the existing widget/tab close lifecycle
   (`closeEvent`/`window_manager.close_all()`), not just be assumed to happen.
6. **Benchmark before/after** using the same methodology as the other two plans (this session's
   `tests/test_log_pool_fetch_perf.py` style) - specifically, measure UI-thread tick time (not
   just total CPU) with N synthetic consumers, to confirm the background-cache design actually
   removes the blocking-tick problem, not just moves the numbers around.

## Non-goals for this doc

- Not re-litigating `plans/lazy-retain-skip-for-fetch-scans.md`'s retain/release-skip optimization
  - that's still valid and complementary; it reduces the *worker* thread's per-call cost, this doc
    is about getting that cost off the *UI* thread's tick entirely. Do both, in either order.
- Not designing a full generic "reactive data pipeline" framework - deliberately kept to "one
  cache class + reuse of three things that already exist," not a bigger abstraction.

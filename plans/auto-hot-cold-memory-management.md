# Automatic hot/cold storage sizing based on system free memory

**Status**: implemented. `psutil` wheel-coverage verification (see "Decisions made" below)
confirmed a `cp36-abi3` wheel covers every `gui-core` platform target, so `psutil` was promoted to
a real dependency and the hand-rolled fallback was not needed.

## Decisions made with the user before implementing

- **`psutil` dependency**: conditional. Promote it to a real dependency (`gui-core` extras, see
  "Memory query" below) *only if* it ships prebuilt wheels (no client-side compilation) for every
  Python version/platform combo this app actually targets. If any target lacks a wheel and would
  fall back to building from source on the end user's machine, hand-roll the platform queries
  instead. **This needs a one-time verification step at implementation start** (check PyPI's
  `psutil` file listing against blinkview's supported Python versions/platforms) before writing any
  code against it - see "Memory query" below for what that check needs to confirm and the
  hand-rolled fallback shape if it fails.
- **Hysteresis margin**: one segment's worth of bytes (`log_pool.final_buffer_bytes`) - ties the
  grow/shrink gap to the same unit the governor actually moves in, no separate config knob needed.
- **Shrink rate limit**: a small fixed cap, `MAX_SEGMENTS_EVICTED_PER_TICK = 4` - bounds worst-case
  `log_pool._lock` hold time predictably regardless of how large a sudden memory spike is. A
  persistent spike beyond 4 segments' worth just takes a few extra poll ticks to fully react to
  (ticks are seconds apart, so still fast in absolute terms - see "Poll interval").
- **Governor ownership**: `CentralStorage`. Constructed in `apply_config()` right next to where
  `CircularLogPool`/`ColdStorageArchiver` already get wired up (same method already has the
  "runtime dynamic updates" branch that calls `update_max_pieces()` by hand today); stopped in
  `CentralStorage.stop()`, not `Registry.stop()` - keeps all hot/cold tier lifecycle in one place
  and avoids adding another step to `Registry.stop()`'s already-delicate teardown ordering.

## Goal

Today `CircularLogPool`'s hot-tier size is a static config knob (`max_pieces` x
`buffer_size_mb`, defaults `2 x 128MB` - see `core/limits.py`). The ask: let the hot tier grow to
use most of whatever RAM is actually free, and shrink (evicting oldest segments to cold storage)
the moment free memory gets tight - reacting to the *whole system's* memory pressure (other
programs opening, not just this app's own usage), not a number picked once at startup. A
configurable floor keeps some minimum number of segments hot no matter how much pressure there is,
so recent scrollback never becomes disk-latency-bound.

## What already exists and gets reused

`CircularLogPool.update_max_pieces(new_max_pieces)` (`core/numpy_log.py:534`) already does exactly
the mechanical part of this: shrinking the ceiling immediately evicts the oldest hot segments via
`_evict_hot_segment`, which - when a `ColdStorageArchiver` is configured - archives them to disk
instead of dropping them; growing the ceiling just raises it (no immediate backfill, natural growth
resumes as new segments roll in). This is already dynamically callable at runtime and already
exercised by `CentralStorage.apply_config()`'s "runtime dynamic updates" branch when the user edits
`max_pieces` by hand.

**This means the new work is almost entirely a policy/monitoring layer, not new eviction
mechanics** - a background component that watches system free memory and periodically computes a
new target piece count, then calls `update_max_pieces()` with it. `update_cold_max_pieces()`
already exists too if cold-tier sizing ever needs to react to disk pressure the same way, but that
is out of scope here (see Non-goals).

## Precondition: this only works with cold storage actually enabled

`_evict_hot_segment` archives to cold storage *only if* `self._archiver is not None`
(`cold_storage_enabled=True` at `CircularLogPool` construction, see
`CentralStorage._resolve_cold_storage_dir`) - otherwise it just calls `hot_segment.release()`,
i.e. **silently drops the evicted data**. An auto-memory-management feature built on top of
`update_max_pieces()` without this precondition would turn "evict to cold storage" into "delete
data under memory pressure," the opposite of the intent. `CentralStorage.apply_config()` must
refuse to enable auto-memory-management (log a warning, leave it off) if
`cold_storage_enabled=False` at the same time.

## New component: `HotTierMemoryGovernor`

A new plain-Python class in `core/`. **Correction after initial implementation**: rather than
owning its own `threading.Thread` (the shape originally sketched here, modeled on
`ColdStorageArchiver`), it schedules itself on the registry's existing `TaskManager`
(`shared.tasks.run_periodic`/`stop_periodic`) - the same periodic-task infrastructure already used
by `io/adb_reader.py`, `io/uart.py`, and `io/source_handshake.py`. This is exactly the "periodic
small task" case that machinery exists for; a dedicated OS thread per periodic policy loop was
unnecessary duplication. No `BaseDaemon`/factory machinery either way - this isn't a
batch-subscriber, just a scheduled callback:

```python
class HotTierMemoryGovernor:
    def __init__(
        self,
        log_pool: CircularLogPool,
        get_available_bytes: Callable[[], int],   # injected, see "Memory query" below
        min_hot_pieces: int,
        max_hot_pieces: Optional[int],
        target_free_bytes: int,
        poll_interval_sec: float = 3.0,
        logger=None,
    ):
        ...
    def start(self) -> None: ...
    def stop(self, timeout: float = 5.0) -> None: ...
```

### Policy (runs once per poll tick)

1. Read `available = get_available_bytes()`.
2. Estimate current hot-tier byte size: `len(log_pool.segments) * segment_bytes`, where
   `segment_bytes` is `log_pool.final_buffer_bytes` (the size hot segments settle at once
   `_apply_real_world_heuristics` has run - an approximation, since early "probe" segments and the
   currently-filling active segment are usually smaller; acceptable given the governor
   self-corrects every poll tick rather than needing to be exactly right once).
3. Compute headroom: `slack = available - target_free_bytes`.
   - `slack < 0` (below the free-memory floor): **shrink**. New target piece count =
     `current_pieces - ceil(-slack / segment_bytes)`, clamped to `>= min_hot_pieces`. This is the
     one case that must react promptly - see "Poll interval" below.
   - `slack > hysteresis_band` (comfortably above the floor): **grow**, by at most one segment's
     worth per tick (see "Rate limiting"). New target = `current_pieces + 1`, clamped to
     `<= max_hot_pieces` if a ceiling is configured.
   - Otherwise (within the hysteresis band): no-op - this is what stops the governor oscillating
     by ±1 segment every tick right at the threshold boundary.
4. If the computed target differs from `log_pool.max_pieces`, call
   `log_pool.update_max_pieces(target)`.

### Hysteresis

Grow and shrink must not share the same trigger line, or a workload sitting exactly at the
threshold would flap every poll tick (evict a segment, immediately allowed to grow it back, evict
again...). Use a band: shrink triggers below `target_free_bytes`, grow triggers only once
`available >= target_free_bytes + segment_bytes` (**one segment's worth of bytes** - decided
above) - `hysteresis_margin` is therefore not a separate config knob, just
`log_pool.final_buffer_bytes` read at policy-evaluation time.

### Rate limiting

`update_max_pieces()`'s shrink path holds `log_pool._lock` for its entire eviction loop -
`batch_append()` (ingestion) takes the same lock. A pathological reading (e.g. another process
suddenly allocating tens of GB in one shot) could otherwise ask the governor to shrink by hundreds
of segments in a single `update_max_pieces()` call, holding the lock long enough to visibly stall
ingestion. **Decided above**: cap a single poll tick's shrink step at
`MAX_SEGMENTS_EVICTED_PER_TICK = 4` segments, continuing to shrink further on the *next* tick if
pressure persists, rather than always jumping straight to the fully-computed target in one call -
i.e. step 3's shrink branch becomes `new_target = max(min_hot_pieces, current_pieces - min(ceil(-slack
/ segment_bytes), MAX_SEGMENTS_EVICTED_PER_TICK))`. This constant is a starting guess, not a
benchmarked value - worth a quick sanity check against a realistic `buffer_size_mb` early in
implementation (measure `update_max_pieces()`'s wall time evicting 4 segments at the default
128MB size) and adjusted if the lock-hold time is either surprisingly cheap (cap could be looser)
or still too expensive (cap needs to shrink further, or eviction needs to move the actual
archiving hand-off outside the lock - it already is async via `ColdStorageArchiver`'s queue, so
this is more likely to be fine than not).

### Memory query - `available`, not `free`, and a dependency decision

"Free" memory (as commonly reported) is misleading on Linux, where most physical RAM sits in
reclaimable page cache and reports as "used." The correct cross-platform figure is what `psutil`
calls `available` (`psutil.virtual_memory().available`) - already correctly abstracts the
per-OS distinction (Windows `GlobalMemoryStatusEx`'s `ullAvailPhys`, Linux
`MemAvailable`/`meminfo`, macOS `vm_stat`).

**`psutil` is currently only a `dev`-group dependency** (`pyproject.toml`), not shipped to end
users. **Decided above**: promote it to the `gui-core` optional-extras group (alongside
`numpy`/`numba` - this feature is desktop-app-only, so core headless/CLI installs never need it)
*conditionally on it not requiring client-side compilation* for blinkview's supported Python
version/platform matrix.

**Verification step required before writing any governor code**: check PyPI's file listing for the
`psutil` version pyproject.toml would pin against every `(python_version, platform)` combination
`gui-core` is expected to run on (at minimum: the Windows/CPython version this dev environment
targets, plus whatever other platforms/Python versions the project currently claims support for -
check existing `pyproject.toml` `requires-python`/classifiers) - confirm each has a `.whl` (not
just an `.sdist`) on PyPI. `pip download psutil --no-deps -d /tmp/x --python-version X --platform Y
--only-binary=:all:` against each target is a fast way to confirm without actually installing
anything.

- **If every target has a wheel**: add `psutil` to `gui-core`, use
  `psutil.virtual_memory().available` directly.
- **If any target is missing a wheel** (e.g. an experimental/free-threaded Python build, or an
  unusual architecture): hand-roll `get_available_bytes()` instead - three small platform branches
  behind one function (Windows: `ctypes.windll.kernel32.GlobalMemoryStatusEx`'s `ullAvailPhys`
  field; Linux: parse `MemAvailable:` out of `/proc/meminfo`; macOS: `vm_stat` output or the
  `host_statistics64` Mach API via `ctypes`) - no new dependency, more code/platform-testing
  surface, but avoids forcing a source build onto any user. Either implementation sits behind the
  same `get_available_bytes: Callable[[], int]` constructor parameter below, so this choice is
  fully isolated from the rest of the governor's design - nothing else in this plan changes based
  on which path is taken.

Either way, `get_available_bytes` is **constructor-injected as a plain callable**, matching this
codebase's existing pattern for testable environment queries (`Registry.now_ns`/`TimeUtils`) - so
`HotTierMemoryGovernor`'s policy logic can be unit-tested against a fake stream of readings with no
real OS dependency and no timing flakiness.

### Poll interval

A single default interval has to serve two different needs: react fast enough to a sudden spike
(another program launching a big allocation) to avoid the system hitting real memory pressure
before we've shrunk, while not busy-polling `psutil` needlessly. Lean on a short default (2-3s) -
`update_max_pieces()`'s own eviction path is cheap (archiving is handed off to
`ColdStorageArchiver`'s async queue, not synchronous disk I/O - see "Rate limiting" above for the
one thing that *does* cost real time), so a short interval isn't expensive on its own merits, just
on `psutil` call overhead, which is negligible at this cadence.

## Config surface (`CentralStorage`, alongside the existing cold-storage `@configuration_property`s)

- `auto_memory_management_enabled` (bool, default `False` - opt-in; changes default behavior and
  adds a background thread + new dependency, shouldn't silently turn on for existing configs).
- `min_hot_pieces` (int, default matching today's `max_pieces` default) - the floor; `max_pieces`
  itself becomes purely the *initial* value fed to `update_max_pieces()` before the governor's
  first tick, once auto-management is enabled.
- `max_hot_pieces` (int, default `0` = unbounded except by memory pressure itself) - optional
  safety ceiling even under abundant free memory.
- `target_free_memory_mb` (int, default e.g. `4096`, matching the user's "below 4GB, start
  evicting" framing).
- `memory_poll_interval_sec` (float, default `3.0`).

`min_hot_pieces`/`max_hot_pieces` are deliberately segment **counts**, not byte budgets - matches
"a minimum number of segments" from the request, and reuses the exact knob `update_max_pieces`
already takes; no new byte-vs-piece conversion needed at the config boundary, only internally
inside the governor's own policy step above.

The governor's live-computed `max_pieces` value is **never written back into persisted config** -
it's a runtime-only quantity recomputed every session from current system state, not a user
preference. The config file keeps recording the user's `min_hot_pieces`/starting `max_pieces`.

## Growing back doesn't un-evict already-cold segments

Worth stating explicitly since the request's framing ("could happily keep 60GB... in hot storage")
might read as a single elastic pool: once a segment has been archived to cold storage and evicted
from `log_pool.segments`, growing `max_pieces` back up later does **not** pull it back into RAM -
it stays on disk, still fully queryable through the existing hot+cold unified snapshot API
(`get_snapshot()`/segment_filter etc. already scan both tiers transparently), just at cold-tier
(mmap) read latency instead of hot (RAM) latency. Growing the ceiling only means *future* segments
get to stay hot longer before eviction resumes. This matches how cold storage already behaves
today for ordinary `max_pieces`-ceiling evictions - the governor doesn't change that contract, just
who decides when to shrink/grow.

## Testing strategy

- **Policy unit tests** (no threading, no real `CircularLogPool`): a pure function
  `compute_target_pieces(available_bytes, current_pieces, segment_bytes, min_hot_pieces,
  max_hot_pieces, target_free_bytes) -> int` (hysteresis margin and the per-tick shrink cap are
  both derived from `segment_bytes`/`MAX_SEGMENTS_EVICTED_PER_TICK` internally, not separate
  params), extracted from the governor so the shrink/grow/hysteresis/clamping logic is directly
  testable against a table of
  (reading, expected target) cases - mirrors this project's preference for testing pure decision
  logic separately from its I/O/threading wrapper (see e.g. `PlaybackFollowMachine`'s split of
  `handle()` from Qt-level dispatch).
- **Real-pool integration test**: construct a real `CircularLogPool` with `cold_storage_enabled`,
  push enough rows to fill several hot segments, drive `HotTierMemoryGovernor` with a fake
  `get_available_bytes` callable returning a scripted sequence of readings (plenty free -> tight ->
  plenty free again), and assert against `len(log_pool.segments)`/`len(log_pool.cold_segments)` at
  each step - same "drive the real thing, not just the kernel" habit this project already applies
  to `CircularLogPool`'s other dynamic-resize methods (`tests/test_numpy_log_cold_tier.py`).
- **Regression test for the cold-storage-disabled guard**: `CentralStorage.apply_config()` with
  `auto_memory_management_enabled=True, cold_storage_enabled=False` must log a warning and leave
  the governor off, not silently start evicting-as-deleting.

## Open questions to resolve before implementing

All four load-bearing design questions were resolved with the user (see "Decisions made with the
user before implementing" above) except:

1. **Any GUI surface wanted?** e.g. a toolbar readout of "hot tier: N segments (X MB), system free:
   Y MB" so the user can see the governor reacting, or is this meant to be fully invisible/
   automatic with just a log line per resize? Not required for a first pass either way - default
   plan is log-line-only (matches `ColdStorageArchiver`'s own logging-not-UI precedent), a GUI
   surface can be added later without changing anything about the governor itself.

## Non-goals

- Not making `update_cold_max_pieces()` (cold-tier disk-space pressure) part of this pass - the
  request is specifically about *RAM* pressure driving hot-tier size; cold-tier disk-space
  management would be a separate, differently-shaped problem (disk free space, not system RAM).
- Not dynamically resizing `buffer_size_mb` (per-segment byte size) - only the piece *count* is
  ever adjusted; segment byte size stays whatever the user configured.
- Not retroactively promoting already-cold segments back to hot when memory becomes abundant again
  (see "Growing back doesn't un-evict already-cold segments" above).
- Not touching per-process (this app's own RSS) memory accounting - the governor reacts to
  system-wide available memory, which already implicitly includes this app's own footprint (as our
  hot tier grows, system-reported `available` drops accordingly) - no separate self-accounting
  needed, the feedback loop is self-limiting by construction via the same `available` reading.

# Automatic hot/cold storage sizing based on system free memory

**Status**: proposed, not yet implemented. This doc is the plan to review before any code changes.

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

A new plain-Python class in `core/`, modeled on `ColdStorageArchiver`'s shape (own
`threading.Thread`, no `BaseDaemon`/factory machinery - this isn't a batch-subscriber, just a
periodic policy loop) rather than a full daemon:

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
`available >= target_free_bytes + hysteresis_margin` (e.g. `hysteresis_margin = one segment's
worth of bytes`, or a configurable fraction of `target_free_bytes` - needs a decision, see Open
Questions).

### Rate limiting

`update_max_pieces()`'s shrink path holds `log_pool._lock` for its entire eviction loop -
`batch_append()` (ingestion) takes the same lock. A pathological reading (e.g. another process
suddenly allocating tens of GB in one shot) could otherwise ask the governor to shrink by hundreds
of segments in a single `update_max_pieces()` call, holding the lock long enough to visibly stall
ingestion. Cap how much a single poll tick's shrink step can request (e.g. at most N segments per
tick, continuing to shrink further on the *next* tick if pressure persists) rather than always
jumping straight to the fully-computed target in one call.

### Memory query - `available`, not `free`, and a dependency decision

"Free" memory (as commonly reported) is misleading on Linux, where most physical RAM sits in
reclaimable page cache and reports as "used." The correct cross-platform figure is what `psutil`
calls `available` (`psutil.virtual_memory().available`) - already correctly abstracts the
per-OS distinction (Windows `GlobalMemoryStatusEx`'s `ullAvailPhys`, Linux
`MemAvailable`/`meminfo`, macOS `vm_stat`).

**`psutil` is currently only a `dev`-group dependency** (`pyproject.toml`), not shipped to end
users - this feature would need to promote it to a real runtime dependency (or the `gui-core`
optional-extras group, alongside `numpy`/`numba`). Alternative: hand-roll the three platform
queries via `ctypes`/`/proc/meminfo` parsing to avoid the new dependency - meaningfully more code
and platform-testing surface for something `psutil` already solves correctly. Leaning towards
adding `psutil` as a real dependency; flagged as an open question below since it's a footprint
change, not a pure implementation detail.

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
  max_hot_pieces, target_free_bytes, hysteresis_margin) -> int`, extracted from the governor so the
  shrink/grow/hysteresis/clamping logic is directly testable against a table of
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

1. **Add `psutil` as a real runtime dependency, or hand-roll the platform queries?** Leaning
   `psutil` (correctness/maintenance cost of hand-rolled `MemAvailable` parsing isn't worth it) -
   needs your sign-off since it's a footprint/dependency-policy decision, not just an implementation
   detail. If added, where: `gui-core` extras (feature is really only meaningful for the desktop
   app) or unconditionally in core `dependencies`?
2. **Hysteresis margin**: fixed byte amount, one-segment's-worth, or a percentage of
   `target_free_memory_mb`? Affects how "twitchy" growth feels once the system settles near the
   threshold.
3. **Shrink rate limit**: how many segments per poll tick is safe to evict in one
   `update_max_pieces()` call before the lock-hold time becomes noticeable? Needs a quick
   benchmark against a realistic `buffer_size_mb`, not just a guessed constant.
4. **Where does `HotTierMemoryGovernor` get owned/started/stopped?** Natural fit is
   `CentralStorage` (constructs it in `apply_config()` alongside `self.log_pool`, right next to
   where `ColdStorageArchiver` already gets wired up; stops it in `CentralStorage.stop()` or
   `Registry.stop()` alongside the existing `log_pool.release_all()` teardown) - confirm before
   implementing since `Registry.stop()`'s teardown ordering there is already delicate (see the
   `_dump_id_registry`/cold-dir-capture comments in `Registry.stop()`).
5. **Any GUI surface wanted?** e.g. a toolbar readout of "hot tier: N segments (X MB), system free:
   Y MB" so the user can see the governor reacting, or is this meant to be fully invisible/
   automatic with just a log line per resize? Not required for a first pass either way.

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

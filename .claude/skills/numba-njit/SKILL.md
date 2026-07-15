---
name: numba-njit
description: Use when adding, refactoring, or reviewing a Numba-jitted (@app_njit) kernel in blinkview - naming, argument design (NamedTuple bundling), warmup registration/ordering, the multi-stage-pipeline gotcha where an optional LogBundle column needs updating in every merge/copy kernel, the omitted-default-argument call-site perf trap, NamedTuple-vs-tuple benchmarking pitfalls, diagnosing/fixing slow Numba compile times (inline="always" on dispatchers, profiling warmup callbacks cold), and the read-only-vs-writable EMPTY_* placeholder mismatch (build the empty singleton itself as read-only via np.frombuffer, matching EMPTY_BYTES_RO, instead of warming every real/empty combination).
---

# Numba kernels in blinkview

Patterns arrived at while building and reviewing `@app_njit` kernels across a session that
touched kv/text filtering, PID/TID plumbing through the ingestion pipeline, and the warmup
registry. Read this before writing a new kernel, adding a parameter to an existing one, or
reviewing someone else's.

## 1. Every `@app_njit` function name must start with `nb_`

No exceptions - including small `inline="always"` helpers and "private" leading-underscore
kernels (`_is_ws` → `nb_is_ws`, `_copy_snapshot_state` → `nb_copy_snapshot_state`, dropping the
underscore rather than keeping it before `nb_`). This was enforced as a repo-wide rename; keep it
that way for anything new. When renaming an existing kernel, grep the whole repo (`src/` and
`tests/`) for the bare name - these functions get imported and called across module boundaries
constantly (e.g. `ops/pipeline.py` calling into `ops/modules.py`, `ops/codec_adb_long.py`,
`ops/strings.py`), so a rename that only touches the definition site will silently break callers.

## 2. Kernel argument design: bundle related arguments into a NamedTuple, pass it whole

Numba accepts a `NamedTuple` as a single typed argument - this is exactly how `segment: LogBundle`
already gets passed to every segment-processing kernel. Don't flatten a NamedTuple's fields back
out into individually-named parameters when calling a *different* kernel; pass the NamedTuple
itself instead.

**Before** (what this looked like the first time kv-filter and text-search conditions were added
to `nb_segment_filter_reversed`/`nb_filter_segment` - 15 extra parameters between the two
features):

```python
def nb_segment_filter_reversed(
    segment, effective_mask, out_indices, max_matches,
    start_seq=SEQ_NONE, end_seq=SEQ_NONE, start_ts=TS_UNSPECIFIED, end_ts=TS_UNSPECIFIED,
    kv_cond_keys_buf=EMPTY_KV_BYTES, kv_cond_keys_off=EMPTY_KV_OFFSETS, kv_cond_keys_len=EMPTY_KV_LENGTHS,
    kv_cond_vals_buf=EMPTY_KV_BYTES, kv_cond_vals_off=EMPTY_KV_OFFSETS, kv_cond_vals_len=EMPTY_KV_LENGTHS,
    kv_num_conditions=0, kv_field_delim=32, kv_kv_delim=61,
    text_needle_buf=EMPTY_TEXT_BYTES, text_needle_len=0,
    text_dev_mask=EMPTY_BOOL_MASK, text_mod_mask=EMPTY_BOOL_MASK,
):
```

**After** - each condition-set collapses to one parameter, using the whole `KvConditionArrays`/
`TextSearchArrays` NamedTuple as a single payload (see §11 for why these are *required*
parameters with the old default spelled out only in a comment, not live `=VALUE` defaults):

```python
def nb_segment_filter_reversed(
    segment, effective_mask, out_indices, max_matches,
    start_seq,  # =SEQ_NONE,
    end_seq,  # =SEQ_NONE,
    start_ts,  # =TS_UNSPECIFIED,
    end_ts,  # =TS_UNSPECIFIED,
    kv,  # =EMPTY_KV_CONDITIONS,      # KvConditionArrays
    kv_field_delim,  # =CHAR_SPACE,
    kv_kv_delim,  # =CHAR_EQUALS,
    text,  # =EMPTY_TEXT_SEARCH,      # TextSearchArrays
):
    ...
    kv_ok = kv.num_conditions == 0 or nb_row_matches_kv_conditions(
        segment.buffer, segment.offsets[i], segment.lengths[i],
        kv.cond_keys_buf, kv.cond_keys_off, kv.cond_keys_len,
        kv.cond_vals_buf, kv.cond_vals_off, kv.cond_vals_len,
        kv.num_conditions, kv_field_delim, kv_kv_delim,
    )
```

Notes:
- `kv_field_delim`/`kv_kv_delim` stayed as separate scalar params rather than joining the
  NamedTuple - they're delimiter *configuration* (never varied anywhere in the codebase, always
  `CHAR_SPACE`/`CHAR_EQUALS`), not baked condition *data*. Don't bundle unrelated config into a
  data NamedTuple just to hit a lower param count; bundle what's conceptually one payload.
- Every optional NamedTuple parameter still has a shared empty-singleton value to pass at
  call sites that don't need the feature (`EMPTY_KV_CONDITIONS`, `EMPTY_TEXT_SEARCH` - see
  `ops/kv_filter.py`/`ops/text_filter.py`) - but that value must be passed *explicitly* at every
  call site, not relied on as a live default. See §11.
- This also fixes call-site readability at every one of the (often 3-6) call sites across
  `_fetch_live`/`_fetch_history`/warmup shapes - `kv=kv, text=text` instead of an 11-line block
  repeated verbatim per call site.

## 3. Numba `NamedTuple` bool fields are not compile-time literals

`LogBundle.has_pids`, `has_levels`, etc. are ordinary runtime-checked booleans as far as Numba's
type system is concerned - two `LogBundle` instances with different `has_pids` values share the
*same* compiled specialization (the field's Numba *type* is `boolean`, not a baked `True`/`False`
literal). Two practical consequences:
- A warmup call that builds a dummy batch with `has_pids=False` still fully warms the
  `if bundle.has_pids: ...` branch for real batches where it's `True` at runtime - you don't need
  a warmup call per has-flag combination.
- Conversely, don't assume a numpy array's *size* being different avoids recompilation the same
  way - Numba specializes by dtype/ndim, not length, so an empty array and a 1000-element array of
  the same dtype hit the identical compiled specialization.
- **But size is not the only thing that can silently differ - read-only-ness is also part of the
  type, and it's easy to get this backwards.** See §15: a real, non-empty payload built via
  `np.frombuffer(some_bytes, ...)` is *read-only*, a different Numba type than an `EMPTY_*`
  singleton built via plain `np.empty(...)` (*writable*) - so naively reusing an `np.empty(...)`
  placeholder does **not** cover the "real data present" case the way same-dtype/different-size
  arrays do. The fix isn't to warm every real/empty combination, though - it's to build the
  `EMPTY_*` placeholder itself as a read-only array (`np.frombuffer(b"", ...)`) so it's typed
  identically to the real payload in the first place. §15 has the full story and the existing
  repo convention (`EMPTY_BYTES_RO`) this should have followed from the start.

## 4. Multi-stage pipelines: adding an optional `LogBundle` column means auditing *every* stage

This bit hard once already (adding `pids`/`tids` to `LogBundle`): a log row in this codebase
flows through several independently-constructed batches before it's queryable -
**parser output batch → Reorder's merge kernel → CentralStorage's segment-copy kernel → a
model's per-widget extract kernel** - and *each* of those is its own `PooledLogBatch` creation
site plus its own hand-written "copy these columns" kernel
(`ops/pipeline.py`/`nb_process_batch_kernel`, `core/reorderer.py`'s `nb_hybrid_merge_and_copy`,
`ops/segments.py`'s `nb_copy_batch_to_segment`, `ops/segments.py`'s
`nb_segment_extract_fields`). Getting a new column correctly parsed at the source but forgetting
even *one* of these hops silently drops it downstream with zero errors - the column just reads as
all-zeros everywhere past that point, which looks exactly like "the parser never wrote it" and is
easy to mis-diagnose as a parsing bug when it's actually a plumbing gap several stages later.

When adding a new optional `LogBundle` column:
1. Add the field + `has_X` flag to `LogBundle` (`core/types/log_batch.py`) and thread `has_X`
   through `PooledLogBatch`/`NumpyBatchManager` (mirror `has_levels` exactly).
2. Find **every** `pool_create(PooledLogBatch, ...)` / `pool.create(PooledLogBatch, ...)` call
   site in the actual data path the new column needs to survive (grep `has_levels=True` - nearly
   every real batch-creation call site sets it, so it's a reliable way to enumerate them) and add
   `has_X=True` to the ones that should carry the new column.
3. Find every kernel that copies a `LogBundle` into a *different* `LogBundle` (grep for
   `has_levels and` / `.has_levels\[` patterns, or just `s_start:s_end\] = batch\.` /
   `out_idx\] = src_bundle\.` style block-copy lines) and add the new column's copy there too.
4. **Zero-fill, don't skip, when the destination has the column but the source doesn't** (e.g. a
   CAN or system-generated row merging into the same output batch as ADB rows). `array_pool`
   does not zero-fill on acquire - a skipped write leaves whatever stale value a *previous,
   unrelated* row left in that recycled memory slot, which reads as a real (wrong) value rather
   than an obviously-blank one:
   ```python
   if out_bundle.has_pids:
       out_bundle.pids[out_idx] = src_bundle.pids[r_id] if src_bundle.has_pids else 0
   ```
5. Verify end-to-end by chaining the real kernels together in a throwaway script (not just unit
   tests per-kernel) - construct one parsed row, run it through parse → merge → segment-copy in
   sequence, and print the field after each stage. This is the fastest way to catch a stage that
   was missed, since per-stage unit tests will each pass individually while the column still gets
   dropped between two of them.

## 5. Warmup registration: `register_warmup` lives in `warmup_registry.py`, not `warmup.py`

`core/warmup_registry.py` holds only `_WARMUP_CALLBACKS` (a list of `(priority, callback)` pairs)
and the `register_warmup` decorator. `core/warmup.py` holds `NumbaWarmupHelper` (the dummy
pool/registry/log_pool environment) and imports `register_warmup` from `warmup_registry` like
everyone else.

**Why the split:** `warmup.py` pulls in a wide swath of the codebase to build its dummy
environment (parsers, storage, formatting configs, etc). If a core class like `CircularLogPool`
or `TimeSyncEngine` decorated its own `warmup()` with something imported from `warmup.py`, and
`warmup.py` itself (transitively) imports that same class's module, you get a circular import.
Routing through the tiny, dependency-free `warmup_registry.py` sidesteps the whole problem: any
module can depend on it without pulling in `warmup.py`.

If you see `from blinkview.core.warmup import register_warmup` anywhere, it should almost always
be `from blinkview.core.warmup_registry import register_warmup` instead — the only things that
should still import from `warmup.py` are `NumbaWarmupHelper` itself (for type hints, always
`TYPE_CHECKING`-only or a local/lazy import) and, transitively, `run_all()`'s own use of the
registry.

## 6. Pattern: put `warmup(helper)` on the class that owns the kernel, not in `warmup.py`

Every kernel's dummy-data setup should live as a `@staticmethod` on the class whose kernel it's
exercising:

```python
@staticmethod
@register_warmup  # or @register_warmup(priority=100) - see §7
def warmup(helper: "NumbaWarmupHelper"):
    """One-line description of which kernel(s) this compiles and what data it needs from
    helper (e.g. "requires rows already in helper.log_pool, provided by
    CircularLogPool.warmup, priority=100")."""
    ...
```

`warmup.py` should end up containing almost no kernel-exercising code itself - just
`NumbaWarmupHelper.__init__` (building the dummy `array_pool`/`registry`/`log_pool`/`pid_history`/
`shared` context) and `run_all()` (sort + loop over `_WARMUP_CALLBACKS`, cleanup in `finally`).
Examples: `CircularLogPool.warmup` (`numpy_log.py`), `IdHistory.warmup` (`core/id_history.py`),
`IndexedStringTable.warmup` (`core/id_registry/tables.py`), `TimeSyncEngine.warmup`
(`time_sync_engine.py`), `TelemetryPlotter.warmup` (`plotter.py`), `LogViewerWidget.warmup`
(`log_viewer.py`), `LogTableModel.warmup` (`log_table_viewer.py`), `BinaryParser.warmup`/
`_warmup_config` (`binary_parser.py`), `TempLogFilter.warmup` (`module_filter_table.py`),
`Reorder.warmup` (`reorderer.py`), `LatestModuleValueTracker.warmup` (`module_snapshot.py`).

The `helper: "NumbaWarmupHelper"` type hint is always a bare forward-ref string and
`NumbaWarmupHelper` is never actually imported at runtime in these files (only under
`TYPE_CHECKING`, or not at all) - the IDE will flag it as undefined, that's expected.

A warmup doesn't need `helper`'s dummy pool/registry at all if the class manages its own plain
numpy state independently of `array_pool` (e.g. `IndexedStringTable`, which allocates via
`np.empty`/`np.resize` directly) - just build the dummy instances inline and exercise the real
public API (`register_name(...)`, not the kernels directly), same as any other caller would.

## 7. Ordering: explicit priority, not import-order luck

`register_warmup` accepts an optional `priority: int` kwarg (default `0`), usable bare
(`@register_warmup`) or as a factory (`@register_warmup(priority=100)`).
`NumbaWarmupHelper.run_all()` sorts `_WARMUP_CALLBACKS` by descending priority before executing
(Python's stable sort keeps registration order among ties).

This matters because several callbacks assume **log data already exists** in `helper.log_pool`
(e.g. formatting/filtering kernels need rows to filter, telemetry kernels need a discoverable
channel). That producer is `CircularLogPool.warmup`, which populates `helper.log_pool` via
`batch_append` and declares `priority=100` for exactly this reason - it used to rely on
`numpy_log.py` happening to import before any UI widget module ("module load order, not
registration order, decided who ran first"), which worked but was fragile and undocumented at the
call site. Prefer an explicit priority over relying on import order for any new callback with a
real ordering dependency; document *why* in the docstring either way (see `CircularLogPool.warmup`
for the phrasing).

## 8. Retiring an old "warm up on first run" block

Older code sometimes warmed up its own kernel lazily, guarded by an instance flag, inside `run()`:

```python
def __init__(self):
    self.numba_needs_compile = True

def run(self):
    ...
    if self.numba_needs_compile:
        try:
            # build dummy batch, call the kernel once
            ...
        except Exception as e:
            self.logger.exception(...)
        self.numba_needs_compile = False
    ...
```

Once the same kernel gets a proper `@register_warmup` callback (compiled up front at app start,
before any real thread runs), this block is redundant - by the time `run()` executes, the kernel
is already compiled. Delete the flag entirely (`__init__` and the `if self.numba_needs_compile:`
block in `run()`/`finally`), don't just leave it as a dead no-op assignment. Move the dummy-data
setup verbatim into the new `warmup(helper)` staticmethod, swapping `self.shared`/`self.logger`/
`self.time_ns()` for `helper.shared`/`helper.logger`/`helper.time_ns()` (or whichever of
`helper.array_pool`/`helper.log_pool`/`helper.registry`/`helper.pid_history`/`helper.warmup_mod`/
`helper.floats_mod` the block actually needs).

## 9. Testing a kernel directly (not through the whole app)

- Build a minimal `LogBundle` by hand rather than going through real ingestion - see
  `make_bundle`/`make_out_bundle` helpers in `tests/test_ops_segments.py`, mirrored in
  `tests/test_log_table_viewer.py` and `tests/test_ops_codec_adb_long.py`.
- **`np.frombuffer(some_bytes, dtype=BYTE)` is read-only.** A kernel that writes into its input
  buffer (e.g. `nb_parse_adb_tag` injecting a bracket character in place) will raise
  `NumbaTypeError: Cannot modify readonly array of type: readonly array(uint8, 1d, C)` at typing
  time, not at the write itself. Wrap the source in `bytearray(...)` first:
  `np.frombuffer(bytearray(text), dtype=BYTE)`.
- Parser-pipeline kernels (`nb_execute_parser_pipeline` and friends) expect a
  `ParserPipelineBundle`, not the raw `.pipeline` tuple - if you're driving one directly for a
  test, pull `parser.bundle().pipeline` for `nb_execute_parser_pipeline`'s `parser_bundles` arg,
  but pass the whole `parser.bundle()` object (not `.pipeline`) to `nb_process_batch_kernel`'s
  `parser` arg, matching real call sites (`self._frame_parser.bundle()` in `binary_parser.py`).
- Short-circuit `and`/`or` work the same as plain Python inside `@app_njit` - `text_needle_len ==
  0 or nb_bytes_contains_ci(...)` will not evaluate (or bounds-check) the right side when the
  needle is empty. Lean on this instead of nested `if`s for optional-feature gating.

## 10. After moving or adding kernel code, verify - don't just trust the refactor

- `ast.parse` (or `python -m py_compile`) the touched files first - cheap syntax check before
  anything else.
- Import the real app entry point (`import blinkview.ui.main_window`) - this is the fastest way to
  catch a newly-introduced circular import, since it transitively imports nearly everything. Note
  a pre-existing, unrelated gotcha: importing certain `ui.widgets.*`/`core.device_identity`
  modules as the *very first* touch of that dependency cluster raises a circular-import error that
  only avoids tripping when something else warms `core.id_registry` first - `main_window` and
  `tests/conftest.py` (which pre-imports `blinkview.core.registry`) both avoid it; a fresh
  standalone script or a test file that sorts alphabetically first might not.
- Actually call the new `warmup(helper)` with a minimal fake helper (just the attributes the
  method touches) to confirm the kernel compiles and runs, not just that it imports - or run
  `NumbaWarmupHelper.run_all()` against a real `Registry` for full end-to-end coverage, which also
  exercises priority ordering.
- Run the test suite. `tests/test_registry_memory.py::test_module_registration_density` is a
  pre-existing order-dependent flake (fails when run as part of the full suite depending on prior
  test state, passes in isolation) unrelated to kernel changes - don't chase it if nothing else
  fails.

## 11. Never omit more than one *trailing* default-valued argument at a call site

`@app_njit` function default values are a per-call-site performance trap, not just a convenience.
In Numba nopython mode, an omitted argument is typed as `Omitted(value=...)` - a *different* type
signature than the same value passed explicitly. This was discovered chasing why
`nb_segment_filter_reversed` cost 350-670µs per call from its `log_viewer.py` hot path (~60Hz GUI
tick) despite doing almost no work per call - 1000x more than the ~300ns raw dispatch overhead of
a comparably simple `@app_njit` function. Microbenchmarking isolated it precisely:

- A **single trailing** omitted default is cheap (~150-300ns, matching baseline dispatch).
- **Two or more omitted defaults**, even a clean trailing run with nothing explicit after them,
  already costs ~21µs/call.
- An omitted default **followed by an explicitly-passed parameter later in the signature** (a
  "gap" - e.g. `kv_field_delim`/`kv_kv_delim` omitted but `text` passed explicitly right after) is
  the worst case: **~220-260µs/call**. This is a genuine per-call dispatcher slow path, confirmed
  via a 10,000-iteration warm loop (not a one-time recompilation) and an interleaved-pattern test
  (ruling out cross-call-site dispatch-cache thrashing as an extra factor).

`nb_segment_filter_reversed`/`nb_filter_segment` (`ops/segments.py`) were hit by exactly this: 7
default-valued trailing params (`start_seq, end_seq, start_ts, end_ts, kv, kv_field_delim,
kv_kv_delim, text`), called from ~11 call sites across `log_viewer.py`/`log_table_viewer.py`, each
omitting a different irregular subset. The fix: **stripped the live defaults from both
signatures** (see §2's current example), keeping the old default value as a trailing `# =VALUE`
comment for reference, and made every call site pass every one of these parameters explicitly.
An omitted parameter is now a `TypeError` at the call, not a silent perf trap.

When writing or reviewing an `@app_njit` function:
- If it has default-valued parameters, **every call site must pass all of them explicitly** -
  never rely on the default, even for a single one, since it's easy for a *different* call site to
  later omit a different subset and reintroduce the "gap" pattern.
- Prefer no live defaults at all: use `param,  # =VALUE` (required parameter, old default kept as
  a comment) instead of `param=VALUE`. This makes forgetting a parameter an immediate, loud
  `TypeError` instead of a silent tens-to-hundreds-of-µs regression.
- If you must keep a live default (e.g. a genuinely single-call-site helper), that's fine - the
  risk is specifically *multiple call sites with differing omission patterns*, not defaults in
  general.

**The axis that actually matters: which side of the Python↔Numba boundary the call happens on.**
A repo-wide audit (grep every `@app_njit` function for default-valued parameters, then check each
call site) found exactly 3 surviving live-default functions: `nb_table_get_max_string_len`
(`ops/formatting.py`, `fallback_default=3`) and `nb_bundle_push`/`nb_bundle_push_len`
(`ops/segments.py`, `pid=0, tid=0`). All three are safe to leave as-is, but for two *different*
reasons worth distinguishing:
- `nb_table_get_max_string_len` has one call site, and it passes the value explicitly anyway - no
  omission actually occurs.
- `nb_bundle_push`'s `pid`/`tid` omission at `io/can_bus.py`'s `nb_can_push` call site is safe
  **not** because it's "only one call site with this particular omission pattern", but because
  `nb_can_push` is *itself* `@app_njit`-decorated - the call into `nb_bundle_push` is
  **Numba-to-Numba**, resolved entirely at compile time (the compiler already knows both
  functions' full signatures and bakes the omitted values in as compile-time constants). The
  `Omitted(value=...)` typing/dispatch cost documented above is specifically a **Python-call-site**
  phenomenon - it's the cost of Python's calling convention resolving which specialization to
  invoke and marshaling arguments across the language boundary before compiled code ever runs.
  Two `@app_njit` functions calling each other never pay it, no matter how many such internal call
  sites exist or how their omission patterns differ from each other.
- Practical audit rule: when you find a default-valued `@app_njit` parameter, check whether its
  *caller* is plain Python or itself `@app_njit`. Only Python-side callers need the "pass
  everything explicitly" discipline above; njit-to-njit callers omitting a default are free.

## 12. NamedTuple vs. plain tuple: a toy microbenchmark's ratio does not generalize

Chasing the same `nb_segment_filter_reversed` cost from §11 also raised: is `LogBundle`/
`KvConditionArrays`/`TextSearchArrays` being `NamedTuple`s (vs. plain tuples) itself adding
overhead? Two rounds of microbenchmarking gave *contradictory* answers depending on how faithfully
the benchmark matched the real kernel - the lesson is about benchmark methodology, not a verdict
against NamedTuples:

- A **toy** 2-level-nested benchmark (a handful of small sub-NamedTuples, simple field access)
  showed NamedTuple calls costing ~2.1x-4.5x more than plain tuples, both to compile and to call.
- A **mockup rebuilt to match the real shape exactly** - `SyncState`'s actual 16 fields,
  `UnifiedParserState`'s real 3-level nesting (`state.timestamp.sync.X`), `UnifiedParserConfig`'s
  real 6-field/3-branch shape, a 12-branch dispatcher mirroring `ops/pipeline.py`'s
  `nb_process_bundle` - showed **no meaningful difference** (~1.02x, compile time and call
  overhead both). The toy benchmark's ratio simply didn't transfer once field count, nesting
  depth, and dispatch shape matched the real kernel.

Takeaway: don't extrapolate a NamedTuple-vs-tuple (or any Numba micro-optimization) verdict from a
small isolated benchmark to a real kernel with a different shape. If a performance question is
worth answering, rebuild the benchmark to match the *real* type's field count, nesting depth, and
call pattern - not a simplified stand-in - before trusting the ratio. Also confirmed separately
(§11's investigation): field access *inside* a NamedTuple (`.field`) vs. an equivalent tuple
(`[index]`) costs the same once hoisted to a local before a hot loop - the real, measured cost
everywhere in this investigation was call-boundary argument handling (§11) and forced inlining
(§13 below), never the NamedTuple/tuple choice itself.

## 13. `inline="always"` on a multi-branch dispatcher inflates *compile* time, not runtime

`ops/pipeline.py`'s `nb_process_bundle` dispatches across ~12 `ParserID` branches, each calling a
distinct, substantial leaf-parser function (`nb_parse_log_level`, `nb_parse_adb_tag`, etc.), and
was decorated `@app_njit(inline="always")`. Forcing this onto a *dispatcher* (as opposed to a
single small helper) means every call site must inline the entire branch tree - Numba has to
compile all ~12 leaf functions together, once, at first use, **regardless of how many of those
branches the specific call actually exercises**. This was root-caused by profiling
`BinaryParser.warmup()` (`parsers/binary_parser.py`) with `BLINKVIEW_NO_CACHE=1` (forces a true
cold compile, bypassing the on-disk cache `cache=True` normally reuses - see §14): its first
`_warmup_config()` call, with **zero** pipeline steps, still cost ~25s; the next four calls (1, 2,
3, 4 steps) cost ~0.5s then ~0.001s each - all the cost was the dispatcher's one-time compile, not
the pipeline's actual length.

Fix applied: dropped `inline="always"` from `nb_process_bundle` and the equivalent
`nb_dispatch_frame_decoder` (`ops/frame_dispatch.py`) - both are dispatch-level functions, not
per-row hot-loop bodies, so a real (non-inlined) function call there costs little at runtime.
Measured result: `BinaryParser.warmup()`'s cold-compile time dropped ~21% (25.65s -> ~20.1s), full
test suite unaffected.

**Explicitly tested and reverted**: also tried removing `inline="always"` from the 12 individual
*leaf* parser functions themselves (`nb_parse_log_level`, `nb_parse_fixed_width_name`, etc. -
these genuinely run in the per-byte parsing hot loop). Measured **no additional compile-time
benefit** (slightly worse, within noise) - so this was reverted to avoid trading away real
runtime hot-loop performance for a win that didn't materialize. **Lesson: don't blanket-remove
`inline="always"` everywhere a slow compile is suspected - profile which specific function's
removal actually helps compile time before touching a hot-loop leaf function, since the dispatcher
level and the leaf level can behave completely differently.**

## 14. Diagnosing a slow warmup/compile budget: profile real callbacks with the cache disabled

Don't guess which registered `@register_warmup` callback dominates a slow startup, and don't trust
timing from a normal run - `cache=True` (the default, `core/numba_config.py`) means Numba reuses
on-disk `.nbi`/`.nbc` files from any prior run of the same code, so a warm-cache measurement can
look 4-5x faster than what a fresh clone / cache-invalidating code change will actually pay.

To get real numbers: build a real `Registry`/`NumbaWarmupHelper` (same pattern §9's testing notes
already recommend), then iterate `blinkview.core.warmup_registry._WARMUP_CALLBACKS` yourself
(sorted by priority, same as `NumbaWarmupHelper.run_all()`) and time each callback individually,
with `BLINKVIEW_NO_CACHE=1` set in the environment to force true recompilation:

```python
from blinkview.core.registry import Registry
from blinkview.core.warmup import NumbaWarmupHelper
from blinkview.core.warmup_registry import _WARMUP_CALLBACKS

registry = Registry(session_name="warmup_profile", log_dir=some_tmp_dir)
helper = NumbaWarmupHelper(registry.system_ctx)
for priority, callback in sorted(_WARMUP_CALLBACKS, key=lambda item: item[0], reverse=True):
    t0 = time.perf_counter()
    callback(helper)
    print(callback.__qualname__, time.perf_counter() - t0)
```

This is what found `BinaryParser.warmup()` was ~80-84% of total cold-compile time (and, drilling
further, that a *single* call inside it - not pipeline length - was the entire cost; see §13).
Don't stop at "which callback is slow" - if one dominates, re-run its own internal steps
individually the same way to find the actual long pole, rather than assuming the whole callback
is uniformly expensive.

## 15. An `EMPTY_*` NamedTuple placeholder must match the real payload's read-only-ness, not just its dtype

Found while adding free-text search to `LogViewerWidget` (`log_viewer.py`) and then auditing
`LogTableModel.warmup()` (`log_table_viewer.py`) for the same bug: both widgets' `warmup()`
called `nb_segment_filter_reversed`/`nb_filter_segment` with `kv=EMPTY_KV_CONDITIONS,
text=EMPTY_TEXT_SEARCH` only, on the assumption (stated explicitly in the old docstrings, and in
the old wording of §3) that dtype/ndim is all that determines a Numba array specialization - so
an empty placeholder array "covers" a real, non-empty one of the same dtype.

That assumption is *incomplete*: Numba also types an array's **read-only vs. writable** flag as
part of its signature (`array(uint8, 1d, C)` vs. `readonly array(uint8, 1d, C)` - the same
distinction §9's testing notes call out for why you must `bytearray(...)`-wrap a test buffer
before feeding it to a kernel that writes into it). `EMPTY_KV_CONDITIONS`/`EMPTY_TEXT_SEARCH`
(`ops/kv_filter.py`/`ops/text_filter.py`) were built from plain `np.empty(0, dtype=BYTE)` -
ordinary *writable* arrays. But the moment a user actually types a kv condition or search string,
`build_kv_condition_arrays`/`build_text_search_arrays` hand back buffers built via
`np.frombuffer(bytes(...), dtype=BYTE)` - **read-only**, because `bytes` objects are immutable.
So "kv filter active"/"text filter active" were each a genuinely different compiled specialization
of `nb_segment_filter_reversed`/`nb_filter_segment` from the all-empty case, and warming only the
empty combination left the other three to JIT-compile live, on the ~10Hz GUI-thread fetch tick, the
first time a real user query hit each shape. Symptom in production: `[UI Monitor]` thread-lag
warnings in the 600-700ms range, and the Numba disk cache visibly compiling new `.N.nbc`
specializations (`segments.nb_segment_filter_reversed-*.2.nbc`, `*.3.nbc`, ...) well after the app
had already started up.

**The wrong fix (tried first, reverted): warm every real/empty combination.** Build a real,
non-empty instance of each optional NamedTuple payload and loop over all
`(real-or-empty, real-or-empty)` combinations at every `warmup()` call site. This *works*, but it's
solving a self-inflicted problem the hard way - it multiplies every call site by 2^(number of
optional payloads) instead of removing the type mismatch that caused it, and burns extra warmup
time doing so.

**The actual fix: make the `EMPTY_*` placeholder read-only too**, so it's typed *identically* to
the real payload and only one specialization ever exists:

```python
# Before: a writable placeholder that silently doesn't match the real (read-only) payload.
EMPTY_KV_BYTES = np.empty(0, dtype=BYTE)

# After: read-only, matching build_kv_condition_arrays' real cond_*_buf np.frombuffer(...) result.
EMPTY_KV_BYTES = np.frombuffer(b"", dtype=BYTE)
```

This is not a new trick - it's an existing repo convention that the kv/text code just didn't
follow: `core/types/empty.py` already defines `EMPTY_BYTES_RO = np.frombuffer(b"", dtype=BYTE)`
specifically for this purpose, and it's already used by `core/types/modules.py`'s `prefix_bytes`
field and `parsers/frame_parsers.py` (`prefix_bytes = EMPTY_BYTES_RO` as the empty case,
`np.frombuffer(self.prefix.encode("ascii"), ...)` as the real case - exact same shape as
kv/text's `*_buf` fields). Once `EMPTY_KV_BYTES`/`EMPTY_TEXT_BYTES` were fixed the same way, the
`warmup()` methods reverted to their original single-call-per-shape form - no combinatorial
loop needed - and a real kv+text call after warmup measured ~22µs (dispatch overhead only, no
live compile).

**When adding a new optional NamedTuple payload with an array field that might be populated via
`np.frombuffer`/bytes/`.setflags(write=False)`/any other read-only-producing path**: define its
`EMPTY_*` placeholder as read-only from the start (reuse `EMPTY_BYTES_RO` if the dtype matches, or
build your own via `np.frombuffer(b"", dtype=...)`), matching the real builder's output type. Only
fall back to warming multiple real/empty combinations if the two truly cannot be made the same
type (e.g. one path is fundamentally not frombuffer-representable) - that's the expensive
workaround, not the first move.

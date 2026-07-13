---
name: numba-warmup
description: Use when adding a new Numba-jitted (@app_njit) kernel in blinkview, or when moving/refactoring existing warmup code between core classes and NumbaWarmupHelper (warmup.py). Covers the register_warmup decorator pattern, why it lives in warmup_registry.py instead of warmup.py, ordering guarantees, and how to retire an old ad-hoc "warm up on first run" block in favor of it.
---

# Numba kernel warmup in blinkview

`NumbaWarmupHelper.run_all()` (`core/warmup.py`) exists so every `@app_njit` kernel gets
JIT-compiled once, up front during app startup (`Registry.start()`), instead of paying the
compile cost on the first real batch a user's device sends. This note is the pattern arrived at
after moving every ad-hoc warmup block in the codebase onto it.

## 1. The registration mechanism: `register_warmup` lives in `warmup_registry.py`, not `warmup.py`

`core/warmup_registry.py` holds only `_WARMUP_CALLBACKS` (a list) and the `register_warmup`
decorator. `core/warmup.py` holds `NumbaWarmupHelper` (the dummy pool/registry/log_pool
environment) and imports `register_warmup` from `warmup_registry` like everyone else.

**Why the split:** `warmup.py` pulls in a wide swath of the codebase to build its dummy
environment (parsers, storage, formatting configs, etc). If a core class like `CircularLogPool`
or `TimeSyncEngine` decorated its own `warmup()` with something imported from `warmup.py`, and
`warmup.py` itself (transitively) imports that same class's module, you get a circular import.
Depending on which module happens to be imported first in the process, this either works by luck
or blows up with `ImportError: cannot import name ... from partially initialized module`. Routing
through the tiny, dependency-free `warmup_registry.py` sidesteps the whole problem: any module can
depend on it without pulling in `warmup.py`.

If you see `from blinkview.core.warmup import register_warmup` anywhere, it should almost always
be `from blinkview.core.warmup_registry import register_warmup` instead — the only things that
should still import from `warmup.py` are `NumbaWarmupHelper` itself (for type hints, always
`TYPE_CHECKING`-only or a local/lazy import) and, transitively, `run_all()`'s own use of the
registry.

## 2. Pattern: put `warmup(helper)` on the class that owns the kernel, not in `warmup.py`

Every kernel's dummy-data setup should live as a `@staticmethod` on the class whose kernel it's
exercising, decorated:

```python
@staticmethod
@register_warmup
def warmup(helper: "NumbaWarmupHelper"):
    """One-line description of which kernel(s) this compiles and what data it needs from
    helper (e.g. "requires rows already in helper.log_pool, provided by
    CircularLogPool.warmup")."""
    ...
```

`warmup.py` should end up containing almost no kernel-exercising code itself - just
`NumbaWarmupHelper.__init__` (building the dummy `array_pool`/`registry`/`log_pool`/`shared`
context) and `run_all()` (loop over `_WARMUP_CALLBACKS`, cleanup in `finally`). Examples already
following this pattern: `CircularLogPool.warmup` (`numpy_log.py`), `TimeSyncEngine.warmup`
(`time_sync_engine.py`), `TelemetryPlotter.warmup` (`plotter.py`), `LogViewerWidget.warmup`
(`log_viewer.py`), `BinaryParser.warmup`/`_warmup_config` (`binary_parser.py`),
`TempLogFilter.warmup` (`module_filter_table.py`), `Reorder.warmup` (`reorderer.py`),
`LatestModuleValueTracker.warmup` (`module_snapshot.py`).

The `helper: "NumbaWarmupHelper"` type hint is always a bare forward-ref string and
`NumbaWarmupHelper` is never actually imported at runtime in these files (only under
`TYPE_CHECKING`, or not at all) - the IDE will flag it as undefined, that's expected and matches
every other `warmup()` in the codebase.

## 3. Ordering: callbacks run in *module import order*, not registration order you control

`register_warmup`'s docstring says it plainly: "module load order, not registration order,
matters." `_WARMUP_CALLBACKS.append(func)` happens once, whenever that class's module is first
imported anywhere in the process (decorators execute at class-body evaluation time). `run_all()`
just iterates the list in whatever order that ended up being.

This matters because several callbacks assume **log data already exists** in `helper.log_pool`
(e.g. formatting/filtering kernels need rows to filter, telemetry kernels need a discoverable
channel). That producer is `CircularLogPool.warmup`, which populates `helper.log_pool` via
`batch_append`. It's safe to rely on this running first because `CircularLogPool` lives in
`numpy_log.py`, which is imported at module level by `core/central_storage.py` — core
infrastructure loaded long before any UI widget module — so its callback registers (and thus
runs) ahead of the UI-widget callbacks in practice. Don't fight this by trying to control ordering
explicitly; if you add a new callback that needs log data present, just document the dependency in
its docstring (as the existing ones do) rather than adding a priority system.

## 4. Retiring an old "warm up on first run" block

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
`helper.array_pool`/`helper.log_pool`/`helper.registry`/`helper.warmup_mod`/`helper.floats_mod`
the block actually needs).

## 5. After moving code, verify - don't just trust the refactor

- `ast.parse` the touched files (cheap syntax check before anything else).
- Import the real app entry point (`import blinkview.ui.main_window`) - this is the fastest way to
  catch a newly-introduced circular import, since it transitively imports nearly everything.
- Actually call the new `warmup(helper)` with a minimal fake helper (just the attributes the
  method touches - `array_pool`, `time_ns`, maybe a real `IDRegistry`) to confirm the kernel
  compiles and runs, not just that it imports.
- Run the test suite. `tests/test_registry_memory.py::test_initial_import_baseline` is a
  pre-existing environment-sensitive memory threshold unrelated to warmup changes - don't chase it
  if nothing else fails.

# Runtime session rotation (main window Clear)

## Status: phases 1-3 implemented (2026-09-27)

Phase 3 as built:

- **Clear button** (`BlinkMainWindow.clear_button`, a `QToolButton`):
  - Left click → `start_session_rotation()`. Right click → `_prompt_session_rotation_name()`
    (`QInputDialog`, prefilled with the current name; a cancelled or blank name does nothing).
  - Disabled in replay mode, and while a rotation runs (a second request is refused).
- **Order:** `start_session_rotation()` first calls `file_manager.save_gui()` on the UI thread.
  That writes the workspace copies plus the old session's `gui_state`/`gui_config` `final`
  snapshots (layout, open tabs with widget state, floating windows). A `_SessionRotationWorker`
  on a `QThread` then runs `Registry.rotate_session()`. Back on the UI thread,
  `_on_session_rotated` re-points the GUI `ConfigManager`'s autosave, writes the new session's
  GUI `start` snapshots (`FileManager.snapshot_gui_start()`), and shows a toast.
- **Close during a rotation** is deferred until the rotation finishes (`_close_after_rotation`),
  so `Registry.stop()` never races it.
- **Views:** `Registry.session_generation` is bumped at the end of `rotate_session()`, and read
  through `core/session_generation.py`'s `session_generation_of()`, which tolerates test doubles.
  Each view polls it in `apply_updates()`:
  - `LogViewerWidget` drops its per-tab Clear floor and redraws from the tail.
  - `LogTableViewerWidget` goes live.
  - `TelemetryPlotter` resets its buffers to `SEQ_NONE`.
  - `TelemetryTableModel` re-derives its layout.
  - `PlaybackControlWidget` puts the clock into LIVE, on the UI thread, and forgets a pending
    mark-in.
  - `TelemetryWatch` needs nothing: the tracker reset alone clears it.
- Caught while testing: `QThread.wait()` in the finished slot blocked for its full 5 s timeout.
  The `worker.finished → thread.quit` connection is queued behind that same slot, so `quit()`
  is now called directly first.
- Tests: `tests/test_session_rotation_ui.py`.
- Not yet verified by clicking through the real running app - only via the real
  `BlinkMainWindow`/widgets in tests.

Phase 2 as built:

- `CircularLogPool.rotate_session(new_cold_dir)`: under the pool lock it detaches the hot and
  cold tiers, starts a fresh active segment, and attaches a new `ColdStorageArchiver` at the
  new session's `cold/`. Outside the lock, it persists the old tiers into the old archiver's
  directory (or discards them when not persisting). `sequence` is **not** reset.
  - `ColdStorageArchiver.set_on_archived()` (callback swap under a callback lock, called
    *before* taking the pool lock to avoid deadlock) routes in-flight archives to the old
    session.
  - `archive(block=True)` waits for queue space instead of dropping. The old hot tier is
    usually bigger than the queue depth (4).
- `CentralStorage.rotate_session()` resolves the new cold dir after `FileManager.rotate()` has
  switched folders.
- `LatestModuleValueTracker.reset()`.
- `Registry.rotate_session()` does both of the above in order, dumps `id_registry.json` into
  the old cold dir, compresses it, and clears the named ranges. Everything runs on the
  caller's thread, so phase 3 must call it off the UI thread.
- Tests: `tests/test_numpy_log_cold_tier.py::TestRotateSession`, plus in
  `tests/test_session_rotation_e2e.py` the per-session cold-storage contents (checked by
  remounting them) and the tracker reset.

**Found and fixed along the way (pre-existing, unrelated to rotation):** `FileLogger` lost
every batch processed between two flushes except the last one. Both `LogRowBatchProcessor`
and `BinaryBatchProcessor` overwrote their output buffer on each `process()` call, and
`_ensure_capacity()` discarded unflushed bytes when growing. Both now append (`_append_space`)
and carry bytes over on growth. Covered by `tests/test_file_logger.py::TestWithRealProcessors`.

**Also fixed (pre-existing):** `CircularLogPool.release_all()` - the shutdown persist path -
handed hot segments to the archiver with the non-blocking `archive()`, so its 4-deep queue
dropped everything past the first 4 at close (the newest data of the session). It now uses
`block=True`, like rotation. Only the ingestion-path eviction (`_evict_hot_segment`) stays
non-blocking, by design. Covered by
`tests/test_numpy_log_cold_tier.py::TestPersistColdStorageOnClose::test_release_all_persists_a_hot_tier_larger_than_the_archiver_queue`.

Phase 1 as built:

- `FileManager.rotate(display_name=None)`: `_build_metadata()`/`_finalize_metadata()` are
  factored out of `__init__`/`stop()`. It also adds `_create_unique_session_dir()` (`_2`, `_3`,
  ... on a same-second collision), a `_lock` around metadata writes, and `_rotation_lock`
  around whole rotations.
- `FileLogger.request_session_rotation()` returns a `SessionRotationRequest`
  (`closed`/`resume` events), and `run()` handles it between batches through
  `_rotate_session()`. **Side effect:** `run()` now polls its queue every
  `QUEUE_POLL_TIMEOUT_S` (0.5 s) instead of every 120 s, so it notices a rotation promptly.
  This also means `flush_interval` now fires when no new batches arrive; before, a lone batch
  could sit unflushed for up to 120 s.
- `Registry.rotate_session(display_name=None)` writes the registry config `final` into the old
  folder and `start` into the new one, and re-points `config.autosave_path`.
- Tests: `tests/test_file_logger.py::TestSessionRotation`,
  `tests/test_file_manager.py::TestRotate`, `tests/test_session_rotation_e2e.py`.

Replaces the parked virtual-sessions approach (branch `feature/virtual-sessions`,
`plans/virtual-sessions.md` there). That approach kept everything in one recording and filtered
every view by time. This one makes **Clear start a real new session**: a new session folder with
its own logs, metadata, cold storage and config snapshots, loadable on its own later through
"Load Session...".

## Decided

- **Runtime rotation, not a restart.** The `Registry` object, `IDRegistry` (devices/modules),
  sources, pipelines and hardware connections all stay up. Only the storage side rotates.
- **Past sessions are viewed via replay** ("Load Session..."), not through an in-app dropdown.
- **Batch queues are not touched.** Data queued at rotation time goes to the new session - see
  §2.
- **Clear empties the in-memory log pool**, after persisting its contents to the old session
  (§3). Otherwise the pool's cold-storage archiver, which is bound to one directory at
  construction, would keep evicting new-session rows into the *old* session's `cold/` folder.
- **Clear is disabled in replay mode.**
- **Session naming:** a left click keeps the current display name (the timestamp prefix already
  differs, plus the collision guard in §1). A **right click** opens an input dialog to name the
  new session first (§4).

## What is tied to the session folder today

Everything below is resolved from `FileManager.session_dir`, which is created once in
`FileManager.__init__` and never changes afterwards.

| Item | Where | Rotation need |
|---|---|---|
| `metadata.json` (status, created/finished, per-logger parts/bytes) | `FileManager.metadata`, `write_metadata()` | finalize old (`finished`, `finished_at`, duration) and start a fresh dict for the new folder |
| Unified log `session.NNNN.<ext>` | central's `FileLogger` (`logging_id="session"`), `get_path_for_log()` | close + compress the current part, reopen at part 0 in the new folder |
| Raw per-source logs `src_*.NNNN.bin` | each source's `FileLogger` via `BaseDaemon.apply_config` | same as the unified log |
| Cold storage `<session>/cold/` + `id_registry.json` + `cold-archive/` | `CentralStorage._resolve_cold_storage_dir`, `CircularLogPool._archiver`, `Registry._dump_id_registry` / `_compress_persisted_cold_storage` | persist old (same as shutdown does) and point a new archiver at the new `cold/` |
| Config snapshots `<cfg>.start/autosave/final.json` | `Registry.__init__`: `ConfigManager(..., autosave_path=get_session_path(suffix="autosave"))`, `save_full_config(...start)` | write `final` into old, `start` into new, re-point `ConfigManager.autosave_path` |
| GUI config/state snapshots, `gui/` | `FileManager.save_gui_*`, `_snapshot_master_to_session`, MainWindow's GUI `ConfigManager` autosave path | same as config snapshots |
| `playback_ranges.json` | `Registry._save_playback_ranges` via `get_playback_ranges_path()` | save to old, clear the in-memory store |

`session_lister.list_sessions` only needs a `metadata.json`, and `unified_log_parts` needs a
`session.*` file. So a correctly rotated folder shows up in "Load Session..." with no lister
changes.

## Design

### Entry point

`Registry.rotate_session()`, called from a new main-window toolbar **Clear** action. It runs
the heavy work off the UI thread through `system_ctx.tasks` and reports completion to the UI
(toast). Clear is disabled while a rotation is in progress and in replay mode.

### 1. `FileManager.rotate(display_name: Optional[str] = None) -> (old_dir, new_dir)`

`display_name`, if given, replaces `session_display_name` (sanitized with `_sanitize`, same as
at construction). It flows into both the new folder name and `metadata["project"]["display_name"]`,
which is what "Load Session..." shows. `None` keeps the current name.
`Registry.rotate_session(display_name=None)` passes it through.

- Finalize and write the old `metadata.json` (factor this out of `stop()` so both share it).
- Create the new session dir with `_create_session_dir()`. **Guard against collisions**: the
  name has one-second resolution and `mkdir(exist_ok=True)` would silently merge two sessions
  when Clear is pressed twice within a second.
- Build a fresh `metadata` dict (factor the construction out of `__init__`). Add
  `previous_session_id` here and `next_session_id` on the old one, so the chain is traceable.
- Keep `_file_loggers` registered. Their per-logger metadata entries are re-created at part 0.
- Re-point `ConfigManager.autosave_path` (registry config + GUI config) and write the
  `start`/`final` snapshots.

#### Close-time saves must also happen at rotation

Some state is only written when the program closes. A rotation has to write the same things
into the old session before switching folders, or the old session's archive would have no
`final` snapshots.

| Written only at close today | Where | At rotation |
|---|---|---|
| GUI state (window geometry, dock visibility, open tabs with each widget's `get_state()`, floating windows, current tab) | `UIStateHandler.get_data()` via `FileManager.save_gui()` → `save_gui_state("final")`, called from `MainWindow.closeEvent` | call `save_gui()` → updates the workspace master and writes the old session's `gui_state.final` |
| GUI config | `save_gui()` → `save_gui_config("final")` | same call |
| Registry config `final` snapshot | `FileManager.stop()` → `config.save_full_config(get_session_path("final"))` | factor out of `stop()` into a shared `finalize_session_snapshots()` used by both |
| GUI `ConfigManager` (`<cfg>.gui.json`: watches) - only its autosave goes to the session | `MainWindow.__init__` | `save_full_config()` of it to the old session's `gui.final`, then re-point its `autosave_path` |

**Threading:** `get_data()` walks live widgets (`get_state()`), so these saves must run on the
**UI thread**, *before* the background rotation task starts. The order is:

1. UI thread: `save_gui()` + registry/GUI config `final` → old session.
2. Background task: rotate `FileManager`, the loggers and the pool.
3. New session: `start` snapshots through the existing `_snapshot_master_to_session("gui_config"
   / "gui_state")` (it copies the workspace master saved in step 1) plus the registry config
   `start`.

`Registry` must not import Qt, so it takes an optional `before_rotate` callback (or MainWindow
does step 1 itself before calling `rotate_session()`). The second option is preferred - it keeps
the ordering visible in one place.

### 2. File loggers: rotate on their own thread

`FileLogger.run()` owns its file handle, so rotation must happen inside its loop, not from the
caller's thread.

- Add `FileLogger.request_session_rotation()`, which sets a flag or generation counter. The
  loop checks it between batches: flush, `_close_and_compress_final_part()` (already compresses
  and bumps `part_index`), reset `part_index = 0`, then `open_file()` resolves into the new
  `session_dir`.
- The ordering matters: `FileManager.session_dir` must switch before any logger reopens, and
  every logger must have closed its old part before the old metadata is written as finished.
  The rotation therefore waits (with a timeout) for each logger to acknowledge. Metadata writes
  from logger threads (`update_logger_stats`) currently go to whichever dir is current, so
  either take a lock or route writes by generation.

**Where the cut lands: at whatever each logger sees next (decided).** Batch queues are left
alone. Anything still waiting in a logger's `BatchQueue` when it sees the rotation flag is
written to the *new* session. This applies to the unified log and the raw source logs alike.
There is no sequence-based cut, no queue marker, and no batch splitting.

Consequence: each logger's boundary sits wherever its own backlog ended, typically a few
milliseconds apart. If §3 also rotates the pool, the in-memory/cold-storage boundary can differ
from the unified-log boundary by that same backlog. Tests assert "every row lands in exactly one
of the two folders' unified logs", not an exact split point.

### 3. Log pool: persist the old contents, start empty

Add `CircularLogPool.rotate_session(new_cold_dir)`, executed **on central's ingestion thread**
between batches (add a request flag to `CentralStorage.run`, same pattern as the loggers). This
makes the cut atomic with respect to ingestion.

- Persist the old contents the same way `Registry.stop()` does: move hot segments into the
  archiver (`release_all()`'s persist branch), then `_dump_id_registry(old_cold)`. Compress
  in the background (`_compress_persisted_cold_storage`), off central's thread.
- Create a fresh archiver at `<new_session>/cold/`.
- **Do not reset `self.sequence`.** `clear()` resets it to `SEQ_NONE`, which would break every
  widget's forward watermark (`latest_seq_seen`, `ModuleBuffer.last_seq`,
  `LatestModuleValueTracker.last_known_seq`). Sequences stay monotonic across sessions.
- Risk: flushing a large hot tier to disk synchronously on central's thread stalls ingestion
  (the input queue then drops under backpressure). Measure this. If it's too slow, hand the
  segment objects to the archiver and write them from a task. `archive()` already "takes
  ownership", so check whether it can write asynchronously.
- `PlaybackClock` and `LatestModuleValueTracker` hold the pool **object**. Mutating it in place
  (instead of replacing `central.log_pool`) keeps those references valid. The tracker still
  needs a `reset()` so it drops pre-rotation latest values.

### 4. UI

- Toolbar **Clear** button, as a `QToolButton` rather than a plain `QAction`, because it needs
  a right-click handler (same widget style as the existing `watch_button`):
  - **Left click** → `registry.rotate_session()`, keeping the current display name.
  - **Right click** (`setContextMenuPolicy(Qt.CustomContextMenu)` + `customContextMenuRequested`)
    → `QInputDialog.getText(...)`, prefilled with the current display name. OK with a non-empty
    name → `registry.rotate_session(display_name=name)`. Cancel or an empty name → nothing
    happens.
  - Tooltip mentions both ("Start a new session - right-click to name it").
  - Show a toast with progress from compressing the old session, and the new session's name
    when it's done.
- Views reset through a `registry.session_generation` counter that widgets poll in
  `apply_updates()`. On a change they call their existing `clear_logs()`/`clear()`: log
  viewer, log table viewer, plotter, telemetry table, TelemetryWatch. The playback clock goes
  LIVE, and the named-ranges combo empties because the store was cleared.
- Update the window title if it ever shows the session name (it currently shows
  project/profile only - no change needed).

### 5. Replay-side checks (no expected changes)

- Loading a rotated old session: metadata has `finished_at`, the unified log parts are
  complete, `cold/id_registry.json` is present, and `replay_session_bounds_ns` comes out
  correct.
- Loading the *new* session after exit: same checks.

## Phases

1. **FileManager + FileLogger rotation, pool untouched.** Rotation of metadata, logs and
   config snapshots. Not user-facing yet: the Clear button only lands in phase 3, once the
   pool rotates too.
2. **Pool + cold storage rotation** (§3), the tracker reset, and the ID-registry dump.
3. **UI wiring** (§4), plus replay verification (§5).

## Tests

- `FileManager.rotate`: under `tmp_path` (per the config-isolation memory, pass `config_path`
  under `tmp_path`). Checks that old metadata is finished, the new dir is unique even within
  the same second, metadata is chained, and the autosave paths are re-pointed.
- `FileLogger`: rows before and after a rotation request end up in the two folders; part
  indexes restart at 0; the old final part is compressed.
- Real-registry e2e (the "verify pipeline end-to-end" memory applies): with logging and cold
  storage enabled, push rows → rotate → push rows → stop. Each folder's unified log and
  cold-storage contents hold only its own rows, and `UnifiedLogReplay` of the old folder
  yields exactly the pre-Clear rows.
- A widget e2e for the view reset driven by `session_generation`.
- Close-time saves: with a real MainWindow and a tab open, rotate. The old session folder then
  has `gui_state.final`/`gui_config.final`/config `final` snapshots matching the current
  layout, and the new folder has the `start` snapshots.
- The Clear button: left click rotates with the unchanged name; right click → a monkeypatched
  `QInputDialog.getText` returning a name → the new folder and metadata carry the sanitized
  name; a cancelled dialog → no rotation.
- A stress test with ingestion running while rotating: every row lands in exactly one of the
  two unified logs (none dropped or duplicated), and ingestion doesn't stall past a budget.

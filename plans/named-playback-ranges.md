# Named playback ranges + precise jog-wheel scrubbing

## Status: implemented (2026-07-27)

Two features requested together, both motivated by the large disk-tiered scrollback added in
`plans/mmap-coldstore.md`: named time ranges ("clips", DaVinci-Resolve-style) and a press-and-drag
jog wheel for row-accurate scrubbing with the cursor hidden.

## Decisions made with the user before implementing

- **Range persistence**: saved alongside the session's own raw captured log data (a sidecar JSON
  in `FileManager.session_dir`), not the shared per-profile `gui_config.json` - so ranges travel
  with a specific recording, not a profile setting.
- **Scrub granularity**: exact log rows (not a fixed time increment) - one row is the atomic unit
  here, so stepping is frame-accurate the way video-editor frame-stepping is.
- **Scrub speed**: variable, driven by drag velocity - slow drag steps ~1 row/event, fast drag
  accelerates into a coarse shuttle.
- **Interaction model**: press-and-hold-drag jog wheel (Maya/Resolve-style), not click-to-arm.

## What was built

### 1. `core/playback_ranges.py` - data model + persistence

`PlaybackRange` (NamedTuple: id/name/start_ts_ns/end_ts_ns, with `.normalized()` for swapped
mark-in/mark-out order) and `PlaybackRangeStore` (add/remove/rename/clear, `on_change` callback,
JSON load/save). Plain Python, no Qt/filesystem coupling in the store itself - mirrors
`PlaybackClock`'s shape.

### 2. Persistence wiring - `storage/file_manager.py` + `core/registry.py`

- `FileManager.get_playback_ranges_path()` - a **fixed** filename (`playback_ranges.json`, not
  routed through `get_session_path`'s `<config_file_name>`-prefixed naming) so a later run can
  find it by simple, stable-name lookup.
- `Registry.playback_ranges` (a `PlaybackRangeStore`) is created alongside `playback_clock`.
  `_save_playback_ranges` (its `on_change` callback) always writes into *this* session's own
  folder.
- `Registry._discover_replay_ranges_path()` / `_load_replay_playback_ranges()`, called once
  `self.sources` is built: duck-types on a `file_path` attribute across every configured source
  (works for `BinaryFileReader`/`FileTailReader` without importing either), and if that file lives
  inside a previous session's folder containing a `playback_ranges.json`, merges it in
  (`replace=False`) - so replaying a previous capture (blinkview's existing dev-replay workflow)
  automatically brings back whatever ranges were saved during/after that original capture.

### 3. Row-accurate stepping - `core/numpy_log.py` + `core/playback_clock.py`

`CircularLogPool.find_ts_n_rows_away(current_ts_ns, delta_rows)` walks the combined hot+cold
chronological row sequence (via `get_snapshot()`/`get_reversed_snapshot()` and `np.searchsorted`
per segment) to find the timestamp exactly `delta_rows` positions away, clamping at either end.
Ties at `current_ts_ns` are treated as "at" the current position on both sides, so stepping by 1
from a tie always lands on a genuinely different row. Unfiltered (no level/module masking) -
`PlaybackClock` has no filter concept, and this is a fast plain-Python/numpy scan (small counts,
not a hot ingestion path), not a new Numba kernel.

`PlaybackClock.step_rows(delta_rows)` delegates to it, entering REPLAY and clamping exactly like
`seek()`.

### 4. `ui/widgets/jog_wheel_button.py` - the drag control

`JogWheelButton(QToolButton)`: press grabs the mouse and hides the cursor (`Qt.BlankCursor`); each
move computes `dx`/`dt` since the last event, maps it through `_velocity_to_row_delta` (a pure,
directly-unit-tested function - dead zone below `DEAD_ZONE_PX_S`, ~1 row at `REFERENCE_PX_S`,
superlinear acceleration above it via `ACCELERATION_EXPONENT`), accumulates fractional rows so
slow drags still make progress, and emits `stepRequested(int)`. After each move the cursor is
warped back to the press origin (`QCursor.setPos`) - the classic "infinite drag" technique, so a
long scrub session never runs off the screen edge; the resulting synthetic move event has `dx=0`
against that origin, which the dead-zone check already treats as a no-op, so no reentrancy guard
was needed. Release ungrabs/restores the cursor.

### 5. `ui/widgets/playback_control.py` - wiring

- `SeekBarWidget.set_ranges()`/paint: translucent bands drawn under the track for each range.
- `PlaybackControlWidget` gained: the jog wheel (`stepRequested -> clock.step_rows`), Mark-In/
  Mark-Out buttons (capture `clock.current_ts_ns`, Mark Out opens `QInputDialog.getText` for a
  name and calls `playback_ranges.add`), and a combo box listing ranges (selecting one seeks to
  its start). The combo only rebuilds when the range id set actually changes, so it doesn't reset
  the user's current selection on every heartbeat tick.

## Testing

Per this project's habit (and the `blinkview-playback-wiring` skill's explicit §6 requirement) of
verifying cross-cutting widget wiring with a *real* end-to-end test, not just isolated
kernel/class-level ones:

- `tests/test_playback_ranges.py` - `PlaybackRangeStore`/`PlaybackRange` in isolation (add/remove/
  rename/normalize/JSON round-trip/file persistence).
- `tests/test_playback_ranges_persistence.py` - real `Registry`: ranges actually land in the
  session folder on disk, and a second real `Registry` (simulating a replay run) discovers and
  merges a prior session's saved ranges via a duck-typed fake source's `file_path`.
- `tests/test_numpy_log.py::TestFindTsNRowsAway` - real `CircularLogPool`, multi-segment row
  stepping, clamping, and the timestamp-tie case.
- `tests/test_playback_clock.py` - `step_rows()` delegation/clamping against a fake pool.
- `tests/test_jog_wheel_button.py` - the velocity->row-delta curve as a pure function (dead zone,
  reference speed, "faster drag = more rows, not just more distance"), plus press/drag/release
  state transitions via synthesized `QMouseEvent`s.
- `tests/test_playback_control.py` - mark-in/mark-out/combo/jog-wheel wiring against a fake
  registry (extended `FakeRegistry`/`FakeLogPool` to carry `playback_ranges`/
  `find_ts_n_rows_away`).
- `tests/test_playback_control_e2e.py` - real `Registry`+`GUIContext`+`PlaybackControlWidget`,
  real ingested rows: jog-wheel steps land on exact real row timestamps (via the real
  `CircularLogPool.find_ts_n_rows_away`), mark-in/out persists a real range to a real session
  folder on disk, selecting a range seeks the real clock, and the seek bar's range-band paint path
  runs against real data.

Full suite: 1939 passed, 0 regressions.

## Follow-up: zoomed-in scrubber for the active range (2026-07-27)

Added a second row to `PlaybackControlWidget` (`zoom_row`, hidden unless a range is active):
a `SeekBarWidget` bounded to just the active range's `[start_ts_ns, end_ts_ns]` instead of the
whole session span, so a short named range can still be scrubbed precisely on a long recording
where the full-session seek bar would give it only a few pixels. `_active_range_id` tracks which
range is "zoomed" - set when selecting a range from the combo, or automatically after Mark
Out creates a new one; cleared (hiding the row) if that range is later removed from the store.
`_sync_zoom_bar()` (called from `_sync_from_clock`) drives it the same way `_sync_ranges()` drives
the combo/bands. The zoom bar's own `seekRequested`/`scrubStarted`/`scrubEnded` reuse the exact
same handlers as the main seek bar, so it drives the same shared `PlaybackClock` - it's a
different view onto the same timeline, not a separate scrub mode. Covered in
`tests/test_playback_control.py::TestZoomSeekBar`.

## Follow-up: auto-enter DVR mode + default "whole recording" range on replay (2026-07-27)

Loading a replay (any configured source reading from a file rather than a live device) now:

1. **Auto-enters REPLAY mode immediately**, without waiting for the user to notice they loaded a
   replay and click the status button. `Registry._enter_replay_mode_if_detected()` (called from
   `configure_system()`, right after `_load_replay_playback_ranges()`) detects this via
   `_is_replay_session()` - the same `file_path` duck-typing `_discover_replay_ranges_path`
   already used, now factored out into a shared `_iter_replay_source_dirs()` generator.
2. **Creates a default "Full recording" named range** spanning the *entire* replayed session,
   sourced from that session's own `metadata.json` (`created_at`/`finished_at`, written by
   `FileManager`) rather than the currently-streamed-in `PlaybackClock` bounds - the file replays
   at a throttled rate (mimicking live ingestion), so bounds only reflect what's arrived so far;
   metadata gives the true full extent immediately, without waiting for the replay to catch up
   or building any bounds-tracking/persistence-throttling machinery. Matched by name
   (`Registry.DEFAULT_REPLAY_RANGE_NAME`), not id, so replaying a replay-of-a-replay doesn't pile
   up duplicates across generations - `_load_replay_playback_ranges()`'s merge already carries
   the name forward.
3. **Lands the playhead at the start of the recording once real data exists** -
   `PlaybackClock.enter_replay_when_ready(at_ts_ns)` is like `enter_replay()` but safe to call
   before the pool has any data (bounds still `[0, 0]`, so an immediate clamp would force
   `current_ts_ns` to 0): it flips to REPLAY mode right away but defers the actual seek to the
   first `tick()` where `bounds_max_ns` becomes nonzero. Any real `seek()`/`step_rows()`/
   `go_live()`/`enter_replay()` call in the meantime cancels the pending one (`_cancel_pending_seek()`),
   so a user action can never be clobbered by a stale deferred target landing later.

Metadata timestamps are wall-clock (`datetime.now(timezone.utc)`), parsed to epoch-ns on the
assumption that log row timestamps are real epoch time too (true for this app's normal ingestion
path) - a close-enough default marker, not pixel-precise (device clock sync lag between session
start and the first actual log row can shift it slightly); the user can always re-mark it via
Mark In/Mark Out.

Tests: `tests/test_playback_clock.py::TestEnterReplayWhenReady` (immediate vs. deferred seek,
cancellation by a real seek/go_live before the deferred one resolves), and
`tests/test_registry_replay_autoenter.py` (real `Registry` + real prior session with genuine
`metadata.json` via `FileManager.stop()` - default range creation/bounds, no-metadata and
no-file-source no-ops, dedup across replay generations, and an end-to-end check that the
playhead actually lands on the metadata-derived start once real rows are ingested).

## Follow-up: the auto-enter-replay feature above didn't fire for the real "Load Session..." path (2026-07-27)

Root cause: this app has **two** separate replay mechanisms, and the previous follow-up only
wired the wrong one.

- The **dev-replay workflow**: a configured `BinaryFileReader`/`FileTailReader` source whose
  `file_path` happens to point at a file inside a previous session's folder. Detected via
  `file_path` duck-typing across `registry.sources` - this is what
  `_enter_replay_mode_if_detected()` (previous follow-up) auto-triggers on at
  `configure_system()` time.
- The **real production path**: the "Load Session..." menu / `blink replay <session>` CLI, which
  goes through `MainWindow.start_replay(session_info)` -> constructs a `UnifiedLogReplay` and
  subscribes it **directly to `registry.central`**, entirely bypassing `registry.sources`. It
  also runs well after `configure_system()` already finished (at menu-click time, or via a
  `QTimer.singleShot` in `ui/run.py` for the CLI case) - so nothing at `configure_system()` time
  could have detected it even in principle.

Since the previous follow-up's logic only lived inside `configure_system()`/`_enter_replay_mode_
if_detected()`, none of it - not auto-entering REPLAY, not the default range, not even loading
the session's own saved `playback_ranges.json` - ever ran for a real "Load Session..." load.

Fix: extracted the shared logic into a new public `Registry.load_replay_session(session_dir)` -
merges in `session_dir/playback_ranges.json`, builds the `DEFAULT_REPLAY_RANGE_NAME` range from
`session_dir/metadata.json`, and calls `enter_replay_when_ready()`, exactly like before. Both
entry points now call it:

- `_enter_replay_mode_if_detected()` (dev-replay workflow) searches all candidate source
  directories for one with either sidecar file (not just the first, matching what
  `_discover_replay_ranges_path` used to do alone) and calls `load_replay_session` on it.
- `MainWindow.start_replay()` (`ui/main_window.py`) calls `registry.load_replay_session(session_
  info.path)` directly right after starting the `UnifiedLogReplay` - `session_info.path` (from
  `utils/session_lister.py`'s already-parsed `SessionInfo`) is exactly the session's own folder,
  no discovery needed.

`configure_system()` no longer calls the old standalone `_load_replay_playback_ranges()`
separately (now redundant - `load_replay_session` covers it), though that method is kept as-is
for its own existing direct-call tests.

Tests: `tests/test_registry_replay_autoenter.py::TestLoadReplaySession` covers
`load_replay_session` directly (ranges + default range + replay mode together, string-vs-Path
input, and the no-`finished_at` / still-active-session case). `MainWindow.start_replay` itself
has no direct test (no existing precedent for testing `main_window.py`) - the one line it adds is
a straight call into the now-well-tested `load_replay_session`.

## Known gap - not testable from here

The jog wheel's actual *feel* (cursor truly hidden, drag truly infinite, acceleration curve
feeling right at the keyboard) needs a real interactive session - `QCursor.setPos`/cursor-hiding
under the offscreen Qt platform used by the test suite doesn't exercise real OS cursor behavior.
The pure velocity-curve math and the Qt state-machine (grab/dragging flag/signal emission) are
covered directly; the tuning constants (`DEAD_ZONE_PX_S=8`, `REFERENCE_PX_S=60`,
`ACCELERATION_EXPONENT=1.6`) are reasonable starting points, not validated against a real trackpad/
mouse feel. Worth running the app and adjusting them if the shuttle feels too twitchy or too slow.

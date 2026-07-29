---
name: blinkview-playback-wiring
description: Use when wiring the global registry.playback_clock (LIVE/REPLAY scrubbing) into a new UI widget in blinkview - log/table/telemetry views, or anything else that needs to "follow" playback instead of live data. Covers the PlaybackClock/PlaybackControlWidget contract, the shared PlaybackFollowMachine (core/playback_follow.py) that LogViewerWidget/LogTableViewerWidget/TelemetryTable drive, the dirty-flag/buffer-duck-typing traps the machine's transition table structurally prevents, when a small "follow window" vs. a large "browse window" actually matters, how to add a timestamp-bounded backend fetch next to an existing sequence-watermark one, and why kernel/buffer-class unit tests alone are not enough to catch the real bugs here.
---

# Wiring registry.playback_clock into a new widget (blinkview)

Lessons from wiring the global playback clock into `LogViewerWidget` (`ui/widgets/log_viewer.py`),
`LogTableViewerWidget` (`ui/widgets/log_table_viewer.py`), `TelemetryTable`
(`ui/widgets/telemetry_table.py`), and `TelemetryPlotter` (`ui/widgets/plotter.py`). Read this
before adding REPLAY-scrubbing support to another widget, or before touching any of the four
implementations above.

The first three widgets share `core/playback_follow.py`'s `PlaybackFollowMachine` (see
`plans/playback-follow-state-machine.md` for the design history) - a plain-Python state machine
that replaced what used to be four independently-reimplemented booleans per widget
(`follow_playback`/`is_paused`/`_playback_anchored`/`force_live`) with one `FollowState` enum
(`LIVE`/`FOLLOWING`/`FROZEN`) plus an explicit transition table. `TelemetryPlotter` predates this
and still uses the older raw-boolean pattern described in §2 below - it hasn't been migrated.

## 1. The clock contract - read-only, single ticker

`registry.playback_clock` (`core/playback_clock.py`) is a plain-Python `PlaybackClock`: `mode`
(`PlaybackMode.LIVE`/`REPLAY`), `current_ts_ns` (virtual time cursor, epoch ns), `is_playing`,
`speed`, `bounds_min_ns`/`bounds_max_ns`. **Only `PlaybackControlWidget` ever calls
`clock.tick(...)`** - it's constructed first in `BlinkMainWindow.__init__`, before any tab, so it
always ticks before any other widget's `apply_updates()` runs in the same `GUIContext` heartbeat.
Every consumer must only *read* `mode`/`current_ts_ns`/`is_playing` - calling `tick()` a second
time double-advances `current_ts_ns` against a stale wall-clock delta.

Add a `_clock()` helper to any new widget, verbatim:
```python
def _clock(self):
    registry = self.gui_context.registry
    return registry.playback_clock if registry is not None else None
```

## 2. `PlaybackFollowMachine` - one state machine, shared, instead of four booleans per widget

`core/playback_follow.py` (plain Python, no Qt, mirrors `PlaybackClock`'s own read-only-consumer
style). Each widget owns one instance and drives it entirely through `handle(event, clock_snapshot)`
- never mutates the resulting `.state`/`.force_live` directly except in the widget's own
`_redraw_history()`/`_go_live()` "make it live" mechanic (see below).

- **States**: `FollowState.LIVE` (showing the true live tail), `FOLLOWING` (ts-anchored to
  `clock.current_ts_ns`, re-fetched every tick), `FROZEN` (anchored to a fixed window; only an
  explicit resume/scroll-to-live-edge event exits it).
- **Events** (`FollowEvent.*`): `Tick` (every `apply_updates()` heartbeat), `ScrolledAway`,
  `ScrolledToLiveEdge`, `TogglePause(checked)`, `ToggleForceLive(checked)`, `ClogDetected`.
- **`force_live` is a policy flag on the machine, not a fourth state.** It only redirects one
  transition (`Tick` while `REPLAY` -> `LIVE` instead of `FOLLOWING`) and is checked again on
  `TogglePause(False)` (resume) to decide `LIVE` vs. rejoining `FOLLOWING`. This is what collapses
  what used to be a combinatorial flag space into three states.
- **`handle()` returns a `FollowAction`** (`kind`: `NOOP`/`FETCH_LIVE`/`FETCH_FOLLOWING`/`FREEZE`,
  plus `anchor_ts_ns`, `auto`, and `from_state`) telling the widget *what* to do - the machine
  never touches Qt or fetch mechanics itself. `from_state` matters for `FREEZE`: transitioning from
  `LIVE` means picking a fresh anchor from the live buffer (`_enter_history_mode`/
  `_enter_history_at_top_row`); transitioning from `FOLLOWING` means the ts-anchored window already
  on screen is kept in place, just marked frozen - no re-fetch.
- **`supports_freeze=False`** (used by `TelemetryTable`, which has no scroll/pause concept) makes
  `ScrolledAway`/`ClogDetected`/`TogglePause(True)` silent no-ops instead of needing to be
  special-cased by the caller - the widget can wire up the full event set unconditionally.
- **The "make it live" mechanic forces state directly, not via an event.** `LogViewerWidget.
  _redraw_history()`/`LogTableViewerWidget._go_live()` set `self._playback.state = FollowState.LIVE`
  unconditionally at the end, rather than trusting every caller to have already transitioned it
  through `handle()`. This matters because several call sites (`_refresh_view()`'s already-live
  shortcut, `_reanchor_history()`'s "caught up while paging forward" fallback) redraw live content
  without themselves being a machine-event site - without the direct force, those paths would leave
  `.state` stuck at `FROZEN` while the view has already gone live. Mirrors the pre-machine
  behavior, where `_set_pause_ui(False)` unconditionally cleared `is_paused` regardless of caller.
- **Read-only compat properties, not raw attributes.** Each widget exposes `is_paused`/
  `follow_playback`/`_playback_anchored`/`force_live` as `@property`s derived from
  `self._playback.state`/`.force_live` (plus, for `_playback_anchored`, the widget's own
  `view_mode`/`model.mode` - see the next bullet) - kept for every existing call site/test that
  reads the old names, deliberately not reintroducing them as independently-settable state.
- **`_playback_anchored` needs one more condition than raw `state is FOLLOWING`.** A `Tick` can
  transition `LIVE -> FOLLOWING` before its own fetch has found anything at that instant
  (`_reanchor_history` no-ops, leaving `view_mode`/`model.mode` untouched) - the machine's `state`
  becomes `FOLLOWING` optimistically regardless, since it's a "should we be attempting to follow"
  signal, not a "did the last attempt succeed" one. The property therefore checks BOTH: `self.
  _playback.state is FollowState.FOLLOWING and self.view_mode == LogViewMode.HISTORY` (
  `LogTableViewerWidget` additionally checks `self.model.row_count > 0`). Branch code that decides
  whether a manual scroll should freeze-in-place vs. anchor-from-the-live-buffer must use this
  combined property, not raw `self._playback.state`, or a scroll during that no-data instant would
  wrongly try to freeze a ts-anchored window that was never actually populated.
- **Writing a new widget-level unit test?** Give the stub a real `PlaybackFollowMachine()` (it's
  plain Python, trivially constructible) rather than plain booleans - `handle()` mutates it for
  real, so assertions read `.state`/`.force_live` directly instead of trying to fake mutation of a
  `SimpleNamespace` attribute. See `tests/test_log_viewer_scrub.py`/
  `tests/test_log_table_viewer_scrub.py` for the pattern.

## 3. The dirty-flag / buffer-duck-typing traps the machine's structure prevents

These are the two bugs that motivated the design above - kept here as historical rationale for
*why* the machine's structure looks the way it does, not as a live bug list (both are now
structurally prevented, see the callouts below and Phase 0's transition-table tests in
`tests/test_playback_follow_machine.py`). Neither was caught by per-piece unit tests - see §6.

**Trap A - freezing your own following.** `LogViewerWidget`'s original `_reanchor_history` called
`_set_pause_ui(True)` on every LIVE→HISTORY transition (which set `self.is_paused = True`, and the
Pause button visually flipped to "Resume"). The very first REPLAY-follow tick *is* a LIVE→HISTORY-
shaped transition, so this fired for it too - which then blocked every subsequent follow tick,
since the follow condition required `not self.is_paused`. Net effect: the widget followed exactly
once per REPLAY session, then silently froze until the user manually clicked Resume. The original
fix was a guard (`if was_live and anchor_ts is None: ...`); the machine now prevents this class of
bug structurally - `FREEZE` is a distinct `FollowAction` kind from `FETCH_FOLLOWING`, and only ever
fires from `ScrolledAway`/`ClogDetected`/`TogglePause(True)`, never from a plain `Tick` transition
into `FOLLOWING` (see `tests/test_playback_follow_machine.py::TestLiveStateTicks::
test_opening_while_replay_already_active_follows_without_a_pause`).

The inverse bug shape: after a user *manually* detaches (scrolls/pans away from a followed view),
if nothing marks the widget as "frozen", there is no way back in - a Pause/Resume-style button
still shows "unpaused" and clicking it tries to *pause* an already-detached view instead of
*resuming* following. The machine's `ScrolledAway` handling always transitions to `FROZEN`
regardless of prior state (`LIVE` or `FOLLOWING`), so this pairing can't be forgotten per call site
the way it could when each widget hand-rolled its own detach logic. If wiring a *new* widget that
doesn't go through the shared machine (e.g. extending `TelemetryPlotter`), still apply this lesson
manually: always pair a detach with whatever your widget's "I am frozen, here is how to un-freeze"
state/affordance is.

**Trap B - a new render buffer missing dirty-tracking fields an old one has.** When a widget's
render path (`_update_plots`/`_update_overview` in `TelemetryPlotter`) reads `buf.is_dirty`/
`buf.is_dirty_overview`/`buf.head` off whichever buffer object happens to be "active" this tick, a
*new* buffer class built to hold REPLAY-scrub data (`ReplayWindowBuffer` in `core/buffers.py`) must
carry the **exact same duck-typed attribute surface** as the original live buffer class it's meant
to substitute for (`ModuleBuffer`), not just the same `.bundle()` return contract. Missing
`is_dirty`/`is_dirty_overview` here shipped as a live `AttributeError` crash the first time REPLAY
mode actually tried to render - every kernel-level and buffer-class-level unit test passed, because
none of them called the render path with the new buffer type. Before shipping a new buffer/state
class meant to be swapped in for an existing one:
- Grep every call site that reads attributes off "whichever buffer is active" (not just the ones
  you touched) and check each field's presence on the new class too.
- If the new class doesn't have a natural equivalent of an existing field (e.g. `ModuleBuffer.head`
  is a ring-buffer write cursor; `ReplayWindowBuffer` never wraps), add a `@property` that returns
  the semantically-equivalent value (`head` -> `size`, for a flat non-wrapping buffer) rather than
  changing every call site to branch on buffer type.
- Any per-tick "did anything change" flag (`is_dirty`) needs the *same* set-on-update/clear-after-
  consumers-run lifecycle as the original - including in every place the original gets swept back
  to `False` (there were two separate reset loops in `apply_updates()`; both needed a second loop
  added for the new buffer dict).

## 4. Small "follow window" vs. large "browse window" - only where it actually matters

`LogViewerWidget` follows the clock by re-fetching a *small* window every ~100ms while playing
(sized off the actual visible row count via `visible_row_count()`/`_update_viewport_row_budget`,
see §7), then upgrades to its full manual-browsing window size (`HISTORY_BEFORE`/`HISTORY_AFTER`)
the instant the user scrolls and detaches. This exists because *rendering* text rows has a real,
row-count-proportional cost (`QPlainTextEdit.setPlainText`/highlighter re-run).

`TelemetryPlotter` does **not** need this same follow/browse cap distinction, because its render
path is already downsample-bounded (`nb_slice_and_downsample` slices+bins to a fixed output size
regardless of how many raw samples were fetched) - the only real cost driver there is *how many raw
samples get pulled from `log_pool`*, which scales with `view_duration × sample_rate`, not with
follow-vs-browse state. A single fetch-cost safety cap (`TelemetryPlotter.REPLAY_FETCH_CAP`) used
identically in both the follow and manual-pan code paths was sufficient; the *span* passed to the
fetch (`before_span_ns`/`after_span_ns`) is derived from whatever time range is actually going to
be rendered (half the view duration while following; the literal panned-to range once detached),
not from a separate small/large constant pair.

**Lesson**: don't reflexively copy the log-viewer's two-tier cap design onto a new widget - check
first whether that widget's *render* cost actually scales with raw fetched-row count, or whether
it's already bounded by something else downstream (a downsampler, a fixed-size viewport, etc).

## 5. Adding a timestamp-bounded fetch next to an existing sequence-watermark one

Both widgets needed a "fetch data for an arbitrary point in the past" path where the existing
backend only supported "fetch everything newer than a forward watermark":

- `ops/segments.py`'s `segment_filter`/`segment_filter_reversed` already accepted `start_ts`/
  `end_ts` as full alternatives to `start_seq`/`end_seq` (an earlier, unrelated addition) - no new
  kernel needed for `LogViewerWidget`, just a new caller passing ts bounds instead of seq bounds.
- `ops/telemetry.py` had **no** ts-bounded extraction kernel at all (only a backward,
  seq-watermark-only scan, `nb_extract_telemetry_segment_to_end`). Two new kernels were added,
  `nb_extract_telemetry_segment_window_backward`/`_forward`, each reusing the exact binary-search
  bounding technique already established in `ops/segments.py` (`nb_fast_find_first_ge`/
  `nb_fast_find_first_gt`) plus the existing float-extraction primitive
  (`nb_extract_floats_from_bytes`). Bundle the two ts bounds into a small `NamedTuple`
  (`TsWindowBundle`) rather than adding two more positional params, per the `numba-njit` skill §2.
- **Bound semantics to get right** (verified with direct kernel tests, not just read from the
  source): `end_ts` is an *inclusive* upper bound; `start_ts` is an *inclusive* lower bound -
  unlike `start_seq`, which is exclusive. A "before anchor" scan must pass `end_ts = anchor - 1` to
  exclude the anchor row itself; an "after anchor" scan passes `start_ts = anchor` directly (no
  `-1`) since it's already inclusive. Getting this asymmetry backwards double-counts or drops the
  anchor row.
- Write the new orchestration function (`fetch_telemetry_window` in `core/numpy_log.py`) so a
  backward scan (newest-to-oldest, writing right-to-left into the output array) and a forward scan
  (oldest-to-newest, writing left-to-right) land pre-sorted ascending when concatenated - no merge/
  sort step needed. This mirrors `log_viewer.py`'s `_fetch_history_window`'s before/after split.
- **Batch-lifetime gotcha writing a test for this**: `PooledLogBatch.__exit__` calls `.release()`.
  Any call that pushes the batch into the pool (`log_pool.batch_append(batch)`,
  `registry.central.put(batch)`) must happen *inside* the `with batch:` block, not after - calling
  it after silently operates on an already-released (emptied) batch, and looks like "the fetch
  found nothing" rather than an obvious error.

## 6. Unit tests on kernels/buffer classes in isolation are necessary but not sufficient

Every piece passed its own unit tests (kernel-level extraction tests against a real
`CircularLogPool`, `ReplayWindowBuffer` construction/update/bundle tests) before the
`AttributeError` in §3-Trap-B ever shipped - because no test actually constructed the real widget
and called `apply_updates()`/the render methods together. For any widget-level playback wiring,
also write **one real end-to-end test** that:
- Builds a real `Registry` + `GUIContext` (see `tests/test_plotter_playback.py` for the minimal
  construction recipe - `Registry(...).configure_system()`, `GUIContext().set_registry(...)`/
  `set_theme(StyleConfig())`, a real device/module via `registry.id_registry`, real data pushed via
  `array_pool.create(PooledLogBatch, ...)` + `log_pool.batch_append(...)` inside the batch's `with`
  block).
- Constructs the actual widget class against that context (use the existing `qapp` session fixture
  from `tests/conftest.py`).
- Drives it through the full state cycle: LIVE fetch → enter REPLAY → several follow ticks
  (`clock.tick(...)` then `widget.apply_updates(force=True)`) → simulate the widget's manual-
  detach trigger directly (call the pan/scroll handler, don't try to synthesize real mouse events)
  → `clear()` while still in REPLAY → `clock.go_live()` → confirm `follow_playback` reset.

For the three `PlaybackFollowMachine`-backed widgets, also add pure transition-table tests to
`tests/test_playback_follow_machine.py` for any new state/event combination - these are fast (no
Qt/Registry) and are what makes the end-to-end test's failures easy to localize, but they're
**additive, not a replacement**: the machine's own tests can't catch a widget wiring the right
`FollowAction.kind` to the wrong fetch call, or a fetch mechanic silently drifting out of sync with
what `view_mode`/`model.mode` claims - only the real end-to-end test below catches that class of
bug, same as it originally caught Trap B.

This is the level that actually catches missing-attribute/wrong-buffer-selected bugs; kernel- and
class-level tests should still exist (they're what make the end-to-end test's failures easy to
localize), just don't treat them as sufficient on their own for this kind of cross-cutting wiring.

## 7. Viewport-derived sizing needs `showEvent`, not just `resizeEvent`

If a widget sizes anything off its own current viewport (`LogViewerWidget.max_rows`, derived from
`SearchableLogArea.visible_row_count()`), computing it once in `__init__` and then trusting
`resizeEvent` to correct it later is not reliable at startup. A widget's real geometry (final
splitter allocation, whether it's the initially-active tab) frequently isn't settled until Qt
actually shows it - `resizeEvent` can fire during construction/initial layout with a transient,
too-small size that then simply never fires again (a tab that's created already at "full size" and
never subsequently resized). Add a `showEvent` override that also recomputes the sizing, in
addition to `resizeEvent`:
```python
def showEvent(self, event):
    super().showEvent(event)
    self._update_viewport_row_budget()
```
Keep the `resizeEvent` hook too (splitter drags/window resizes after the initial show still need
to recompute) - the two are complementary, not a replacement for each other.

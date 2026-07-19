---
name: blinkview-playback-wiring
description: Use when wiring the global registry.playback_clock (LIVE/REPLAY scrubbing) into a new UI widget in blinkview - log/table/telemetry views, or anything else that needs to "follow" playback instead of live data. Covers the PlaybackClock/PlaybackControlWidget contract, the follow_playback local-override pattern, the is_paused/dirty-flag traps that silently break continuous following or crash mid-render, when a small "follow window" vs. a large "browse window" actually matters, how to add a timestamp-bounded backend fetch next to an existing sequence-watermark one, and why kernel/buffer-class unit tests alone are not enough to catch the real bugs here.
---

# Wiring registry.playback_clock into a new widget (blinkview)

Lessons from wiring the global playback clock into `LogViewerWidget` (`ui/widgets/log_viewer.py`)
and `TelemetryPlotter` (`ui/widgets/plotter.py`). Read this before adding REPLAY-scrubbing support
to another widget (e.g. `LogTableViewerWidget`, `TelemetryTable`), or before touching either of the
two implementations above.

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

## 2. `follow_playback` - a per-widget local override, not a global flag

Every open widget follows REPLAY automatically by default (one global transport, matches its
one-instance-per-session design) - but a widget the user manually interacts with (scrolls a log
view, pans a plot) should detach *itself* from following without affecting the clock or any other
open widget. Pattern: a per-widget `self.follow_playback = True` bool.

- **Detach**: set `False` the moment the user manually navigates (scroll away from the tail, pan/
  zoom a plot). Never let scrolling back to "wherever the clock currently is" implicitly re-attach
  it - only two things may reset it to `True`:
  1. The clock itself transitioning back to `LIVE` (do this once, e.g. right after computing
     `clock = self._clock()` at the top of `apply_updates()`: `if clock is not None and clock.mode
     is PlaybackMode.LIVE: self.follow_playback = True`).
  2. An explicit user "Resume following" action, if the widget has one (`LogViewerWidget`'s Pause/
     Resume button - see §3).
- **Never** reset it merely because the user's manual pan happened to land back on the live edge/
  current playhead position while the clock is still `REPLAY` - that's surprising and makes
  "detach" feel unreliable.

## 3. The is_paused / dirty-flag traps - both caused real, shipped bugs

Two related but distinct bugs, both only surfaced once the whole thing ran together (not caught by
per-piece unit tests - see §6):

**Trap A - freezing your own following.** `LogViewerWidget`'s existing `_reanchor_history` called
`_set_pause_ui(True)` on every LIVE→HISTORY transition (which sets `self.is_paused = True`, and
the Pause button visually flips to "Resume"). The very first REPLAY-follow tick *is* a LIVE→
HISTORY-shaped transition, so this fired for it too - which then blocked every subsequent follow
tick, since the follow condition required `not self.is_paused`. Net effect: the widget followed
exactly once per REPLAY session, then silently froze until the user manually clicked Resume. Fix:
gate that specific pause-setting call so it only fires for the *manual/seq-anchored* transition
path, never the ts-anchored/follow one (`if was_live and anchor_ts is None: ...`).

The inverse bug shape: after a user *manually* detaches (scrolls/pans away from a followed view),
if nothing marks the widget as "frozen" (e.g. never calls the equivalent of `_set_pause_ui(True)`),
there is no way back in - a Pause/Resume-style button still shows "unpaused" and clicking it tries
to *pause* an already-detached view instead of *resuming* following. Always pair a detach with
whatever your widget's "I am frozen, here is how to un-freeze" state/affordance is, even if
nothing else about the frozen behavior needs to change.

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

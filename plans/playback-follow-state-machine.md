# A proper state machine for the playback-follow / force-live system

**Status**: implemented (2026-07-29), all 4 phases. `core/playback_follow.py`
(`PlaybackFollowMachine`/`FollowState`/`FollowEvent`/`FollowAction`) is shared by `TelemetryTable`,
`LogViewerWidget`, and `LogTableViewerWidget`; `TelemetryPlotter` was out of scope and still uses
the older raw-boolean pattern. `blinkview-playback-wiring` skill §2/§3 rewritten to document the
machine as current design. Full suite (2214 tests) green throughout every phase.

## What actually shipped vs. the plan below

- **Open question 1** (does `model.mode`/`view_mode` become redundant?) resolved as planned: it
  stays a separate widget-owned concept. Turned out to need one extra wrinkle - `_playback_anchored`
  couldn't be a raw `state is FOLLOWING` check, because a `Tick` can optimistically transition into
  `FOLLOWING` before its own fetch has found anything at that instant. The property became `state
  is FOLLOWING AND view_mode == HISTORY` (`LogTableViewerWidget` additionally checks `row_count >
  0`), preserving the exact invariant the old raw flag had by only ever being set `True` after a
  successful fetch. See skill §2's writeup.
- **Open question 2** (is `TelemetryTable` well-served by the same class?): yes, cleanly - it uses
  exactly the `LIVE`/`FOLLOWING` half of the table with `supports_freeze=False`, and its
  `apply_updates()` dispatch is ~5 lines shorter than before.
- **Open question 3** (`is_scrubbing`): resolved as leaned - stayed a widget-level pre-filter
  (checked before ever calling `handle()`), not part of `ClockSnapshot`/the machine.
- **Open question 4** (naming collision): `FollowState.LIVE` vs `PlaybackMode.LIVE` - kept both
  names as planned; no confusion in practice since they're always accessed via distinct enum types.
- **New finding not anticipated in the plan**: `_redraw_history()`/`_go_live()` needed to force
  `self._playback.state = FollowState.LIVE` directly (not via an event) rather than trusting every
  caller to have already transitioned it - several existing call sites (`_refresh_view()`'s
  already-live shortcut, `_reanchor_history()`'s "caught up while paging forward" fallback) redraw
  live content without themselves being a machine-event site. Without the direct force, those paths
  would've left `.state` stuck at `FROZEN` while the view had already gone live - a state/view
  desync the pre-machine code never had, since `is_paused` was a plain flag `_set_pause_ui(False)`
  already unconditionally cleared regardless of caller. Forcing state directly in the two "make it
  live" mechanics reproduces that same unconditional-clear behavior exactly.
- **Test strategy**: went with option (a) (keep the same read surface via read-only `@property`s)
  and treated it as the *permanent* answer, not a temporary Phase-4 stepping stone to option (b) -
  the properties are legitimate, not a hack, and a mechanical rename across 6 test files for no
  behavioral benefit wasn't worth the risk. Stub-based unit tests
  (`test_log_viewer_scrub.py`/`test_log_table_viewer_scrub.py`/`test_log_table_viewer_widget.py`)
  were updated to give their `SimpleNamespace` stubs a real `PlaybackFollowMachine()` instance
  (plain Python, trivially constructible) so `handle()` mutates it for real, rather than trying to
  fake mutation of a plain attribute.

---

**Original plan below**, kept for the design rationale (states/events/transition table) that's now
also documented in the skill.

## Context

`LogViewerWidget`, `LogTableViewerWidget`, and `TelemetryTable` each independently track "is this
widget following the global `registry.playback_clock`, paused, clock-anchored, or pinned live" via
a cluster of instance-attribute booleans, re-derived per widget:

- `follow_playback` - local override: has the user detached this widget from following REPLAY
- `is_paused` - manual freeze (scroll-away, explicit Pause click, or clog auto-pause)
- `_playback_anchored` - is the currently-displayed window actually clock-anchored right now
- `force_live` - added most recently (see `plans/` conversation this doc follows on from): pins
  the widget to the live tail regardless of clock mode
- `view_mode` (`LogViewerWidget`) / `model.mode` (`LogTableViewerWidget`) - LIVE vs HISTORY,
  a semi-independent rendering-level flag that's supposed to track the above but is set/read at
  separate call sites

None of this is wrong today - it's tested, and the `blinkview-playback-wiring` skill catalogs the
sharp edges (Trap A: a pause-setting call firing on the wrong transition edge silently froze
REPLAY-following after one tick; Trap B: a new buffer class missing a duck-typed attribute the
render path expected). But it's a state machine implemented as four loose booleans plus scattered
`if`-guards, duplicated three times with small variations, instead of one explicit machine. Adding
`force_live` this session meant auditing every existing `if` branch in two widgets to see whether
it also needed `and not self.force_live` bolted on - that audit cost is exactly what a real state
machine would eliminate for the *next* addition.

## Current state audit

| Widget | Has `is_paused`/scroll-detach? | Has `force_live`? | Fetch mechanism |
|---|---|---|---|
| `LogViewerWidget` | yes (`_enter_history_mode`, `_on_scroll_value_changed`) | yes (just added) | text-area append/reanchor via `LogTextFetcher` |
| `LogTableViewerWidget` | yes, near-identical to above | yes (just added) | `LogTableStore` model fetch |
| `TelemetryTable` | no - always follows or is forced live, no manual scroll/pause concept | yes (just added) | `LatestModuleValueTracker` snapshot swap |

`TelemetryTable` is structurally simpler - it never freezes on a browsable window, so it only ever
needs two of the three states below. That asymmetry matters for the design (see "Non-goals").

## Proposed design

### States (a real enum, not booleans)

```python
class FollowState(Enum):
    LIVE = "live"          # showing the true live tail
    FOLLOWING = "following"  # ts-anchored to clock.current_ts_ns, re-fetches every tick
    FROZEN = "frozen"       # anchored to a fixed seq/ts window; only a Resume-equivalent exits
```

Key simplification: **`force_live` is not a fourth orthogonal state.** It's a per-widget policy
flag that changes which state a `ClockTick(REPLAY)` event transitions *into* (`LIVE` instead of
`FOLLOWING`) - not a new state to cross with the other three. Once this collapses out, the
"combinatorial flag space" problem mostly disappears: `is_paused`/`_playback_anchored`/
`follow_playback` were four names for overlapping facts about which of exactly three states the
widget is in.

### Events

```python
class FollowEvent:
    ClockTick(mode, current_ts_ns, is_playing, is_scrubbing)  # every apply_updates() heartbeat
    UserScrolledAway                                          # manual scroll off the live/followed edge
    UserScrolledToLiveEdge                                    # manual scroll back to the true live tail
    UserTogglePause(checked: bool)                             # explicit Pause/Resume button
    UserToggleForceLive(checked: bool)                         # the Live override button
    ClogDetected                                               # velocity-tracker auto-pause trigger
```

### Transition table (draft - to be finalized against every existing test case)

| From | Event | Guard | To | Action |
|---|---|---|---|---|
| `LIVE` | `ClockTick(REPLAY)` | `not force_live` | `FOLLOWING` | fetch ts-anchored window |
| `LIVE` | `ClockTick(REPLAY)` | `force_live` | `LIVE` | no-op |
| `LIVE` | `UserScrolledAway` | - | `FROZEN` | anchor on current top-of-viewport row |
| `LIVE` | `ClogDetected` | - | `FROZEN` (auto) | anchor on current top-of-viewport row |
| `FOLLOWING` | `ClockTick(REPLAY, ts changed)` | - | `FOLLOWING` | re-fetch ts-anchored window |
| `FOLLOWING` | `ClockTick(LIVE)` | - | `LIVE` | fetch live tail |
| `FOLLOWING` | `UserScrolledAway` | - | `FROZEN` | freeze current window, clear local follow |
| `FOLLOWING` | `UserToggleForceLive(True)` | - | `LIVE` | fetch live tail |
| `FROZEN` | `UserTogglePause(False)` | `clock LIVE or force_live` | `LIVE` | fetch live tail |
| `FROZEN` | `UserTogglePause(False)` | `clock REPLAY and not force_live` | `FOLLOWING` | re-anchor to clock ts |
| `FROZEN` | `UserScrolledToLiveEdge` | `clock LIVE or force_live` | `LIVE` | fetch live tail |
| `FROZEN` | *(paging within history)* | - | `FROZEN` | re-fetch adjacent window |
| any | `ClockTick` while `is_scrubbing` | - | unchanged | no-op (ignore the transient scrollbar event) |

`TelemetryTable` only ever exercises the `LIVE`/`FOLLOWING` rows (no scroll/pause concept), so it
instantiates the same machine but never receives `UserScrolledAway`/`UserTogglePause`/
`ClogDetected` events.

### Shared implementation shape

A plain-Python class, no Qt dependency - mirrors `PlaybackClock`'s own style (`core/playback_clock.py`)
so it stays testable headless and consistent with this codebase's existing convention of keeping
state machines out of the UI layer:

```python
# core/playback_follow.py (new)
class PlaybackFollowMachine:
    def __init__(self, supports_freeze: bool = True):
        self.state = FollowState.LIVE
        self.force_live = False
        self._supports_freeze = supports_freeze  # False for TelemetryTable

    def handle(self, event) -> FollowAction:
        """Pure function of (self.state, event) -> new self.state + a FollowAction telling the
        widget what to actually do (FETCH_LIVE / FETCH_FOLLOWING(ts) / FREEZE(anchor) / NOOP).
        No Qt, no fetch calls, no widget references - the widget owns *how* to fetch; this owns
        *when* and *whether*."""
```

Each widget keeps its own fetch mechanics (`LogTextFetcher`, `LogTableStore`, module-value
snapshot swap - these are genuinely different enough not to share) but replaces its own
`follow_playback`/`is_paused`/`_playback_anchored` bookkeeping with one `PlaybackFollowMachine`
instance and a small `match action:` dispatch in `apply_updates()`.

## Migration plan

1. **Phase 0 - build the machine in isolation.** `core/playback_follow.py` +
   `tests/test_playback_follow_machine.py`, pure Python, no Qt/widget involved. Port every edge
   case currently proven by real-widget tests and the skill doc's trap catalog into explicit
   transition tests: Trap A (pause-on-wrong-edge), the scrub-ignore guard, "is_paused always
   wins", "opening while REPLAY already active follows without a manual pause", clog
   auto-pause, force_live's `LIVE`-instead-of-`FOLLOWING` redirect. This is the actual value of
   the exercise - a table of legal transitions that's checkable independent of any widget.

2. **Phase 1 - pilot on `TelemetryTable`.** Smallest surface (2 states, no freeze), and it's the
   widget `force_live` was most recently added to, so the diff is easiest to review against known-
   correct current behavior. Swap `TelemetryTableModel`'s clock-branch in `apply_updates()` to go
   through the machine; existing tests in `test_telemetry_table_playback.py` should pass
   unmodified if the machine's observable behavior matches (see "Test strategy" below on read
   surface).

3. **Phase 2 - `LogViewerWidget`.** Larger surface (`FROZEN`, scroll-detach, clog protection,
   `_poll_history_tail`). Highest risk of subtle behavior drift given how much the skill doc says
   was hard-won here - do this one carefully, one transition at a time, running
   `test_log_viewer_playback.py` and `test_log_viewer_scrub.py` after each.

4. **Phase 3 - `LogTableViewerWidget`.** Near-duplicate of Phase 2's logic today
   (`_go_live`/`_reanchor_history`/`_on_scroll_value_changed` mirror `LogViewerWidget` almost
   exactly) - once Phase 2 lands, this should mostly be "point it at the same
   `PlaybackFollowMachine` class, swap the fetch adapter." If the two widgets' transition logic
   is now byte-for-byte identical (only the fetch adapter differs), that's a strong signal the
   machine abstraction is right; if they still need widget-specific transition overrides, that's
   worth flagging back before Phase 4.

5. **Phase 4 - retire the old flags, update the skill doc.** Delete `follow_playback`/`is_paused`/
   `_playback_anchored` as raw instance attributes once nothing reads them directly (route through
   `self._playback.state`/`self._playback.force_live` instead, or keep thin read-only properties
   for callers that still want the old names). Rewrite `blinkview-playback-wiring` skill §3 ("The
   is_paused / dirty-flag traps") to describe the machine's transition table as the *current*
   design, keeping the trap descriptions as historical "why this shape was chosen" rationale
   rather than a live bug list.

## Test strategy

Every widget-level test currently asserts on the raw flags (`viewer.follow_playback`,
`viewer._playback_anchored`, `viewer.is_paused`, `viewer.view_mode`). Two options, to decide before
Phase 1:

- **(a) Keep the same read surface.** Add read-only properties (`follow_playback ->
  self._playback.state is FollowState.FOLLOWING`, etc.) so every existing test keeps passing
  unmodified - the machine is purely an internal refactor from the test suite's point of view.
- **(b) Migrate tests to assert on `widget._playback.state`/`.force_live` directly**, deleting the
  compatibility properties. Cleaner long-term, but touches every test in
  `test_log_viewer_playback.py`, `test_log_viewer_scrub.py`, `test_log_table_viewer_playback.py`,
  `test_log_table_viewer_scrub.py`, `test_log_table_viewer_widget.py`,
  `test_telemetry_table_playback.py` in the same pass as the production code change - higher risk
  of a rewritten assertion silently testing something different from the original.

Leaning (a) for Phases 1-3 (minimize risk, prove the machine matches existing behavior exactly),
then (b) as a deliberate, separate cleanup pass in Phase 4 once the machine is trusted.

New tests needed either way: the Phase 0 pure-`PlaybackFollowMachine` transition-table tests
(fast, no Qt/Registry setup - a real gap today, since all current coverage is at the real-widget
level per the skill's §6 argument that per-piece tests aren't sufficient - the machine's own unit
tests are additive, not a replacement for the real-widget end-to-end tests, which stay to catch
wiring bugs the machine can't see, like Trap B's missing-attribute crash).

## Risks / open questions to resolve before implementing

1. **Does `LogTableViewerWidget`'s `model.mode` (LIVE/HISTORY) become redundant with
   `FollowState`, or does it stay a separate, model-owned concept the widget syncs to?** Current
   lean: keep `model.mode` as a rendering-level detail the widget's action dispatch sets
   (`FETCH_LIVE` -> `model.enter_live_mode()`), not something the machine itself tracks - avoids
   the machine needing to know about each widget's separate model class.
2. **Is `TelemetryTable` actually well-served by the same class, or is a 2-state machine
   overkill/underkill for it?** It has no freeze concept at all today; forcing it through a
   `supports_freeze=False` flag on a 3-state machine is a plan-time judgment call, not a proven
   simplification. Worth confirming after Phase 1 whether it actually reduced
   `TelemetryTableModel.apply_updates()`'s complexity or just moved it.
3. **Where does `is_scrubbing` fit?** Today it's read directly off the clock at each guard site
   (`clock.is_scrubbing`) to suppress a transient scrollbar `valueChanged` mid-drag. Does the
   machine need to accept it as part of every `ClockTick`, or is it cleaner left as a widget-level
   pre-filter before any event even reaches the machine? Leaning the latter - it's a UI-input
   debounce concern, not a playback-follow *state* concern.
4. **Naming collision**: `FollowState.LIVE`/`FollowEvent` vs `PlaybackMode.LIVE` (the clock's own
   enum) - need distinct enough names in code (not just this doc) that a reader can't confuse
   "the clock is LIVE" with "this widget's follow-state is LIVE" (they're related but distinct -
   `force_live` is exactly the case where they diverge).

## Non-goals

- Not touching `PlaybackClock` itself - it stays the single global source of truth
  (`mode`/`current_ts_ns`/`is_playing`/`is_scrubbing`/bounds), untouched by this refactor.
- Not merging `LogViewerWidget`/`LogTableViewerWidget`'s actual fetch mechanics into one shared
  class - only the follow/pause/live *decision* logic is shared; text-area vs table-model fetching
  stay separate, genuinely different implementations.
- Not changing any observable behavior/timing for a real user in this pass - the goal is an
  internal refactor that existing end-to-end tests should be able to verify is behavior-preserving
  (mode (a) in "Test strategy") before any test assertions themselves change.

# View layout presets

**Status:** implemented 2026-09-29 (not committed). Code: `ui/utils/view_presets.py`,
`UIStateHandler.apply_layout`, `BlinkMainWindow.populate_view_menu` / `*_view_preset`,
`FileManager.get_profile_path`, `WindowManager.find`. Tests: `tests/test_view_presets.py`,
`tests/test_view_presets_apply.py`, plus one case in `tests/test_duplicate_profile.py`. The
follow-ups at the bottom are still open.

## Problem

Turning monitors off makes Windows move every top-level window onto whatever screen is left.
Then two things break the saved layout:

- On exit (or session rotation), `save_gui()` writes the moved positions into
  `<profile>.gui_state.json`, so the good layout is lost.
- On startup with a monitor missing, `restore_window_geometry_safe()` sees floating windows
  off-screen and calls `reattach_to_main()`, which turns them into tabs. After that, the
  layout can't recover by itself even once the monitors are back.

The fix is layout presets you save by hand: capture a layout while all monitors are on, then
load it again after turning them back on.

## Decisions (agreed 2026-09-29)

| Question | Choice |
| --- | --- |
| Load semantics | **Layout only.** Open views are matched by name and only moved or resized, and switched between floating and tabbed when needed. What's inside each view (plot signals, filters, ...) is left as it is now. Preset views that aren't open are created from the preset's saved params. Open views missing from the preset are left alone. |
| Storage | **Separate file** `<profile>.view_presets.json`, written right away on every change. It always goes to the live profile, never to the replay scratch copy. |
| UI | **New "View" toolbar button** next to "Menu". Toolbar order: Menu, View, Rotate, Playback, Devices, then the rest. |

## Storage

`<profile>.view_presets.json`:

```json
{
  "version": 1,
  "presets": {
    "Desk - 3 screens": {
      "saved_at": "2026-09-29T10:12:00",
      "screens": [{"name": "\\\\.\\DISPLAY1", "geometry": [0, 0, 2560, 1440]}, ...],
      "state": { ...exactly UIStateHandler.get_data() output... }
    }
  }
}
```

- `state` uses the same structure as `.gui_state.json`, so the apply code reads the same keys
  (`window_geometry`, `window_state`, `open_tabs`, `floating_windows`, `*_visible`,
  `current_tab_index`).
- `screens` is a fingerprint of `QGuiApplication.screens()` at save time. It drives the warning
  before applying a preset meant for other screens, and the screen-change offer below.
- `offer_on_screen_change` (default true) turns the screen-change offer on or off.
- **Path:** add `FileManager.get_profile_path(type_name)`, which returns
  `config_dir / f"{config_file_name}.{type_name}.json"` and does **not** apply the
  replay-scratch redirect that `get_config_path()` does. Presets belong to you, not to a
  session, so saving one during a replay should still reach the live profile.
- Writes go through `atomic_json_dump`.
- `duplicate_profile()` already renames every `<name>.*` file, so the new file is copied along
  with the profile. Add a test for it anyway.

## Code layout

### `ui/utils/view_presets.py` (new)

- `ViewPresetStore(path)`: plain JSON, no Qt, so it's easy to unit-test.
  `list() -> list[str]`, `get(name)`, `save(name, state, screens)` (overwrites),
  `rename(old, new)`, `delete(name)`. Every mutation writes the file immediately. A missing or
  corrupt file counts as empty, and a corrupt file is kept as `.bak` rather than overwritten
  silently.
- `current_screen_fingerprint() -> list[dict]` and
  `screens_match(saved, current) -> bool`, compared by geometry and ignoring names.

### `UIStateHandler.apply_layout(state, on_complete=None)` (new, in `ui_state_handler.py`)

Steps, in order:

1. **Main window:** `_restore_geometry_until_settled(state["window_geometry"], ...)`. It's
   reused as-is because DWM can ignore early moves here too, for example on a maximized
   window changing screen. The remaining steps chain off its `on_complete`.
2. **Docks and toolbars:** `restoreState(window_state)`, then the
   `sources_visible` / `pipelines_visible` / `playback_visible` flags, handled the same way as
   in `load_ui_state()`, including the rule that replay mode always shows playback.
3. **Index live views by name.** Tabs are keyed by `tabText(i)`. Floating windows are keyed by
   `content.tab_name` from `window_manager._windows` (add a public
   `WindowManager.find(name) -> (window, content) | None`). Preset entries are keyed by
   `params.get("tab_name") or entry["name"]`, the same rule `load_ui_state()` uses.
   `create_widget()` already refuses duplicate names, so names are unique.
4. **Tabs in the preset** (`open_tabs`, in order):
   - The view is floating now: reattach it without destroying the widget. Use
     `DetachedTabWindow.reattach_to_main()`, then close the wrapper with `force_destroy()` or
     similar. **Check that `closeEvent` doesn't reattach a second time.**
   - It's already a tab: nothing to do.
   - It's missing: `create_widget(cls, name, as_window=False, params=entry["params"])`.
   - Then reorder with `tabBar().moveTab()` to follow the preset order (preset tabs first,
     other tabs after), and select the tab named at `current_tab_index` in the preset.
5. **Floating windows in the preset:**
   - The view is a tab now: `detach_tab(index)`. Change it to return the new
     `DetachedTabWindow`.
   - It's already floating: use that window.
   - It's missing: `create_widget(..., as_window=True, show=False, params=...)`.
   - For each of these windows, set `reattach_on_close` from the preset, then apply the saved
     geometry. Use the same "ghost mode" as startup: opacity 0, show, restore after 100 ms,
     then opacity 1.
   - **Refactor:** move the ghost-mode closure out of `load_ui_state()` into a shared helper
     `_place_floating_windows(pairs, on_complete)` used by both callers, so the countdown logic
     isn't duplicated.
6. Call `on_complete`.

The apply never closes a view, so it can't lose the state inside one.

Off-screen fallback: `restore_window_geometry_safe()` still reattaches a floating window whose
saved position is on a screen that isn't connected. That's the right fallback, but it's why the
menu warns first when the screen fingerprint doesn't match.

### `BlinkMainWindow` (`main_window.py`)

- A `view_btn = QToolButton("View")` with `InstantPopup` and a `QMenu` filled in
  `aboutToShow` (same pattern as `app_menu`), placed right after `main_menu_btn`.
  - Toolbar order: Menu, View, Rotate, Playback, Devices | Live Logs, System Logs, Telemetry,
    Watch | msg/s. Playback stays a toolbar toggle, not a View menu entry.
  - One entry per preset, sorted, each labelled with its screen count. Clicking
    one applies the preset. If the screens don't match, it shows a confirm dialog first.
  - **Save Current Layout As…**: a `QInputDialog`. Entering an existing name asks before
    overwriting.
  - **Update** ▸ (preset list): overwrites a preset with the current layout.
  - **Rename** ▸ / **Delete** ▸ (preset list), with a confirm on delete.
  - The whole section is disabled until `load_ui_state` has finished, because applying a preset
    during the staged startup would race `_restore_docks_and_tabs`.
- The store is built from `file_manager.get_profile_path("view_presets")`. After a successful
  apply, show a toast: `Layout '<name>' applied`.

## Tests

- `tests/test_view_presets_store.py`: save, list, get, overwrite, rename, delete, and
  corrupt-file handling, all under `tmp_path`.
- `FileManager.get_profile_path()` still returns the live-profile path in replay mode, while
  `get_config_path()` is redirected. Pass `config_path` under `tmp_path` (see the
  real-registry isolation note).
- `duplicate_profile()` copies and renames `<name>.view_presets.json`. This can go in the
  existing `tests/test_duplicate_profile.py`.
- `apply_layout` with pytest-qt on a real `BlinkMainWindow`:
  - Tab → floating, floating → tab, and a missing view gets created.
  - A view not in the preset is left alone.
  - Content survives: a tab's `get_state()` params are unchanged after it's moved.
  - Tab order and the current tab match the preset.
  - Clean up with `shiboken6.delete`.
- Manual check with real `blink` on a scratch profile copy: save a preset with all monitors on,
  turn one off, restart, turn it back on, then apply.

## Screen-change offer (implemented 2026-09-29)

- `screenAdded` / `screenRemoved` restart a 2 s single-shot timer (`SCREEN_CHANGE_SETTLE_MS`),
  because waking monitors fire a burst of events.
- When the timer fires, `_on_screens_settled()` compares the connected screens with the last
  known set. If nothing really changed (a monitor flickered off and on), it stops. Otherwise it
  dismisses any earlier offer that's still showing.
- If there is a match, it shows a 20 s toast (`SCREEN_CHANGE_PROMPT_SECONDS`): "Screens changed.
  Apply layout 'X'?" with an **Apply** button. The match comes from
  `ViewPresetStore.best_match()`: the most recently saved preset with exactly this screen
  arrangement. Presets without a fingerprint never match.
- There's no offer before the startup restore has finished, or when "Offer Layout When Screens
  Change" (View menu) is off.
- Hovering over the toast pauses its countdown (existing toast behaviour).

## Out of scope / follow-ups

- **Protect the autosave:** at `save_gui()` time, if fewer screens are connected than at
  startup, skip overwriting the workspace `.gui_state.json` (session snapshot only) or ask
  first. That fixes the underlying data loss, while presets are the manual recovery.
- Per-preset option to also restore view contents (full rebuild), if it's ever wanted.

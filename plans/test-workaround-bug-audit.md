# Audit: unit tests that work around bugs instead of fixing them

## Status: partially actioned (2026-07-28)

Prompted by an earlier session finding one such case (`tests/test_ops_dispatch.py`'s
priming-frame workaround for `ops/dispatch.py`'s `nb_process_batch_kernel` dropping the first
frame on a fresh `FrameState`), this was a full audit of `tests/` for similar cases: tests whose
setup or assertions are shaped to dodge/tolerate a real source bug rather than the bug being fixed.

Two Explore agents searched independently and converged on the same candidates. One finding was
acted on; the others are documented here, not yet fixed.

A follow-up pass swept the remaining areas not yet covered (io/ readers, UI widgets, remaining
core/ops/parsers tests) with 3 more Explore agents in parallel; findings folded in below.

## Fixed

- **Circular import between `core.device_identity` and `core.id_registry.registry`.**
  `tests/conftest.py` used to pre-import `blinkview.core.registry` at module load specifically to
  force a "safe" import order before any test could trigger the cycle. Root cause: importing a
  submodule of a package always runs the package's `__init__.py` first, so anything that imports
  `core.device_identity` before anything imports `core.id_registry` would hit `ImportError:
  cannot import name 'DeviceIdentity' from partially initialized module`. Fixed by moving the
  `DeviceIdentity`/`ModuleIdentity` imports under `TYPE_CHECKING` (quoting the annotations) in
  every module that only needed them for type hints — `core/id_registry/registry.py`,
  `parsers/binary_parser.py`, `parsers/can_parser.py`, `parsers/key_value.py`,
  `parsers/module_gen.py` — with local runtime imports added at the 3 call sites in `registry.py`
  that actually instantiate/`isinstance`-check the real classes (`get_device`, `resolve_module`,
  `resolve_device`). The `conftest.py` pre-warm import was then removed as unnecessary. A fresh-
  subprocess regression test (`tests/test_device_identity_import_order.py`) now imports
  `core.device_identity` first with nothing else on the import graph, which is the only way to
  actually catch an import-order regression — an in-process test can't, since other test modules
  have already primed `sys.modules` by the time it runs.

- **`UIStateHandler.load_ui_state()` dropped its startup-completion callback on every fresh
  profile.** Found not by reading tests, but while building a real subprocess smoke test for
  `blink gui` (see "Added: real subprocess/CLI tests" below) - the app looked like it hung forever
  under `QT_QPA_PLATFORM=offscreen`, printing nothing past `[Registry] get_schema_by_path:
  path=/pipelines...`. A `py-spy dump` on the live process showed the main thread genuinely idle
  inside `app.exec()` (not deadlocked), which pointed at a *dropped callback* rather than a hang.
  `src/blinkview/ui/utils/ui_state_handler.py::UIStateHandler.load_ui_state()` only assigned
  `self.ui_restored_cb = ui_state_restored_cb` *after* its "no saved `gui_state.json` yet" early
  return - true for every brand-new profile, including this test's isolated one. So
  `on_ui_restoration_complete()` ran with `self.ui_restored_cb` still `None` from `__init__`,
  silently dropping the callback (`BlinkMainWindow._start_registry`, which calls
  `registry.start()`). The window would show, but sources/ingestion/periodic tasks would never
  start - on literally every fresh install, with no error printed anywhere. Fixed by moving the
  `self.ui_restored_cb = ...` assignment before the early-return check. Regression test:
  `tests/test_ui_state_handler.py` (confirmed it fails against the pre-fix code via `git stash`).

## Not yet fixed (documented, dodged by tests rather than fixed in source)

- **COBS/SLIP `frame_delimiter` clobbering.** `CobsDecoder`/`SlipDecoder.__init__` set
  `self.frame_delimiter = 0x00`/`0xC0` as a plain instance attribute instead of using
  `override_property(...)` like the sibling `AdbDecoder` does correctly. The real construction
  path (`hydrate_config()` → `apply_config()`) re-hydrates from the schema default (`10`, i.e.
  `\n`) and silently overwrites it, so any COBS/SLIP decoder built the normal way silently uses
  the wrong delimiter. `tests/test_frame_decoders.py` has a `configure()` helper that exercises
  this exact construction path for `LineDecoder`/`PreFramedDecoder` but is never applied to
  `CobsDecoder`/`SlipDecoder` — an omission, not a wrong assertion. No test currently catches
  this. Already tracked in memory as `project_cobs_slip_delimiter_clobbered`; user has previously
  chosen to skip testing/fixing these two decoders.
  - Fix sketch: convert both `__init__`s to `override_property("frame_delimiter", default=...)`.

- **`ConfigManager.apply_patch` relative-path bug.** A jsonpatch location missing its leading `/`
  silently fails to persist (caught and logged, not raised) instead of the relative-path
  promotion actually applying. `tests/test_telemetry_watch_widget.py`'s `watch` fixture always
  constructs `TelemetryWatch(gui_context, state={"id": watch_id})` specifically to avoid
  `__init__`'s bare `'watches'` (no leading slash) fallback path that would trigger this, so the
  bug itself is never exercised by any test.
  - Fix sketch: make `apply_patch` raise (or correctly promote) on a missing leading slash; only
    then can the fixture switch to the shorter/relative construction path without silently
    swallowing a real bug.

- **`DeviceIdentity` root-essential quirk.** `DeviceIdentity` always constructs its root
  `ModuleIdentity` as `is_essential=True` regardless of the `default_essential` argument passed to
  `get_device`/`DeviceIdentity(...)`. `tests/test_device_identity.py::
  test_default_essential_true_does_not_affect_root` locks this in with a comment calling it a
  "quirk" rather than confirming it's intentional design.
  - Before changing anything here: read `DeviceIdentity.__init__` in
    `src/blinkview/core/device_identity.py` to determine whether the root module is deliberately
    always-essential (plausible — it may represent the device itself, which should never be
    prunable) or whether `default_essential` was meant to propagate to the root too and doesn't.

- **`SettingsManager.unset()` doesn't validate `scope`.** `src/blinkview/core/settings_manager.py`'s
  `set(key, value, scope="project")` raises `ValueError` on an unrecognized scope string, but
  `unset(key, scope="project")` has no such check — `target = self._project if scope == "project"
  else self._global` means any non-`"project"` string (a typo like `"globl"`, or garbage) silently
  routes to global scope instead of raising. `tests/test_settings_manager.py::TestUnset::
  test_unlike_set_any_non_project_scope_string_routes_to_global` explicitly documents this as "a
  real asymmetry" and locks in the permissive routing rather than the bug being fixed. Unlike the
  other entries here this isn't a dodged-in-a-fixture case — it's a straight regression test
  pinning wrong behavior as correct.
  - Fix sketch: make `unset()` validate `scope` the same way `set()` does (raise `ValueError` on
    anything other than `"project"`/`"global"`), then update the test to assert the raise instead.

- **`ConfigManager.apply_patch`'s blast radius is broader than first scoped.** The already-tracked
  missing-leading-slash bug (above) turns out to be one symptom of a wider issue: `apply_patch`
  wraps the entire patch-apply/save/notify sequence in one broad `except Exception: pass` (logs,
  doesn't raise), so *any* invalid jsonpatch op — not just a missing leading slash — silently
  no-ops instead of surfacing an error. Confirmed via `tests/test_config_manager.py::
  TestApplyPatch::test_invalid_patch_is_caught_and_leaves_data_unchanged` (a `remove` of a
  non-existent path also silently no-ops). Same underlying fix as above would need to narrow the
  `except` to the specific jsonpatch errors it's meant to tolerate, if any, rather than swallowing
  everything.

## Test-quality issues (no source bug, but the test can't actually fail)

- **`tests/test_dynamic_config_widget.py::TestApply::
  test_invalid_config_shows_critical_and_does_not_send`** (lines ~195-209): asserts
  `gui_context.config_manager.sent == [] or len(calls) >= 0` — the second half of the `or` is
  always true, so the assertion can never fail regardless of actual behavior. Root cause per the
  test's own comment: the intended scenario (a blank required string) doesn't actually violate the
  jsonschema (`required` only checks key presence, not truthiness), so the test never exercises the
  "does not send" path it's named for. No source bug — `dynamic_config.py`'s validate/apply path is
  confirmed correct elsewhere in the same file — but this specific test should be tightened (pick
  an input that actually fails validation) or renamed to match what it really checks.

## Fixed (found while writing coverage tests for config_handler.py/ui_state_handler.py/window_manager.py)

- **`config_handler.py`'s `scope_name` mislabeling.** In `handle_config()`, when not inside a
  project and `--list` is passed, the code correctly falls back to `GlobalSettings()` and sets
  `scope_name = "global (fallback)"` - but the very next line, `scope_name = "local"`, was at the
  same indent level as the surrounding `if not settings._path:` block rather than in its `else`,
  so it unconditionally ran afterward and clobbered the fallback label. The underlying settings
  object was always correct; only the printed scope name lied (said "Local" when actually listing
  global settings). Fixed by moving `scope_name = "local"` into an explicit `else` tied to
  `settings._path` being truthy. Regression test:
  `tests/test_config_handler.py::TestProjectScope::
  test_not_in_a_project_with_list_falls_back_to_global_and_labels_it_correctly`.

- **`ui_state_handler.py`'s floating-window restore counter, part 2.** Beyond the dropped-callback
  bug above, `load_ui_state()`'s floating-window loop had a second related bug: `windows_to_restore`
  is initialized to the *total* saved-window count, but only decremented inside the
  per-window completion closure - which is never scheduled for a window whose `create_widget()`
  call returns `None` (a saved widget class that no longer exists). One stale/removed floating
  widget in a user's saved layout meant the counter could never reach zero, so
  `on_ui_restoration_complete()` (and thus `registry.start()`) would never fire - same silent-hang
  class as the dropped-callback bug, reachable any time a user's saved UI state references a
  widget class that's since been renamed or removed, not just on a fresh profile. Fixed by
  decrementing (and checking for completion) on the `continue` path too. Regression tests:
  `tests/test_ui_state_handler.py::TestLoadUiStateFloatingWindows::
  test_a_stale_unknown_widget_class_still_lets_startup_complete` and
  `test_all_floating_windows_unknown_still_completes` (confirmed both fail against the pre-fix
  code via `git stash`).

## Added: real subprocess/CLI tests

Beyond auditing existing tests, added test coverage for `blink` itself, which had none:

- `tests/test_blink_cli.py` - in-process tests of `blinkview.__main__.main()`'s argparse
  dispatcher: subcommand routing (gui/cli/daemon), the "no args"/"bare flags default to gui"
  argument-injection logic, and that an exception from a dispatched command exits with code 1.
- `tests/test_blink_gui_subprocess.py` - launches the real `blink gui` command as an actual OS
  subprocess (not an in-process call with mocked-out Registry/QApplication like
  `tests/test_run.py`) and waits for it to reach real registry startup
  (`"[Registry] Starting central storage..."`), then tears it down. Two real hazards had to be
  isolated to make this safe: (1) `UpdateWidget.ensure_update_path()` pops a blocking modal
  `QFileDialog` unless `settings["update.path"]` is already a valid repo - solved by pre-seeding
  an isolated global `settings.json` pointing at this repo itself (satisfies
  `Updater.is_valid_repo()` without a second checkout); (2) this repo has a real
  `.blinkview/project.json` from actual dogfooding use, so the default project-root discovery
  would resolve into and could write real files there - solved via the `BLINK_PROJECT_ROOT` env
  var, which `get_project_root()` already special-cases as exactly this kind of override.
  Building this test is what surfaced the `UIStateHandler` bug above.
- `tests/test_config_handler.py`, `tests/test_ui_state_handler.py` (expanded), and
  `tests/test_window_manager.py` (new) - full coverage for the three files above 80% but not yet
  well-covered, requested directly by the user after seeing a `--cov` report. All three files are
  now at 100% line coverage. Building the `WindowManager` tests hit a real cross-test isolation
  trap: a `qtbot.wait(50)` call (pumping the shared process-wide `QApplication`'s event loop for a
  fixed wall-clock time) let a stray `QTimer.singleShot` left behind by an unrelated test module
  (`test_update_widget.py`) fire mid-test and hit an already-deleted widget from *that* test,
  failing this one with an unrelated `RuntimeError` - but only when run as part of the full suite,
  never in isolation. Fixed by using `shiboken6.delete(window)` for an immediate, synchronous C++
  destruction (fires `destroyed` right away) instead of `deleteLater()` + waiting out the event
  loop - avoids pumping the shared loop at all, so it can't pick up unrelated pending timers.

## Checked and ruled out (no workaround, legitimate tests)

- `tests/test_pipeline_manager.py`, `tests/test_run.py`, `tests/test_time_utils.py`,
  `tests/test_registry_fresh_process_registration.py`, `tests/test_reorderer.py`,
  `tests/test_numpy_log_cold_tier.py`/`test_unified_log_replay.py`/`test_ops_segments.py`,
  `tests/test_log_table_viewer.py`, `tests/test_ops_codec_adb_long*.py` — all inspected and found
  to be genuine tests of intentional behavior (idempotency, caching, pool-sizing granularity,
  already-fixed regressions), not bug avoidance.
- Mocking density in `tests/test_config_manager.py`, `tests/test_main_window.py`,
  `tests/test_run.py` was spot-checked, not exhaustively reviewed line-by-line — flagged as low
  priority for a deeper pass if ever revisited, but all sampled mocking targets were legitimate
  hardware/OS/network boundaries (ADB, JLink/RTT, sockets, subprocess, Qt dialogs, filesystem),
  not the function under test's own internals.
- Follow-up pass (io/ readers): `tests/test_adb_reader.py`, `tests/test_rtt_reader.py`,
  `tests/test_uart_reader.py`, `tests/test_tcp_client_reader.py`, `tests/test_tcp_server_reader.py`,
  `tests/test_udp_reader.py` — fully reviewed, no new findings beyond the already-known AdbReader
  polite-exit and RTT drain-timeout bugs. No test files exist for `serial_time_syncer.py`,
  `adb_time_syncer.py`, `source_handshake.py`, `logging.py`'s `LoggerReader`, or `raw_logger.py`'s
  `RawLogger` — consistent with those already being tracked as WIP/broken/excluded.
- Follow-up pass (UI widgets): `tests/test_main_window.py`, `tests/test_run.py`,
  `tests/test_playback_control.py`/`test_playback_control_e2e.py`, `tests/test_session_lister.py`,
  `tests/test_widget_registry.py`, `tests/test_base_sidebar_widget.py`,
  `tests/test_update_widget.py`/`test_update_checker.py`, `tests/test_log_table_viewer*.py`,
  `tests/test_config_widget_factory.py`, `tests/test_config_tool_button_widget.py` — fully
  reviewed, no new source-bug workarounds found (see the two entries above for what the pass did
  turn up). `test_config_tool_button_widget.py`'s `TestBuildDeviceSubmenu` patches `QMenu.addMenu`
  to route around a PySide6 binding quirk (`QAction.menu()`'s Python wrapper GC-deleting the C++
  submenu) — judged a genuine Qt binding boundary, not blinkview's own logic, so not treated as an
  app bug.
- Follow-up pass (core/ops/parsers): `core/central_storage.py`, `core/numpy_log.py`,
  `core/numpy_batch_manager.py`, `core/cold_segment.py`, `core/cold_storage_archiver.py`,
  `core/id_history.py`, `core/factory_category_registry.py`, `core/module_snapshot.py`,
  `core/config_manager.py`, `core/sources.py`, `core/task_manager.py`, `core/warmup_registry.py`,
  `core/configurable.py`, `ops/reorderer.py`, `ops/segments.py`, `ops/zephyr_timestamp.py`,
  `ops/codec*`, `parsers/adb_decoder.py`, `parsers/frame_parsers.py`, `parsers/parser.py`,
  `parsers/multi_rule_key_value.py` — fully reviewed; only new finding was `core/settings_manager.py`
  (above). `tests/test_sources_manager.py`'s `apply_config` first-call-doesn't-start pattern looked
  like the known dispatch-kernel priming shape but confirmed as intentional two-phase boot
  sequencing (`Registry.start()` calls `configure_system()` then `sources.start()`), not a bug.

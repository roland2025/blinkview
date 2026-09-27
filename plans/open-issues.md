# Open issues, unfinished work, and roadmap — consolidated (updated 2026-09-27)

A single index over everything currently open across `plans/*.md`, `ROADMAP.md`, and Claude's
session memory. Each item links back to its source doc for full detail rather than duplicating
it. Nothing here should be treated as urgent by default — it's a map of what's known, not a
priority queue.

Last re-verified against `main` @ `6c89868` (v0.15.1.dev0): every "still open" item below was
re-checked in source, and the regression tests that pin the bugs (`test_ops_dispatch.py`,
`test_registry_bootstrap.py`, `test_settings_manager.py`, `test_device_identity.py`,
`test_frame_decoders.py`) all still pass, i.e. they still describe current behavior.

## Real bugs, confirmed, not yet fixed

- **`parsers/frame_decoders.py`: `CobsDecoder`/`SlipDecoder.frame_delimiter` gets clobbered**
  (memory: `project_cobs_slip_delimiter_clobbered`). Both `__init__`s still set a plain instance
  attribute (`0x00`/`0xC0`); the real `hydrate_config()`→`apply_config()` construction path
  overwrites it back to the base schema's `10` (newline). Fix sketch: convert both to
  `override_property("frame_delimiter", default=...)` like `AdbDecoder` already does. User has
  previously chosen to skip testing/fixing these two decoders.
- **`core/registry.py`: `Registry.start()` crashes on a genuinely empty `{}` config file**
  (memory: `project_registry_start_crashes_on_empty_config`). `configure_system()`'s
  `self.plugins.apply_config(self.config.get_by_path("/plugins"))` raises on the missing key,
  silently swallowed, leaving `central`/`reorder` as `None`; `start()` guards `central.start()` but
  later dereferences `self.central.log_pool` unguarded when building `LatestModuleValueTracker`.
  Pinned by `tests/test_registry_bootstrap.py`, not fixed. Two-part fix needed: a `None`-guard in
  `start()`, plus `get_by_path` calls tolerating a missing config section.
- **`core/config_manager.py`: `ConfigManager.apply_patch` silently swallows errors**
  (`plans/test-workaround-bug-audit.md`). The whole patch-apply/save/notify sequence is still
  wrapped in one broad `except Exception` that only prints — any invalid jsonpatch op silently
  no-ops instead of raising. Specific known trigger: a *base* `path` argument without a leading
  `/` (e.g. `TelemetryWatch.__init__`'s bare `'watches'` fallback) produces non-absolute op paths
  that jsonpatch rejects (memory: `project_telemetry_watch_apply_patch_relative_path_bug`); its test
  fixture routes around this rather than exercising it. Fix: normalize/validate the base path and
  narrow the `except`.
- **`core/settings_manager.py`: `SettingsManager.unset()` doesn't validate `scope`**
  (memory: `project_settings_manager_unset_scope_unvalidated`). `set()` raises `ValueError` on an
  unrecognized scope; `unset()` still routes anything that isn't literally `"project"` to global.
  A regression test pins the permissive (wrong) behavior as correct. Fix: make `unset()` validate
  like `set()`, then flip the test to assert the raise.
- **`core/device_identity.py`: root `ModuleIdentity` always `is_essential=True`**
  (memory: `project_device_identity_root_essential_quirk`). The root is constructed without passing
  `default_essential`, so it takes `ModuleIdentity`'s `is_essential=True` default. Locked in by a
  test comment calling it a "quirk" — intent not yet confirmed; may be deliberate (root represents
  the device itself, shouldn't be prunable).
- **`io/adb_reader.py`: `_shell_sync_handler()` is dead code** (memory:
  `project_adb_reader_dead_shell_sync_handler`). References a nonexistent `self._syncer_engine`,
  never called; leftover from the unfinished time-sync work. Delete or move when that lands.
- **`io/rtt.py`: `_drain_stale_data()` pays the full ~1.5s timeout when the buffer is already
  empty at connect** (memory: `project_rtt_drain_stale_data_slow_when_empty`). The "buffer dry"
  exit only fires when `total_drained > 0`; a target with nothing stale busy-waits through the full
  absolute timeout. Not a correctness bug, just an avoidable connect-time delay.
- **`tests/test_dynamic_config_widget.py`: unfalsifiable assertion** (test-quality, not a source
  bug) — `TestApply::test_invalid_config_shows_critical_and_does_not_send` asserts
  `... == [] or len(calls) >= 0`, where the second half is always true. The scenario (blank
  required string) doesn't actually violate the jsonschema (`required` only checks key presence).
  Needs a real failing input, or a rename to match what it actually checks.

## Known dead / unfinished code (excluded from test-coverage work)

Don't propose tests for these until the user says they're ready, per repeated past instruction:

- `io/source_handshake.py` — WIP (memory: `project_source_handshake_unfinished`).
- `core/plugin_manager.py` — WIP (memory: `project_plugin_manager_unfinished`).
- `io/adb_time_syncer.py`, `io/serial_time_syncer.py` — WIP (memory: `project_time_syncers_unfinished`).
- `core/time_sync_engine.py` — deferred, no stated reason, unlike the WIP ones above; fair to
  re-offer in a future test batch (memory: `project_time_sync_engine_deferred`).

## Design docs written but only partially implemented

- **`plans/lazy-retain-skip-for-fetch-scans.md`** — shipped for the two watermark-based follow
  paths (2026-07-29). Still open: the ts-windowed paths (`fetch_telemetry_window`'s plus_one edge
  case, `build_snapshot_as_of`), plus the unresolved design questions in the doc (hot-segment
  retain strategy, where the skip-before-retain logic should live, a correctness test for a raced
  mid-scan retain failure).
- **`plans/background-fetch-cache-for-telemetry-consumers.md`** — design sketch, not implemented,
  "needs more thought before starting." Would amortize per-tick fetch cost across 40+ simultaneous
  telemetry consumers sharing one Qt timer.
- **`plans/generic-desktop-log-parsing-gaps.md`** — gaps 1 (timestamp string parsing) and 2 (live
  file tailing) are implemented; gap 1 keeps growing as a fixed set of formats (rsyslog added
  2026-09-25), which de facto answers the old "strftime engine vs. fixed enum" question in favour
  of the enum. Gap 3, **multi-line message assembly** for newline-framed sources (folding Python
  tracebacks/Java stack traces into one row), is still open. Its old extension point
  (`parsers/assembler.py`'s `AssemblerFactory`) has since been deleted, so picking it up needs a
  new registration point.
- **`plans/replay.md`** — first pass (load a previous session's unified log into Central Storage)
  is implemented (`parsers/unified_log_replay.py`, `registry.replay_mode`, CLI entry). The doc's
  explicitly out-of-scope follow-ons remain unbuilt: interleaving multiple replayed sessions
  (would need the Reorder layer) and richer UI entry points. Paced playback/scrubbing is covered
  separately by the playback clock work.
- **`plans/adb-pid-history.md`** — core is implemented: `core/id_history.py` (`IdHistory`, with
  the Numba walk in `ops/id_history.py`), fed by `AdbReader`'s periodic PID poll into
  `registry.pid_history`, and resolved in `LogTableViewerWidget`. Open: updates come from a 15s
  `ps` poll rather than the doc's suggested `ActivityManager` start/died logcat events, so
  short-lived processes can be missed; and the follow-ups the doc names (a "Process" column in
  the text log viewer, `process=<name>` support in the logfmt kv filter) aren't built.
- **`plans/lazy-cold-segment-unpacking.md`** — **not pursued**, superseded by the simpler fix in
  `plans/cold-storage-compression.md`. Kept only in case the remaining laziness win (skipping
  segments scrubback never visits) is revisited later.

## Roadmap ideas (no design work done yet — `ROADMAP.md`)

Unordered, no timeline, straight from the roadmap doc:

- **Core/perf**: bidirectional plot-to-log cross-probing; automated clock-skew calibration wizard
  across async transports; reorder-layer smart delay period (auto-increase up to N seconds when a
  Zephyr-style deferred-logging pattern is detected).
- **Advanced/remote**: headless mode with a remote GUI client over ZMQ; RTEdbg protocol support;
  external signal ingestion (Saleae/Sigrok logic-analyzer captures correlated with logs).
- **Analytics/automation**: local LLM integration (Ollama/llama.cpp) for anomaly explanation and
  auto-generating parser configs from raw samples; declarative cross-device system assertions for
  CI/HIL; a formal third-party plugin API.
- **UI/UX**: Dear ImGui/ImPlot-based high-refresh-rate plotting; a bidirectional hex/type converter
  widget with contextual log-selection inspection; manual cross-device log sync (anchor points +
  linear-regression clock-skew fit + persisted sync metadata).

## For context: recently closed out (not open, listed so they aren't re-litigated)

- **First frame dropped on a fresh `FrameState`** — discarding up to the first delimiter is
  deliberate resync for sources joined mid-frame (UART), so it's now a per-decoder setting,
  `frame_resync_on_start` (default `True` = old behavior, `AdbDecoder` defaults to `False`; set `False` for TCP/file-style
  sources that start on a boundary). `BinaryParser.run()` passes it into
  `FrameState(start_synced=...)`. Fixed alongside: an oversized frame whose delimiter arrived in
  the same chunk also discarded the *next* valid frame (`nb_decode_loop` dropped out of sync
  instead of staying on the boundary). Tests: `tests/test_ops_dispatch.py::TestStartupResync`,
  `tests/test_binary_parser.py::TestRunRealIngestion`.

- **`plans/logfmt-kv-filter.md`** — implemented and merged (`ops/kv_filter.py`, wired through
  `utils/log_filter.py`, `core/log_fetch.py`, `ops/segments.py`, and both log viewers). The
  `feature/kv-filter` branch has no commits beyond `main`.
- **`io/adb_reader.py` polite shell exit** — fixed 2026-09-27: `_cleanup_process()` wrote a `str`
  to the binary stdin pipe (silently swallowed `TypeError`). Now writes `b"exit
"`, closes stdin,
  and waits up to 0.5s before force-terminating. Tests: `tests/test_adb_reader.py::TestCleanupProcess`.
- **`storage/raw_logger.py`'s `RawLogger`** — deleted 2026-09-27: broken constructor, no
  `@FileLoggerFactory.register`, unreferenced anywhere.
- **`io/logging.py`'s `LoggerReader`** (Python `logging` root-logger ingestion, `"logging"` source
  type) — deleted 2026-09-27 rather than repaired: besides a broken constructor, `run()` targeted a
  stale pool/batch API and nothing downstream parsed `LogRecord`s.
- **`parsers/assembler.py`** (`AssemblerFactory`/`BaseAssembler`, TransformStep-era remnant) —
  deleted in `0ab4ace`.
- `plans/mmap-coldstore.md`, `plans/cold-storage-compression.md`,
  `plans/auto-hot-cold-memory-management.md`, `plans/named-playback-ranges.md`,
  `plans/playback-follow-state-machine.md`, `plans/kv-extractor-numba-backend.md`,
  `plans/fetch-telemetry-window-cold-segment-perf.md` — all marked implemented in their own docs.
- The `test-workaround-bug-audit.md` circular-import fix, the `UIStateHandler`
  dropped-callback/floating-window-counter bugs (memory:
  `project_ui_state_handler_dropped_callback_fixed`), and the `config_handler.py` `scope_name`
  mislabeling bug are fixed.

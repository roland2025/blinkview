# Replay: load a previous session's unified log back into Central Storage

## Context
BlinkView already persists every session's fully-decoded, human-readable log to disk (the "Unified File Writer" — `storage/file_logger.py`'s `FileLogger` with the default `log_row` batch processor), and sessions have grown to multi-gigabyte retained history. Today that retention is write-only: nothing reads it back. "Replay" closes that gap — load a previous session's unified log back into the same `CircularLogPool`/Central Storage that live ingestion populates, so the existing Log Viewer, Table Viewer, Telemetry Table, and Plotter all work against it **unmodified**, with no new "replay mode" UI needed in those consumers.

This plan covers only the first step, per the scoping already agreed in discussion: **get replayed data into storage so existing viewers can read it.** Playback/scrub controls, interleaving multiple replayed sessions, and UI entry points beyond a minimal trigger are explicitly out of scope for this pass (see below).

## Design decisions made during discussion (and why)

**Replay source: the unified text log, not the raw per-source `.bin` capture.** Two options were considered:
1. Replay the raw per-source capture (`BinaryBatchProcessor`/`format_binary_batch` — timestamp + undecoded bytes only) back through the original protocol's parser (e.g. `parsers/binary_parser.py`'s `BinaryParser` and its `SerialParserThread`/`ZephyrMinimalParser`/etc. subclasses).
2. Replay the unified text log (`LogRowBatchProcessor`/`format_log_row_batch` — fully decoded, multi-device-merged text) through a new purpose-built reader.

Option 1 was **rejected**: `BinaryParser` binds a single `device_id` at construction (`self.local.device_id`, threaded into both `frame_ctx` and `parser_ctx` in `apply_config`) — it's fundamentally a *per-device* pipeline. The unified log interleaves multiple devices into one merged stream; feeding it through a single `BinaryParser` instance would mis-attribute every row to whichever device that instance happened to be configured for. Making `BinaryParser` multi-device-aware would be a real architecture change, not a small one. Also note: `storage/raw_logger.py`'s `RawLogger` class looks stale (constructor signature doesn't match the current no-arg `FileLogger.__init__`, missing the `@FileLoggerFactory.register(...)` decorator every active logger has) — not confirmed as what's actually producing raw capture files today, another reason to not build on it.

Option 2 is the recommended approach. Key fact that makes it tractable: `format_log_row_batch` (`ops/formatting.py`) writes a **fixed, unconditional** line grammar — unlike the interactive viewers' configurable `nb_segment_format` (which takes a `FormattingConfig` for per-tab show_ts/show_dev/show_lvl toggles), the unified log is always exactly:
```
YYYY-MM-DDTHH:MM:SS.uuuuuuZ <LEVEL> <DEVICE> <MODULE>: <MESSAGE>\n
```
One stable grammar to reverse-parse, no per-file config to detect or recover.

**Why a new custom component instead of reusing `BinaryParser`:** the unified log needs device/module resolved **per row** (from the text columns), not **per parser instance** (`BinaryParser`'s model). That's a structurally different, simpler shape — not a modification of `BinaryParser`, a new small component that resolves device/module per line via the same discovery calls (`id_registry.get_device()`/`get_module()`) every live parser already uses, then pushes rows via the existing generic bundle-insert kernel. Most of the low-level machinery is reused as-is; only the line tokenizer is genuinely new.

## Recommended implementation

New component, e.g. `parsers/unified_log_replay.py` (name TBD at implementation time) — a one-shot "source + pipeline" combined into a single purpose-built reader, not a live `io/` source and not a `BinaryParser` subclass:

1. **Read** the unified log file in chunks (plain file I/O, no special format needed since it's just newline-delimited text).
2. **Tokenize** each line against the fixed grammar above: split timestamp / level / device / module / message. This is new code, but simple/bounded — same class of work as the existing frame decoders' line-splitting logic, likely expressible as a Numba kernel given the fixed, delimiter-based structure (mirrors the style of `ops/formatting.py`'s writer-side helpers, just inverted).
3. **Resolve names to IDs per row**: `id_registry.get_device(device_name)` / `.get_module(module_name)` (same discovery-on-first-encounter pattern every live parser uses via `DeviceIdentity.get_module` — reused as-is, no new ID-mapping infrastructure).
4. **Insert rows** via the existing generic `nb_bundle_push`/`nb_bundle_push_len` kernel (`ops/segments.py:280-331`) — already accepts arbitrary per-row `device`/`module`/`level`/`seq` ints, not tied to any single-device assumption.
5. **Distribute** resulting batches into Central Storage the same way a live source's parser does (`self.distribute(batch_out)` pattern) — straight to storage is sufficient for a single replayed file (already chronologically ordered by construction); the Reorder layer is only needed if multiple replayed sessions are ever interleaved, which is out of scope here.

## Known limitations of this approach (acceptable for this pass)
- **Timestamp precision**: the unified log stores microsecond precision (`.uuuuuuZ`); original nanosecond precision from live capture is not recoverable from this file. Acceptable — replay is for correlation/review, not sub-microsecond timing analysis.
- **Timezone**: `format_log_row_batch` writes UTC (no tz offset applied, unlike the interactive viewer's `nb_segment_format`); the reverse-parser must treat the timestamp as UTC when reconstructing `ts_ns`.
- **Fresh sequence IDs and device metadata**: replayed rows get new sequence IDs assigned by Central Storage as if freshly ingested (not the original session's sequence numbers), and devices/modules get default flags (e.g. `is_essential`) since that metadata isn't present in the text log. Cosmetic; not expected to matter for review/correlation use cases.

## Out of scope for this pass
- Playback/scrub/seek controls (VCR-style speed/pause) — this pass is "load it all in," not paced/time-accurate playback.
- Interleaving multiple replayed sessions or replaying alongside a live capture in the same window.
- UI polish beyond a minimal trigger (e.g. a menu action to pick a unified log file and kick off the reader) — reuse existing menu/dialog patterns (`populate_main_menu` in `ui/main_window.py`) rather than designing new UI chrome now.
- Confirming/fixing `storage/raw_logger.py`'s apparent staleness — noted as a discrepancy, not addressed here since replay doesn't depend on it.

## Verification
- Unit test the line tokenizer against known-good sample lines produced by the actual `format_log_row_batch` writer (round-trip: format a synthetic bundle, then parse the output back and confirm timestamp/level/device/module/message match the originals).
- Unit test per-row device/module resolution against a log containing multiple interleaved devices, confirming each row lands on the correct device/module (this is the exact case `BinaryParser` couldn't handle — make sure the new component actually does).
- Integration check: replay a real multi-GB unified log file and confirm the Log Viewer / Table Viewer / Telemetry Table / Plotter show the replayed data correctly without any changes to those consumers (manual check, not run by the assistant, per prior agreement).

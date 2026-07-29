# Generalizing parsing for desktop/console application log files - gap analysis

## Status update (2026-07-27)

Gap 1 (timestamp string parsing) and Gap 2 (live file tailing) are now implemented, taking the
"lighter first cut" option discussed in "Open decision" below:

- `ops/desktop_timestamp.py`: `nb_parse_iso8601_desktop` (`ParserID.TS_ISO8601`, registered as
  `timestamp_iso8601_desktop`) parses `YYYY-MM-DD HH:MM:SS[.,]fff` (Python `logging`, log4j, plain
  ISO8601, no bracket wrapper) - reuses `ops/timestamps.py`'s existing
  `nb_parse_iso8601_to_ns` field-extraction math with `offset_sec=0`. `nb_parse_syslog_timestamp`
  (new `ParserID.TS_SYSLOG`, registered as `timestamp_syslog`) parses classic RFC3164 syslog `Mon
  DD HH:MM:SS`; since that format has no year field, one is supplied via a new
  `UnifiedParserConfig.syslog_year` field (either a configured `year`, or the current year at
  config-apply time). Both route through the existing `nb_project_synced_ns` auto-sync path, same
  as `TS_ZEPHYR_REALTIME`. A third format (a fully generic strftime-style engine, or `TS_CUSTOM_STRFTIME`)
  remains unimplemented - not needed yet per the "extend later" plan.
- `io/file_tail_reader.py`'s `FileTailReader` (`file_tail`) polls a file for appended bytes
  (`from_start` to replay existing content or `tail -f`-style skip-to-end), detects truncation/log
  rotation via inode + size-shrink checks and reopens from the start, and does **not** do
  multi-line assembly - each decoded frame is forwarded independently.

`parsers/assembler.py`'s `AssemblerFactory`/`BaseAssembler` (Gap 3's would-be extension point) was
confirmed unused anywhere in the codebase and deleted, along with `FactoryCategory.PIPELINE_ASSEMBLER`.
Gap 3 (multi-line assembly across separate newline-framed rows) remains unaddressed and would need
a new concrete registration point if picked up later - but note the Gap 3 section below: it's only
a gap for newline-delimited framing. A producer using NUL-delimited (`\0`) framing already gets
multi-line messages for free, since embedded `\n`s just ride through as payload inside one frame.

## Context

blinkview's parsing pipeline was built around embedded/hardware telemetry (ADB, CAN, UART, JLink
RTT), where timestamps are raw device-relative counters synced via `core/time_sync_engine.py`, and
framing is either length-prefixed (ADB) or fixed-size (CAN). This document inventories what's
already reusable vs. what's actually missing to parse generic desktop/console application logs
(Python `logging`, log4j, syslog-style output) instead, where timestamps are human-readable
wall-clock strings embedded in the log text itself. This followed on from a broader discussion
concluding that live file tailing - not syslog specifically - is the real missing primitive, with
syslog/desktop-log formats being examples of what would ride on top of it once parsing supports
them generically.

This is a research/analysis document, not an implementation plan - no code has been written. See
`plans/factory-category-registry.md` for an unrelated, separately-scoped implementation plan from
the same session.

## What already works today (no gap)

### 1. Generic newline framing - production-ready as-is

`parsers/frame_decoders.py`'s `LineDecoder` (registered `"line_decoder"`, `CodecID.NEWLINE` ->
`ops/codecs.py:40 nb_decode_newline_frame`) already does exactly "split on a configurable delimiter
byte, no length prefix, optional trailing-`\r` strip, optional ANSI/non-printable filtering." This
is framing-agnostic and works for any plain-text log file as-is - it is not tied to ADB's
length-prefixed binary framing or CAN's fixed-size binary frames (`parsers/can_parser.py` /
`ops/segments.py`'s `nb_can_push`, a completely separate path).

`core/types/parsing.py:56-64`'s full `CodecID` enum: `NONE=0`, `NEWLINE=10`, `COBS=20`, `SLIP=30`,
`ADB_LONG=40`, `PLUGIN=99` (reserved). Only `NEWLINE` and `ADB_LONG` are actually wired into the
dispatcher (`ops/frame_dispatch.py:22-45`); COBS/SLIP kernels exist (`ops/codecs.py:143,186`) but
are commented out in the dispatcher and currently unreachable.

### 2. Generic log-level word matching - already includes a Python-logging preset

`ops/levels.py`'s `nb_parse_log_level` (`ParserID.LEVEL_NAME_MAP=13`) matches an arbitrary-length
token at a cursor position against a registered string table (first-byte short-circuit + a
null-terminator prefix-boundary check, so `"INFO"` can't accidentally match `"INFOMAN"`).
`parsers/frame_parsers.py` already ships several presets registered via `FrameSectionParserFactory`,
including **`"log_level_python"`** - `INFO`/`DEBUG`/`WARNING`/`ERROR`/`CRITICAL`, i.e. Python
`logging`'s exact vocabulary (from `utils/log_level.py`'s `LogLevel.LIST_CONF`). Others: nRF-style
(`<info>`/`<warn>`/...), Zephyr-style (`<inf>`/`<wrn>`/...), ESP-IDF-style (I/E/W/D/V), ADB-style
single chars, and an empty user-defined slot.

**Caveat**: this matcher operates at whatever cursor position the *previous* pipeline stage left
off at - it's a positional match, not a free scan-anywhere regex. For a typical desktop log line
(`2026-01-15 10:23:01,456 INFO myapp.module: message`), it needs the timestamp stage ahead of it to
correctly consume the timestamp text first and leave the cursor sitting right before `INFO`. That
timestamp stage is gap #1 below.

### 3. Generic epoch-integer timestamps - reusable if the format happens to match

`ops/timestamps.py:54 nb_parse_int_timestamp` / `ParserID.TS_INTEGER` (`IntegerTimestampParser` in
`parsers/frame_parsers.py`) already handles a raw epoch integer (sec/ms/us/ns, selectable via a
`timestamp_precision` config) at a fixed cursor position. Useful only for logs that literally start
with a Unix epoch integer - not helpful for human-readable date/time strings.

## Real gaps, in priority order

### Gap 1 (highest priority): no human-readable timestamp string parser

`ParserID.TS_ISO8601=2` and `TS_CUSTOM_STRFTIME=3` (`core/types/parsing.py:24-32`) are **declared
but completely unimplemented** - grepping the entire `src/blinkview` tree for either name outside
their own enum declaration line returns zero hits. No kernel, no dispatch branch in
`ops/pipeline.py`, no `FrameSectionParser` subclass implements them.

The only text-timestamp parsing that exists at all, `ops/timestamps.py:17 nb_parse_iso8601_to_ns`,
is hardcoded to ADB's exact `[ YYYY-MM-DD HH:MM:SS.mmm` bracket skeleton (fixed byte offsets,
called from exactly one place: `ops/codec_adb_long.py:434`'s ADB-specific header parser). It cannot
be reused for syslog's `Mon DD HH:MM:SS`, Python logging's `YYYY-MM-DD HH:MM:SS,mmm`, or log4j's
`yyyy-MM-dd HH:mm:ss,SSS` without a rewrite.

Separately, `parsers/unified_log_replay.py:30-34`'s `_parse_ts_ns` does call Python's
`datetime.strptime`, but it only reverses blinkview's *own* fixed output grammar (written by
`ops/formatting.py`'s `nb_format_log_row_batch`) via a hardcoded regex - a one-shot dev tool for
replaying blinkview's own prior log files, not a generic configurable external-format parser.

**Why this is the highest-priority gap**: it blocks gap-free use of the log-level matcher (#2
above) on any non-ADB text source, and it's needed regardless of which reader eventually supplies
the bytes (live tail, static file replay, or anything else).

**Implementation note**: Numba can't call Python's `datetime.strptime` inside an `@app_njit`
kernel, so this needs either (a) a hand-rolled fixed-width field scanner generalizing
`nb_parse_iso8601_to_ns`'s existing Julian-day math to a configurable year/month/day/hour/min/sec/
frac layout, or (b) a lighter first cut supporting just the 2-3 most common desktop formats
(ISO8601 `YYYY-MM-DD HH:MM:SS[.,]ffffff`, syslog `Mon DD HH:MM:SS`) as distinct `ParserID`s,
deferring a fully generic format-string engine until a real third format is actually needed.

### Gap 2: no live file-tailing reader

`io/binary_file_reader.py`'s `BinaryFileReader` is a dev-replay tool: it injects pre-recorded bytes
at a simulated fixed rate, and on EOF either loops back to byte 0 (`_reset_source()`, `f.seek(0)`)
or permanently stops - it never re-checks for newly appended bytes. Grepping every reader in
`io/*.py` for size-polling, inotify, or rotation/inode-change detection patterns returns zero
matches anywhere. Every real desktop-log use case (watching a live-growing log file, surviving log
rotation via `logrotate`/`RotatingFileHandler`) needs a genuinely new reader.

### Gap 3: no multi-line log entry assembly - but only for newline-delimited framing

Every decoder treats one delimited frame as one complete, independent log row - this is only a gap
when the frame delimiter itself is `\n`. `LineDecoder`/`nb_decode_newline_frame`'s delimiter byte
is a plain config value, not hardcoded to `\n` - if the log producer instead writes NUL-delimited
records (`\0`-terminated instead of `\n`-terminated), a multi-line stack trace's embedded `\n`s are
just payload bytes that ride through inside one frame untouched, and multi-line messages already
work today with zero new code. The real gap only applies to the common case where the producer
newline-delimits records (the default for stdout, syslog, Python `logging`'s default handler,
etc.) and a multi-line message has been split across several newline-framed rows that need
re-joining after the fact.

For that newline-delimited case: `parsers/assembler.py` (`AssemblerFactory`) was a 22-line empty
ABC shell (`BaseAssembler (configurable, bindable)` + `AssemblerFactory(BaseFactory)`) with zero
concrete subclasses anywhere in the codebase - confirmed unused and deleted (see status update
above) - there is no "this line has no timestamp/level prefix, so append it to the previous entry"
primitive anywhere.

ADB's own decoder (`nb_decode_adb_long_frame`) does fold multi-line stack-trace-style ADB messages
into one frame, by scanning ahead for the *next* ADB-format header
(`nb_is_adb_long_header_monotonic`/`nb_is_adb_long_header_iso`) - but this trick is entirely
hardcoded to ADB's own timestamp punctuation skeleton and does not generalize to arbitrary text.

Stack traces and multi-line messages (extremely common in desktop app logs - Python tracebacks,
Java stack traces) would each become a separate, timestamp-less/level-less row today under
newline-delimited framing, without a new assembler primitive (or the producer switching to NUL
delimiting).

## Recommended build order

1. **Timestamp string parsing** (gap 1) - highest leverage: unblocks the level-matcher for
   non-ADB text, needed regardless of source, and fits the existing `FrameSectionParser`/
   `ParserID` extension pattern exactly (same shape as `IntegerTimestampParser`, parsing a
   formatted string instead of a raw int).
2. **Live file tailing** (gap 2) and **multi-line assembly** (gap 3) - each independently useful,
   but architecturally larger (a new `BaseReader` subclass; a new `AssemblerFactory` concrete class
   plus wiring into the frame-decode loop to look back one row). Better scoped as separate
   follow-ups once #1 is in place and proven against a real desktop log sample.

## Open decision

Gap 1's implementation needs a choice: a fully generic strftime-style format engine (more effort,
handles any format up front) vs. a small enum of the 2-3 most common concrete formats (less
effort, extend later if a fourth format is actually needed). No code has been written for any of
this yet - this document is research/analysis only.

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The agent API's log reading (the `read_logs` tool, and the reply half of `send`): fetch rows
from central storage by sequence cursor, optionally waiting for new ones. Also the `summary`
tool, which counts the matching rows per module and level instead of returning them.

Everything goes through the log viewer's own helpers - LogFilter, LogSegmentScanner,
LogTextFetcher and the FormattingConfig formatter (core/log_fetch.py) - so an agent gets the
same filtering and the same lines a log tab shows. Only the request parsing, the cursor
bookkeeping, the wait loop and summary's totals live here."""

import time
from datetime import datetime, timedelta, timezone
from datetime import time as dt_time
from typing import TYPE_CHECKING, Callable, NamedTuple, Optional

import numpy as np

from blinkview.core import dtypes
from blinkview.core.agent_api import AgentApiError
from blinkview.core.dtypes import SEQ_NONE, SEQ_TYPE, TS_TYPE, TS_UNSPECIFIED
from blinkview.core.log_fetch import LogSegmentScanner, LogTextFetcher
from blinkview.core.types.formatting import FormattingConfig
from blinkview.core.warmup_registry import register_warmup
from blinkview.ops.kv_filter import EMPTY_KV_CONDITIONS
from blinkview.ops.segments import LEVEL_SLOTS, nb_count_rows_by_module_level, segment_filter
from blinkview.ops.text_filter import EMPTY_TEXT_SEARCH
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel
from blinkview.utils.utc_offset import get_local_utc_offset_seconds

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper

DEFAULT_MAX_LINES = 200
HARD_MAX_LINES = 2000
DEFAULT_LAST_N = 50
DEFAULT_AROUND_LINES = 50
DEFAULT_MAX_MODULES = 500
HARD_MAX_MODULES = 5000
DEFAULT_SUMMARY_MODULES = 50
HARD_SUMMARY_MODULES = 1000
MAX_WAIT_SECONDS = 60.0
POLL_SECONDS = 0.05

# BlinkView's own log lines (pipeline/source state, and this API's "send to ..." audit lines).
SYSTEM_DEVICE = "system"

# The mask handed to LogSegmentScanner is sized past the current module count, so modules
# registered between building it and the scanner slicing it are still covered.
_MASK_SLACK = 1024
_NO_SIDEBAR_MASK = np.zeros(0, dtype=dtypes.LEVEL_TYPE)

_LEVELS = {level.name_conf: level for level in LogLevel.LIST_UI}
_LEVELS["WARN"] = LogLevel.WARN

# nb_count_rows_by_module_level's level columns, by name: a row without a level, then one per
# level bit.
_LEVEL_SLOT_NAMES = ["NONE"] + [
    getattr(LogLevel.from_value(1 << bit), "name_conf", f"LEVEL_{1 << bit}") for bit in range(LEVEL_SLOTS - 1)
]
# scan_history_window's "no cap": every matching row is wanted.
_ALL_ROWS = 1 << 62
_TS_MAX = np.iinfo(TS_TYPE).max
_TS_MIN = np.iinfo(TS_TYPE).min

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

_FORMAT_FLAGS = ("show_ts", "show_date", "show_dev", "show_lvl", "show_mod", "show_rx_ts")


class _Step(NamedTuple):
    text: str
    rows: int
    cursor: int
    more_pending: bool  # the cap was hit: matching rows newer than `cursor` remain


def _number(args: dict, key: str, default, low, high):
    value = args.get(key, default)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise AgentApiError(f"'{key}' must be a number")
    return min(max(value, low), high)


def _string(args: dict, key: str) -> str:
    value = args.get(key)
    if value is None:
        return ""
    if not isinstance(value, str):
        raise AgentApiError(f"'{key}' must be a string")
    return value


def _find_device(id_registry, name: str):
    """Lookup only: IDRegistry.get_device would register a misspelled name as a new device."""
    devices = id_registry.get_all_devices()
    for device in devices:
        if device.name == name:
            return device
    raise AgentApiError(f"unknown device '{name}', known: {sorted(d.name for d in devices)}")


def _find_module(id_registry, device, path: str):
    for module in id_registry.get_all_modules():
        if module.device is device and module.name == path:
            return module
    raise AgentApiError(f"device '{device.name}' has no module '{path}' (it may not have logged yet)")


def _format_config(args: dict) -> FormattingConfig:
    defaults = FormattingConfig()
    fields = {}
    for flag in _FORMAT_FLAGS:
        value = args.get(flag, getattr(defaults, flag))
        if not isinstance(value, bool):
            raise AgentApiError(f"'{flag}' must be true or false")
        fields[flag] = value
    fields["ts_precision"] = int(_number(args, "ts_precision", defaults.ts_precision, 0, 9))
    return FormattingConfig(**fields)


def _parse_time(text: str, tz_offset_sec: int, key: str = "around") -> int:
    """A time as the returned lines show it ("2026-10-09 20:52:26.508", or ISO 8601 with 'T',
    'Z' or an offset), or a time of day alone (today's date) -> epoch ns, the unit of row
    timestamps. Without an offset in the text it is read in the timezone the lines are
    formatted in (`tz_offset_sec`)."""
    display_tz = timezone(timedelta(seconds=tz_offset_sec))
    text = text.strip()
    try:
        if "-" not in text.lstrip("-") and ":" in text:
            moment = datetime.combine(datetime.now(display_tz).date(), dt_time.fromisoformat(text))
        else:
            moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        raise AgentApiError(
            f"cannot read '{key}' time '{text}': use e.g. '2026-10-09 20:52:26.508' or '20:52:26'"
        ) from None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=display_tz)

    since_epoch = moment - datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (since_epoch.days * 86400 + since_epoch.seconds) * 1_000_000_000 + since_epoch.microseconds * 1000


def _format_ts(ts_ns: int, tz_offset_sec: int) -> str:
    """Epoch ns -> "2026-10-09 20:52:26.508", the log lines' own layout."""
    moment = _EPOCH + timedelta(microseconds=int(ts_ns) // 1000 + tz_offset_sec * 1_000_000)
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def list_modules(registry, args: dict) -> dict:
    """The `list_modules` tool: devices and the module paths under each, for `read_logs`'
    `device`/`module` filters."""
    id_registry = registry.id_registry
    device_name = _string(args, "device")
    contains = _string(args, "contains").lower()
    include_system = args.get("include_system", False) or device_name == SYSTEM_DEVICE
    max_modules = int(_number(args, "max_modules", DEFAULT_MAX_MODULES, 1, HARD_MAX_MODULES))
    only_device = _find_device(id_registry, device_name) if device_name else None

    by_device = {}
    for device in id_registry.get_all_devices():
        if only_device is not None and device is not only_device:
            continue
        if device.name == SYSTEM_DEVICE and not include_system:
            continue
        by_device[device.id] = {"name": device.name, "modules": []}

    listed = 0
    total = 0
    for module in list(id_registry.get_all_modules()):
        entry = by_device.get(module.device.id)
        if entry is None or not module.name or (contains and contains not in module.name.lower()):
            continue
        total += 1
        if listed < max_modules:
            entry["modules"].append(module.name)
            listed += 1

    for entry in by_device.values():
        entry["modules"].sort()
    return {"devices": list(by_device.values()), "modules_listed": listed, "truncated": total > listed}


def _make_scanner(registry, args: dict) -> LogSegmentScanner:
    """The log viewer's scanner over central storage, with its LogFilter built from the
    request's `device`, `module`, `min_level`, `contains`, `kv` and `include_system`."""
    id_registry = registry.id_registry
    central = registry.central
    if central is None or central.log_pool is None:
        raise AgentApiError("central storage is not running")

    device_name = _string(args, "device")
    module_path = _string(args, "module")
    if module_path and not device_name:
        raise AgentApiError("'module' needs 'device'")
    device = _find_device(id_registry, device_name) if device_name else None
    module = _find_module(id_registry, device, module_path) if module_path else None

    level_name = _string(args, "min_level").upper() or "ALL"
    if level_name not in _LEVELS:
        raise AgentApiError(f"unknown 'min_level' '{level_name}', one of: {sorted(_LEVELS)}")

    # The module goes into the per-module mask below, not into LogFilter: its
    # filtered_module_children mode selects a module's submodules *without* the module itself.
    log_filter = LogFilter(id_registry, device, log_level=_LEVELS[level_name])
    log_filter.set_text_filter(_string(args, "contains"))
    log_filter.set_kv_filter(_string(args, "kv"))

    include_system = args.get("include_system", False) or device_name == SYSTEM_DEVICE
    system_device = next((d for d in id_registry.get_all_devices() if d.name == SYSTEM_DEVICE), None)
    hide_system = system_device is not None and not include_system

    def sidebar_filter():
        """The scanner's per-module level mask (what the viewer's filter sidebar provides).
        Rebuilt whenever the scanner re-bakes, i.e. each time a module is registered, so a
        submodule appearing during a wait is picked up."""
        if module is None and not hide_system:
            return False, _NO_SIDEBAR_MASK

        size = id_registry.module_count() + _MASK_SLACK
        if module is not None:
            mask = np.full(size, LogLevel.OFF.value, dtype=dtypes.LEVEL_TYPE)
            mask[module.id] = LogLevel.ALL.value
            mask[id_registry.get_descendant_ids(module.id)] = LogLevel.ALL.value
        else:
            mask = np.full(size, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)
        if hide_system:
            system_ids = system_device.get_all_module_ids()
            mask[system_ids[system_ids < size]] = LogLevel.OFF.value
        return True, mask

    return LogSegmentScanner(
        id_registry,
        lambda: central.log_pool,
        log_filter,
        get_sidebar_filter=sidebar_filter,
        get_show_hidden=lambda: True,
    )


def read_logs(registry, args: dict, stop_is_set: Callable[[], bool] = lambda: False) -> dict:
    """Runs one `read_logs` request against `registry`'s central storage. Blocks the calling
    thread for up to `wait_s`. See agent_mcp.py's tool description for the arguments."""
    id_registry = registry.id_registry
    scanner = _make_scanner(registry, args)
    log_pool = registry.central.log_pool
    fetcher = LogTextFetcher(scanner, registry.system_ctx.array_pool, tables_provider=id_registry.bundle)

    # --- Formatting: the log viewer's formatter ---
    format_cfg = _format_config(args)
    utc = bool(args.get("utc", False))
    tz_offset_sec = 0 if utc else get_local_utc_offset_seconds()

    # --- Range and waiting ---
    max_lines = int(_number(args, "max_lines", DEFAULT_MAX_LINES, 1, HARD_MAX_LINES))
    wait_s = _number(args, "wait_s", 0, 0, MAX_WAIT_SECONDS)
    quiet_s = _number(args, "quiet_ms", 0, 0, MAX_WAIT_SECONDS * 1000) / 1000.0
    until = _string(args, "until")
    after_seq = _number(args, "after_seq", None, 0, float("inf"))

    result = {
        "session_id": registry.file_manager.session_dir.name,
        "utc_offset_minutes": tz_offset_sec // 60,
    }

    def finish(text: str, rows: int, cursor: int, **extra) -> dict:
        result.update(lines=rows, next_seq=int(cursor), **extra)
        result["text"] = text
        return result

    around = _string(args, "around")
    if around:
        # Time anchor: the viewer's "follow the playback clock" fetch, centred on a timestamp.
        if after_seq is not None or wait_s:
            raise AgentApiError("'around' cannot be combined with 'after_seq' or 'wait_s'")
        anchor_ts = _parse_time(around, tz_offset_sec)
        before = int(_number(args, "before", DEFAULT_AROUND_LINES, 0, HARD_MAX_LINES))
        after = int(_number(args, "after", DEFAULT_AROUND_LINES, 0, HARD_MAX_LINES))
        latest = int(log_pool.latest_sequence())
        window = fetcher.fetch_history_window(
            anchor_ts=anchor_ts, before_cap=before, after_cap=after, format_cfg=format_cfg, tz_offset_sec=tz_offset_sec
        )
        more_after = window.after_count >= after and window.newest_seq is not None
        return finish(
            window.text,
            window.before_count + window.after_count,
            window.newest_seq if more_after else latest,
            before_lines=window.before_count,  # the lines from here on are at or after 'around'
            after_lines=window.after_count,
            older_rows_exist=not window.reached_start,
        )

    if after_seq is None and not wait_s:
        # No cursor yet: the newest matching rows, like opening a log tab.
        last_n = min(int(_number(args, "last_n", DEFAULT_LAST_N, 1, HARD_MAX_LINES)), max_lines)
        tail = fetcher.fetch_live_tail(max_rows=last_n, format_cfg=format_cfg, tz_offset_sec=tz_offset_sec)
        return finish(
            tail.text, tail.total_new_rows, tail.highest_seq_seen, older_rows_exist=not tail.reached_live_edge
        )

    latest = int(log_pool.latest_sequence())
    cursor = latest if after_seq is None else int(after_seq)
    if cursor > latest:
        # A cursor from a previous run of the app (sequences continue across session rotation,
        # but restart with the process).
        cursor = int(SEQ_NONE)
        result["cursor_reset"] = True

    def step(cursor: int, cap: int) -> _Step:
        """Matching rows newer than `cursor`, oldest first, at most `cap`."""
        fetcher.latest_seq_seen = cursor
        tail = fetcher.fetch_live_tail(max_rows=cap, format_cfg=format_cfg, tz_offset_sec=tz_offset_sec)
        if tail.reached_live_edge:
            return _Step(tail.text, tail.total_new_rows, int(fetcher.latest_seq_seen), False)

        # More than `cap` new rows: the tail scan kept the newest ones, which would skip the
        # rows right after the cursor. Page forward from the cursor instead.
        page = fetcher.fetch_history_window(
            anchor_seq=cursor + 1, before_cap=0, after_cap=cap, format_cfg=format_cfg, tz_offset_sec=tz_offset_sec
        )
        if page.after_count >= cap and page.newest_seq is not None:
            return _Step(page.text, page.after_count, int(page.newest_seq), True)
        return _Step(page.text, page.after_count, int(tail.highest_seq_seen), False)

    chunks = []
    rows = 0
    truncated = False
    stopped_on = None
    deadline = time.monotonic() + (wait_s or 0)
    last_row_time = None

    while True:
        new = step(cursor, max_lines - rows)
        cursor = new.cursor
        if new.rows:
            chunks.append(new.text)
            rows += new.rows
            last_row_time = time.monotonic()

        if new.more_pending or rows >= max_lines:
            truncated = new.more_pending
            stopped_on = "max_lines"
            break
        if not wait_s:
            break
        if until:
            if new.rows and until in new.text:
                stopped_on = "until"
                break
        elif not quiet_s and rows:
            stopped_on = "first_lines"
            break

        now = time.monotonic()
        if quiet_s and last_row_time is not None and now - last_row_time >= quiet_s:
            stopped_on = "quiet"
            break
        if now >= deadline:
            stopped_on = "timeout"
            break
        if stop_is_set():
            stopped_on = "shutdown"
            break
        time.sleep(POLL_SECONDS)

    extra = {"truncated": truncated}
    if wait_s:
        extra["stopped_on"] = stopped_on
    return finish("".join(chunks), rows, cursor, **extra)


def summary(registry, args: dict) -> dict:
    """The `summary` tool: how many rows match, per module and level, over everything central
    storage holds or a part of it. Same filters as `read_logs`; no line text is formatted."""
    id_registry = registry.id_registry
    scanner = _make_scanner(registry, args)
    log_pool = registry.central.log_pool

    tz_offset_sec = 0 if args.get("utc", False) else get_local_utc_offset_seconds()
    max_modules = int(_number(args, "max_modules", DEFAULT_SUMMARY_MODULES, 1, HARD_SUMMARY_MODULES))

    # --- Range: one lower bound at most, and an optional upper one ---
    after_seq = _number(args, "after_seq", None, 0, float("inf"))
    since_s = _number(args, "since_s", None, 0, float("inf"))
    start = _string(args, "start")
    end = _string(args, "end")
    if sum((after_seq is not None, since_s is not None, bool(start))) > 1:
        raise AgentApiError("give one of 'after_seq', 'since_s' and 'start', not several")

    result = {
        "session_id": registry.file_manager.session_dir.name,
        "utc_offset_minutes": tz_offset_sec // 60,
    }
    # Rows appended while this runs are left to the next call: next_seq is exactly what was counted.
    latest = int(log_pool.latest_sequence())
    anchor_seq = None
    if after_seq is not None:
        if after_seq > latest:
            result["cursor_reset"] = True  # a cursor from a previous run of the app, see read_logs
        else:
            anchor_seq = int(after_seq) + 1
    start_ts = None
    if since_s is not None:
        start_ts = registry.now_ns() - int(since_s * 1e9)
    elif start:
        start_ts = _parse_time(start, tz_offset_sec, "start")
    end_ts = TS_TYPE(_parse_time(end, tz_offset_sec, "end")) if end else TS_UNSPECIFIED
    max_seq = SEQ_TYPE(latest)

    # Sized past the current module count, like the scanner's mask (see _MASK_SLACK).
    counts = np.zeros((id_registry.module_count() + _MASK_SLACK, LEVEL_SLOTS), dtype=np.int64)
    span = np.array([_TS_MAX, _TS_MIN], dtype=TS_TYPE)

    def count_rows(segment, indices_array, match_count):
        nb_count_rows_by_module_level(segment.bundle, indices_array, match_count, max_seq, end_ts, counts, span)

    scanner.scan_history_window(
        anchor_seq=anchor_seq,
        anchor_ts=start_ts,
        before_cap=0,
        after_cap=_ALL_ROWS,
        consume_before=count_rows,  # never called: before_cap is 0
        consume_after=count_rows,
    )

    def level_counts(per_slot) -> dict:
        """{level name: rows} for the levels that occurred, most severe first."""
        return {
            _LEVEL_SLOT_NAMES[slot]: int(per_slot[slot]) for slot in range(LEVEL_SLOTS - 1, -1, -1) if per_slot[slot]
        }

    per_module = counts.sum(axis=1)
    groups = []
    for module_id in np.flatnonzero(per_module).tolist():
        module = id_registry.module_from_int(module_id)
        groups.append((int(per_module[module_id]), module.device.name, module.name or "-", module_id))
    groups.sort(key=lambda group: (-group[0], group[1], group[2]))
    shown = groups[:max_modules]

    rows = int(per_module.sum())
    result.update(
        rows=rows,
        levels=level_counts(counts.sum(axis=0)),
        modules=len(groups),
        modules_listed=len(shown),
        truncated=len(groups) > len(shown),
        # Pass as after_seq (here or to read_logs) for what was logged since this call.
        next_seq=latest,
    )
    if rows:
        result["first_time"] = _format_ts(span[0], tz_offset_sec)
        result["last_time"] = _format_ts(span[1], tz_offset_sec)
    result["text"] = "".join(
        f"{total} {device_name} {module_name} "
        + " ".join(f"{name}={count}" for name, count in level_counts(counts[module_id]).items())
        + "\n"
        for total, device_name, module_name, module_id in shown
    )
    return result


@register_warmup
def warmup_summary(helper: "NumbaWarmupHelper"):
    """Compiles nb_count_rows_by_module_level with the argument types summary() passes. Needs
    rows already in helper.log_pool (CircularLogPool.warmup, priority=100)."""
    print("[Warmup] agent summary ...")

    mod_count = max(10, helper.registry.module_count())
    effective_mask = np.full(mod_count, LogLevel.ALL.value, dtype=dtypes.LEVEL_TYPE)
    counts = np.zeros((mod_count, LEVEL_SLOTS), dtype=np.int64)
    span = np.array([_TS_MAX, _TS_MIN], dtype=TS_TYPE)

    with helper.log_pool.get_snapshot() as segments, helper.log_pool.acquire_indices_buffer() as indices:
        for segment in segments:
            if segment.size == 0:
                continue
            match_count = segment_filter(
                segment.bundle,
                effective_mask=effective_mask,
                out_indices=indices.array,
                max_matches=1000,
                start_seq=0,
                kv=EMPTY_KV_CONDITIONS,
                text=EMPTY_TEXT_SEARCH,
            )
            nb_count_rows_by_module_level(
                segment.bundle, indices.array, match_count, SEQ_TYPE(1 << 62), TS_UNSPECIFIED, counts, span
            )
            break

    print("[Warmup] agent summary ... done")


def current_sequence(registry) -> Optional[int]:
    """The cursor to pass as `after_seq` for "everything from now on"."""
    central = registry.central
    if central is None or central.log_pool is None:
        return None
    return int(central.log_pool.latest_sequence())

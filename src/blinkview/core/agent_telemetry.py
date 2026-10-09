# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""The agent API's telemetry tools, on the same backend calls the GUI makes - no GUI involved:

- `telemetry_latest`: the latest message per module, from Registry.module_value_tracker (what
  the telemetry table and watch lists show).
- `telemetry_series`: the numeric history of one module, through get_telemetry_anchor /
  fetch_telemetry_arrays (what a plot draws)."""

from datetime import datetime, timedelta, timezone

import numpy as np

from blinkview.core.agent_api import AgentApiError
from blinkview.core.agent_log_read import SYSTEM_DEVICE, _find_device, _find_module, _number, _string
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_log import (
    allocate_discovery_workspace,
    allocate_telemetry_workspace,
    fetch_telemetry_arrays,
    get_telemetry_anchor,
)
from blinkview.utils.utc_offset import get_local_utc_offset_seconds

DEFAULT_MAX_VALUES = 200
HARD_MAX_VALUES = 5000
DEFAULT_MAX_POINTS = 100
HARD_MAX_POINTS = 2000
# Newest samples considered for one series request (statistics and decimation run over these).
SERIES_FETCH_CAP = 20_000

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


def _tz_offset_sec(args: dict) -> int:
    return 0 if args.get("utc", False) else get_local_utc_offset_seconds()


def _format_ts(ts_ns: int, tz_offset_sec: int) -> str:
    """Epoch ns -> "2026-10-09 20:52:26.508", the log lines' own layout."""
    moment = _EPOCH + timedelta(microseconds=int(ts_ns) // 1000 + tz_offset_sec * 1_000_000)
    return moment.strftime("%Y-%m-%d %H:%M:%S.") + f"{moment.microsecond // 1000:03d}"


def _number_text(value: float) -> str:
    return format(float(value), ".10g")


def _finite(value) -> "float | None":
    """A JSON-safe number: NaN/inf (not valid JSON) become null."""
    value = float(value)
    return value if np.isfinite(value) else None


def _channel_stats(channel: int, column: np.ndarray) -> dict:
    finite = column[np.isfinite(column)]
    stats = {"channel": channel, "first": _finite(column[0]), "last": _finite(column[-1])}
    if len(finite):
        stats.update(min=float(finite.min()), max=float(finite.max()), mean=float(finite.mean()))
    return stats


def telemetry_latest(registry, args: dict) -> dict:
    id_registry = registry.id_registry
    tracker = registry.module_value_tracker
    if tracker is None:
        raise AgentApiError("the value tracker is not running yet")

    device_name = _string(args, "device")
    module_path = _string(args, "module")
    if module_path and not device_name:
        raise AgentApiError("'module' needs 'device'")
    device = _find_device(id_registry, device_name) if device_name else None
    module = _find_module(id_registry, device, module_path) if module_path else None
    contains = _string(args, "contains").lower()
    include_system = args.get("include_system", False) or device_name == SYSTEM_DEVICE
    changed_after = int(_number(args, "changed_after_seq", 0, 0, float("inf")))
    max_values = int(_number(args, "max_values", DEFAULT_MAX_VALUES, 1, HARD_MAX_VALUES))
    tz_offset_sec = _tz_offset_sec(args)

    wanted_ids = None
    if module is not None:
        wanted_ids = {module.id, *(int(i) for i in id_registry.get_descendant_ids(module.id))}

    rows = []
    with tracker.get_snapshot() as snapshot:
        bundle = snapshot.bundle()
        next_seq = int(snapshot.last_known_seq)
        for candidate in list(id_registry.get_all_modules()):
            module_id = candidate.id
            if module_id >= bundle.count or not candidate.name:
                continue
            seq = int(bundle.sequence_ids[module_id])
            if seq == 0 or seq <= changed_after:
                continue
            if wanted_ids is not None and module_id not in wanted_ids:
                continue
            owner = candidate.device
            if device is not None and owner is not device:
                continue
            if owner.name == SYSTEM_DEVICE and not include_system:
                continue
            if contains and contains not in candidate.name.lower():
                continue
            rows.append(
                (owner.name, candidate.name, int(bundle.timestamps[module_id]), snapshot.get_message(module_id))
            )

    rows.sort()
    shown = rows[:max_values]
    text = "".join(f"{_format_ts(ts, tz_offset_sec)} {dev} {mod} = {value}\n" for dev, mod, ts, value in shown)
    return {
        "values": len(shown),
        "truncated": len(rows) > len(shown),
        # Pass as changed_after_seq to get only what changed since this call.
        "next_seq": next_seq,
        "utc_offset_minutes": tz_offset_sec // 60,
        "text": text,
    }


def telemetry_series(registry, args: dict) -> dict:
    id_registry = registry.id_registry
    central = registry.central
    if central is None or central.log_pool is None:
        raise AgentApiError("central storage is not running")
    log_pool = central.log_pool
    array_pool = registry.system_ctx.array_pool

    device_name = _string(args, "device")
    module_path = _string(args, "module")
    if not device_name or not module_path:
        raise AgentApiError("'device' and 'module' are required")
    module = _find_module(id_registry, _find_device(id_registry, device_name), module_path)

    max_points = int(_number(args, "max_points", DEFAULT_MAX_POINTS, 1, HARD_MAX_POINTS))
    since_s = _number(args, "since_s", None, 0, float("inf"))
    tz_offset_sec = _tz_offset_sec(args)

    # Same two steps as a plot opening on a module: find how many numeric channels its
    # messages carry, then extract the samples.
    anchor_seq, channels = get_telemetry_anchor(
        log_pool, module.id, SEQ_NONE, allocate_discovery_workspace(), SERIES_FETCH_CAP
    )
    if channels <= 0:
        raise AgentApiError(f"'{device_name}.{module_path}' has no numeric values (see its text with telemetry_latest)")
    start_seq = int(anchor_seq) - 1 if anchor_seq > 0 else int(SEQ_NONE)
    with fetch_telemetry_arrays(
        array_pool,
        log_pool,
        module.id,
        start_seq,
        channels,
        allocate_telemetry_workspace(channels),
        SERIES_FETCH_CAP,
    ) as batch:
        times_ns = batch.times_int64.copy()
        values = batch.values.copy()

    fetch_capped = len(times_ns) >= SERIES_FETCH_CAP
    if since_s is not None and len(times_ns):
        keep = times_ns >= registry.now_ns() - int(since_s * 1e9)
        times_ns, values = times_ns[keep], values[keep]

    count = len(times_ns)
    result = {
        "device": device_name,
        "module": module_path,
        "channels": int(channels),
        "samples": count,
        "utc_offset_minutes": tz_offset_sec // 60,
    }
    if fetch_capped:
        result["older_samples_not_read"] = True  # only the newest SERIES_FETCH_CAP were considered
    if count == 0:
        result.update(samples_returned=0, text="")
        return result

    result["first_time"] = _format_ts(times_ns[0], tz_offset_sec)
    result["last_time"] = _format_ts(times_ns[-1], tz_offset_sec)
    result["stats"] = [_channel_stats(channel, values[:, channel]) for channel in range(channels)]

    # Evenly spaced picks when there are more samples than asked for; first and last always kept.
    picks = np.unique(np.linspace(0, count - 1, min(count, max_points)).round().astype(np.int64))
    result["samples_returned"] = len(picks)
    result["text"] = "".join(
        f"{_format_ts(times_ns[i], tz_offset_sec)} {' '.join(_number_text(v) for v in values[i])}\n" for i in picks
    )
    return result

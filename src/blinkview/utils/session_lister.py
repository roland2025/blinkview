# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

# Deliberately lives in utils/ (a namespace package with no __init__.py side effects) rather
# than storage/ - storage/__init__.py eagerly imports file_logger, which transitively pulls in
# numba/ops/parsers/id_registry (~600 modules, ~0.5s). This module only reads metadata.json
# files off disk, so `blink replay --list` should stay a lightweight, fast operation.

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple, Optional

from blinkview.core.settings_manager import SettingsManager
from blinkview.utils.global_settings import get_blink_home
from blinkview.utils.project_settings import get_project_root, get_workspace_dir

_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_]+")

# FileManager.get_path_for_log pads part indexes to 4 digits, so part 10000 is just wider -
# hence \d{4,}, and sorting by int rather than by name.
_UNIFIED_PART_RE = re.compile(r"session\.(\d{4,})\.log(\.zst)?")
# Same as storage/log_file_archive.ARCHIVE_SUFFIX - not imported from there, see the module comment
# above.
ARCHIVE_SUFFIX = ".zst"


def _sanitize(name: str) -> str:
    """Mirrors FileManager._sanitize - must match how session folder names were built."""
    clean = _SANITIZE_RE.sub("_", name)
    clean = re.sub(r"_+", "_", clean)
    return clean.strip("_") or "Unnamed"


class SessionInfo(NamedTuple):
    session_id: str  # folder name, e.g. "20260314_124429_Examples_Can_Untitled"
    path: Path
    display_name: str
    profile: str
    status: str
    created_at: Optional[str]
    finished_at: Optional[str]
    duration_seconds: Optional[float]
    # Bytes the unified log's FileLogger has written (metadata.json loggers.session.total_bytes),
    # uncompressed. None when the metadata has no such entry.
    log_bytes: Optional[int] = None


def resolve_log_root(log_dir=None, settings: Optional[SettingsManager] = None) -> tuple[Path, str]:
    """Resolves the same <log_dir>/<project_name> root FileManager writes sessions under,
    without creating any directories - mirrors storage/file_manager.py's FileManager.__init__
    project_name/log_dir precedence (lines ~47-74) read-only."""
    settings = settings or SettingsManager()

    project_dir = get_project_root()
    standalone_mode = project_dir is None
    project_name = _resolve_project_name(settings, project_dir)

    if log_dir is None:
        log_dir = settings.get("log_dir")
    if standalone_mode:
        if log_dir is None:
            log_dir = get_blink_home() / "logs"
    else:
        if log_dir is None:
            log_dir = "logs"

    return Path(log_dir), project_name


def resolve_active_profile_dir(settings: Optional[SettingsManager] = None) -> Path:
    """The folder of the profile a plain `blink` run would use - FileManager.__init__'s
    profile_name precedence (active_profile, default_profile, then the project name standalone
    or "default"), read-only, without --profile/--config. Nothing is created."""
    settings = settings or SettingsManager()
    project_dir = get_project_root()
    profile_name = (
        settings.get("active_profile")
        or settings.get("default_profile")
        or (_resolve_project_name(settings, project_dir) if project_dir is None else "default")
    )
    return get_workspace_dir() / "profiles" / _sanitize(profile_name)


def _resolve_project_name(settings: SettingsManager, project_dir: Optional[Path]) -> str:
    project_name = settings.get("project_name")
    if project_name is None:
        project_name = project_dir.name if project_dir else None
    if project_name is None:
        project_name = Path.cwd().name
    return _sanitize(project_name)


def list_sessions(log_dir: Path, project_name: str) -> list[SessionInfo]:
    """Enumerates <log_dir>/<project_name>/* session folders, reading each metadata.json.
    Folders without a metadata.json (not a session dir, or still being created) are skipped."""
    project_dir = Path(log_dir) / project_name
    if not project_dir.is_dir():
        return []

    sessions = []
    for entry in sorted(project_dir.iterdir()):
        if not entry.is_dir():
            continue
        meta_path = entry / "metadata.json"
        if not meta_path.is_file():
            continue
        try:
            meta = json.loads(meta_path.read_text())
        except (OSError, json.JSONDecodeError):
            continue

        project_meta = meta.get("project", {})
        config_meta = meta.get("config", {})
        sessions.append(
            SessionInfo(
                session_id=meta.get("session_id", entry.name),
                path=entry,
                display_name=project_meta.get("display_name", entry.name),
                profile=config_meta.get("profile", ""),
                status=meta.get("status", "unknown"),
                created_at=meta.get("created_at"),
                finished_at=meta.get("finished_at"),
                duration_seconds=meta.get("duration_seconds"),
                log_bytes=_unified_log_bytes(meta),
            )
        )

    sessions.sort(key=lambda s: s.created_at or "", reverse=True)
    return sessions


def _unified_log_bytes(meta: dict) -> Optional[int]:
    try:
        total = meta["loggers"]["session"]["total_bytes"]
    except (KeyError, TypeError):
        return None
    return total if isinstance(total, int) else None


def resolve_session(
    log_dir: Path,
    project_name: str,
    name: Optional[str] = None,
    last: bool = False,
    require_unified_log: bool = True,
) -> Optional[SessionInfo]:
    """Resolves a single session by --last (most recent) or by session_id/display_name match.

    Sessions with no unified log (nothing to replay) are excluded by default - a session
    still `active` when queried, or one whose FileLogger never wrote data, isn't a valid
    replay target."""
    sessions = list_sessions(log_dir, project_name)
    if require_unified_log:
        sessions = [s for s in sessions if unified_log_parts(s)]
    if not sessions:
        return None

    if last:
        return sessions[0]  # already sorted newest-first

    if name is None:
        return None

    for s in sessions:
        if s.session_id == name:
            return s
    for s in sessions:
        if s.display_name == name:
            return s
    for s in sessions:
        if name.lower() in s.display_name.lower() or name.lower() in s.session_id.lower():
            return s

    return None


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None:
        return "unfinished"
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m {seconds:02d}s"
    hours, minutes = divmod(minutes, 60)
    return f"{hours}h {minutes:02d}m"


def _parse_timestamp(value: Optional[str]) -> Optional[datetime]:
    """metadata.json timestamps are UTC ISO 8601 (FileManager appends a trailing 'Z' after an
    already-offset isoformat())."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(value.rstrip("Z"))
    except ValueError:
        return None
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def format_session_label(session_info: SessionInfo) -> str:
    """Human-readable menu label: 'YYYY-MM-DD HH:MM (duration)  display_name [profile]',
    created_at shown in local time."""
    started = "????-??-?? ??:??"
    if session_info.created_at:
        dt = _parse_timestamp(session_info.created_at)
        started = dt.astimezone().strftime("%Y-%m-%d %H:%M") if dt else session_info.created_at

    label = f"{started} ({_format_duration(session_info.duration_seconds)})  {session_info.display_name}"
    if session_info.profile:
        label += f" [{session_info.profile}]"
    return label


def _format_size(size: Optional[int]) -> str:
    if size is None:
        return "?"
    if size < 1024:
        return f"{size} B"
    value = size / 1024
    for unit in ("KB", "MB"):
        if value < 1024:
            return f"{value:.1f} {unit}"
        value /= 1024
    return f"{value:.1f} GB"


def describe_session(session_info: SessionInfo) -> dict:
    """Everything `blink replay --list` shows about a session, as JSON-ready values.

    A session that never reached FileManager's finish (still recording, or the process was
    killed) has no duration_seconds - it is then estimated from the newest unified log part's
    mtime and flagged `duration_estimated`. Likewise `log_bytes` falls back to the parts' size on
    disk (smaller than the log itself for compressed parts)."""
    parts = unified_log_parts(session_info)
    started = _parse_timestamp(session_info.created_at)
    finished = _parse_timestamp(session_info.finished_at)

    disk_bytes = 0
    last_write = 0.0
    for part in parts:
        try:
            stat = part.stat()
        except OSError:
            continue
        disk_bytes += stat.st_size
        last_write = max(last_write, stat.st_mtime)

    duration = session_info.duration_seconds
    estimated = False
    if duration is None and started is not None and last_write:
        duration = round(max(0.0, last_write - started.timestamp()), 3)
        estimated = True

    return {
        "session_id": session_info.session_id,
        "name": session_info.display_name,
        "profile": session_info.profile,
        "status": session_info.status,
        "started_at": started.isoformat() if started else None,
        "finished_at": finished.isoformat() if finished else None,
        "duration_seconds": duration,
        "duration_estimated": estimated,
        "log_bytes": session_info.log_bytes or disk_bytes,
        "parts": len(parts),
        "path": str(session_info.path),
    }


def format_session_table(sessions: list[SessionInfo]) -> str:
    """The `blink replay --list` table, one session per line. NAME is last since it is the only
    column that may contain spaces."""
    rows = [("SESSION ID", "STARTED (local)", "DURATION", "SIZE", "STATUS", "PROFILE", "NAME")]
    for session_info in sessions:
        info = describe_session(session_info)
        started = _parse_timestamp(info["started_at"])
        duration = "?" if info["duration_seconds"] is None else _format_duration(info["duration_seconds"])
        rows.append(
            (
                info["session_id"],
                started.astimezone().strftime("%Y-%m-%d %H:%M") if started else "?",
                f"~{duration}" if info["duration_estimated"] else duration,
                _format_size(info["log_bytes"]),
                info["status"],
                info["profile"] or "-",
                info["name"],
            )
        )

    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]) - 1)]
    return "\n".join("  ".join(cell.ljust(width) for cell, width in zip(row, widths)) + "  " + row[-1] for row in rows)


def unified_log_parts(session_info: SessionInfo) -> list[Path]:
    """Returns the central FileLogger's session.NNNN.log[.zst] parts for a session, one per part
    index, in index order.

    Registry.configure_system() gives `central`'s FileLogger local_ctx.logging_id="session"
    (registry.py), so this is the unified log a live run writes - distinct from the raw
    per-source chunk files also living in the session folder.

    Only exact part names match - never a compress_file `.zst.tmp` still being written. When an
    index exists both plain and compressed, the plain file wins: it's either identical to the
    `.zst` (between compress_file's rename and FileLogger's unlink, or for good if the unlink
    failed after a rotation), or a superset of it (the unlink failed at shutdown, so the part
    index wasn't bumped and a restart() went on appending to the plain file). A plain part can
    still be unlinked after this returns - UnifiedLogReplay falls back to its `.zst` sibling.
    """
    if not session_info.path.is_dir():
        return []
    by_index: dict[int, Path] = {}
    for path in session_info.path.iterdir():
        match = _UNIFIED_PART_RE.fullmatch(path.name)
        if match is None or not path.is_file():
            continue
        index = int(match.group(1))
        if index not in by_index or match.group(2) is None:
            by_index[index] = path
    return [by_index[index] for index in sorted(by_index)]


def part_index(part: Path) -> Optional[int]:
    """The part index in a unified log part's file name (`session.0007.log[.zst]` -> 7), or None
    for any other name."""
    match = _UNIFIED_PART_RE.fullmatch(part.name)
    return int(match.group(1)) if match else None


def existing_part(part: Path) -> Optional[Path]:
    """The part to actually read for a listed `part`. A plain part listed by
    unified_log_parts() can be compressed and unlinked before a reader gets to it (a live
    session rotating, or the rename-then-unlink window) - its `.zst` sibling then holds the same
    content, complete since compress_file fsyncs before renaming."""
    if part.exists():
        return part
    if not part.name.endswith(ARCHIVE_SUFFIX):
        compressed = part.with_name(part.name + ARCHIVE_SUFFIX)
        if compressed.exists():
            return compressed
    return None

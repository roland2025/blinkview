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
from blinkview.utils.project_settings import get_project_root

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


def resolve_log_root(log_dir=None, settings: Optional[SettingsManager] = None) -> tuple[Path, str]:
    """Resolves the same <log_dir>/<project_name> root FileManager writes sessions under,
    without creating any directories - mirrors storage/file_manager.py's FileManager.__init__
    project_name/log_dir precedence (lines ~47-74) read-only."""
    settings = settings or SettingsManager()

    project_dir = get_project_root()
    standalone_mode = project_dir is None

    project_name = settings.get("project_name")
    if project_name is None:
        project_name = project_dir.name if project_dir else None
    if project_name is None:
        project_name = Path.cwd().name
    project_name = _sanitize(project_name)

    if log_dir is None:
        log_dir = settings.get("log_dir")
    if standalone_mode:
        if log_dir is None:
            log_dir = get_blink_home() / "logs"
    else:
        if log_dir is None:
            log_dir = "logs"

    return Path(log_dir), project_name


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
            )
        )

    sessions.sort(key=lambda s: s.created_at or "", reverse=True)
    return sessions


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


def format_session_label(session_info: SessionInfo) -> str:
    """Human-readable menu label: 'YYYY-MM-DD HH:MM (duration)  display_name [profile]'.
    created_at is stored as UTC ISO 8601 (FileManager appends a trailing 'Z' after an
    already-offset isoformat()), shown here in local time."""
    started = "????-??-?? ??:??"
    if session_info.created_at:
        try:
            dt = datetime.fromisoformat(session_info.created_at.rstrip("Z"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            started = dt.astimezone().strftime("%Y-%m-%d %H:%M")
        except ValueError:
            started = session_info.created_at

    label = f"{started} ({_format_duration(session_info.duration_seconds)})  {session_info.display_name}"
    if session_info.profile:
        label += f" [{session_info.profile}]"
    return label


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

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
from pathlib import Path
from typing import NamedTuple, Optional

from blinkview.core.settings_manager import SettingsManager
from blinkview.utils.global_settings import get_blink_home
from blinkview.utils.project_settings import get_project_root

_SANITIZE_RE = re.compile(r"[^A-Za-z0-9_]+")


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


def unified_log_parts(session_info: SessionInfo) -> list[Path]:
    """Returns the central FileLogger's session.NNNN.<ext> parts for a session, in order.

    Registry.configure_system() gives `central`'s FileLogger local_ctx.logging_id="session"
    (registry.py), so this is the unified log a live run writes - distinct from the raw
    per-source chunk files also living in the session folder.
    """
    parts = sorted(session_info.path.glob("session.*"))
    return parts

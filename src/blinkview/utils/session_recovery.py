# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Finalizes a session folder whose recording process died without a clean stop - see
plans/crash-safe-long-sessions.md (phase 2).

Lives in utils/ for the same reason as session_lister.py: nothing here may import the
storage/parsers cluster. zstandard is only imported once there is a part to compress."""

import json
import os
import platform
import re
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple, Optional

from blinkview.utils.atomic_json_dump import atomic_json_dump
from blinkview.utils.project_settings import get_workspace_dir
from blinkview.utils.session_lister import (
    ARCHIVE_SUFFIX,
    HEARTBEAT_STALE_S,
    _parse_timestamp,
    read_session_metadata,
    session_status,
)

# FileManager._create_session_dir: <YYYYmmdd_HHMMSS>_<profile>_<display name>
_SESSION_FOLDER_RE = re.compile(r"(\d{8}_\d{6})_(.+)")
# FileManager.get_path_for_log: <logging_id>.<part index>.<extension>, plus .zst once compressed.
_LOG_PART_RE = re.compile(r"(?P<logger>.+)\.(?P<index>\d{4,})\.(?P<ext>[A-Za-z0-9]+)(?P<zst>\.zst)?")

# Held for the duration of one recover_session() call. Two processes compressing the same part
# would write into one `.zst.tmp` (compress_file) and then delete the only good copy.
LOCK_FILE_NAME = ".recovery.lock"

# A recording process started this long after its session's created_at is still taken to be the
# recorder - created_at and the process start time come from different clocks' readings.
PROCESS_START_SLACK_S = 1.0


class RecoveryResult(NamedTuple):
    session_dir: Path
    # True when this call turned the session into a finished `crashed` one. False for a session
    # that was skipped (see `skipped`) or had already been recovered by an earlier call.
    recovered: bool
    # Why nothing was done, or None. A skipped session is left exactly as it was.
    skipped: Optional[str] = None
    metadata_rebuilt: bool = False
    cold_removed: bool = False
    tmp_removed: int = 0
    parts_compressed: int = 0
    # Steps that failed and are retried by the next call (e.g. a part a replay still has open).
    errors: tuple = ()


def recover_session(session_dir, compress: bool = True, force: bool = False) -> RecoveryResult:
    """Turns the folder of a crashed session into a finished one. Safe to call on any session
    folder, any number of times: a session that is still recording, or that finished cleanly, is
    left alone, and a second call only does what the first could not complete.

    - metadata.json gets `status: "crashed"`, `finished_at`, `duration_seconds` and
      `recovered_at`. An unreadable or missing one is rebuilt from the folder name and the part
      files (`metadata_rebuilt: true`).
    - `cold/` and `cold-archive/` are deleted. The process died before Registry.stop() moved the
      newest rows into them, so replay must parse the unified log instead of mounting them (see
      MainWindow.start_replay). The next replay rebuilds them, complete.
    - Leftover `*.tmp` files are deleted.
    - With `compress`, every part left uncompressed is compressed like a clean stop would have.
      This is the slow step (it reads every such part once) - call from a background task, or
      pass compress=False for the quick part only and call again later.

    A session recorded on another host is only judged by its heartbeat's age, i.e. by comparing
    two machines' clocks, so it is skipped unless `force` is given. `force` never overrides a
    recording process that is still running on this host."""
    session_dir = Path(session_dir)

    # Decided before taking the lock, so that nothing is ever created in a folder that is not
    # ours to touch (a live session's rename_new_session expects metadata.json alone in it).
    _, _, skipped = _assess(session_dir, force)
    if skipped is not None:
        return RecoveryResult(session_dir, False, skipped=skipped)

    if not _acquire_lock(session_dir):
        return RecoveryResult(session_dir, False, skipped="another process is recovering this session")
    try:
        return _recover_locked(session_dir, compress, force)
    finally:
        try:
            (session_dir / LOCK_FILE_NAME).unlink()
        except OSError:
            pass


def _recover_locked(session_dir: Path, compress: bool, force: bool) -> RecoveryResult:
    # Again, now under the lock: another process may have recovered the session in between.
    meta, first_recovery, skipped = _assess(session_dir, force)
    if skipped is not None:
        return RecoveryResult(session_dir, False, skipped=skipped)

    rebuilt = meta is None
    cold_removed = False
    errors = []

    if first_recovery:
        # Before the metadata: status "crashed" is what tells a later call that the cold tier it
        # finds was written by a replay of the recovered session and is to be kept.
        try:
            cold_removed = _remove_cold_storage(session_dir)
        except OSError as e:
            return RecoveryResult(session_dir, False, errors=(f"Could not remove cold storage: {e}",))

        if rebuilt:
            meta = _rebuild_metadata(session_dir)
        _mark_crashed(session_dir, meta)
        try:
            atomic_json_dump(meta, session_dir / "metadata.json")
        except OSError as e:
            return RecoveryResult(
                session_dir, False, cold_removed=cold_removed, errors=(f"Could not write metadata.json: {e}",)
            )

    tmp_removed = _remove_tmp_files(session_dir, errors)
    parts_compressed = _compress_plain_parts(session_dir, meta, errors) if compress else 0

    return RecoveryResult(
        session_dir,
        recovered=first_recovery,
        metadata_rebuilt=rebuilt,
        cold_removed=cold_removed,
        tmp_removed=tmp_removed,
        parts_compressed=parts_compressed,
        errors=tuple(errors),
    )


def _assess(session_dir: Path, force: bool) -> tuple[Optional[dict], bool, Optional[str]]:
    """Returns (metadata or None if unreadable, whether the session still has to be marked as
    crashed, the reason to leave it alone or None)."""
    if not session_dir.is_dir():
        return None, False, "not a directory"

    try:
        meta = read_session_metadata(session_dir)
    except (OSError, ValueError):
        meta = None
    if not isinstance(meta, dict):
        return None, True, _unreadable_metadata_skip_reason(session_dir)

    status = session_status(meta)
    if status != "crashed":
        return meta, False, f"session is {status}"
    if meta.get("status") == "crashed":
        return meta, False, None  # recovered before - only unfinished cleanup is left

    process = meta.get("process") if isinstance(meta.get("process"), dict) else {}
    host = process.get("host") or meta.get("environment", {}).get("node")
    if host and host != platform.node():
        if not force:
            return meta, True, f"recorded on another host ({host})"
    else:
        pid = process.get("pid")
        created = _parse_timestamp(meta.get("created_at"))
        if isinstance(pid, int) and created is not None and _process_running_since(pid, created.timestamp()):
            return meta, True, f"its recording process (pid {pid}) is still running"
    return meta, True, None


def _unreadable_metadata_skip_reason(session_dir: Path) -> Optional[str]:
    """Without metadata there is no pid or heartbeat to judge by. A recording session rewrites
    metadata.json with every heartbeat, so one that stayed unreadable, in a folder nothing was
    written to for HEARTBEAT_STALE_S, has no recorder."""
    if _SESSION_FOLDER_RE.fullmatch(session_dir.name) is None or not _log_parts(session_dir, None):
        return "not a session folder"
    newest = 0.0
    for path in session_dir.iterdir():
        if path.name == LOCK_FILE_NAME:
            continue  # our own
        try:
            newest = max(newest, path.stat().st_mtime)
        except OSError:
            continue
    if time.time() - newest <= HEARTBEAT_STALE_S:
        return "session folder was written to moments ago"
    return None


def _process_running_since(pid: int, timestamp: float) -> bool:
    """True if process `pid` exists and was already running at `timestamp` (epoch seconds) - a
    pid that was reused since belongs to a process started later."""
    import psutil

    try:
        return psutil.Process(pid).create_time() <= timestamp + PROCESS_START_SLACK_S
    except psutil.NoSuchProcess:
        return False
    except psutil.Error:
        return True  # exists but can't be inspected - don't assume it is gone


def _acquire_lock(session_dir: Path) -> bool:
    lock_path = session_dir / LOCK_FILE_NAME
    for _ in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            if not _lock_is_stale(lock_path):
                return False
            try:
                lock_path.unlink()
            except OSError:
                return False
            continue
        except OSError:
            return False
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump({"pid": os.getpid(), "host": platform.node()}, f)
        return True
    return False


def _lock_is_stale(lock_path: Path) -> bool:
    """A lock left behind by a recovery that was itself killed. One held from another host can't
    be checked and is respected."""
    try:
        locked_at = lock_path.stat().st_mtime
        owner = json.loads(lock_path.read_text(encoding="utf-8"))
        pid, host = owner["pid"], owner["host"]
    except OSError:
        return False
    except (ValueError, KeyError, TypeError):
        # Created but not written yet, or its owner died in between.
        return time.time() - locked_at > HEARTBEAT_STALE_S
    if host != platform.node() or not isinstance(pid, int):
        return False
    return not _process_running_since(pid, locked_at)


def _remove_cold_storage(session_dir: Path) -> bool:
    removed = False
    for name in ("cold", "cold-archive"):
        path = session_dir / name
        if path.is_dir():
            shutil.rmtree(path)
            removed = True
    return removed


def _remove_tmp_files(session_dir: Path, errors: list) -> int:
    """Unfinished atomic_json_dump / compress_file writes. Only the session folder itself: its
    `replay/` scratch subfolder belongs to replays of this session."""
    removed = 0
    for path in session_dir.glob("*.tmp"):
        if not path.is_file():
            continue
        try:
            path.unlink()
            removed += 1
        except OSError as e:
            errors.append(f"Could not remove {path.name}: {e}")
    return removed


def _log_parts(session_dir: Path, meta: Optional[dict]) -> list[tuple[Path, "re.Match"]]:
    """Every FileLogger part in the folder (unified log and raw source logs), compressed or not.
    With metadata, only parts of the loggers it lists; without, anything named like one - the
    session's JSON snapshots are the only other files FileManager puts there."""
    loggers = meta.get("loggers") if meta else None
    known = None
    if isinstance(loggers, dict) and loggers:
        known = {(name, entry.get("extension")) for name, entry in loggers.items() if isinstance(entry, dict)}

    parts = []
    for path in sorted(session_dir.iterdir()):
        match = _LOG_PART_RE.fullmatch(path.name)
        if match is None or not path.is_file():
            continue
        if known is not None:
            if (match["logger"], match["ext"]) not in known:
                continue
        elif match["ext"] == "json":
            continue
        parts.append((path, match))
    return parts


def _rebuild_metadata(session_dir: Path) -> dict:
    """What list_sessions/replay need, as far as the folder itself tells: when the session
    started, its profile and display name, and its loggers' parts."""
    stamp, rest = _SESSION_FOLDER_RE.fullmatch(session_dir.name).groups()
    # The folder name carries local time (FileManager._create_session_dir).
    created = datetime.strptime(stamp, "%Y%m%d_%H%M%S").astimezone(timezone.utc)
    profile, display_name = _split_profile_and_name(rest)

    loggers: dict = {}
    for _, match in _log_parts(session_dir, None):
        entry = loggers.setdefault(match["logger"], {"extension": match["ext"], "last_part": 0})
        entry["last_part"] = max(entry["last_part"], int(match["index"]))

    return {
        "session_id": session_dir.name,
        "status": "active",
        "created_at": created.isoformat() + "Z",
        "metadata_rebuilt": True,
        "project": {"display_name": display_name},
        "config": {"profile": profile},
        "loggers": loggers,
    }


def _split_profile_and_name(rest: str) -> tuple[str, str]:
    """`<profile>_<display name>` can't be split on its own, both may contain underscores - the
    longest profile of this workspace that it starts with is taken. Unknown profile: all of it
    becomes the display name."""
    try:
        profiles = [p.name for p in (get_workspace_dir() / "profiles").iterdir() if p.is_dir()]
    except OSError:
        profiles = []
    for profile in sorted(profiles, key=len, reverse=True):
        if rest.startswith(profile + "_"):
            return profile, rest[len(profile) + 1 :]
    return "", rest


def _mark_crashed(session_dir: Path, meta: dict) -> None:
    """The crashed counterpart of FileManager._finalize_metadata, in the same timestamp format."""
    meta["status"] = "crashed"
    meta["recovered_at"] = datetime.now(timezone.utc).isoformat() + "Z"

    finished = _estimate_finish_time(session_dir, meta)
    created = _parse_timestamp(meta.get("created_at"))
    if finished is None or created is None:
        return
    finished = max(finished, created)
    meta["finished_at"] = finished.isoformat() + "Z"
    meta["duration_seconds"] = round((finished - created).total_seconds(), 3)


def _estimate_finish_time(session_dir: Path, meta: dict) -> Optional[datetime]:
    """When the recorder died: its last heartbeat, or the last write to a part if that came
    after it (replay ends its seek bar at finished_at, which must not cut off the last rows).
    A part's mtime is only believed within HEARTBEAT_STALE_S of the heartbeat - a folder copied
    without its timestamps has parts "written" long after the session. Sessions from before the
    heartbeat existed have the parts only."""
    newest = 0.0
    for path, _ in _log_parts(session_dir, meta):
        try:
            newest = max(newest, path.stat().st_mtime)
        except OSError:
            continue
    last_write = datetime.fromtimestamp(newest, timezone.utc) if newest else None

    process = meta.get("process")
    heartbeat = _parse_timestamp(process.get("heartbeat_at")) if isinstance(process, dict) else None
    if heartbeat is None:
        return last_write
    if last_write is None:
        return heartbeat
    return min(max(heartbeat, last_write), heartbeat + timedelta(seconds=HEARTBEAT_STALE_S))


def _compress_plain_parts(session_dir: Path, meta: dict, errors: list) -> int:
    """Same outcome as FileLogger's own compression of a closed part: `<part>.zst` replaces
    `<part>`. An existing `.zst` of a part that is still there uncompressed is overwritten - the
    plain file is the complete one (see session_lister.unified_log_parts). An empty part is
    left as it is, like FileLogger does."""
    plain = []
    for path, match in _log_parts(session_dir, meta):
        try:
            if match["zst"] is None and path.stat().st_size > 0:
                plain.append(path)
        except OSError:
            continue
    if not plain:
        return 0

    import zstandard

    from blinkview.core.zstd_file_compression import compress_file

    compressed = 0
    for path in plain:
        try:
            compress_file(path, path.with_name(path.name + ARCHIVE_SUFFIX))
        except (OSError, zstandard.ZstdError) as e:
            errors.append(f"Could not compress {path.name}: {e}")
            continue
        compressed += 1
        try:
            path.unlink()
        except OSError as e:
            # A replay has the part mapped (Windows). Readers prefer the plain file as long as
            # both exist, and the next call tries again.
            errors.append(f"Could not remove {path.name} after compressing it: {e}")
    return compressed


# --- `blink recover` ---

RECOVER_DESCRIPTION = """Finalizes sessions whose recording was killed (crash, taskkill, power loss).

Without SESSION, every session of this project is checked. Sessions that are still recording
or that finished cleanly are left alone. A crashed one is marked as such with its end time, an
unreadable metadata.json is rebuilt, its partial cold storage and temp files are removed, and
its uncompressed log parts are compressed. Running it again is harmless."""


def setup_recover_parser(parser) -> None:
    from argparse import RawDescriptionHelpFormatter

    parser.description = RECOVER_DESCRIPTION
    parser.formatter_class = RawDescriptionHelpFormatter
    parser.add_argument("sessions", nargs="*", metavar="SESSION", help="session id or session folder (default: all)")
    parser.add_argument(
        "-l", "--logdir", default=None, help="log folder to look sessions up in (default: the project's)"
    )
    parser.add_argument("--no-compress", action="store_true", help="leave uncompressed log parts as they are")
    parser.add_argument(
        "--force",
        action="store_true",
        help="also recover sessions recorded on another host (make sure they are not still recording)",
    )


def run_recover(args) -> None:
    import sys

    from blinkview.utils.session_lister import resolve_log_root

    log_dir, project_name = resolve_log_root(log_dir=args.logdir)
    project_dir = Path(log_dir) / project_name

    failed = False
    if args.sessions:
        folders = []
        for name in args.sessions:
            folder = next((p for p in (project_dir / name, Path(name)) if p.is_dir()), None)
            if folder is None:
                print(f"blink recover: no session folder '{name}' in {project_dir}", file=sys.stderr)
                failed = True
            else:
                folders.append(folder)
    else:
        # Not list_sessions(): it leaves out exactly the sessions whose metadata.json is broken.
        folders = sorted(p for p in project_dir.iterdir() if p.is_dir()) if project_dir.is_dir() else []

    recovered = 0
    broken = 0
    for folder in folders:
        result = recover_session(folder, compress=not args.no_compress, force=args.force)
        failed = failed or bool(result.errors)
        recovered += result.recovered
        broken += _was_broken(result)
        line = _describe_result(result, verbose=bool(args.sessions))
        if line:
            print(f"{folder.name}: {line}")
        for error in result.errors:
            print(f"{folder.name}: {error}", file=sys.stderr)

    if broken:
        print(f"{recovered} recovered of {broken} broken session(s).", file=sys.stderr)
    else:
        print(f"No broken sessions among {len(folders)}.", file=sys.stderr)
    if failed:
        sys.exit(1)


def _was_broken(result: RecoveryResult) -> bool:
    """Whether the session needed recovering when `blink recover` got to it - recovered now, or
    crashed but left alone (another host, a step failed, ...). Not one that is fine, or that an
    earlier run already recovered."""
    if result.skipped is not None:
        healthy = result.skipped.startswith("session is ") or result.skipped in (
            "not a session folder",
            "not a directory",
        )
        return not healthy
    return result.recovered or bool(result.errors)


def _describe_result(result: RecoveryResult, verbose: bool) -> Optional[str]:
    """One line for `blink recover`, or None for a session there is nothing to say about - when
    all sessions are checked, that is every one that finished cleanly."""
    if result.skipped is not None:
        if not verbose and result.skipped in ("session is finished", "not a session folder"):
            return None
        return f"skipped - {result.skipped}"

    done = []
    if result.recovered:
        done.append("metadata rebuilt, marked as crashed" if result.metadata_rebuilt else "marked as crashed")
    if result.cold_removed:
        done.append("partial cold storage removed")
    if result.tmp_removed:
        done.append(f"{result.tmp_removed} temp file(s) removed")
    if result.parts_compressed:
        done.append(f"{result.parts_compressed} log part(s) compressed")
    if done:
        return ", ".join(done)
    return "already recovered" if verbose else None

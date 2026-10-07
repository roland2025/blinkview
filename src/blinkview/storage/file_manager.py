# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import hashlib
import json
import platform
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock, RLock
from time import monotonic
from typing import Any, Callable, Dict, Optional

from blinkview import __version__ as blinkview_version
from blinkview.core.settings_manager import SettingsManager
from blinkview.core.system_context import SystemContext
from blinkview.storage.file_logger import FileLogger
from blinkview.utils.atomic_json_dump import atomic_json_dump
from blinkview.utils.global_settings import get_blink_home
from blinkview.utils.project_settings import get_project_root, get_workspace_dir


def _get_file_hash(path: Path) -> str:
    if not path.exists():
        return "unknown"
    return hashlib.md5(path.read_bytes()).hexdigest()


def get_session_identity(config_path) -> str:
    workspace = get_project_root()
    if workspace:
        try:
            rel = config_path.resolve().relative_to(workspace.resolve())
            return "_".join(rel.with_suffix("").parts)
        except ValueError:
            pass
    return config_path.stem


# How long FileManager.rotate() waits for every running FileLogger to close its current part
# before switching session_dir anyway - a logger thread only notices the request between batches
# (FileLogger.QUEUE_POLL_TIMEOUT_S), plus one flush.
ROTATION_ACK_TIMEOUT_S = 10.0


class FileManager:
    def __init__(
        self,
        session_name: str = None,
        profile_name: str = None,
        log_dir=None,
        config_path=None,
        replay_mode: bool = False,
    ):
        self.system_context: SystemContext = None
        self.gui_context = None

        self._project_dir = get_project_root()
        print(f"[FileManager] project_dir={self._project_dir}")

        self.standalone_mode = self._project_dir is None
        print(f"[FileManager] standalone_mode={self.standalone_mode}")

        self._workspace_dir = get_workspace_dir()
        print(f"[FileManager] workspace_dir={self._workspace_dir}")

        settings = SettingsManager()

        self.provided_config_path = Path(config_path) if config_path else None
        print(f"[FileManager] provided_config_path={self.provided_config_path}")

        self.session_identity = get_session_identity(self.provided_config_path) if self.provided_config_path else None
        print(f"[FileManager] session_identity={self.session_identity}")

        # Resolve project name with the following precedence:
        project_name = settings.get("project_name")

        if project_name is None:
            project_name = self._project_dir.name if self._project_dir else None

        if project_name is None:
            project_name = Path.cwd().name

        self.project_name = self._sanitize(project_name)
        print(f"[FileManager] project_name={self.project_name}")

        self.profile_name = self._sanitize(
            self.session_identity
            or profile_name
            or settings.get("active_profile")
            or settings.get("default_profile")
            or (self.project_name if self.standalone_mode else "default")
        )
        if self.standalone_mode and self.provided_config_path:
            self.profile_name = self._sanitize(f"{self.project_name} {self.provided_config_path.stem}")

        print(f"[FileManager] profile_name={self.profile_name}")

        self._profile_dir = self._workspace_dir / "profiles" / self.profile_name
        self._profile_dir.mkdir(parents=True, exist_ok=True)
        print(f"[FileManager] profile_dir={self._profile_dir}")

        self.config_dir = self.provided_config_path.parent if self.provided_config_path else self._profile_dir
        print(f"[FileManager] config_dir={self.config_dir}")

        self.config_file_name = self.provided_config_path.stem if self.provided_config_path else self.profile_name
        print(f"[FileManager] config_file_name={self.config_file_name}")

        # resolve log_dir with the following precedence:
        if log_dir is None:
            log_dir = settings.get("log_dir")

        if self.standalone_mode:
            if log_dir is None:
                log_dir = get_blink_home() / "logs"
        else:
            if log_dir is None:
                log_dir = "logs"

        self.log_dir = Path(log_dir)
        print(f"[FileManager] log_dir={self.log_dir}")

        self.session_display_name = self._sanitize(session_name or "Untitled")
        print(f"[FileManager] session_display_name={self.session_display_name}")

        self.replay_mode = replay_mode

        # replay_source_dir, once set (by Registry.load_replay_session/ui.run.run), is the
        # folder of the *original* session being replayed. Nothing under it is ever modified
        # directly - get_config_path/get_session_path/get_playback_ranges_path instead mirror
        # into a "replay/" scratch subfolder of it (see _redirect_to_replay_scratch).
        self.replay_source_dir: Optional[Path] = None

        # In replay mode, this session's own folder is created lazily (only if something with
        # nowhere better to go - e.g. FileManager.stop()'s final bookkeeping - actually needs to
        # write) rather than unconditionally on every replay launch, which used to leave an
        # empty timestamped folder behind purely as a side effect of opening a replay.
        # Unique (not exist_ok) for live runs: two instances of one profile (one per board, see
        # --params) started within the same second would otherwise share - and corrupt - one folder.
        self.session_dir = self._create_unique_session_dir() if not replay_mode else self._create_session_dir(False)
        print(f"[FileManager] session_dir={self.session_dir}")

        # Guards session_dir/metadata against FileLogger threads writing their stats while
        # rotate() swaps both over to a new session folder.
        self._lock = RLock()
        # Serializes whole rotations (a second Clear while one is still waiting on loggers).
        self._rotation_lock = Lock()

        # This run's profile parameters (see record_params / core/profile_params.py).
        self.params: Dict[str, Any] = {}
        self.params_files: list = []
        self.params_label: Optional[str] = None
        self.params_session: Optional[str] = None  # the params file's "session", if any
        self.param_origins: Dict[str, str] = {}  # parameter -> "set 'left'" / "--param" / ...
        self.dev_mode = False  # set by the Registry via record_dev_mode()

        # Write initial metadata
        self.metadata = self._build_metadata()

        if not replay_mode:
            self.write_metadata()

        self._file_loggers = []

    def _build_metadata(self) -> dict:
        """Fresh metadata.json content for the current session_dir - used at construction and
        again for each new session started by rotate()."""
        return {
            "session_id": self.session_dir.name,  # Unique ID based on timestamp
            "status": "active",
            "created_at": datetime.now(timezone.utc).isoformat() + "Z",
            "version": blinkview_version,
            "project": {
                "name": self.project_name,
                "display_name": self.session_display_name,
                "mode": "standalone" if self.standalone_mode else "project",
            },
            "config": {
                "profile": self.profile_name,
                "workspace": str(self._workspace_dir.resolve()),
                "source_file": str(self.get_config_path()),
                "source_hash": _get_file_hash(self.get_config_path()),
                "log_dir": str(self.log_dir.resolve()),
                "params": dict(self.params),
                "params_files": [str(p) for p in self.params_files],
            },
            "environment": {
                "cwd": str(Path.cwd().resolve()),
                "argv": sys.argv,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "node": platform.node(),
                # Whether BlinkView's own stats/tuner lines were logged - see utils/dev_mode.py
                "dev_mode": self.dev_mode,
                # "git": self._get_git_info()
            },
            "loggers": {},
        }

    def _finalize_metadata(self, finished_time: datetime):
        """Marks the in-memory metadata as finished (status, finished_at, duration) - shared by
        stop() and rotate(). Doesn't write it; callers decide whether/where to."""
        # We parse the 'created_at' string back into a datetime object
        try:
            start_time = datetime.fromisoformat(self.metadata["created_at"].rstrip("Z")).replace(tzinfo=timezone.utc)
            duration = (finished_time - start_time).total_seconds()
        except Exception:
            duration = 0

        self.metadata["status"] = "finished"
        self.metadata["finished_at"] = finished_time.isoformat() + "Z"
        self.metadata["duration_seconds"] = round(duration, 3)

    def record_params(
        self,
        params: Dict[str, Any],
        params_files=None,
        label: Optional[str] = None,
        session_name: Optional[str] = None,
        origins: Optional[Dict[str, str]] = None,
    ):
        """Stores this run's effective profile parameters in metadata.json (kept across rotate())."""
        self.params = dict(params or {})
        self.params_files = list(params_files or [])
        self.params_label = label
        self.params_session = session_name
        self.param_origins = dict(origins or {})
        self.metadata["config"]["params"] = dict(self.params)
        self.metadata["config"]["params_files"] = [str(p) for p in self.params_files]
        if not self.replay_mode:
            self.write_metadata()

    def record_dev_mode(self, dev_mode: bool) -> None:
        """Stores this run's dev mode in metadata.json (kept across rotate())."""
        self.dev_mode = bool(dev_mode)
        self.metadata["environment"]["dev_mode"] = self.dev_mode
        if not self.replay_mode:
            self.write_metadata()

    def rename_new_session(self, display_name: str) -> bool:
        """Gives the just-created session a different display name (and folder) - for a name
        that is only known once the profile's files can be read, e.g. a params file's
        "session". Only while nothing but metadata.json was written; returns False otherwise."""
        clean = self._sanitize(display_name)
        if clean == self.session_display_name:
            return True
        if self.replay_mode:
            return False
        try:
            if any(f.name != "metadata.json" for f in self.session_dir.iterdir()):
                return False
        except OSError:
            return False

        self.discard_empty_session()
        self.session_display_name = clean
        self.session_dir = self._create_unique_session_dir()
        self.metadata = self._build_metadata()
        self.write_metadata()
        return True

    def discard_empty_session(self):
        """Removes the just-created session folder if nothing but metadata.json was written to it -
        for a startup that is aborted before anything ran (e.g. a bad --param)."""
        try:
            if not self.session_dir.is_dir():
                return
            contents = list(self.session_dir.iterdir())
            if all(f.is_file() and f.name == "metadata.json" for f in contents):
                for f in contents:
                    f.unlink()
                self.session_dir.rmdir()
        except OSError as e:
            print(f"[FileManager] Could not remove empty session folder {self.session_dir}: {e}")

    def _sanitize(self, name: str) -> str:
        # Allow alphanumeric and underscores, replace everything else with '_'
        # Then squeeze multiple underscores into one
        clean = re.sub(r"[^A-Za-z0-9_]", "_", name)
        clean = re.sub(r"_+", "_", clean)
        # Strip leading/trailing underscores and return
        return clean.strip("_") or "Unnamed"

    def set_context(self, system_context, gui_context=None):
        self.system_context = system_context

    def set_gui_context(self, gui_context):
        self.gui_context = gui_context
        self.snapshot_gui_start()

    def snapshot_gui_start(self):
        """Copies the workspace GUI config/state into the current session folder as its `start`
        record - at startup, and for each new session after rotate() (the caller saves the
        workspace layout first, see MainWindow's session rotation). The GUI config is recorded
        from its ConfigManager, like the main config's `start` - `<profile>.gui.json` does not
        exist on disk until the first watch is added."""
        self.save_gui_config(suffix="start")
        self._snapshot_master_to_session("gui_state", self.get_gui_state_path(for_load=True))

    def _create_session_dir(self, create: bool = True) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Identity (Examples_Can) + Display Name (Untitled)
        clean_identity = self.profile_name
        clean_display = self.session_display_name

        # 20260314_124429_Examples_Can_Untitled
        folder_name = f"{timestamp}_{clean_identity}_{clean_display}"

        # Path: logs/ProjectName/20260314_124429_Examples_Can_Untitled
        path = self.log_dir / self.project_name / folder_name
        if create:
            path.mkdir(parents=True, exist_ok=True)
        return path

    def _create_unique_session_dir(self) -> Path:
        """Like _create_session_dir(), but never reuses an existing folder - the name only has
        one-second resolution, and mkdir(exist_ok=True) would silently merge two sessions rotated
        within the same second. Appends _2, _3, ... on collision."""
        base = self._create_session_dir(create=False)
        base.parent.mkdir(parents=True, exist_ok=True)
        candidate = base
        n = 2
        while True:
            # mkdir itself is the check: another process (a second instance of this profile) may
            # create the same name between an exists() test and our mkdir.
            try:
                candidate.mkdir()
                break
            except FileExistsError:
                candidate = base.with_name(f"{base.name}_{n}")
                n += 1
        return candidate

    def _get_git_info(self) -> Dict[str, Any]:
        """Captures basic git metadata."""
        import subprocess

        try:
            # Short hash
            sha = (
                subprocess.check_output(["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.STDOUT)
                .decode()
                .strip()
            )
            # Check for uncommitted changes
            status = subprocess.call(["git", "diff", "--quiet"])
            return {"hash": sha, "dirty": status != 0}
        except Exception:
            return {"hash": "unknown", "dirty": False}

    def write_metadata(self):
        """Writes or updates the metadata.json file in the session folder. Never called while
        replaying (see __init__/replay_mode) - this is always *this run's own* bookkeeping
        folder, never the original session being replayed."""
        with self._lock:
            self._ensure_session_dir()
            meta_file = self.session_dir / "metadata.json"

            with meta_file.open("w") as f:
                json.dump(self.metadata, f, indent=4)

    def _ensure_session_dir(self):
        """Materializes the (possibly not-yet-created, in replay mode) session folder on first
        real write - see the `create=not replay_mode` comment in __init__."""
        self.session_dir.mkdir(parents=True, exist_ok=True)

    def _redirect_to_replay_scratch(self, original_path: Path, filename: str) -> Path:
        """While replaying (self.replay_source_dir set), mirrors `original_path` (which would
        otherwise point at a file inside the *original* session's own folder, or the live
        workspace profile) into a `replay/` scratch subfolder of that original session instead -
        seeding it with a one-time copy of the current content on first access. Every subsequent
        read/write for that file, for the lifetime of this replay, goes through that scratch copy
        only - the original session's files (and the live workspace profile) are never opened for
        writing. Reopening the same session for replay later picks the scratch copy back up
        (still routed through this same method), so edits made during a previous replay of this
        session persist across replay runs without ever having touched the original."""
        scratch_dir = self.replay_source_dir / "replay"
        scratch_dir.mkdir(parents=True, exist_ok=True)
        target = scratch_dir / filename
        if not target.exists() and original_path.exists():
            import shutil

            shutil.copy2(original_path, target)
        return target

    def get_path(self, filename: str) -> Path:
        """Helper to get a full path for a new file within the session folder."""
        self._ensure_session_dir()
        return self.session_dir / filename

    def get_playback_ranges_path(self) -> Path:
        """A fixed filename (not routed through get_session_path's <config_file_name>-prefixed
        naming) so it can be found by simple, stable-name lookup when a *different* run later
        replays a file that lives inside this session's folder - see
        Registry._discover_replay_ranges_path / core/playback_ranges.py.

        While replaying, redirected into that original session's `replay/` scratch subfolder
        (see _redirect_to_replay_scratch) instead of the original playback_ranges.json itself."""
        filename = "playback_ranges.json"
        if self.replay_source_dir is not None:
            return self._redirect_to_replay_scratch(self.replay_source_dir / filename, filename)
        return self.session_dir / filename

    def __repr__(self):
        return f"FileManager(session='{self.session_dir.name}')"

    def save_snapshot(self, paths_to_save: list[str | Path]):
        """
        Snapshots files/directories using shutil for kernel-level
        efficiency and low memory overhead.
        """
        import shutil

        self._ensure_session_dir()
        snapshot_dir = self.session_dir / "snapshot"
        snapshot_dir.mkdir(exist_ok=True)

        # Define the ignore pattern
        ignore_patterns = shutil.ignore_patterns("__pycache__", ".git", ".pytest_cache", "*.pyc", "*.pyo")

        for path_str in paths_to_save:
            src = Path(path_str)
            if not src.exists():
                continue

            dst = snapshot_dir / src.name

            try:
                if src.is_dir():
                    # dirs_exist_ok=True allows overwriting/merging if called twice
                    shutil.copytree(src, dst, ignore=ignore_patterns, dirs_exist_ok=True)
                else:
                    # copy2 preserves metadata (timestamps, permissions)
                    shutil.copy2(src, dst)

            except Exception as e:
                # Using your existing log style
                print(f"[FileManager] Failed to snapshot {src}: {e}")

    def get_path_for_log(self, file_logger: FileLogger, part: int = 0) -> Path:
        """
        Returns a path for a log chunk with 4-digit padding for safety.
        Format: <session_dir>/<log_name>.<part_index>.<extension>
        Example: logs/20260313_202158_TestRun/src_502d8046.0000.bin
        """
        ext = file_logger.batch_processor.extension
        logging_id = file_logger.local.logging_id

        # Using 4-digit padding (0000-9999)
        part_suffix = f"{part:04d}"

        filename = f"{logging_id}.{part_suffix}.{ext}"

        self._ensure_session_dir()
        return self.session_dir / filename

    @property
    def file_logger_count(self) -> int:
        """Number of currently-registered file loggers - lets a caller (Registry.stop's
        shutdown-compression progress reporting) know the total upfront."""
        return len(self._file_loggers)

    def stop(self, on_progress: Optional[Callable[[str], None]] = None):
        """The 'Closer' - Saves final state and stops loggers.

        `on_progress(logging_id)`, if given, is called once per file logger right after its
        `.stop()` returns - that call is what actually triggers that logger's final-part
        compression synchronously (BaseDaemon.stop() joins the logger thread, whose run() loop
        compresses its last part on exit - see FileLogger._close_and_compress_final_part). This
        function only reports what it directly knows (one logger just finished stopping);
        aggregating that into an overall "i of N" count alongside cold storage compression is
        Registry.stop()'s job, not this module's."""
        # Save Final Daemon Config
        if self.system_context:
            # Save final snapshots using the central path logic
            self.system_context.registry.config.save_full_config(self.get_session_path("final"))

        self.save_gui_config(suffix="final")
        self.save_gui_state(suffix="final")

        # Stop Threaded Loggers
        for logger in self._file_loggers:
            logger.stop()
            if on_progress:
                on_progress(logger.local.logging_id)

        # Finalize Manifest
        self._finalize_metadata(datetime.now(timezone.utc))

        # Replay runs never get their own metadata.json - writing one would make this run itself
        # show up as a selectable entry in the Load Session menu (session_lister.list_sessions
        # only considers folders that have one), recreating exactly the clutter this mode exists
        # to avoid. The in-memory dict above is still finalized for anything that inspects it.
        if not self.replay_mode:
            self.write_metadata()

    def rotate(self, display_name: Optional[str] = None) -> tuple[Path, Path]:
        """Ends the current session folder and starts a new one, without stopping anything -
        see plans/session-rotation.md. Returns (old_session_dir, new_session_dir).

        Every running FileLogger is asked to close its current part (on its own thread) and to
        wait; once all have acknowledged (or ROTATION_ACK_TIMEOUT_S passes), the old metadata is
        finalized in the old folder, session_dir switches to a new uniquely-named folder with
        fresh metadata, and the loggers are released to reopen at part 0 there. Data still
        queued for a logger at that moment simply goes to the new session.

        `display_name`, if given, renames the session from here on (new folder name and
        metadata display_name). Snapshots of config/GUI state are the caller's job - they need
        the registry/UI thread (see Registry.rotate_session)."""
        if self.replay_mode or self.replay_source_dir is not None:
            raise RuntimeError("Session rotation is not available while replaying")

        with self._rotation_lock:
            requests = []
            for file_logger in list(self._file_loggers):
                request = file_logger.request_session_rotation()
                if request is not None:
                    requests.append(request)

            try:
                deadline = monotonic() + ROTATION_ACK_TIMEOUT_S
                for request in requests:
                    remaining = max(0.0, deadline - monotonic())
                    if not request.closed.wait(remaining):
                        print("[FileManager] rotate: a file logger did not close its part in time")

                with self._lock:
                    old_dir = self.session_dir
                    old_session_id = self.metadata.get("session_id", old_dir.name)

                    if display_name:
                        self.session_display_name = self._sanitize(display_name)
                    new_dir = self._create_unique_session_dir()

                    self._finalize_metadata(datetime.now(timezone.utc))
                    self.metadata["next_session_id"] = new_dir.name
                    self.write_metadata()

                    self.session_dir = new_dir
                    self.metadata = self._build_metadata()
                    self.metadata["previous_session_id"] = old_session_id
                    # Every logger restarts at part 0 in the new folder (FileLogger resets its own
                    # part_index on its thread once released - see FileLogger._rotate_session).
                    for file_logger in self._file_loggers:
                        entry = self._new_logger_entry(file_logger)
                        entry["last_part"] = 0
                        self.metadata["loggers"][file_logger.local.logging_id] = entry
                    self.write_metadata()
            finally:
                # Always release the loggers, even if something above failed - otherwise they'd
                # sit blocked until their own resume timeout.
                for request in requests:
                    request.resume.set()

        print(f"[FileManager] Rotated session: {old_dir.name} -> {new_dir.name}")
        return old_dir, new_dir

    @staticmethod
    def _new_logger_entry(file_logger: FileLogger) -> dict:
        return {
            "processor": file_logger.batch_processor.__class__.__name__,
            "extension": file_logger.batch_processor.extension,
            "last_part": file_logger.part_index,
            "total_bytes": 0,
        }

    def add_file_logger(self, file_logger: FileLogger):
        logger_id = file_logger.local.logging_id
        if logger_id in self.metadata["loggers"]:
            # RESTORE STATE:
            # We increment the last_part so that re-enabling the logger
            # always starts a fresh file part (e.g., .0000 -> .0001)
            # instead of appending to a potentially closed/interrupted file.
            new_part = self.metadata["loggers"][logger_id].get("last_part", 0) + 1
            file_logger.part_index = new_part
            self.metadata["loggers"][logger_id]["last_part"] = new_part

            print(f"[FileManager] Restored logger {logger_id} to part {new_part}")
        else:
            # INITIALIZE NEW ENTRY:
            self.metadata["loggers"][logger_id] = self._new_logger_entry(file_logger)
        self.write_metadata()

        if file_logger not in self._file_loggers:
            self._file_loggers.append(file_logger)

    def remove_file_logger(self, file_logger: FileLogger):
        if file_logger in self._file_loggers:
            self._file_loggers.remove(file_logger)

    def update_logger_stats(self, file_logger: FileLogger, bytes_written: int, absolute: bool = False):
        """Updates the byte tally. If absolute is True, replaces the value."""
        logger_id = file_logger.local.logging_id
        with self._lock:
            if logger_id in self.metadata["loggers"]:
                if absolute:
                    self.metadata["loggers"][logger_id]["total_bytes"] = bytes_written
                else:
                    current_total = self.metadata["loggers"][logger_id].get("total_bytes", 0)
                    self.metadata["loggers"][logger_id]["total_bytes"] = current_total + bytes_written

                self.write_metadata()

    def _get_gui_dir(self) -> Path:
        """Helper to ensure gui directory exists."""
        gui_dir = self.session_dir / "gui"
        gui_dir.mkdir(exist_ok=True)
        return gui_dir

    def save_gui_config(self, suffix: str = "final"):
        """Records the GUI config (watches) in the session folder. The workspace file
        `<profile>.gui.json` is not written here - its ConfigManager is the only writer."""
        gui_config = getattr(self.gui_context, "gui_config", None)
        if gui_config is None:
            return

        atomic_json_dump(gui_config.get_data(), self.get_session_path("gui", suffix))

    def save_gui_state(self, suffix: str = "autosave", session_only: bool = False):
        """Saves UI layout. If session_only is True, does not touch the Workspace."""
        if not self.gui_context or not hasattr(self.gui_context, "gui_state"):
            return

        data = self.gui_context.gui_state.get_data()

        # Workspace (Live Master) - Skip if session_only is requested
        if not session_only:
            atomic_json_dump(data, self.get_gui_state_path())

        # Session (Historical Archive) - Always save
        atomic_json_dump(data, self.get_session_path("gui_state", suffix))

    def save_gui(self):
        self.save_gui_config("final")
        self.save_gui_state("final")

    def get_config_path(self, type_name: str = None) -> Path:
        """Traffic Cop for Config vs State. While replaying, redirected into the original
        session's `replay/` scratch subfolder (see _redirect_to_replay_scratch) instead of the
        live workspace profile, so editing e.g. the watchlist while replaying an old session
        can't clobber the profile's current live settings."""
        filename = f"{self.config_file_name}.json" if type_name is None else f"{self.config_file_name}.{type_name}.json"
        original = self.config_dir / filename
        if self.replay_source_dir is not None:
            return self._redirect_to_replay_scratch(original, filename)
        return original

    def get_gui_state_path(self, for_load: bool = False) -> Path:
        """The window layout file. An instance started with --params/--param keeps its own
        (`<profile>.<label>.gui_state.json`, e.g. one per board) so several instances of one
        profile don't overwrite each other's layout on exit. Until that file is first saved,
        loading falls back to the shared `<profile>.gui_state.json`."""
        shared = self.get_config_path("gui_state")
        label = getattr(self, "params_label", None)
        if not label:
            return shared
        own = self.get_config_path(f"{self._sanitize(label)}.gui_state")
        if for_load and not own.exists():
            return shared
        return own

    def get_profile_path(self, type_name: str) -> Path:
        """Like get_config_path(), but always the live workspace profile - never redirected into
        a replay scratch folder. For user-owned data that isn't tied to a session (view layout
        presets), so saving one while replaying still lands in the profile."""
        return self.config_dir / f"{self.config_file_name}.{type_name}.json"

    def get_session_path(self, type_name: str = None, suffix: str = None) -> Path:
        """Brands session files with the profile context. While replaying, redirected into the
        original session's `replay/` scratch subfolder instead of that session's own top-level
        files (see _redirect_to_replay_scratch)."""
        base = self.config_file_name if type_name is None else f"{self.config_file_name}.{type_name}"
        filename = f"{base}.{suffix}.json" if suffix else f"{base}.json"
        if self.replay_source_dir is not None:
            return self._redirect_to_replay_scratch(self.replay_source_dir / filename, filename)
        return self.session_dir / filename

    def _snapshot_master_to_session(self, type_name: str, master_path: Optional[Path] = None):
        """
        Copies the live workspace file (or `master_path`) to the session folder as a '.start'
        record. Preserves original timestamps and permissions.
        """
        master_path = master_path or self.get_config_path(type_name)
        session_start_path = self.get_session_path(type_name, suffix="start")

        if master_path.exists():
            try:
                import shutil

                # copy2 preserves metadata/timestamps; read_bytes does not.
                shutil.copy2(master_path, session_start_path)
            except Exception as e:
                # Assuming you have a logger, or sticking to your print style:
                print(f"[FileManager] Failed to snapshot {type_name}: {e}")

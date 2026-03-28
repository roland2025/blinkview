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
from typing import Any, Dict

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


class FileManager:
    def __init__(self, session_name: str = None, profile_name: str = None, log_dir=None, config_path=None):
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

        self.session_dir = self._create_session_dir()
        print(f"[FileManager] session_dir={self.session_dir}")

        # Write initial metadata
        self.metadata = {
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
            },
            "environment": {
                "cwd": str(Path.cwd().resolve()),
                "argv": sys.argv,
                "python": platform.python_version(),
                "platform": platform.platform(),
                "node": platform.node(),
                # "git": self._get_git_info()
            },
            "loggers": {},
        }

        self.write_metadata()

        self._file_loggers = []

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
        self._snapshot_master_to_session("gui_config")
        self._snapshot_master_to_session("gui_state")

    def _create_session_dir(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Identity (Examples_Can) + Display Name (Untitled)
        clean_identity = self.profile_name
        clean_display = self.session_display_name

        # 20260314_124429_Examples_Can_Untitled
        folder_name = f"{timestamp}_{clean_identity}_{clean_display}"

        # Path: logs/ProjectName/20260314_124429_Examples_Can_Untitled
        path = self.log_dir / self.project_name / folder_name
        path.mkdir(parents=True, exist_ok=True)
        return path

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
        """Writes or updates the metadata.json file in the session folder."""
        meta_file = self.session_dir / "metadata.json"

        with meta_file.open("w") as f:
            json.dump(self.metadata, f, indent=4)

    def get_path(self, filename: str) -> Path:
        """Helper to get a full path for a new file within the session folder."""
        return self.session_dir / filename

    def __repr__(self):
        return f"FileManager(session='{self.session_dir.name}')"

    def save_snapshot(self, paths_to_save: list[str | Path]):
        """
        Copies files or directories using pure Pathlib/OS to avoid the Shutil memory tax.
        """
        snapshot_dir = self.session_dir / "snapshot"
        snapshot_dir.mkdir(exist_ok=True)

        # Patterns to ignore
        ignore_set = {"__pycache__", ".git", ".pytest_cache"}
        ignore_ext = {".pyc", ".pyo"}

        for path_str in paths_to_save:
            src = Path(path_str)
            if not src.exists():
                continue

            dst = snapshot_dir / src.name

            try:
                if src.is_dir():
                    # Recursive walk without importing 'shutil'
                    for item in src.rglob("*"):
                        # Check if any part of the path is in our ignore list
                        if any(part in ignore_set for part in item.parts) or item.suffix in ignore_ext:
                            continue

                        # Create the relative destination path
                        relative_dst = dst / item.relative_to(src)

                        if item.is_dir():
                            relative_dst.mkdir(parents=True, exist_ok=True)
                        else:
                            relative_dst.parent.mkdir(parents=True, exist_ok=True)
                            relative_dst.write_bytes(item.read_bytes())
                else:
                    # Direct file copy
                    dst.write_bytes(src.read_bytes())
            except Exception as e:
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

        self.metadata["loggers"][logging_id]["last_part"] = part

        self.write_metadata()

        return self.session_dir / filename

    def stop(self):
        """The 'Closer' - Saves final state and stops loggers."""
        # Save Final Daemon Config
        if self.system_context:
            # Save final snapshots using the central path logic
            self.system_context.registry.config.save_full_config(self.get_session_path("final"))

        self.save_gui_config(suffix="final")
        self.save_gui_state(suffix="final")

        # Stop Threaded Loggers
        for logger in self._file_loggers:
            logger.stop()

        # Finalize Manifest
        finished_time = datetime.now(timezone.utc)

        # Calculate duration
        # We parse the 'created_at' string back into a datetime object
        try:
            start_time = datetime.fromisoformat(self.metadata["created_at"].rstrip("Z")).replace(tzinfo=timezone.utc)
            duration = (finished_time - start_time).total_seconds()
        except Exception:
            duration = 0

        # Update the metadata dictionary
        self.metadata["status"] = "finished"
        self.metadata["finished_at"] = finished_time.isoformat() + "Z"
        self.metadata["duration_seconds"] = round(duration, 3)

        self.write_metadata()

    def add_file_logger(self, file_logger: FileLogger):
        if file_logger in self._file_loggers:
            return

        self.metadata["loggers"][file_logger.local.logging_id] = {
            "processor": file_logger.batch_processor.__class__.__name__,
            "extension": file_logger.batch_processor.extension,
            "last_part": 0,
        }

        self.write_metadata()

        self._file_loggers.append(file_logger)

    def update_logger_stats(self, file_logger: FileLogger, bytes_written: int, absolute: bool = False):
        """Updates the byte tally. If absolute is True, replaces the value."""
        logger_id = file_logger.local.logging_id
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

    def save_gui_config(self, suffix: str = "autosave", session_only: bool = False):
        """Saves GUI preferences. If session_only is True, does not touch the Workspace."""
        if not self.gui_context or not hasattr(self.gui_context, "gui_config"):
            return

        data = self.gui_context.gui_config.get_data()

        # Workspace (Live Master) - Skip if session_only is requested
        if not session_only:
            atomic_json_dump(data, self.get_config_path("gui_config"))

        # Session (Historical Archive) - Always save
        atomic_json_dump(data, self.get_session_path("gui_config", suffix))

    def save_gui_state(self, suffix: str = "autosave", session_only: bool = False):
        """Saves UI layout. If session_only is True, does not touch the Workspace."""
        if not self.gui_context or not hasattr(self.gui_context, "gui_state"):
            return

        data = self.gui_context.gui_state.get_data()

        # Workspace (Live Master) - Skip if session_only is requested
        if not session_only:
            atomic_json_dump(data, self.get_config_path("gui_state"))

        # Session (Historical Archive) - Always save
        atomic_json_dump(data, self.get_session_path("gui_state", suffix))

    def save_gui(self):
        self.save_gui_config("final")
        self.save_gui_state("final")

    def get_config_path(self, type_name: str = None) -> Path:
        """Traffic Cop for Config vs State."""
        if type_name is None:
            return self.config_dir / f"{self.config_file_name}.json"

        return self.config_dir / f"{self.config_file_name}.{type_name}.json"

    def get_session_path(self, type_name: str = None, suffix: str = None) -> Path:
        """Brands session files with the profile context."""
        base = self.config_file_name if type_name is None else f"{self.config_file_name}.{type_name}"
        filename = f"{base}.{suffix}.json" if suffix else f"{base}.json"
        return self.session_dir / filename

    def _snapshot_master_to_session(self, type_name: str):
        """Copies the live workspace file to the session folder as a '.start' record."""

        master_path = self.get_config_path(type_name)
        session_start_path = self.get_session_path(type_name, suffix="start")

        if master_path.exists():
            try:
                # We copy the raw file to preserve exactly what was on disk
                session_start_path.write_bytes(master_path.read_bytes())
            except Exception as e:
                print(f"[FileManager] Failed to snapshot {type_name}: {e}")

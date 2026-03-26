# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import sys
import subprocess
from pathlib import Path
from packaging.version import parse as parse_version

from blinkview.core.settings_manager import SettingsManager


class UpdateError(Exception):
    """Custom exception for update-related failures."""
    pass


class Updater:
    def __init__(self, settings: SettingsManager | None = None):
        self.settings = settings or SettingsManager()

        # Pull configuration directly from the manager
        src_path = self.settings.get("update.path")
        if not src_path or not self.is_valid_repo(src_path):
            raise UpdateError("Update path not set. Run: blink config --global update.path /path/to/repo")

        self.repo_path = Path(src_path).resolve()

        is_editable_val = str(self.settings.get("update.editable", "")).lower()
        self.editable = is_editable_val in ["true", "1", "yes"]

        features_raw = self.settings.get("update.features", "all")
        self.features_suffix = self._parse_features(features_raw)

    def _parse_features(self, features_raw: str) -> str:
        if not features_raw:
            return ""
        clean_features = ",".join([f.strip() for f in features_raw.split(",") if f.strip()])
        return f"[{clean_features}]" if clean_features else ""

    @staticmethod
    def is_valid_repo(path: Path | str) -> bool:
        """
        Checks if a path is a valid BlinkView source tree.
        Can be called without instantiating the class.
        """
        p = Path(path)
        # 1. Basic Git check
        if not (p / ".git").is_dir():
            return False

        # 2. Identity check via pyproject.toml
        if not (p / "pyproject.toml").exists():
            return False

        # 3. Source check
        main_py = p / "src" / "blinkview" / "__main__.py"
        return main_py.is_file()

    def fetch(self) -> None:
        try:
            subprocess.run(
                ["git", "-C", str(self.repo_path), "fetch", "--tags"],
                check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Fetch failed: {e.stderr or e.stdout or str(e)}")

    def get_versions(self, remote: bool = False) -> list[str]:
        cmd = ["git", "-C", str(self.repo_path), "tag", "-l"]
        if remote:
            cmd = ["git", "-C", str(self.repo_path), "ls-remote", "--tags", "origin"]

        try:
            result = subprocess.run(cmd, capture_output=True, text=True, check=True)
            lines = result.stdout.strip().splitlines()
            if remote:
                tags = [line.split("refs/tags/")[-1] for line in lines if "refs/tags/" in line]
            else:
                tags = lines
            return sorted(tags, key=lambda t: parse_version(t), reverse=True) if tags else []

        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Failed to list versions: {e.stderr or str(e)}")

    def get_latest_version(self) -> str | None:
        versions = self.get_versions(remote=False)
        return versions[0] if versions else None

    def install(self, version: str) -> bool:
        try:
            subprocess.run(
                ["git", "-C", str(self.repo_path), "checkout", version],
                check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Failed to checkout {version}: {e.stderr or str(e)}")

        install_target = f"{self.repo_path}{self.features_suffix}"
        cmd = [
            "uv", "tool", "install", install_target,
            "--python", sys.executable,
            "--force", "--refresh"
        ]

        if self.editable:
            cmd.append("--editable")

        if sys.platform == "win32":
            cmd_str = " ".join(cmd)
            detached_cmd = f'cmd /c "timeout /t 2 > nul && {cmd_str}"'
            subprocess.Popen(
                detached_cmd, shell=True,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP
            )
            return True
        else:
            try:
                subprocess.run(cmd, check=True, capture_output=True, text=True)
                return False
            except subprocess.CalledProcessError as e:
                raise UpdateError(f"Installation failed: {e.stderr or str(e)}")

    def upgrade(self, current_version: str) -> tuple[bool, str]:
        self.fetch()
        target = self.get_latest_version()

        if not target:
            raise UpdateError("No tags found in repository.")

        if parse_version(target) <= parse_version(current_version):
            return False, target

        requires_exit = self.install(target)
        return True, target

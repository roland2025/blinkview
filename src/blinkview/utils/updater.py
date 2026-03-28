# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import sys
from pathlib import Path
from time import time

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

        # Retrieve update channel (defaults to stable)
        self.channel = str(self.settings.get("update.channel", "stable")).lower()

    def _parse_features(self, features_raw: str) -> str:
        if not features_raw:
            return ""
        clean_features = ",".join([f.strip() for f in features_raw.split(",") if f.strip()])
        return f"[{clean_features}]" if clean_features else ""

    def _is_version_allowed(self, tag: str) -> bool:
        """Filters versions based on the selected update channel."""
        v = parse_version(tag)

        if self.channel == "stable":
            # Stable: No pre-releases (alpha, beta, rc) and no dev releases
            return not v.is_prerelease and not v.is_devrelease

        elif self.channel == "rc":
            # RC: Allow stable releases and specifically 'rc' pre-releases. Reject dev/alpha/beta.
            if v.is_devrelease:
                return False
            if v.is_prerelease:
                # v.pre is a tuple like ('rc', 1) or ('a', 0)
                return v.pre is not None and v.pre[0] == "rc"
            return True  # It's a stable release

        elif self.channel == "dev":
            # Dev: Unrestricted (stable, rc, alpha, beta, dev)
            return True

        # Fallback to stable if an unknown channel is set
        return not v.is_prerelease and not v.is_devrelease

    @staticmethod
    def is_valid_repo(path: Path | str) -> bool:
        """
        Checks if a path is a valid BlinkView source tree.
        Can be called without instantiating the class.
        """
        p = Path(path)
        # Basic Git check
        if not (p / ".git").is_dir():
            return False

        # Identity check via pyproject.toml
        if not (p / "pyproject.toml").exists():
            return False

        # Source check
        main_py = p / "src" / "blinkview" / "__main__.py"
        return main_py.is_file()

    def fetch(self, force: bool = False) -> bool:
        """
        Runs git fetch if the cooldown has expired or if force is True.
        Returns True if a network fetch was performed, False otherwise.
        """
        import subprocess
        from datetime import datetime

        # Check Cooldown
        last_fetch_ts = self.settings.get("update.last_fetch_time", 0)

        if not force and last_fetch_ts > 0:
            custom_cooldown = self.settings.get("update.cooldown_seconds")

            if custom_cooldown is not None:
                # Option A: Fixed seconds-based cooldown
                if time() - last_fetch_ts < int(custom_cooldown):
                    return False
            else:
                # Option B: Date-change logic (Default)
                last_date = datetime.fromtimestamp(last_fetch_ts).date()
                current_date = datetime.now().date()

                if last_date == current_date:
                    print(f"[Updater] Fetch skipped. Already checked today ({last_date}).")
                    return False

        # Execute Network Call
        try:
            print("[Updater] Contacting remote for updates...")
            subprocess.run(
                ["git", "-C", str(self.repo_path), "fetch", "--tags"], check=True, capture_output=True, text=True
            )

            # Success! Update timestamp
            self.settings.set("update.last_fetch_time", time(), scope="global")
            return True

        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Fetch failed: {e.stderr or e.stdout or str(e)}")

    def get_versions(self, remote: bool = False) -> list[str]:
        import subprocess

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
            # Apply channel filtering before sorting
            valid_tags = [t for t in tags if self._is_version_allowed(t)]
            return sorted(valid_tags, key=lambda t: parse_version(t), reverse=True) if tags else []

        except subprocess.CalledProcessError as e:
            raise UpdateError(f"Failed to list versions: {e.stderr or str(e)}")

    def get_latest_version(self) -> str | None:
        versions = self.get_versions(remote=False)
        return versions[0] if versions else None

    def install(self, version: str) -> bool:
        import subprocess

        # Mark that we are attempting an upgrade to this specific version
        self.settings.set("update.pending_version", version, scope="global")

        try:
            subprocess.run(
                ["git", "-C", str(self.repo_path), "checkout", version], check=True, capture_output=True, text=True
            )
        except subprocess.CalledProcessError as e:
            # If git fails, we haven't actually started the install yet
            self.settings.set("update.pending_version", None, scope="global")
            raise UpdateError(f"Failed to checkout {version}: {e.stderr or str(e)}")

        install_target = f"{self.repo_path}{self.features_suffix}"
        cmd = ["uv", "tool", "install", install_target, "--python", sys.executable, "--force", "--refresh"]

        if self.editable:
            cmd.append("--editable")

        if sys.platform == "win32":
            cmd_str = " ".join(cmd)
            detached_cmd = f'cmd /c "timeout /t 2 > nul && {cmd_str}"'
            subprocess.Popen(
                detached_cmd,
                shell=True,
                creationflags=subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP,
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

    def clear_pending_status(self):
        """Removes the pending version flag from settings."""
        self.settings.set("update.pending_version", None, scope="global")

    def check_version_status(self, current_version_str: str) -> tuple[bool | None, str | None]:
        """
        Compares the current app version against the pending version.
        Returns (Success, VersionString) or (None, None) if no update was pending.
        """
        pending = self.settings.get("update.pending_version")
        if not pending:
            return None, None

        # Clear the flag immediately so we don't nag the user on every launch
        self.settings.set("update.pending_version", None, scope="global")

        try:
            v_current = parse_version(current_version_str)
            v_pending = parse_version(pending)

            # Success is defined as the current version being equal to or newer
            # than what we tried to install.
            return v_current >= v_pending, pending
        except Exception:
            # Fallback for invalid version strings
            return current_version_str == pending, pending

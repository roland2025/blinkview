# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from blinkview.utils.atomic_json_dump import atomic_json_dump
from blinkview.utils.global_settings import get_blink_home
from blinkview.utils.settings import Settings


def get_project_root() -> Optional[Path]:
    """
    Search upwards for a .blinkview directory that acts as a project root.
    STOPS at the User Home to prevent mismatching with global config.
    """

    env_root = os.environ.get("BLINK_PROJECT_ROOT")
    if env_root:
        return Path(env_root).resolve()

    current = Path.cwd().resolve()
    user_home = get_blink_home()

    for parent in [current] + list(current.parents):
        # Check if we found a .blinkview folder
        target = parent / ".blinkview"

        if target.is_dir():
            # VALIDATION: Ensure it's a project (contains project.json)
            # and NOT just the global user folder.
            if (target / "project.json").exists() and parent != user_home:
                return parent

        # STOP SEARCH if we hit the user home directory
        if parent == user_home:
            break

    return None


def get_project_settings_path() -> Optional[Path]:
    root = get_project_root()
    if root:
        return root / ".blinkview" / "project.json"
    return None


def get_workspace_dir() -> Path:
    """
    The Master Resolver: Project wins, User is the fallback.
    """
    # Check if we are inside a project
    project_root = get_project_root()
    if project_root:
        return project_root / ".blinkview"

    # Otherwise, we are in 'Standalone Mode', use Global Home
    return get_blink_home()


class ProjectSettings(Settings):
    def __init__(self):
        super().__init__()

        project_root = get_project_root()
        if project_root:
            self.set_path(project_root / ".blinkview" / "project.json")

    @classmethod
    def supported_keys(cls):
        """Returns a list of supported settings keys."""
        return "active_profile", "project_name", "created_at", "log_dir"

    @classmethod
    def init(cls, path=None):
        """Initializes a new BlinkView project in the specified path (or current directory if None). Creates a .blinkview folder and a project.json marker file."""

        if path is None:
            path = Path.cwd()

        project_folder = Path(path) / ".blinkview"
        project_file = project_folder / "project.json"

        # Prepare Project Data
        data = {
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

        # Write the marker file
        atomic_json_dump(data, project_file)

        print(f"Initialized BlinkView project in '{project_folder.resolve()}'")
        return cls()


def switch_profile(name, create=False):
    """Switches the active profile. If create=True, also creates a new config file if it doesn't exist."""
    workspace = get_workspace_dir()
    project_root = get_project_root()

    profile_dir = workspace / "profiles" / name

    if not profile_dir.exists():
        if create:
            profile_dir.mkdir(parents=True)
            print(f"Created new profile '{name}' at {profile_dir.resolve()}")
        else:
            raise FileNotFoundError(f"Profile '{name}' does not exist at {profile_dir.resolve()}")

    if project_root:
        # If we are in a project, we can also write the active profile to the project settings for easy retrieval
        settings = ProjectSettings.load()
        settings["active_profile"] = name
        settings.save()

        print(f"Switched to profile '{name}' within project '{project_root.name}'")


def get_active_profile_name() -> str:
    try:
        return ProjectSettings().get("active_profile", "default") or "default"
    except Exception:
        return "default"


def duplicate_profile(source: str, new_name: str) -> Path:
    """Copies profile `source` to a new profile `new_name`. Profile files are named after their
    profile (`<name>.json`, `<name>.gui_state.json`, ...), so those prefixes are renamed too -
    otherwise FileManager would not find them under the new profile. Does not switch to it."""
    # FileManager sanitizes profile names the same way; a name that sanitizes to something else
    # would be copied into one folder and then loaded from another.
    clean = re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9_]", "_", new_name)).strip("_")
    if not clean or clean != new_name:
        raise ValueError(f"Invalid profile name '{new_name}' (only letters, digits and '_' allowed; try '{clean}')")

    profiles_path = get_workspace_dir() / "profiles"
    src_dir = profiles_path / source
    dst_dir = profiles_path / new_name

    if not src_dir.is_dir():
        raise FileNotFoundError(f"Profile '{source}' does not exist at {src_dir.resolve()}")
    if dst_dir.exists():
        raise FileExistsError(f"Profile '{new_name}' already exists at {dst_dir.resolve()}")

    shutil.copytree(src_dir, dst_dir)

    prefix = f"{source}."
    for f in dst_dir.iterdir():
        if f.is_file() and f.name.startswith(prefix):
            f.rename(dst_dir / f"{new_name}.{f.name[len(prefix) :]}")

    print(f"Duplicated profile '{source}' to '{new_name}' at {dst_dir.resolve()}")
    return dst_dir


def setup_project_parser(parser):
    """Adds profile-related arguments to the command-line parser."""
    # We use nargs="?" so --list can work without providing a name
    parser.add_argument("profile", nargs="?", type=str, help="The name of the profile to switch to.")
    parser.add_argument(
        "-l", "--list", action="store_true", help="List all available profiles in the current workspace."
    )
    parser.add_argument(
        "-c", "--create", action="store_true", help="Create the profile if it doesn't exist (used with profile name)."
    )
    parser.add_argument(
        "--copy",
        metavar="NEW_NAME",
        type=str,
        help="Copy the given profile (or the active one if omitted) to a new profile named NEW_NAME.",
    )


def handle_profile_args(args):
    """Logic for the 'profile' command."""
    workspace = get_workspace_dir()
    profiles_path = workspace / "profiles"

    # Handle --list
    if args.list:
        if not profiles_path.exists():
            print("No profiles directory found.")
            return

        profiles = [p.name for p in profiles_path.iterdir() if p.is_dir()]

        # Try to identify the active profile for the UI
        active = get_active_profile_name()

        print("--- Available Profiles ---")
        for p in sorted(profiles):
            indicator = "*" if p == active else " "
            print(f"{indicator} {p}")
        return

    # Handle --copy
    if args.copy:
        try:
            duplicate_profile(args.profile or get_active_profile_name(), args.copy)
        except (ValueError, FileNotFoundError, FileExistsError) as e:
            print(f"Error: {e}")
            raise SystemExit(1)
        return

    # Handle Switch/Create
    if args.profile:
        switch_profile(args.profile, create=args.create)
    else:
        # If no profile name and no --list, show help
        print("Error: Please specify a profile name or use --list.")

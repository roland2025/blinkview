# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
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
        return "active_profile", "project_name", "created_at", "log_dir", "dev_mode"

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


def _check_profile_name(name: str):
    """FileManager sanitizes profile names this way; a name that sanitizes to something else
    would be written into one folder and then loaded from another."""
    clean = re.sub(r"_+", "_", re.sub(r"[^A-Za-z0-9_]", "_", name)).strip("_")
    if not clean or clean != name:
        raise ValueError(f"Invalid profile name '{name}' (only letters, digits and '_' allowed; try '{clean}')")


def switch_profile(name, create=False, agent=False):
    """Switches the active profile. If create=True, also creates the profile's folder if it doesn't
    exist. With agent=True a profile that has no config file yet gets one with an `mcp_server`
    pipeline that may change the configuration (see core/default_config.py)."""
    workspace = get_workspace_dir()
    project_root = get_project_root()

    profile_dir = workspace / "profiles" / name
    config_file = profile_dir / f"{name}.json"

    if agent:
        _check_profile_name(name)
        if config_file.exists():
            raise FileExistsError(
                f"Profile '{name}' already has a configuration at {config_file.resolve()}. "
                "Add a pipeline of type 'mcp_server' to it in BlinkView's settings instead."
            )

    if not profile_dir.exists():
        if create:
            profile_dir.mkdir(parents=True)
            print(f"Created new profile '{name}' at {profile_dir.resolve()}")
        else:
            raise FileNotFoundError(f"Profile '{name}' does not exist at {profile_dir.resolve()}")

    if agent:
        from blinkview.core.default_config import agent_profile_config

        atomic_json_dump(agent_profile_config(), config_file)
        print(f"Wrote {config_file.resolve()} with an 'mcp_server' pipeline: an agent may change this profile.")
        print("Register it with Claude Code once:  claude mcp add blinkview -- blink mcp")
        print(f"Then start BlinkView on this profile:  blink -p {name}")

    if project_root:
        # If we are in a project, we can also write the active profile to the project settings for easy retrieval
        settings = ProjectSettings.load()
        settings["active_profile"] = name
        settings.save()

        print(f"Switched to profile '{name}' within project '{project_root.name}'")


def init_agent_project(path=None) -> Path:
    """`blink init --agent`: a project an AI agent can set up from nothing. Makes `path` a project
    if it is not one yet, gives its profile (the active one, else 'default') a configuration with
    an `mcp_server` pipeline that may change it, and registers `blink mcp` in the folder's
    `.mcp.json`, which Claude Code reads. Nothing that already exists is overwritten."""
    from blinkview.core.default_config import agent_profile_config

    root = Path(path) if path is not None else Path.cwd()
    workspace = root / ".blinkview"
    project_file = workspace / "project.json"

    if project_file.exists():
        print(f"'{root.resolve()}' is already a BlinkView project")
    else:
        ProjectSettings.init(root)

    try:
        with open(project_file) as f:
            name = json.load(f).get("active_profile") or "default"
    except (OSError, ValueError, AttributeError):
        name = "default"
    _check_profile_name(name)

    config_file = workspace / "profiles" / name / f"{name}.json"
    if config_file.exists():
        print(
            f"Profile '{name}' already has a configuration, left as it is: add a pipeline of type "
            "'mcp_server' to it in BlinkView's settings."
        )
    else:
        config_file.parent.mkdir(parents=True, exist_ok=True)
        atomic_json_dump(agent_profile_config(), config_file)
        print(f"Wrote profile '{name}' with an 'mcp_server' pipeline: an agent may change this profile.")

    mcp_file = root / ".mcp.json"
    try:
        with open(mcp_file) as f:
            mcp_config = json.load(f)
        servers = mcp_config.setdefault("mcpServers", {})
        if not isinstance(servers, dict):
            raise ValueError("'mcpServers' is not an object")
    except FileNotFoundError:
        mcp_config = {"mcpServers": {}}
        servers = mcp_config["mcpServers"]
    except (OSError, ValueError, AttributeError) as e:
        print(f"Could not read {mcp_file.resolve()} ({e}), left as it is.")
        print("Register the server yourself:  claude mcp add blinkview -- blink mcp")
        servers = None

    if servers is not None:
        if "blinkview" in servers:
            print(f"{mcp_file.name} already has a 'blinkview' server, left as it is.")
        else:
            servers["blinkview"] = {"command": "blink", "args": ["mcp"]}
            atomic_json_dump(mcp_config, mcp_file)
            print(f"Registered `blink mcp` in {mcp_file.resolve()}")

    print("Start BlinkView in this folder (`blink`), then start the agent here and ask it to set up your board.")
    return root


def get_active_profile_name() -> str:
    try:
        return ProjectSettings().get("active_profile", "default") or "default"
    except Exception:
        return "default"


def duplicate_profile(source: str, new_name: str) -> Path:
    """Copies profile `source` to a new profile `new_name`. Profile files are named after their
    profile (`<name>.json`, `<name>.gui_state.json`, ...), so those prefixes are renamed too -
    otherwise FileManager would not find them under the new profile. Does not switch to it."""
    _check_profile_name(new_name)

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
        "--agent",
        "--mcp",
        dest="agent",
        action="store_true",
        help="Give a profile that has no configuration yet one with an 'mcp_server' pipeline that may change "
        "the configuration, so an AI agent (`blink mcp`) can set it up. Usually with --create.",
    )
    parser.add_argument(
        "--copy",
        metavar="NEW_NAME",
        type=str,
        help="Copy the given profile (or the active one if omitted) to a new profile named NEW_NAME.",
    )
    params_group = parser.add_argument_group("profile parameters (one profile for several identical devices)")
    params_group.add_argument(
        "--add-param",
        nargs=2,
        metavar=("NAME", "PATH"),
        help="Declare parameter NAME bound to config PATH (e.g. /sources/src_1eb62594/serial_number). "
        "Repeat for the same NAME to bind more paths.",
    )
    params_group.add_argument("--description", type=str, default=None, help="Description for --add-param.")
    params_group.add_argument(
        "--show-params", action="store_true", help="List the profile's declared parameters and parameter sets."
    )
    params_group.add_argument(
        "--save-params",
        metavar="SET",
        help="Write parameter set SET (<profile>.SET.params.json) from the --param values given "
        "(or from the profile's current defaults if none).",
    )
    params_group.add_argument(
        "--param", metavar="NAME=VALUE", action="append", default=None, help="A value for --save-params. Repeatable."
    )
    params_group.add_argument(
        "--session",
        metavar="NAME",
        default=None,
        help="Default session name stored in the --save-params set (used when blink is started without -s).",
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

    if getattr(args, "add_param", None) or getattr(args, "show_params", False) or getattr(args, "save_params", None):
        from blinkview.core.profile_params import ProfileParamError

        try:
            handle_params_args(args, profiles_path / (args.profile or get_active_profile_name()))
        except (ProfileParamError, FileNotFoundError) as e:
            print(f"Error: {e}")
            raise SystemExit(1)
        return

    # Handle Switch/Create
    if args.profile:
        try:
            switch_profile(args.profile, create=args.create, agent=getattr(args, "agent", False))
        except (ValueError, FileExistsError) as e:
            print(f"Error: {e}")
            raise SystemExit(1)
    else:
        # If no profile name and no --list, show help
        print("Error: Please specify a profile name or use --list.")


def handle_params_args(args, profile_dir: Path):
    """`blink switch [profile] --add-param / --show-params / --save-params`."""
    from blinkview.core.profile_params import (
        MISSING,
        ProfileParamError,
        add_declaration,
        apply_params,
        get_declarations,
        get_pointer,
        list_params_sets,
        params_set_path,
        parse_param_args,
        read_params_file,
        write_params_file,
    )

    name = profile_dir.name
    config_file = profile_dir / f"{name}.json"
    if not config_file.is_file():
        raise FileNotFoundError(f"Profile config not found: {config_file}")

    import json

    with open(config_file, encoding="utf-8") as f:
        config = json.load(f)

    if args.add_param:
        param_name, path = args.add_param
        unset = get_pointer(config, path) is MISSING
        add_declaration(config, param_name, path, args.description)
        atomic_json_dump(config, config_file)
        print(f"Declared parameter '{param_name}' -> {path} in profile '{name}'")
        if unset:
            # Allowed (optional fields like an empty serial_number aren't in the JSON), but it's
            # also what a typo in the field name looks like.
            print(f"Note: '{path.rsplit('/', 1)[-1]}' is not set in the profile yet - check the spelling.")

    if args.save_params:
        declarations = get_declarations(config)
        given = parse_param_args(args.param)
        if given:
            # Validates names and coerces values to the bound fields' types.
            _, values = apply_params(json.loads(json.dumps(config)), given)
        else:
            values = {p: get_pointer(config, d["paths"][0]) for p, d in declarations.items()}
            values = {k: v for k, v in values.items() if v is not MISSING}
        if not values:
            raise ProfileParamError(f"Profile '{name}' declares no parameters - add one with --add-param first")
        target = params_set_path(profile_dir, name, args.save_params)
        write_params_file(target, values, args.session)
        print(f"Wrote parameter set '{args.save_params}' to {target}")
        print(f"Use it with: blink -p {name} --params {args.save_params}")

    if args.show_params:
        declarations = get_declarations(config)
        if not declarations:
            print(f"Profile '{name}' declares no parameters.")
        for param_name, decl in sorted(declarations.items()):
            first = get_pointer(config, decl["paths"][0])
            default = "<missing path>" if first is MISSING else json.dumps(first)
            print(f"{param_name} = {default}")
            for path in decl["paths"]:
                print(f"    {path}")
            if decl["description"]:
                print(f"    {decl['description']}")
        sets = list_params_sets(profile_dir, name)
        print("Parameter sets:" if sets else "Parameter sets: (none)")
        for set_name in sets:
            try:
                values, session = read_params_file(params_set_path(profile_dir, name, set_name))
                details = ", ".join(f"{k}={json.dumps(v)}" for k, v in values.items())
                if session:
                    details = f"session '{session}'; {details}"
            except ProfileParamError as e:
                details = f"unreadable: {e}"
            print(f"    {set_name}: {details}")

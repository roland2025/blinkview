# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def setup_update_parser(parser):
    """Sets up the 'blink update' subparser."""
    subparsers = parser.add_subparsers(dest="update_command", help="Update sub-commands")

    # blink update fetch
    subparsers.add_parser("fetch", help="Fetch latest tags/metadata from the repository")

    # blink update list
    list_p = subparsers.add_parser("list", help="List available versions/tags")
    list_p.add_argument("--remote", action="store_true", help="List remote tags instead of local")

    # blink update install
    inst_p = subparsers.add_parser("install", help="Switch to a specific version")
    inst_p.add_argument("version", help="The version/tag/branch to install (e.g., v0.1.2)")

    # blink update upgrade
    subparsers.add_parser("upgrade", help="Fetch and install the latest version automatically")


def is_blinkview_repo(path) -> bool:
    """Validates that the directory is a valid BlinkView git repository."""
    # 1. Basic Git check
    if not (path / ".git").is_dir():
        return False

    # 2. Identity check via pyproject.toml
    pyproject_path = path / "pyproject.toml"
    if not pyproject_path.exists():
        return False

    main_py = path / "src" / "blinkview" / "__main__.py"
    return main_py.is_file()


def handle_update(args):
    from blinkview.utils.global_settings import GlobalSettings
    from pathlib import Path

    glob = GlobalSettings()
    src_path = glob.get("update.path")

    if not src_path or not Path(src_path).exists():
        print("Error: 'update.path' not set in GlobalSettings or path does not exist.")
        print("Run: blink config --global update.path /path/to/repo")
        return

    repo_path = Path(src_path).resolve()

    if not is_blinkview_repo(repo_path):
        print(f"Error: {repo_path} does not look like a BlinkView source tree.")
        print("Expected to find:")
        print(f"  - {repo_path / '.git'}")
        print(f"  - {repo_path / 'pyproject.toml'}")
        print(f"  - {repo_path / 'src' / 'blinkview' / '__main__.py'}")
        return

    # Dispatch to sub-commands
    if args.update_command == "upgrade":
        _do_upgrade(repo_path, glob)
    elif args.update_command == "fetch":
        _do_fetch(repo_path)
    elif args.update_command == "list":
        _do_list(repo_path, glob, remote=args.remote)
    elif args.update_command == "install":
        _do_install(repo_path, glob, args.version)
    else:
        # Default behavior if no sub-command: show current status

        from blinkview import __version__
        print(f"Current Version: v{__version__}")
        print(f"Source Path: {repo_path}")
        print("Use 'fetch', 'list', or 'install <version>' to manage updates.")


def _do_fetch(path):
    print(f"Checking for updates in {path}...")
    try:
        import subprocess
        subprocess.run(["git", "-C", str(path), "fetch", "--tags", "--prune", "--prune-tags"], check=True)
        print("Successfully fetched latest tags and metadata.")
    except Exception as e:
        print(f"Fetch failed: {e}")


def _do_list(path, _, remote=False):
    cmd = ["git", "-C", str(path), "tag", "-l"]
    if remote:
        cmd = ["git", "-C", str(path), "ls-remote", "--tags", "origin"]

    try:
        import subprocess
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        tags = result.stdout.strip()
        if not tags:
            print("No versions found.")
        else:
            # Sort newest first
            from packaging.version import parse as parse_version
            sorted_tags = sorted(tags, key=lambda t: parse_version(t), reverse=True)
            print("--- Available Versions ---")
            for t in sorted_tags:
                print(f"  {t}")
    except Exception as e:
        print(f"Failed to list versions: {e}")


def _do_install(path, glob, version):
    # Retrieve settings
    is_editable_val = glob.get("update.editable")
    should_be_editable = str(is_editable_val).lower() in ["true", "1", "yes"]

    # Default to 'gui' if no features are specified
    features_raw = glob.get("update.features", "all")

    # Clean and format: "gui, can" -> "[gui,can]"
    features_suffix = ""
    if features_raw:
        clean_features = ",".join([f.strip() for f in features_raw.split(",") if f.strip()])
        if clean_features:
            features_suffix = f"[{clean_features}]"

    print(f"Switching to version: {version}...")

    import subprocess
    try:
        import sys

        # Checkout the requested version
        subprocess.run(["git", "-C", str(path), "checkout", version], check=True, capture_output=True)

        # Build the target string: "/path/to/repo[gui,can]"
        # Converting path to str() explicitly prevents potential Path-object weirdness in f-strings
        install_target = f"{str(path)}{features_suffix}"

        # Build uv tool command
        # --force ensures we overwrite the existing 'blink' shim
        cmd = [
            "uv", "tool", "install",
            install_target,
            "--python", sys.executable,
            "--force"
        ]

        if should_be_editable:
            cmd.append("--editable")

        print(f"Running: {' '.join(cmd)}")
        subprocess.run(cmd, check=True)

        # Cleanup the success message for the user
        mode_str = " (Editable)" if should_be_editable else ""
        feat_info = f" with {features_suffix}" if features_suffix else ""
        print(f"Successfully installed {version}{feat_info}{mode_str}")

    except subprocess.CalledProcessError as e:
        # Provide more context if git or uv fails
        error_msg = e.stderr.decode() if e.stderr else str(e)
        print(f"Installation failed: {error_msg}")
        print(f"Verify 'uv' is in PATH and '{version}' is a valid tag/branch.")


def _get_latest_tag(path) -> str:
    """Finds the highest version tag using proper PEP 440 logic."""
    import subprocess
    from packaging.version import parse as parse_version

    try:
        # Get all tags from the repo
        cmd = ["git", "-C", str(path), "tag", "-l"]
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        tags = result.stdout.strip().splitlines()

        if not tags:
            return None

        # Sort tags by actual version object
        # We handle the 'v' prefix by parsing directly; packaging ignores it
        sorted_tags = sorted(tags, key=lambda t: parse_version(t), reverse=True)
        return sorted_tags[0]
    except Exception as e:
        print(f"Error determining latest tag: {e}")
        return None


def _do_upgrade(path, glob):
    """The 'Final Boss' command: Syncs, selects best target, and installs."""
    # Refresh remote data
    _do_fetch(path)

    # Select Target
    target = _get_latest_tag(path)
    if not target:
        print("No tags found.")
        return

    from packaging.version import parse as parse_version
    from blinkview import __version__

    current_v = parse_version(__version__)
    latest_v = parse_version(target)

    if latest_v <= current_v:
        print(f"BlinkView is up to date (Local: v{__version__}, Latest: {target}).")
        return
    else:
        print(f"New version detected: {target} (Current: v{__version__})")

    # Execution
    _do_install(path, glob, target)

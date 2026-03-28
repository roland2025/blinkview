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


def handle_update(args):
    import sys

    from blinkview import __version__
    from blinkview.utils.updater import UpdateError, Updater

    try:
        # Updater handles all settings resolution internally now!
        updater = Updater()

        if args.update_command == "fetch":
            print(f"Fetching updates in {updater.repo_path}...")
            updater.fetch()
            print("Successfully fetched latest tags and metadata.")

        elif args.update_command == "list":
            versions = updater.get_versions(remote=args.remote)
            if not versions:
                print("No versions found.")
            else:
                print("--- Available Versions ---")
                for v in versions:
                    print(f"  {v}")

        elif args.update_command == "install":
            print(f"Preparing to install version: {args.version}...")
            requires_exit = updater.install(args.version)
            if requires_exit:
                print("Self-update initiated. This window will close to complete the process.")
                sys.exit(0)
            else:
                print(f"Successfully installed {args.version}")

        elif args.update_command == "upgrade":
            print("Checking for upgrades...")
            was_upgraded, latest_ver = updater.upgrade(__version__)
            if not was_upgraded:
                print(f"BlinkView is up to date (Local: v{__version__}, Latest: {latest_ver}).")
            else:
                print(f"Upgrade initiated for {latest_ver}. Closing to complete the process.")
                if sys.platform == "win32":
                    sys.exit(0)
                else:
                    print(f"Successfully upgraded to {latest_ver}")

        else:
            print(f"Current Version: v{__version__}")
            print(f"Source Path: {updater.repo_path}")
            print("Use 'fetch', 'list', or 'install <version>' to manage updates.")

    except UpdateError as e:
        # Beautiful, centralized error handling
        print(f"\nUpdate Error: {e}", file=sys.stderr)

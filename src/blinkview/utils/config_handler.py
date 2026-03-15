# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def setup_config_parser(parser):
    parser.add_argument(
        "--global",
        dest="global_scope",
        action="store_true",
        help="Target global (~/.blinkview) settings"
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all variables set in config file"
    )

    parser.add_argument(
        "--keys",
        action="store_true",
        help="List all valid config keys for the current scope"
    )
    parser.add_argument(
        "--unset",
        action="store_true",
        help="Remove the key from the config"
    )
    # Changed to optional so --list works without a key
    parser.add_argument("key", nargs="?", help="The setting key (e.g., 'logs', 'name')")
    parser.add_argument("value", nargs="?", help="The value to set (leave empty to GET)")

    parser.add_argument("--check-updates", action="store_true", help="Force a check for new versions now")


def handle_config(args):
    """Handles Git-style config get/set/list/unset logic."""
    if args.check_updates:
        from blinkview import __version__
        from blinkview.utils.github_update import GitHubUpdate
        from blinkview.utils.global_settings import GlobalSettings

        # Pull the preference from GlobalSettings
        glob = GlobalSettings()
        use_pre = glob.get("prerelease") in [True, "true", "1", "yes"]

        print(f"Checking for {'prerelease' if use_pre else 'stable'} updates (Current: v{__version__})...")
        has_update, latest = GitHubUpdate.check(force=True, include_pre=use_pre)

        if has_update:
            print(f"🚀 A newer version is available: v{latest}")
            print(f"Visit: {GitHubUpdate.REPO_RELEASES_URL}")
        else:
            print("✅ You are up to date!")
        return

    # Determine Scope
    if args.global_scope:
        from blinkview.utils.global_settings import GlobalSettings
        settings = GlobalSettings()
        scope_name = "global"
    else:
        from blinkview.utils.project_settings import ProjectSettings
        settings = ProjectSettings()
        if not settings._path:
            # If we aren't in a project and didn't specify --global,
            # we check if they just wanted the global list anyway
            if args.list:
                from blinkview.utils.global_settings import GlobalSettings
                settings = GlobalSettings()
                scope_name = "global (fallback)"
            else:
                print("Error: Not in a BlinkView project. Use --global or run 'init'.")
                return
        scope_name = "local"

    # Handle --keys
    if args.keys:
        print(f"Allowed {scope_name} keys: '{', '.join(settings.supported_keys())}'")
        return

    # Handle --list
    if args.list:
        if not settings:
            print(f"--- {scope_name.capitalize()} config is empty ---")
        else:
            # Print sorted list for better readability
            for k, v in sorted(settings.items()):
                print(f"{k}={v}")
        return

    # Validation: If not listing, a key is required
    if not args.key:
        print("Error: config key required. Use --list to see all settings.")
        return

    if args.key and args.key not in settings.supported_keys():
        print(f"Error: '{args.key}' is not a valid {scope_name} setting.")
        print(f"Allowed {scope_name} keys: '{', '.join(settings.supported_keys())}'")
        return

    # Handle --unset
    if args.unset:
        if args.key in settings:
            settings.pop(args.key)
            settings.write()
            print(f"Unset {args.key} ({scope_name})")
        else:
            print(f"Key '{args.key}' was not set in {scope_name} config.")
        return

    # Logic: GET vs SET
    if args.value is None:
        # GET logic
        val = settings.get(args.key)
        if val is not None:
            print(val)
        else:
            print(f"Key '{args.key}' not set in {scope_name} config.")
    else:
        # SET logic
        settings[args.key] = args.value
        settings.write()
        print(f"Set {scope_name} {args.key} to: {args.value}")

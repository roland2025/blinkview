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
        # Check if internal data is empty
        if not settings._data:
            print(f"--- {scope_name.capitalize()} config is empty ---")
        else:
            # Uses the recursive flattener you just added
            for path, val in sorted(settings.flattened_items()):
                print(f"{path}={val}")
        return

    # Validation: If not listing, a key is required
    if not args.key:
        print("Error: config key required. Use --list to see all settings.")
        return

    if not settings.supported_key(args.key):
        print(f"Error: '{args.key.split('.')[0]}' is not a valid {scope_name} setting.")
        print(f"Allowed {scope_name} keys: '{', '.join(settings.supported_keys())}'")
        return

    # Handle --unset
    if args.unset:
        # Using the specific unset_deep method for safety
        if settings.unset_deep(args.key) is not None:
            settings.save()
            print(f"Unset {args.key} ({scope_name})")
        else:
            print(f"Key '{args.key}' was not set in {scope_name} config.")
        return

    # Logic: GET vs SET
    if args.value is None:
        # GET logic using the dot-notation enabled get()
        val = settings.get(args.key)
        if val is not None:
            print(val)
        else:
            print(f"Key '{args.key}' not set in {scope_name} config.")
    else:
        # SET logic using the dot-notation enabled set()
        # Alternatively, you could use settings[args.key] = args.value
        settings.set(args.key, args.value)
        settings.save()
        print(f"Set {scope_name} {args.key} to: {args.value}")

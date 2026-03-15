# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import sys
from argparse import ArgumentParser

from blinkview.ui.cli_args import setup_gui_parser


def run_init(args):
    from blinkview.utils.project_settings import ProjectSettings
    ProjectSettings.init(args.path)


def run_gui(args):
    from .gui import main
    main(args)


def run_daemon(args):
    print(f"🔌 Starting Daemon on port: {args.port}")


# --- Parser Setup ---
def main():
    parser = ArgumentParser(description="BlinkView Telemetry Suite - 2026")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    from blinkview import __version__
    parser.add_argument("-v", "--version", action="version", version=f"BlinkView {__version__}")

    # INIT Command
    init_parser = subparsers.add_parser("init", help="Setup project")
    init_parser.add_argument("path", nargs="?", default=".", help="Directory to initialize (default: current directory)")
    init_parser.set_defaults(func=run_init)

    # PROFILE Command
    profile_parser = subparsers.add_parser("profile", help="Manage profiles")
    from blinkview.utils.project_settings import setup_project_parser, handle_profile_args
    setup_project_parser(profile_parser)
    profile_parser.set_defaults(func=handle_profile_args)

    # GUI Command
    gui_parser = subparsers.add_parser("gui", help="Open Interface")
    setup_gui_parser(gui_parser)

    gui_parser.set_defaults(func=run_gui)

    # DAEMON Command
    daemon_parser = subparsers.add_parser("daemon", help="Background service")
    daemon_parser.add_argument("--port", type=int, default=8000)
    daemon_parser.set_defaults(func=run_daemon)

    config_parser = subparsers.add_parser("config", help="Get and set project or global options")

    # Link to the handler
    from blinkview.utils.config_handler import handle_config, setup_config_parser
    setup_config_parser(config_parser)
    config_parser.set_defaults(func=handle_config)

    args = parser.parse_args()

    if args.command != "config":
        from blinkview.utils.github_update import GitHubUpdate
        msg = GitHubUpdate.get_update_message()
        if msg:
            print(f"[{msg}]\n")

    # --- Execution ---
    if args.command is None:
        # Fallback if the user just types 'blinkview'
        gui_args = gui_parser.parse_args(sys.argv[1:])
        run_gui(gui_args)
    else:
        # This one line replaces the entire 'match' statement!
        args.func(args)


if __name__ == "__main__":
    main()

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import sys
from argparse import ArgumentParser

from blinkview.ui.cli_args import setup_gui_parser, setup_replay_parser


def run_init(args):
    from blinkview.utils.project_settings import ProjectSettings

    ProjectSettings.init(args.path)


def run_gui(args):
    from .gui import main

    main(args)


def run_cli(args):
    from .cli import main

    main()


def run_daemon(args):
    print(f"🔌 Starting Daemon on port: {args.port}")


def run_replay(args):
    # utils/session_lister.py deliberately avoids importing anything from blinkview.storage/
    # blinkview.parsers (the numba/id_registry cluster, ~600 modules) so --list stays fast.
    from blinkview.utils.session_lister import list_sessions, resolve_log_root, resolve_session, unified_log_parts

    log_dir, project_name = resolve_log_root(log_dir=args.logdir)

    if args.list:
        sessions = [s for s in list_sessions(log_dir, project_name) if unified_log_parts(s)]
        if not sessions:
            print(f"No replay sessions found for project '{project_name}' in {log_dir}.")
            return
        for s in sessions:
            print(f"{s.session_id}  [{s.status}]  {s.display_name} (profile={s.profile}, created={s.created_at})")
        return

    session_info = resolve_session(log_dir, project_name, name=args.name, last=args.last)
    if session_info is None:
        print(f"No matching replay session found (name={args.name!r}, last={args.last}).")
        sys.exit(1)

    parts = unified_log_parts(session_info)
    if not parts:
        print(f"Session '{session_info.session_id}' has no unified log to replay.")
        sys.exit(1)

    from blinkview.ui.run import run as run_gui

    run_gui(args, replay_mode=True, replay_session_info=session_info)


# --- Parser Setup ---
def main():
    parser = ArgumentParser(description="BlinkView Telemetry Suite - 2026")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    from blinkview import __version__

    parser.add_argument("-v", "--version", action="version", version=f"BlinkView {__version__}")

    # INIT Command
    init_parser = subparsers.add_parser("init", help="Setup project")
    init_parser.add_argument(
        "path", nargs="?", default=".", help="Directory to initialize (default: current directory)"
    )
    init_parser.set_defaults(func=run_init)

    # PROFILE Command
    profile_parser = subparsers.add_parser("switch", help="Switch profiles")
    from blinkview.utils.project_settings import handle_profile_args, setup_project_parser

    setup_project_parser(profile_parser)
    profile_parser.set_defaults(func=handle_profile_args)

    # GUI Command
    gui_parser = subparsers.add_parser("gui", help="Open Interface")
    setup_gui_parser(gui_parser)

    gui_parser.set_defaults(func=run_gui)

    cli_parser = subparsers.add_parser("cli", help="Command Line Interface")
    cli_parser.set_defaults(func=run_cli)

    # DAEMON Command
    daemon_parser = subparsers.add_parser("daemon", help="Background service")
    daemon_parser.add_argument("--port", type=int, default=8000)
    daemon_parser.set_defaults(func=run_daemon)

    # REPLAY Command
    replay_parser = subparsers.add_parser("replay", help="Load a previously recorded session")
    setup_replay_parser(replay_parser)
    replay_parser.set_defaults(func=run_replay)

    config_parser = subparsers.add_parser("config", help="Get and set project or global options")

    # Link to the handler
    from blinkview.utils.config_handler import handle_config, setup_config_parser

    setup_config_parser(config_parser)
    config_parser.set_defaults(func=handle_config)

    update_parser = subparsers.add_parser("update", help="Manage BlinkView versions")
    from blinkview.utils.cli_updater import handle_update, setup_update_parser

    setup_update_parser(update_parser)
    update_parser.set_defaults(func=handle_update)

    # --- ARGUMENT INJECTION LOGIC ---
    # This checks if the first arg is a valid command.
    # If not, it inserts 'gui' as the first argument so argparse handles it.
    valid_commands = subparsers.choices.keys()

    # sys.argv[0] is the script name. We check sys.argv[1].
    if len(sys.argv) > 1:
        # If it's not a command and not -v/--version/--help...
        if sys.argv[1] not in valid_commands and sys.argv[1] not in ["-h", "--help", "-v", "--version"]:
            sys.argv.insert(1, "gui")
    elif len(sys.argv) == 1:
        # No arguments at all? Default to gui.
        sys.argv.append("gui")

    args = parser.parse_args()

    if args.command != "config":
        from blinkview.utils.github_update import GitHubUpdate

        msg = GitHubUpdate.get_update_message()
        if msg:
            print(f"[{msg}]\n")

    try:
        # --- Execution ---
        if args.command is None:
            # Fallback if the user just types 'blinkview'
            gui_args = gui_parser.parse_args(sys.argv[1:])
            run_gui(gui_args)
        else:
            # This one line replaces the entire 'match' statement!
            args.func(args)
    except Exception:
        # dump formatexc
        from traceback import print_exc

        print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

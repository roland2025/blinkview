# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def setup_gui_parser(parser):
    # Create the mutually exclusive group
    group = parser.add_mutually_exclusive_group()

    group.add_argument(
        "-c", "--config",
        type=str,
        help="Path to a specific .json configuration file",
        default=None
    )
    group.add_argument(
        "-p", "--profile",
        type=str,
        help="Profile name to load",
        default=None
    )

    # Other arguments remain standard
    parser.add_argument(
        "-s", "--session",
        type=str,
        help="Session name for this run (used in file organization)",
        default=None
    )
    parser.add_argument(
        "-l", "--logdir",
        type=str,
        help="Override base log directory",
        default=None
    )

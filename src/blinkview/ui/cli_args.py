# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

def setup_gui_parser(parser):
    parser.add_argument(
        "-c", "--config",
        type=str,
        help="Path to the system configuration file",
        default=None
    )
    parser.add_argument("-s", "--session", type=str, help="Session name for this run (used in file organization)", default=None)
    parser.add_argument("-p", "--project", type=str, help="Project name for this run (used in file organization)", default=None)
    parser.add_argument("-l", "--logdir", type=str, help="Override base log directory (overrides config and .blinkview settings)", default=None)


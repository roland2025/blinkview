# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Window/dialog title prefix: "{project} / {profile} - ".

With several BlinkView instances open (one per project/profile), every window and dialog title
starts with it so the taskbar and Alt+Tab show which instance a window belongs to. Set once by
BlinkMainWindow at startup; one process only ever runs one project/profile."""

_prefix = ""


def set_title_prefix(project_name: str, profile_name: str, params_label: str = None):
    """`params_label` (e.g. "left" from `--params left`) tells apart several instances of one
    profile, one per device: "proj / rtt [left] - "."""
    global _prefix
    suffix = f" [{params_label}]" if params_label else ""
    _prefix = f"{project_name} / {profile_name}{suffix} - "


def titled(title: str) -> str:
    """`title` with the project/profile prefix (unchanged until set_title_prefix() has run)."""
    return f"{_prefix}{title}"

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path

from blinkview.utils.settings import Settings


def get_blink_home() -> Path:
    """Returns ~/.blinkview across all platforms."""
    path = Path.home() / ".blinkview"
    path.mkdir(parents=True, exist_ok=True)
    return path


class GlobalSettings(Settings):
    def __init__(self):
        super().__init__(get_blink_home() / "settings.json")

    @classmethod
    def supported_keys(cls):
        """Returns a list of supported settings keys."""
        return "log_dir", "update_check", "prerelease"

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from json import load
from pathlib import Path

from blinkview.utils.atomic_json_dump import atomic_json_dump


class Settings(dict):
    def __init__(self, path=None):
        super().__init__()
        self._path: Path = None
        if path is not None:
            self.set_path(path)

    def read(self):
        """Reads project settings from the .blinkview folder."""
        try:
            with self._path.open() as f:
                self.update(load(f))

        except Exception as e:
            print(f"[ProjectSettings] Failed to read project settings: {e}")

    def write(self):
        """Writes project settings to the .blinkview folder."""
        atomic_json_dump(self, self._path)

    def set_path(self, path):
        """Sets the path for the settings file and reads existing settings."""
        self._path = Path(path)
        self.read()

    @classmethod
    def load(cls):
        """Loads global settings from the .blinkview folder."""
        return cls()

    @classmethod
    def supported_keys(cls):
        """Returns a list of supported settings keys."""
        return []


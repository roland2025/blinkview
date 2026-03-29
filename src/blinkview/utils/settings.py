# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from json import load
from pathlib import Path

from blinkview.utils.atomic_json_dump import atomic_json_dump


class Settings:
    def __init__(self, path=None):
        self._data = {}
        self._path: Path = None
        if path is not None:
            self.set_path(path)

    def __getitem__(self, key):
        """Allows settings['user.name']"""
        sentinel = object()
        val = self.get(key, default=sentinel)
        if val is sentinel:
            raise KeyError(key)
        return val

    def __setitem__(self, key, value):
        """Allows settings['user.name'] = 'Roland'"""
        self.set(key, value)

    def __delitem__(self, key):
        """Allows del settings['user.name']"""
        if self.unset_deep(key) is None:
            raise KeyError(key)

    def __contains__(self, key):
        sentinel = object()
        return self.get(key, default=sentinel) is not sentinel

    def __repr__(self):
        return f"{self.__class__.__name__}({self._data})"

    def __iter__(self):
        """Allows 'for key in settings' (yields flattened keys)"""
        for path, _ in self.flattened_items():
            yield path

    def read(self):
        """Reads project settings from the .blinkview folder."""
        try:
            with self._path.open() as f:
                content = load(f)
                if isinstance(content, dict):
                    self._data = content

        except Exception as e:
            print(f"[ProjectSettings] Failed to read project settings: {e}")

    def save(self):
        """Writes project settings to the .blinkview folder."""
        atomic_json_dump(self._data, self._path)

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

    def _split_key(self, key_string):
        return key_string.split(".") if key_string else []

    def get(self, key_string, default=None):
        keys = self._split_key(key_string)
        val = self._data
        for k in keys:
            if isinstance(val, dict) and k in val:
                val = val[k]
            else:
                return default
        return val

    def set(self, key_string, value):
        keys = self._split_key(key_string)
        target = self._data
        for k in keys[:-1]:
            # If it's not a dict, overwrite it with one to allow nesting
            if k not in target or not isinstance(target[k], dict):
                target[k] = {}
            target = target[k]
        target[keys[-1]] = value

    def unset_deep(self, key_string):
        keys = self._split_key(key_string)
        if not keys:
            return None

        if len(keys) == 1:
            return self._data.pop(keys[0], None)

        # Use internal get to find the parent
        parent = self.get(".".join(keys[:-1]))
        if isinstance(parent, dict):
            val = parent.pop(keys[-1], None)
            # Optional: Clean up empty parent dicts
            return val
        return None

    def flattened_items(self, data=None, prefix=""):
        target = data if data is not None else self._data
        for k, v in target.items():
            key_path = f"{prefix}{k}"
            if isinstance(v, dict) and v:
                yield from self.flattened_items(v, prefix=f"{key_path}.")
            else:
                yield key_path, v

    @classmethod
    def supported_key(cls, key_string):
        """Checks if a primary key is supported."""
        primary_key = key_string.split(".")[0]
        return primary_key in cls.supported_keys()

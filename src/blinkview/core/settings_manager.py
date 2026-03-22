# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any, Optional, Dict

from blinkview.utils.global_settings import GlobalSettings
from blinkview.utils.project_settings import ProjectSettings, get_project_root


class SettingsManager:
    def __init__(self):
        # Only initialize project settings if we are actually inside a project
        self._project = ProjectSettings() if get_project_root() else None
        self._global = GlobalSettings()

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieves a setting with cascading fallback:
        1. Project Settings (if available)
        2. Global Settings
        3. Default value
        """
        # Check Project scope first
        if self.is_project:
            val = self._project.get(key, default=None)
            if val is not None:
                return val

        # Fallback to Global scope
        return self._global.get(key, default=default)

    def set(self, key: str, value: Any, scope: str = "project"):
        """
        Sets a setting in the specified scope ('project' or 'global').
        Saves automatically after setting.
        """
        if scope == "project":
            if not self.is_project:
                raise RuntimeError("Cannot set project setting: No project root found.")
            self._project.set(key, value)
            self._project.save()

        elif scope == "global":
            self._global.set(key, value)
            self._global.save()

        else:
            raise ValueError(f"Invalid scope '{scope}'. Use 'project' or 'global'.")

    def unset(self, key: str, scope: str = "project"):
        """Removes a key from the specified scope."""
        target = self._project if scope == "project" else self._global

        if not target:
            raise RuntimeError(f"Cannot unset {scope} setting: Scope not initialized.")

        target.unset_deep(key)
        target.save()

    @property
    def is_project(self) -> bool:
        """Helper to check if we are currently in a project context."""
        return self._project is not None

    def all_resolved(self) -> Dict[str, Any]:
        """Returns a flat dictionary of all settings, with project overrides applied."""
        # Start with global
        resolved = dict(self._global.flattened_items())

        # Override with project if it exists
        if self.is_project:
            resolved.update(dict(self._project.flattened_items()))

        return resolved

    # --- Magic Methods for Dictionary-like Access ---

    def __getitem__(self, key: str):
        sentinel = object()
        val = self.get(key, default=sentinel)
        if val is sentinel:
            raise KeyError(key)
        return val

    def __setitem__(self, key: str, value: Any):
        # Default to project scope if available, otherwise global
        scope = "project" if self.is_project else "global"
        self.set(key, value, scope=scope)

    def __contains__(self, key: str):
        sentinel = object()
        return self.get(key, default=sentinel) is not sentinel

    def __repr__(self):
        status = "Project" if self.is_project else "Standalone"
        return f"SettingsManager(mode={status}, keys={list(self.all_resolved().keys())})"

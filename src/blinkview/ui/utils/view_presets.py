# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Saved view layout presets (plans/view-layout-presets.md).

Stored in `<profile>.view_presets.json` next to `.gui_state.json`. Each preset's "state" has the
exact same structure as `.gui_state.json` (UIStateHandler.get_data()), plus a fingerprint of the
screens connected when it was saved, so the View menu can warn before applying a layout meant
for a different monitor setup."""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional

from blinkview.utils.atomic_json_dump import atomic_json_dump

PRESETS_FILE_VERSION = 1


class ViewPresetStore:
    """Plain-JSON store for named layout presets. Every mutation is written to disk immediately -
    unlike .gui_state.json, which is only written on shutdown/session rotation."""

    def __init__(self, path: Path):
        self.path = Path(path)
        data = self._load()
        self._presets: dict = data.get("presets", {})
        self._offer_on_screen_change: bool = bool(data.get("offer_on_screen_change", True))

    def _load(self) -> dict:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            if not isinstance(data.get("presets", {}), dict):
                raise ValueError("'presets' is not an object")
            return data
        except Exception as e:
            # Keep the unreadable file around instead of silently overwriting it on the next save.
            backup = self.path.with_name(self.path.name + ".bak")
            print(f"[ViewPresetStore] Could not read {self.path} ({e}); moved it to {backup.name}")
            try:
                self.path.replace(backup)
            except OSError:
                pass
            return {}

    def _write(self):
        atomic_json_dump(
            {
                "version": PRESETS_FILE_VERSION,
                "offer_on_screen_change": self._offer_on_screen_change,
                "presets": self._presets,
            },
            self.path,
        )

    @property
    def offer_on_screen_change(self) -> bool:
        """Whether to offer a matching preset (toast) when monitors are connected/disconnected."""
        return self._offer_on_screen_change

    @offer_on_screen_change.setter
    def offer_on_screen_change(self, value: bool):
        self._offer_on_screen_change = bool(value)
        self._write()

    def best_match(self, screens: list) -> Optional[str]:
        """The preset to offer for `screens`: among presets saved with exactly this screen
        arrangement, the most recently saved one. Presets without a fingerprint never match here -
        they can't tell which setup they belong to."""
        matches = [
            (preset.get("saved_at", ""), name)
            for name, preset in self._presets.items()
            if preset.get("screens") and screens_match(preset["screens"], screens)
        ]
        return max(matches)[1] if matches else None

    def names(self) -> list[str]:
        return sorted(self._presets, key=str.casefold)

    def __contains__(self, name: str) -> bool:
        return name in self._presets

    def get(self, name: str) -> Optional[dict]:
        return self._presets.get(name)

    def save(self, name: str, state: dict, screens: list[dict]):
        """Adds or overwrites preset `name`."""
        self._presets[name] = {
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "screens": screens,
            "state": state,
        }
        self._write()

    def rename(self, old: str, new: str):
        if old not in self._presets:
            raise KeyError(old)
        if new in self._presets and new != old:
            raise ValueError(f"A preset named '{new}' already exists")
        self._presets[new] = self._presets.pop(old)
        self._write()

    def delete(self, name: str):
        if self._presets.pop(name, None) is not None:
            self._write()


def current_screen_fingerprint() -> list[dict]:
    """Geometry of every connected screen, sorted so screen enumeration order doesn't matter."""
    from qtpy.QtGui import QGuiApplication

    screens = []
    for screen in QGuiApplication.screens():
        geo = screen.geometry()
        screens.append({"name": screen.name(), "geometry": [geo.x(), geo.y(), geo.width(), geo.height()]})
    screens.sort(key=lambda s: s["geometry"])
    return screens


def screens_match(saved: Optional[list], current: list) -> bool:
    """Compares by geometry only - Windows display names (\\\\.\\DISPLAY1) can renumber after a
    monitor power cycle while the layout stays identical. A preset without a fingerprint matches."""
    if not saved:
        return True
    return sorted(s.get("geometry") for s in saved) == sorted(s.get("geometry") for s in current)

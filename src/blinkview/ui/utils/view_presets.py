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
    unlike .gui_state.json, which is only written on shutdown/session rotation.

    Several BlinkView instances may share one profile (one per board, see --params), so the file
    is the source of truth: reads pick up another instance's changes (re-read only when the file
    changed), and each mutation re-reads the file and changes just its own preset rather than
    writing back a stale copy of all of them."""

    def __init__(self, path: Path):
        self.path = Path(path)
        data = self._load()
        self._presets: dict = data.get("presets", {})
        self._offer_on_screen_change: bool = bool(data.get("offer_on_screen_change", True))
        self._sig = self._signature()

    def _read(self) -> dict:
        """The file's content ({} if missing). Raises on an unreadable/invalid file."""
        if not self.path.exists():
            return {}
        data = json.loads(self.path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("presets", {}), dict):
            raise ValueError("'presets' is not an object")
        return data

    def _load(self) -> dict:
        try:
            return self._read()
        except Exception as e:
            # Keep the unreadable file around instead of silently overwriting it on the next save.
            backup = self.path.with_name(self.path.name + ".bak")
            print(f"[ViewPresetStore] Could not read {self.path} ({e}); moved it to {backup.name}")
            try:
                self.path.replace(backup)
            except OSError:
                pass
            return {}

    def _signature(self):
        try:
            st = self.path.stat()
            return st.st_mtime_ns, st.st_size
        except OSError:
            return None

    def _adopt(self, data: dict):
        self._presets = data.get("presets", {})
        self._offer_on_screen_change = bool(data.get("offer_on_screen_change", True))

    def _refresh(self):
        """Picks up changes saved by another instance - one stat() when there are none."""
        sig = self._signature()
        if sig == self._sig:
            return
        try:
            data = self._read()
        except Exception as e:  # e.g. hand-edited into invalid JSON: keep what we have
            print(f"[ViewPresetStore] Could not re-read {self.path}: {e}")
            return
        self._adopt(data)
        self._sig = sig

    def _mutate(self, change):
        """Read-modify-write: applies `change(presets)` to the file's current presets (falling
        back to ours if it's unreadable) and writes the result. `change` may raise to abort."""
        try:
            data = self._read()
        except Exception as e:
            print(f"[ViewPresetStore] Could not re-read {self.path} before saving ({e}); using the loaded presets")
            data = {"presets": self._presets, "offer_on_screen_change": self._offer_on_screen_change}
        presets = dict(data.get("presets", {}))
        change(presets)
        data = {
            "version": PRESETS_FILE_VERSION,
            "offer_on_screen_change": bool(data.get("offer_on_screen_change", self._offer_on_screen_change)),
            "presets": presets,
        }
        return data

    def _commit(self, data: dict):
        atomic_json_dump(data, self.path)
        self._adopt(data)
        # Not our own stat(): another instance may write right after us - re-read next time.
        self._sig = None

    @property
    def offer_on_screen_change(self) -> bool:
        """Whether to offer a matching preset (toast) when monitors are connected/disconnected."""
        self._refresh()
        return self._offer_on_screen_change

    @offer_on_screen_change.setter
    def offer_on_screen_change(self, value: bool):
        data = self._mutate(lambda presets: None)
        data["offer_on_screen_change"] = bool(value)
        self._commit(data)

    def best_match(self, screens: list) -> Optional[str]:
        """The preset to offer for `screens`: among presets saved with exactly this screen
        arrangement, the most recently saved one. Presets without a fingerprint never match here -
        they can't tell which setup they belong to."""
        self._refresh()
        matches = [
            (preset.get("saved_at", ""), name)
            for name, preset in self._presets.items()
            if preset.get("screens") and screens_match(preset["screens"], screens)
        ]
        return max(matches)[1] if matches else None

    def names(self) -> list[str]:
        self._refresh()
        return sorted(self._presets, key=str.casefold)

    def __contains__(self, name: str) -> bool:
        self._refresh()
        return name in self._presets

    def get(self, name: str) -> Optional[dict]:
        self._refresh()
        return self._presets.get(name)

    def save(self, name: str, state: dict, screens: list[dict]):
        """Adds or overwrites preset `name`."""
        entry = {
            "saved_at": datetime.now().isoformat(timespec="seconds"),
            "screens": screens,
            "state": state,
        }
        self._commit(self._mutate(lambda presets: presets.__setitem__(name, entry)))

    def rename(self, old: str, new: str):
        def change(presets):
            if old not in presets:
                raise KeyError(old)
            if new in presets and new != old:
                raise ValueError(f"A preset named '{new}' already exists")
            presets[new] = presets.pop(old)

        self._commit(self._mutate(change))

    def delete(self, name: str):
        removed = []
        data = self._mutate(lambda presets: removed.append(presets.pop(name, None) is not None))
        if removed[0]:
            self._commit(data)
        else:
            self._refresh()


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

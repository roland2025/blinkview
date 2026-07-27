# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Named playback time ranges ("clips", DaVinci-Resolve-style) - see plans/named-playback-ranges.md.

Plain Python, no Qt - mirrors PlaybackClock (core/playback_clock.py). Persisted as a JSON sidecar
file next to the session's raw captured log data (FileManager.session_dir), so a range set survives
process restarts and - since blinkview's replay workflow is literally pointing a BinaryFileReader/
FileTailReader at a file inside a previous session's folder - travels with that session's data when
it's later reopened for replay (see Registry._load_playback_ranges_from_source).
"""

import json
from typing import Callable, List, NamedTuple, Optional
from uuid import uuid4


class PlaybackRange(NamedTuple):
    id: str
    name: str
    start_ts_ns: int
    end_ts_ns: int

    def normalized(self) -> "PlaybackRange":
        """A range's start/end can end up swapped depending on mark-in/mark-out order - callers
        that care about direction (seeking, drawing) should always go through this rather than
        assume start_ts_ns <= end_ts_ns."""
        if self.start_ts_ns <= self.end_ts_ns:
            return self
        return self._replace(start_ts_ns=self.end_ts_ns, end_ts_ns=self.start_ts_ns)


class PlaybackRangeStore:
    """In-memory list of PlaybackRanges plus JSON load/save. `on_change` (if given) fires after
    every mutation, so a caller (Registry) can persist to disk without this class knowing about
    filesystem paths at all - keeps it trivially unit-testable and consistent with PlaybackClock's
    "plain Python, no I/O of its own" shape."""

    def __init__(self, on_change: Optional[Callable[[], None]] = None):
        self._ranges: List[PlaybackRange] = []
        self._on_change = on_change

    @property
    def ranges(self) -> List[PlaybackRange]:
        return list(self._ranges)

    def add(self, name: str, start_ts_ns: int, end_ts_ns: int) -> PlaybackRange:
        rng = PlaybackRange(id=uuid4().hex, name=name, start_ts_ns=start_ts_ns, end_ts_ns=end_ts_ns).normalized()
        self._ranges.append(rng)
        self._notify()
        return rng

    def remove(self, range_id: str) -> bool:
        before = len(self._ranges)
        self._ranges = [r for r in self._ranges if r.id != range_id]
        removed = len(self._ranges) != before
        if removed:
            self._notify()
        return removed

    def rename(self, range_id: str, new_name: str) -> bool:
        for i, r in enumerate(self._ranges):
            if r.id == range_id:
                self._ranges[i] = r._replace(name=new_name)
                self._notify()
                return True
        return False

    def get(self, range_id: str) -> Optional[PlaybackRange]:
        for r in self._ranges:
            if r.id == range_id:
                return r
        return None

    def clear(self):
        if not self._ranges:
            return
        self._ranges = []
        self._notify()

    def _notify(self):
        if self._on_change is not None:
            self._on_change()

    def to_json_data(self) -> dict:
        return {
            "version": 1,
            "ranges": [
                {"id": r.id, "name": r.name, "start_ts_ns": r.start_ts_ns, "end_ts_ns": r.end_ts_ns}
                for r in self._ranges
            ],
        }

    def load_json_data(self, data: dict, *, replace: bool = True):
        """Loads ranges from parsed JSON (see to_json_data). If replace is False, incoming ranges
        are merged in by id (existing entries with the same id are overwritten, new ids appended)
        rather than discarding whatever's already in memory - used when opportunistically pulling
        in a replay source's saved ranges alongside this session's own."""
        incoming = [
            PlaybackRange(
                id=item["id"],
                name=item["name"],
                start_ts_ns=int(item["start_ts_ns"]),
                end_ts_ns=int(item["end_ts_ns"]),
            )
            for item in data.get("ranges", [])
        ]

        if replace:
            self._ranges = incoming
        else:
            by_id = {r.id: r for r in self._ranges}
            for r in incoming:
                by_id[r.id] = r
            self._ranges = list(by_id.values())

        self._notify()

    def save_to_file(self, path):
        from blinkview.utils.atomic_json_dump import atomic_json_dump

        atomic_json_dump(self.to_json_data(), path)

    def load_from_file(self, path, *, replace: bool = True) -> bool:
        """Returns True if a ranges file existed and was loaded, False if there was nothing to
        load (not an error - most sessions/replay sources simply have no saved ranges yet)."""
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError, OSError):
            return False

        self.load_json_data(data, replace=replace)
        return True

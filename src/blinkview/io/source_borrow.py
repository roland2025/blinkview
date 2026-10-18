# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Lends a running source's port to an external tool (a flasher) on request - the blink side of
the protocol in blinkview/borrow.py.

A reader with `allow_borrow` owns one SourceBorrowWatcher while it runs (BaseReader.start() /
stop()). The watcher only polls the borrow file and drives the reader's pause interface
(request_pause / is_paused / request_resume); the reader stays the only thing that touches its
port.
"""

from threading import Lock
from time import monotonic, sleep
from typing import TYPE_CHECKING, Callable, Optional

from blinkview import borrow

if TYPE_CHECKING:
    from blinkview.io.BaseReader import BaseReader

# Once a second: a borrow then waits up to ~1 s for `.lent`, and the port comes back up to ~1 s
# after the return - small next to a flash. reset_after_return covers the boot log.
POLL_S = 1.0

# How long check() waits for the reader thread to close the port before writing `.lent`. The
# serial read timeout is the source's `delay` (100 ms by default), so this is normally a few
# hundred ms; past it the ack is written on a later tick instead.
PAUSE_CONFIRM_S = 2.0


class SourceBorrowWatcher:
    def __init__(
        self,
        reader: "BaseReader",
        paths: borrow.BorrowPaths,
        timeout_s: float,
        logger,
        tasks,
        clock: Callable[[], float] = monotonic,
        pid_alive: Callable[[object], bool] = borrow.pid_alive,
    ):
        self.reader = reader
        self.paths = paths
        self.timeout_s = timeout_s
        self.logger = logger
        self.tasks = tasks
        self._clock = clock
        self._pid_alive = pid_alive

        self._lock = Lock()
        self._task_id: Optional[str] = None

        # The borrow currently served (its JSON), None while idle.
        self._lent_info: Optional[dict] = None
        self._lent_since = 0.0
        # Whether `.lent` was written for the current borrow.
        self._acked = False
        # A borrow that outlived timeout_s: ignored until its file changes (a new `ts`).
        self._ignored_ts = None

    @property
    def is_lent(self) -> bool:
        return self._lent_info is not None

    def start(self):
        """Clears this source's leftovers and starts polling. A `.borrow` found now is a crash
        leftover, or belongs to a tool that already has the port open - there is nothing to lend
        either way: blink never had the port, and can't open it while the tool holds it."""
        for path in (self.paths.borrow, self.paths.lent):
            try:
                if borrow.remove(path):
                    self.logger.info("Removed leftover borrow file %s", path)
            except OSError as e:
                self.logger.error("Failed to remove borrow file %s", path, exc=e)

        self.logger.info("Lends the port on request: %s", self.paths.borrow)
        with self._lock:
            if self._task_id is None:
                self._task_id = self.tasks.run_periodic(POLL_S, self.check)

    def stop(self):
        with self._lock:
            if self._task_id is not None:
                try:
                    self.tasks.stop_periodic(self._task_id)
                except Exception as e:
                    self.logger.error("Failed to stop the borrow watcher", exc=e)
                self._task_id = None
            if self._lent_info is not None:
                self._end()

    def check(self):
        """One poll: start, confirm or end a borrow."""
        with self._lock:
            try:
                info = borrow.read_borrow(self.paths)
                if self._lent_info is None:
                    if info is not None:
                        self._begin(info)
                else:
                    self._serve(info)
            except Exception as e:
                self.logger.exception("Borrow watcher failed", exc=e)

    # --- states ---

    def _begin(self, info: dict):
        if self._ignored_ts is not None and info.get("ts") == self._ignored_ts:
            return
        self._ignored_ts = None

        self._lent_info = info
        self._lent_since = self._clock()
        self._acked = False
        self.reader.request_pause(f"lent to {_who(info)}")
        self._confirm(wait_s=PAUSE_CONFIRM_S)

    def _serve(self, info: Optional[dict]):
        lent = self._lent_info
        held_s = self._clock() - self._lent_since

        if info is None:
            self.logger.info("Returned after %.1f s", held_s)
            self._end()
            return

        if info.get("ts") != lent.get("ts"):
            # The same tool (or another) borrowed again without a return in between: keep the
            # port lent, and judge the new owner from now on.
            self._lent_info = lent = info
            self._lent_since = self._clock()

        if not self._pid_alive(lent.get("pid")):
            self.logger.warn(
                "Borrow by %s ended without a return (the process is gone), taking the port back", _who(lent)
            )
            self._ignored_ts = lent.get("ts")
            self._end()
            return

        if self.timeout_s and held_s > self.timeout_s:
            self.logger.warn(
                "Borrow by %s still active after %.0f s (borrow_timeout_s), taking the port back", _who(lent), held_s
            )
            self._ignored_ts = lent.get("ts")
            self._end()
            return

        if not self._acked:
            self._confirm(wait_s=0)

    def _confirm(self, wait_s: float):
        """Writes `.lent` once the reader has actually closed its port."""
        deadline = self._clock() + wait_s
        while not self.reader.is_paused:
            if self._clock() >= deadline:
                return
            sleep(0.02)
        try:
            borrow.write_lent(self.paths, self._lent_info)
            self._acked = True
            self.logger.info("Lent to %s", _who(self._lent_info))
        except OSError as e:
            self.logger.error("Failed to write %s", self.paths.lent, exc=e)

    def _end(self):
        try:
            borrow.remove(self.paths.lent)
        except OSError as e:
            self.logger.error("Failed to remove %s", self.paths.lent, exc=e)
        self._lent_info = None
        self._acked = False
        self.reader.request_resume()


def _who(info: dict) -> str:
    by = info.get("by") or "a tool"
    pid = info.get("pid")
    return f"{by} (pid {pid})" if pid else by

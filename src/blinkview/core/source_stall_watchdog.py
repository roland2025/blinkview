# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Notices a running source that has stopped receiving data, and reconnects it.

A stalled J-Link RTT session reads 0 bytes while the probe still reports open and connected, so no
driver check ever fails: what stops is ingestion. Every source stamps BaseReader._last_ingest_ns on
each distribute(); this watchdog compares those stamps with each source's stall_warn_s and
stall_reconnect_s (0 = off), on the TaskManager rather than the reader thread, so it also notices a
reader stuck inside a driver call. See the wiki plan source-stall-detection.
"""

from concurrent.futures import Future
from dataclasses import dataclass
from time import monotonic_ns
from typing import TYPE_CHECKING, Callable, Iterable, Optional

if TYPE_CHECKING:
    from blinkview.core.task_manager import TaskManager
    from blinkview.io.BaseReader import BaseReader

STALL_CHECK_INTERVAL_S = 2.0
# Reconnect attempts back off by doubling up to this interval, then keep retrying at it.
STALL_RECONNECT_MAX_S = 600
# Silent after this many reconnects: say that reconnecting is not what fixes this one.
STALL_RECONNECTS_BEFORE_HINT = 3

_NS = 1_000_000_000


@dataclass
class _Stall:
    """One source's state between ticks. A new one starts with every batch and every start()."""

    last_ingest_ns: int
    started_ns: int
    warned: bool = False
    silent_since_ns: int = 0  # when the stall that was warned about began, for the "resumed" line
    reconnects: int = 0
    last_attempt_ns: int = 0
    hinted: bool = False
    pending: Optional[Future] = None

    @property
    def stalled(self) -> bool:
        return self.warned or self.reconnects > 0


class SourceStallWatchdog:
    def __init__(
        self,
        get_sources: Callable[[], Iterable["BaseReader"]],
        task_manager: "TaskManager",
        clock: Callable[[], int] = monotonic_ns,
        interval_s: float = STALL_CHECK_INTERVAL_S,
        logger=None,
    ):
        self._get_sources = get_sources
        self._tasks = task_manager
        self._clock = clock
        self.interval_s = interval_s
        self._logger = logger
        self._task_id: Optional[str] = None
        self._states: dict = {}  # {source: _Stall}, only for running sources with a threshold set

    def start(self) -> None:
        if self._task_id is None:
            self._task_id = self._tasks.run_periodic(self.interval_s, self._tick_safe)

    def stop(self) -> None:
        if self._task_id is not None:
            self._tasks.stop_periodic(self._task_id)
            self._task_id = None
        self._states.clear()

    def _tick_safe(self) -> None:
        try:
            self.tick()
        except Exception as e:
            if self._logger:
                self._logger.exception("Source stall watchdog tick failed", exc=e)

    def tick(self) -> None:
        now = self._clock()
        watched = set()
        for source in list(self._get_sources()):
            warn_s = getattr(source, "stall_warn_s", 0) or 0
            reconnect_s = getattr(source, "stall_reconnect_s", 0) or 0
            if (warn_s <= 0 and reconnect_s <= 0) or not source.enabled or not source.is_running:
                continue
            watched.add(source)
            if source.is_connected:
                self._check(source, now, warn_s, reconnect_s)
            # Not connected: the reader is already retrying and logging that. Keep the state, so a
            # reconnect this watchdog asked for does not reset its backoff.

        for source in [s for s in self._states if s not in watched]:
            del self._states[source]

    def _check(self, source: "BaseReader", now: int, warn_s: int, reconnect_s: int) -> None:
        ingest = source._last_ingest_ns
        started = source._started_ns

        state = self._states.get(source)
        if state is None or state.started_ns != started:
            state = self._states[source] = _Stall(ingest, started)

        # A (re)connect restarts the clock, so the connect and drain time never counts as silence.
        quiet_since = max(state.last_ingest_ns, started, source._ready_ns)

        if ingest != state.last_ingest_ns:
            if state.stalled:
                silent_s = (ingest - state.silent_since_ns) / _NS
                if state.reconnects:
                    self._link(source).info(
                        "Data resumed after %.0f s without data (%d reconnects).", silent_s, state.reconnects
                    )
                else:
                    self._link(source).info("Data resumed after %.0f s without data.", silent_s)
            self._states[source] = _Stall(ingest, started)
            return

        silent_s = (now - quiet_since) / _NS

        if warn_s > 0 and not state.warned and silent_s >= warn_s:
            self._warn(source, state, now, silent_s)

        if reconnect_s <= 0 or not source.supports_reconnect:
            return
        if state.pending is not None and not state.pending.done():
            return  # the last reconnect has not returned yet

        interval_s = min(reconnect_s * 2**state.reconnects, STALL_RECONNECT_MAX_S)
        since_attempt_s = (now - max(quiet_since, state.last_attempt_ns)) / _NS
        if since_attempt_s < interval_s:
            return

        if not state.warned:
            self._warn(source, state, now, silent_s)
        if state.reconnects >= STALL_RECONNECTS_BEFORE_HINT and not state.hinted:
            state.hinted = True
            hint = f" {source.stall_hint}" if source.stall_hint else ""
            self._link(source).warn("%d reconnects have not brought the data back.%s", state.reconnects, hint)

        state.reconnects += 1
        state.last_attempt_ns = now
        total_s = (now - state.silent_since_ns) / _NS
        self._link(source).warn("Reconnecting (attempt %d) after %.0f s without data.", state.reconnects, total_s)
        state.pending = self._tasks.run_task(self._reconnect, source)

    def _warn(self, source: "BaseReader", state: _Stall, now: int, silent_s: float) -> None:
        state.warned = True
        state.silent_since_ns = now - int(silent_s * _NS)
        self._link(source).warn("No data for %.0f s.", silent_s)
        # The warning is already out: a diagnosis that blocks in a driver call delays nothing.
        self._tasks.run_task(self._describe, source)

    def _describe(self, source: "BaseReader") -> None:
        try:
            text = source.describe_stall()
        except Exception as e:
            text = f"diagnosis failed: {e}"
        if text:
            self._link(source).warn("Stall diagnosis: %s", text)

    def _reconnect(self, source: "BaseReader") -> None:
        try:
            if not source.reconnect():
                self._link(source).warn("Reconnect did not run.")
        except Exception as e:
            self._link(source).error("Reconnect failed.", exc=e)

    @staticmethod
    def _link(source: "BaseReader"):
        return source.logger_link or source.logger

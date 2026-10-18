# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from time import monotonic_ns
from typing import Callable, Iterable, Optional

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.configurable import configuration_factory, configuration_property
from blinkview.core.constants import FactoryCategory, SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.logger import BaseLogger

PutFnType = Callable[[Iterable[tuple]], None]


@configuration_factory(FactoryCategory.SOURCE)
@configuration_property(
    "name",
    type="string",
    default="default_source",
    required=False,
    ui_order=1,
    description="Name of the source device (for logging purposes)",
    ui_remember=True,
)
@configuration_property(
    "_note", title="Note", type="string", ui_order=-1, description="Add a not for your own reference."
)
@configuration_property(
    "stall_warn_s",
    title="Warn after no data (s)",
    type="integer",
    default=0,
    minimum=0,
    ui_order=90,
    description="Log a warning when this source has received nothing for this many seconds. 0 turns it off.",
)
@configuration_property(
    "stall_reconnect_s",
    title="Reconnect after no data (s)",
    type="integer",
    default=0,
    minimum=0,
    ui_order=91,
    description="Reconnect this source when it has received nothing for this many seconds, and retry at "
    "doubling intervals while it stays silent. 0 turns it off. Only sources that can reconnect themselves use it.",
)
class BaseReader(BaseDaemon):
    # A reader that implements reconnect(); the stall watchdog (core/source_stall_watchdog.py) only
    # warns about the others.
    supports_reconnect = False
    # Added to the watchdog's warning once reconnecting has not brought data back.
    stall_hint: Optional[str] = None
    # A reader that can give its connection up on request and take it back - the pause interface
    # below. Such a reader can lend its port to an external tool (io/source_borrow.py) when it
    # declares the `allow_borrow` / `borrow_name` / `borrow_timeout_s` config options.
    supports_pause = False

    stall_warn_s: int
    stall_reconnect_s: int

    def __init__(self):
        super().__init__()

        # monotonic_ns() stamps read by the stall watchdog: the last distribute(), the last start(),
        # and the last time the reader (re)connected - see _mark_ready().
        self._last_ingest_ns = 0
        self._started_ns = 0
        self._ready_ns = 0

        # Pause interface: request_pause() sets this, the reader thread closes its connection and
        # reports it through is_paused.
        self._pause_requested = False
        self._borrow_watcher = None

        self.targets = [SysCat.PARSER]
        self.logger_state: Optional[BaseLogger] = None
        self.logger_state_open: BaseLogger = None
        self.logger_state_lent: Optional[BaseLogger] = None
        self.logger_link: BaseLogger = None
        # self.logger_state_connected: Optional[BaseLogger] = (
        #     None  # will track connected state... data has moved in last x seconds
        # )

    def get_commands(self) -> list[tuple[str, str]]:
        """Exposes custom features to GUI/CLI layers.

        Returns:
            A list of (command_token, human_readable_name) tuples.
        """
        return []

    def start(self):
        self._started_ns = self._ready_ns = monotonic_ns()
        self._pause_requested = False
        super().start()
        if self.is_running:
            self._start_borrow_watcher()

    def stop(self, timeout=5.0):
        self._stop_borrow_watcher()
        super().stop(timeout)

    # --- Pause interface (supports_pause) ---

    def request_pause(self, reason: str) -> None:
        """Asks the reader thread to close its connection and keep it closed until
        request_resume(). Returns at once; is_paused turns True once the connection is closed."""
        if self.logger_link:
            self.logger_link.info("Pausing: %s", reason)
        self._pause_requested = True

    def request_resume(self) -> None:
        """Lets the reader thread open its connection again."""
        self._pause_requested = False

    @property
    def is_paused(self) -> bool:
        """True while the connection is closed because of request_pause(). Readers that support
        pausing override this."""
        return False

    def default_borrow_name(self) -> str:
        """The borrow name used when `borrow_name` is empty - for a serial port, the port's own name."""
        return ""

    def _start_borrow_watcher(self):
        if not self.supports_pause or not getattr(self, "allow_borrow", False) or self._borrow_watcher is not None:
            return
        from blinkview import borrow
        from blinkview.io.source_borrow import SourceBorrowWatcher

        name = getattr(self, "borrow_name", "") or self.default_borrow_name()
        try:
            paths = borrow.resolve(name)
        except ValueError as e:
            self.logger_link.error("Can't lend the port: no usable borrow name (set borrow_name)", exc=e)
            return

        self._borrow_watcher = SourceBorrowWatcher(
            self,
            paths,
            timeout_s=getattr(self, "borrow_timeout_s", 0) or 0,
            logger=self.logger_link,
            tasks=self.shared.tasks,
        )
        self._borrow_watcher.start()

    def _stop_borrow_watcher(self):
        watcher, self._borrow_watcher = self._borrow_watcher, None
        if watcher is not None:
            watcher.stop()
        self._pause_requested = False

    @property
    def is_connected(self) -> bool:
        """False while the reader knows it has no connection (and is already retrying): the stall
        watchdog waits instead of counting that as a stall. Readers that track it override this."""
        return True

    def _mark_ready(self):
        """Called by a reader when its connection comes up: the stall clock starts from here."""
        self._ready_ns = monotonic_ns()

    def distribute(self, batch):
        self._last_ingest_ns = monotonic_ns()
        super().distribute(batch)

    def reconnect(self) -> bool:
        """Drops the connection so the reader opens it again, called by the stall watchdog on a worker
        thread. Returns False when it could not. Readers that implement it set supports_reconnect."""
        return False

    def describe_stall(self) -> Optional[str]:
        """What the reader can tell about a stall (driver status, ...), logged with the watchdog's
        warning. Called on a worker thread; must not raise."""
        return None

    def apply_config(self, config: dict):
        # 1. Apply configuration via the parent framework
        changed = super().apply_config(config)

        self.logger_state = self.logger_state or self.logger.child("state")
        self.logger_state_open = self.logger_state_open or self.logger_state.child("open", essential=True)
        self.logger_link = self.logger_link or self.logger.child("link", essential=True)
        if self.supports_pause:
            # 1 while the port is lent to an external tool - tells "lent" apart from "broken"
            # where state.open alone reads 0 for both.
            self.logger_state_lent = self.logger_state_lent or self.logger_state.child("lent")
        # self.logger_state_connected = self.logger_state_connected or self.logger_state.child("connected")

        return changed


@register_factory_category(FactoryCategory.SOURCE)
class DeviceFactory(BaseFactory[BaseReader]):
    pass

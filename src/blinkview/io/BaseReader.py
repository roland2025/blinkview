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

    stall_warn_s: int
    stall_reconnect_s: int

    def __init__(self):
        super().__init__()

        # monotonic_ns() stamps read by the stall watchdog: the last distribute(), the last start(),
        # and the last time the reader (re)connected - see _mark_ready().
        self._last_ingest_ns = 0
        self._started_ns = 0
        self._ready_ns = 0

        self.targets = [SysCat.PARSER]
        self.logger_state: Optional[BaseLogger] = None
        self.logger_state_open: BaseLogger = None
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
        super().start()

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
        # self.logger_state_connected = self.logger_state_connected or self.logger_state.child("connected")

        return changed


@register_factory_category(FactoryCategory.SOURCE)
class DeviceFactory(BaseFactory[BaseReader]):
    pass

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


from time import perf_counter, sleep, time
from types import SimpleNamespace
from typing import Any, Callable, List, NamedTuple

import numpy as np

from ..core.base_daemon import BaseDaemon
from ..core.batch_queue import BatchQueue
from ..core.configurable import (
    configuration_factory,
    configuration_property,
    on_config_change,
    override_property,
)
from ..core.constants import SysCat
from ..core.device_identity import DeviceIdentity
from ..core.factory import BaseFactory
from ..core.limits import BATCH_MAXLEN
from ..core.log_row import LogRow
from ..core.numpy_batch_manager import PooledLogBatch
from ..core.reusable_batch_pool import TimeDataEntry
from ..core.types.parsing import SyncState, create_default_sync
from ..utils.log_level import LogLevel

# Define the signature for a transformation
TransformFunc = Callable[[Any], Any]


@configuration_factory("parser")
@configuration_property(
    "max_batch",
    type="integer",
    default=BATCH_MAXLEN,
    description="Maximum number of log entries to buffer before flushing",
    ui_order=1,
)
@configuration_property(
    "delay",
    type="integer",
    default=30,
    description="Maximum time (in milliseconds) to hold a batch before flushing",
    ui_order=2,
)
@configuration_property(
    "sources_",
    type="array",
    required=True,
    items={"type": "string", "_reference": "/sources"},
    default=[],
)
@configuration_property(
    "name",
    type="string",
    default="pipeline",
    required=True,
    description="Name of the source device (for logging purposes)",
)
@configuration_property(
    "_note",
    title="Note",
    type="string",
    ui_order=-1,
    description="Add a not for your own reference.",
)
@configuration_property(
    "time_sync",
    type="object",
    required=False,
    _factory="time_sync",
)
class BaseParser(BaseDaemon):
    max_batch: int
    delay: int
    name: str
    time_sync: dict

    TRACKER_CAPACITY = 1024
    AVG_NAME_LEN = 64

    def __init__(self):
        super().__init__()
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put

        self.targets: List[SysCat] = [SysCat.REORDER, SysCat.STORAGE]
        self.sync_state = None
        self.time_syncer = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        factory_build = self.shared.factories.build

        time_sync_conf = getattr(self, "time_sync", None)
        self.logger.debug(f"time_sync config: {time_sync_conf}")
        if time_sync_conf is not None:
            if self.sync_state is None:
                self.sync_state: SyncState = create_default_sync(self.shared.time_ns())

                # print(f"BinaryParser initial sync state: {self.sync_state}")
            sync_ctx = SimpleNamespace(
                get_logger=self.logger.child_creator("decoder"),
                parser=self,
            )
            self.time_syncer = factory_build("time_sync", time_sync_conf, system_ctx=self.shared, local_ctx=sync_ctx)

        return changed


class ParserFactory(BaseFactory[BaseParser]):
    pass

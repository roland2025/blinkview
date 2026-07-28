# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace
from typing import Any, Callable, List

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.configurable import (
    configuration_factory,
    configuration_property,
)
from blinkview.core.constants import FactoryCategory, SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category
from blinkview.core.limits import BATCH_MAXLEN
from blinkview.core.types.parsing import SyncState, create_default_sync

# Define the signature for a transformation
TransformFunc = Callable[[Any], Any]


@configuration_factory(FactoryCategory.PARSER)
@configuration_property(
    "max_batch",
    type="integer",
    default=BATCH_MAXLEN,
    hidden=True,
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
    ui_remember=True,
)
@configuration_property(
    "_note",
    title="Note",
    type="string",
    ui_order=-1,
    description="Add a note for your own reference.",
    ui_remember=True,
)
@configuration_property(
    "time_sync",
    type="object",
    hidden=True,
    required=False,
    _factory=FactoryCategory.TIME_SYNC,
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

        try:
            time_sync_conf = getattr(self, "time_sync", None)
            self.logger.debug(f"time_sync config: {time_sync_conf}")
            if time_sync_conf is not None:
                if self.sync_state is None:
                    self.sync_state: SyncState = create_default_sync(self.shared.time_ns())

                    # print(f"BinaryParser initial sync state: {self.sync_state}")
                sync_ctx = SimpleNamespace(
                    get_logger=self.logger.child_creator("time"),
                    parser=self,
                )
                if self.time_syncer is not None:
                    self.time_syncer.stop()

                    self.unsubscribe(self.time_syncer)

                    self.unregister_child(self.time_syncer)
                    self.time_syncer = None
                self.time_syncer = factory_build(
                    FactoryCategory.TIME_SYNC, time_sync_conf, system_ctx=self.shared, local_ctx=sync_ctx
                )
                self.subscribe(self.time_syncer)
                self.register_child(self.time_syncer)
        except Exception as e:
            self.logger.exception("failed to init timesync.", e)
        return changed


@register_factory_category(FactoryCategory.PARSER)
class ParserFactory(BaseFactory[BaseParser]):
    pass

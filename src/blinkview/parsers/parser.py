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
    description="Name of the device this pipeline's output lines are logged as. Give each pipeline its own: "
    "a key/value pipeline ignores the lines of its own device. Bound when the pipeline is created.",
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
        old_time_sync = getattr(self, "time_sync", None)
        changed = super().apply_config(config)

        try:
            time_sync_conf = getattr(self, "time_sync", None)
            self.logger.debug("time_sync config: %s", time_sync_conf)
            if time_sync_conf is not None and self.sync_state is None:
                self.sync_state: SyncState = create_default_sync(self.shared.time_ns())
            # Only a change to time_sync itself rebuilds the syncer (or removes it when time sync
            # is now off). The manager applies every pipeline's config on any pipeline edit, and a
            # rebuilt syncer starts a new engine that has to learn the drift again (~70 s).
            if time_sync_conf != old_time_sync:
                self._remove_time_syncer()
                self._load_time_syncer()
            elif self.time_syncer is None:
                self._load_time_syncer()
        except Exception as e:
            self.logger.exception("failed to init timesync.", exc=e)
        return changed

    def track_subscription(self, source_obj):
        super().track_subscription(source_obj)
        try:
            if self.time_syncer is None:
                self._load_time_syncer()
        except Exception as e:
            self.logger.exception("failed to load timesync.", exc=e)

    def _load_time_syncer(self):
        """Builds the time syncer when the `time_sync` setting is enabled. Its `type` picks the
        syncer; without one, the syncer is the one this pipeline's source declares
        (BaseReader.time_syncer_type), loaded once the pipeline is bound to such a source
        (track_subscription). The first bound source with a syncer type is the one used."""
        time_sync_conf = getattr(self, "time_sync", None)
        if not time_sync_conf or not time_sync_conf.get("enabled", False):
            return

        with self._subscribers_lock:
            sources = list(self._subscriptions)
        source = next((s for s in sources if getattr(s, "time_syncer_type", None)), None)

        syncer_type = time_sync_conf.get("type")
        if syncer_type is None:
            if source is None:
                return  # not bound to a source with a syncer (yet)
            syncer_type = source.time_syncer_type
            self.logger.info("Loading time syncer '%s' for source %s", syncer_type, source)
        elif source is None and sources:
            source = sources[0]

        self._build_time_syncer({**time_sync_conf, "type": syncer_type}, source=source)

    def _remove_time_syncer(self):
        if self.time_syncer is not None:
            self.time_syncer.stop()
            self.unsubscribe(self.time_syncer)
            self.unregister_child(self.time_syncer)
            self.time_syncer = None

    def _build_time_syncer(self, time_sync_conf: dict, source):
        """`source` is the reader the syncer sends its pings through; None leaves the syncer to
        take this pipeline's first upstream subscription."""
        self._remove_time_syncer()

        sync_ctx = SimpleNamespace(
            get_logger=self.logger.child_creator("time"),
            parser=self,
            source=source,
        )
        self.time_syncer = self.shared.factories.build(
            FactoryCategory.TIME_SYNC, time_sync_conf, system_ctx=self.shared, local_ctx=sync_ctx
        )
        self.subscribe(self.time_syncer)
        self.register_child(self.time_syncer)


@register_factory_category(FactoryCategory.PARSER)
class ParserFactory(BaseFactory[BaseParser]):
    pass

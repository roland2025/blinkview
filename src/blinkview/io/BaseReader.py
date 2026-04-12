# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock, Thread
from time import perf_counter
from typing import Callable, Iterable

from ..core.base_daemon import BaseDaemon
from ..core.configurable import configuration_factory, configuration_property
from ..core.constants import SysCat
from ..core.device_identity import DeviceIdentity
from ..core.factory import BaseFactory
from ..core.logger import SystemLogger
from ..core.reusable_batch_pool import BatchPool
from ..utils.log_level import LogLevel
from ..utils.settings_updater import update_object_from_config

PutFnType = Callable[[Iterable[tuple]], None]


@configuration_factory("source")
@configuration_property(
    "name",
    type="string",
    default="default_source",
    required=False,
    ui_order=1,
    description="Name of the source device (for logging purposes)",
)
@configuration_property(
    "_note", title="Note", type="string", ui_order=-1, description="Add a not for your own reference."
)
class BaseReader(BaseDaemon):
    def __init__(self):
        super().__init__()

        self.targets = [SysCat.PARSER]

        # self.device_id: DeviceIdentity = device_id
        # self._timestamp_fn = time_ns
        # self.push_log = push_log_cb
        # self._mod_id_reader = device_id.get_module('_reader')


class DeviceFactory(BaseFactory[BaseReader]):
    pass

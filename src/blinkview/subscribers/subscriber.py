# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.constants import FactoryCategory, SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category


class BaseSubscriber(BaseDaemon):
    def __init__(self):
        super().__init__()

        self.sources = [SysCat.STORAGE, SysCat.REORDER, SysCat.PARSER]

        self.input_queue = BatchQueue()

        self.put = self.input_queue.put


class SubscriberFactory(BaseFactory[BaseSubscriber]):
    pass


@register_factory_category(FactoryCategory.TIME_SYNC)
class TimeSyncerFactory(BaseFactory[SubscriberFactory]):
    pass

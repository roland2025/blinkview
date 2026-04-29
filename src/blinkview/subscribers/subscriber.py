# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from ..core.base_daemon import BaseDaemon
from ..core.batch_queue import BatchQueue
from ..core.constants import SysCat
from ..core.factory import BaseFactory


class BaseSubscriber(BaseDaemon):
    def __init__(self):
        super().__init__()

        self.sources = [SysCat.STORAGE, SysCat.REORDER, SysCat.PARSER]

        self.input_queue = BatchQueue()

        self.put = self.input_queue.put


class SubscriberFactory(BaseFactory[BaseSubscriber]):
    pass


class TimeSyncerFactory(BaseFactory[SubscriberFactory]):
    pass

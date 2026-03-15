# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import List

from .base_configurable import configuration_property, configuration_factory, override_property
from .base_daemon import BaseDaemon
from .constants import SysCat
from .factory import BaseFactory


@configuration_factory("reorder")
@configuration_property("delay", type="integer", default=100, description="Delay window in milliseconds for reordering logs")
@override_property("enabled", default=True, hidden=True)
class BaseReorder(BaseDaemon):
    delay: int

    def __init__(self):
        super().__init__()
        self.targets: List[SysCat] = [SysCat.STORAGE]


class ReorderFactory(BaseFactory[BaseReorder]):
    pass

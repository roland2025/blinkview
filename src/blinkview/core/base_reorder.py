# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import List

from blinkview.core.base_daemon import BaseDaemon
from blinkview.core.configurable import configuration_factory, configuration_property, override_property
from blinkview.core.constants import FactoryCategory, SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.factory_category_registry import register_factory_category


@configuration_factory(FactoryCategory.REORDER)
@configuration_property(
    "delay", type="integer", default=100, description="Delay window in milliseconds for reordering logs"
)
@override_property("enabled", default=True, hidden=True)
class BaseReorder(BaseDaemon):
    delay: int

    def __init__(self):
        super().__init__()
        self.targets: List[SysCat] = [SysCat.STORAGE]


@register_factory_category(FactoryCategory.REORDER)
class ReorderFactory(BaseFactory[BaseReorder]):
    pass

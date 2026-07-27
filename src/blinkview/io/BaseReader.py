# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

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
class BaseReader(BaseDaemon):
    def __init__(self):
        super().__init__()

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

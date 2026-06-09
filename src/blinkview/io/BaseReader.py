# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Callable, Iterable

from ..core.base_daemon import BaseDaemon
from ..core.configurable import configuration_factory, configuration_property
from ..core.constants import SysCat
from ..core.factory import BaseFactory

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

    def get_commands(self) -> list[tuple[str, str]]:
        """Exposes custom features to GUI/CLI layers.

        Returns:
            A list of (command_token, human_readable_name) tuples.
        """
        return []


class DeviceFactory(BaseFactory[BaseReader]):
    pass

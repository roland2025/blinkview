# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from .factory_registry import FactoryRegistry  # Adjust import path as needed
    from .id_registry import IDRegistry  # Adjust import path as needed
    from .registry import Registry
    from .reusable_batch_pool import PoolManager
    from .settings_manager import SettingsManager
    from .task_manager import TaskManager


@dataclass(frozen=True)
class SystemContext:
    time_ns: Callable[[], int]
    registry: "Registry"
    id_registry: "IDRegistry"
    factories: "FactoryRegistry"
    tasks: "TaskManager"
    settings: "SettingsManager"
    pool: "PoolManager"

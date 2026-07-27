# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from blinkview.core.array_pool import NumpyArrayPool
    from blinkview.core.factory_registry import FactoryRegistry  # Adjust import path as needed
    from blinkview.core.id_history import IdHistory
    from blinkview.core.id_registry import IDRegistry  # Adjust import path as needed
    from blinkview.core.registry import Registry
    from blinkview.core.settings_manager import SettingsManager
    from blinkview.core.task_manager import TaskManager


@dataclass(frozen=True)
class SystemContext:
    time_ns: Callable[[], int]
    registry: "Registry"
    id_registry: "IDRegistry"
    factories: "FactoryRegistry"
    tasks: "TaskManager"
    settings: "SettingsManager"
    array_pool: "NumpyArrayPool"
    pid_history: "IdHistory"

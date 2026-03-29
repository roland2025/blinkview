# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Callable

from blinkview.core.configurable import (
    configurable,
    configuration_property,
    on_config_change,
    override_property,
)
from blinkview.core.factory import BaseFactory
from blinkview.utils.log_level import LogLevel


@configurable
class BaseLogLevelMap:
    pass


class LogLevelMapFactory(BaseFactory[BaseLogLevelMap]):
    pass


@LogLevelMapFactory.register("default")
@configuration_property(
    "mapping",
    type="object",
    title="Level Text Mappings",
    description="Map specific text strings found in logs to LogLevels.",
    # This allows the UI to add arbitrary keys (the text)
    # and select a LogLevel from a dropdown for each.
    additionalProperties={
        "type": "integer",
        "enum": [lvl.value for lvl in LogLevel.LIST_CONF],
        "enum_descriptions": [lvl.name_conf for lvl in LogLevel.LIST_CONF],
        "default": LogLevel.INFO.value,
    },
    default={
        "T": LogLevel.TRACE.value,
        "D": LogLevel.DEBUG.value,
        "I": LogLevel.INFO.value,
        "W": LogLevel.WARN.value,
        "E": LogLevel.ERROR.value,
        "F": LogLevel.FATAL.value,
        "C": LogLevel.CRITICAL.value,
    },
)
class LevelMap(BaseLogLevelMap):
    mapping: dict

    def __init__(self):
        super().__init__()
        self._lookup = {}
        self._bake_internal()

    @on_config_change("mapping")
    def _on_mapping_changed(self, new_mapping, _):
        """Re-bakes the internal lookup dictionary whenever the config changes."""
        self._bake_internal()

    def _bake_internal(self):
        """Converts integer values from config into LogLevel objects for fast lookup."""
        self._lookup = {text: LogLevel.from_value(val, LogLevel.INFO) for text, val in self.mapping.items()}

    def get_level(self, text: str, default: LogLevel = None) -> LogLevel:
        """Returns the LogLevel object for the given text string."""
        return self._lookup.get(text, default)

    def levels(self):
        """Returns the set of all LogLevel objects currently in the mapping."""
        return self._lookup.values()


@LogLevelMapFactory.register("nrf")
@override_property(
    "mapping",
    # type="object",
    title="NRF Level Mappings",
    description="Predefined mapping for Nordic NRF logs.",
    default={
        "<trace>": LogLevel.TRACE.value,
        "<debug>": LogLevel.DEBUG.value,
        "<info>": LogLevel.INFO.value,
        "<warn>": LogLevel.WARN.value,
        "<warning>": LogLevel.WARN.value,
        "<error>": LogLevel.ERROR.value,
        "<fatal>": LogLevel.FATAL.value,
        "<critical>": LogLevel.CRITICAL.value,
    },
)
class NrfLevelMap(LevelMap):
    pass


@LogLevelMapFactory.register("zephyr")
@override_property(
    "mapping",
    # type="object",
    title="NRF Level Mappings",
    description="Predefined mapping for Nordic NRF logs.",
    default={
        "<dbg>": LogLevel.DEBUG.value,
        "<inf>": LogLevel.INFO.value,
        "<wrn>": LogLevel.WARN.value,
        "<err>": LogLevel.ERROR.value,
    },
)
class NrfLevelMap(LevelMap):
    pass


@LogLevelMapFactory.register("custom")
@override_property("mapping", title="Custom level Mappings", description="Custom mapping for logs.", default={})
class CustomLevelMap(LevelMap):
    pass

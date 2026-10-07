# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Optional

from blinkview.core import dtypes
from blinkview.core.configurable import (
    configuration_property,
    override_property,
)
from blinkview.core.frame_warmup_registry import SHARED_KERNEL_WARMUP_WEIGHT, frame_section_warmup
from blinkview.core.id_registry.tables import IndexedStringTable
from blinkview.ops.levels import nb_parse_log_level_stage
from blinkview.parsers.frame_parsers import FrameSectionParser, FrameSectionParserFactory
from blinkview.utils.log_level import LogLevel


@FrameSectionParserFactory.register("log_level_default")
@frame_section_warmup("log_level_default")
@configuration_property(
    "mapping",
    type="object",
    title="Level Text Mappings",
    description="Map specific text strings found in logs to LogLevels.",
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
class LevelMap(FrameSectionParser):
    mapping: dict

    def __init__(self):
        super().__init__()
        self._table: Optional[IndexedStringTable] = None

        self._lookup = {}
        self._table_bundle = None

    def apply_config(self, config):
        changed = super().apply_config(config)

        # 1. Cleanup old resources
        if self._table:
            self._table.release()

        # 2. Prepare items
        items = list(self.mapping.items())
        count = len(items)

        # 3. Initialize Table and Values Array
        # We use sequential IDs (0 to count-1) to keep the table dense
        self._table = IndexedStringTable(initial_capacity=count, buffer_size_bytes=128, values_dtype=dtypes.VALUES_TYPE)

        for i, (text, level_val) in enumerate(items):
            # Register string at index i
            self._table.register_name(i, text, level_val)

        self._lookup = {text: LogLevel.from_value(val, LogLevel.INFO) for text, val in self.mapping.items()}

        self._table_bundle = self._table.bundle()

        return changed

    def kernel(self, octx, fctx, n):
        nb_parse_log_level_stage(octx, fctx, n, self._table_bundle)

    def table(self):
        return self._table

    def release(self):
        """Explicitly release pool resources."""
        if self._table:
            self._table.release()
            self._table = None

    def __del__(self):
        self.release()

    def levels(self):
        """Returns the set of all LogLevel objects currently in the mapping."""
        return self._lookup.values()


@FrameSectionParserFactory.register("log_level_nrf")
@frame_section_warmup("log_level_nrf", warmup_weight=SHARED_KERNEL_WARMUP_WEIGHT)
@override_property(
    "mapping",
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


@FrameSectionParserFactory.register("log_level_zephyr")
@frame_section_warmup("log_level_zephyr", warmup_weight=SHARED_KERNEL_WARMUP_WEIGHT)
@override_property(
    "mapping",
    title="Zephyr Level Mappings",
    description="Predefined mapping for Zephyr RTOS logs.",
    default={
        "<dbg>": LogLevel.DEBUG.value,
        "<inf>": LogLevel.INFO.value,
        "<wrn>": LogLevel.WARN.value,
        "<err>": LogLevel.ERROR.value,
    },
)
class ZephyrLevelMap(LevelMap):
    pass


@FrameSectionParserFactory.register("log_level_zephyr_minimal")
@frame_section_warmup("log_level_zephyr_minimal", warmup_weight=SHARED_KERNEL_WARMUP_WEIGHT)
@override_property(
    "mapping",
    title="Zephyr Level Mappings",
    description="Predefined mapping for Zephyr Minimal RTOS logs.",
    default={
        "I:": LogLevel.INFO.value,
        "E:": LogLevel.ERROR.value,
        "W:": LogLevel.WARN.value,
        "D:": LogLevel.DEBUG.value,
    },
)
class ZephyrMinimalLevelMap(LevelMap):
    pass


@FrameSectionParserFactory.register("log_level_idf")
@frame_section_warmup("log_level_idf", warmup_weight=SHARED_KERNEL_WARMUP_WEIGHT)
@override_property(
    "mapping",
    title="ESP-IDF Level Mappings",
    description="Predefined mapping for ESP_IDF log levels.",
    default={
        "I": LogLevel.INFO.value,
        "E": LogLevel.ERROR.value,
        "W": LogLevel.WARN.value,
        "D": LogLevel.DEBUG.value,
        "V": LogLevel.TRACE.value,
    },
)
class EspIdfLevelMap(LevelMap):
    pass


@FrameSectionParserFactory.register("log_level_custom")
@frame_section_warmup("log_level_custom", warmup_weight=SHARED_KERNEL_WARMUP_WEIGHT)
@override_property("mapping", title="Custom level Mappings", description="Custom mapping for logs.", default={})
class CustomLevelMap(LevelMap):
    pass


@FrameSectionParserFactory.register("log_level_python")
@frame_section_warmup("log_level_python", warmup_weight=SHARED_KERNEL_WARMUP_WEIGHT)
@override_property(
    "mapping",
    title="Python Logging levels",
    description="Predefined mapping for python logging module levels.",
    default={
        "INFO": LogLevel.INFO.value,
        "ERROR": LogLevel.ERROR.value,
        "WARNING": LogLevel.WARN.value,
        "DEBUG": LogLevel.DEBUG.value,
        "CRITICAL": LogLevel.CRITICAL.value,
    },
)
class PythonLoggingLevelMap(LevelMap):
    pass

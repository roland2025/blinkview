# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from enum import StrEnum, auto


class SysCat(StrEnum):
    DEVICE = auto()
    PARSER = auto()
    REORDER = auto()
    STORAGE = auto()


class FactoryCategory:
    """Category names for FactoryRegistry - see core/factory_category_registry.py for the
    decorator that registers a Factory class under one of these."""

    REORDER = "reorder"
    CENTRAL = "central"
    SOURCE = "source"
    PARSER = "parser"
    TIME_SYNC = "time_sync"
    PIPELINE_ASSEMBLER = "pipeline_assembler"
    LOGGING_PROCESSOR = "logging_processor"
    FILE_LOGGING = "file_logging"
    FRAME_DECODER = "frame_decoder"
    FRAME_PARSER = "frame_parser"
    FRAME_SECTION_PARSER = "frame_section_parser"
    KEY_VALUE_RULE = "key_value_rule"

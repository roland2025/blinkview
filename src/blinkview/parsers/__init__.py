# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo


# Every submodule here registers a Factory type (@XFactory.register(...)) or a factory category
# (@register_factory_category(...)) as an import-time side effect. The "as x" re-export alias on
# each is deliberate, not decorative: it's the standard pyflakes/ruff/pyright signal for "this
# import is an intentional public re-export, not an accidental unused one" - see
# core/factory_category_registry.py's module docstring for the registration mechanism itself.
from blinkview.parsers import adb_decoder as adb_decoder
from blinkview.parsers import binary_parser as binary_parser
from blinkview.parsers import can_parser as can_parser
from blinkview.parsers import frame_decoders as frame_decoders
from blinkview.parsers import frame_parsers as frame_parsers
from blinkview.parsers import key_value as key_value
from blinkview.parsers import module_gen as module_gen
from blinkview.parsers import multi_rule_key_value as multi_rule_key_value
from blinkview.parsers import parser as parser
from blinkview.parsers import time_sync_profiler as time_sync_profiler

__all__ = [
    "adb_decoder",
    "binary_parser",
    "can_parser",
    "frame_decoders",
    "frame_parsers",
    "key_value",
    "module_gen",
    "multi_rule_key_value",
    "parser",
    "time_sync_profiler",
]

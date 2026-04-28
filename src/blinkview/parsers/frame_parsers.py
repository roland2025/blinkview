# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace
from typing import List

import numpy as np

from blinkview.core import dtypes
from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable, configuration_property
from blinkview.core.factory import BaseFactory
from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.core.system_context import SystemContext
from blinkview.core.types.modules import (
    MODULE_TEMP_ID_BASE,
    DynamicWidthConfig,
    FixedWidthConfig,
    ModuleTrackerState,
)
from blinkview.core.types.parsing import (
    CodecID,
    EmptyUnifiedParserConfig,
    EmptyUnifiedParserState,
    ParserConfig,
    ParserID,
    ParserPipelineBundle,
    TimeParserState,
    UnifiedParserConfig,
    UnifiedParserState,
    UnusedSyncState,
)
from blinkview.ops.constants import EMPTY_STATE
from blinkview.ops.generic import SkipWordsConfig, skip_words_parser
from blinkview.ops.modules import parse_fixed_width_name, parse_module_tags_statemachine
from blinkview.utils.log_level import LogLevel
from blinkview.utils.utc_offset import get_local_utc_offset_seconds


@configurable
@bindable
class FrameParser:
    shared: SystemContext
    local: SimpleNamespace
    pass


class FrameParserFactory(BaseFactory[FrameParser]):
    pass


@configurable
@bindable
class FrameSectionParser(FrameParser):
    shared: SystemContext

    local: SimpleNamespace


class FrameSectionParserFactory(FrameParserFactory):
    pass


@configuration_property(
    "parser_errors_hidden",
    type="boolean",
    title="Hide parser errors",
    default=False,
    required=True,
)
@configuration_property(
    "steps",
    type="array",
    required=True,
    ui_order=15,
    items={
        "type": "object",
        "_factory": "frame_section_parser",
        "title": "Parser step",
    },
)
@configuration_property(
    "filter_squash_spaces",
    title="Squash spaces",
    type="boolean",
    ui_order=22,
    default=False,
    required=True,
)
@FrameParserFactory.register("default")
class GenericFrameParser(FrameParser):
    """Frame processor with no special encoding"""

    filter_squash_spaces: bool
    parser_errors_hidden: bool

    def __init__(self):
        self.pipeline = []

        self.post_process = self.no_post_process

    def apply_config(self, config: dict):
        changed = super().apply_config(config)
        """
        Expects a JSON structure like:
        "steps": [
            {"type": "decode", "encoding": "utf-8"},
            {"type": "ansi_filter"},
            {"type": "replace", "search": "\r", "replace": ""}
        ]
        """

        pipeline: List[FrameSectionParser] = []
        local_ctx = SimpleNamespace(
            device_id=self.local.device_id,
            sync_state=self.local.sync_state,
        )
        for step_cfg in config.get("steps", []):
            step = self.shared.factories.build(
                "frame_section_parser", config=step_cfg, system_ctx=self.shared, local_ctx=local_ctx
            )

            pipeline.append(step)

        self.pipeline = pipeline

        return changed

    def bundle(self):
        device_identity = self.local.device_id

        p_config = ParserConfig(
            level_default=LogLevel.INFO.value,
            level_error=LogLevel.ERROR.value,
            module_log=device_identity.get_module("log").id,
            module_unknown=device_identity.get_module("unknown").id,
            device_id=device_identity.id,
            report_error=not self.parser_errors_hidden,
            filter_squash_spaces=self.filter_squash_spaces,
        )

        pipe = []
        post_process_steps = []
        for item in self.pipeline:
            pipe.append(item.bundle())
            pp_fn = getattr(item, "post_process", None)
            if pp_fn and callable(pp_fn):
                post_process_steps.append(pp_fn)

        count = len(post_process_steps)

        if count == 0:
            self.post_process = self.no_post_process

        elif count == 1:
            self.post_process = post_process_steps[0]

        elif count == 2:
            p1, p2 = post_process_steps

            def pp_2(batch):
                r1 = p1(batch)
                r2 = p2(batch)
                return r1 or r2

            self.post_process = pp_2

        elif count == 3:
            p1, p2, p3 = post_process_steps

            def pp_3(batch):
                r1 = p1(batch)
                r2 = p2(batch)
                r3 = p3(batch)
                return r1 or r2 or r3

            self.post_process = pp_3

        elif count == 4:
            p1, p2, p3, p4 = post_process_steps

            def pp_4(batch):
                r1 = p1(batch)
                r2 = p2(batch)
                r3 = p3(batch)
                r4 = p4(batch)
                return r1 or r2 or r3 or r4

            self.post_process = pp_4

        else:
            # Fallback for the rare case of 5+ steps
            pp_tuple = tuple(post_process_steps)

            def pp_gen(batch):
                changed = False
                for pp in pp_tuple:
                    if pp(batch):
                        changed = True
                return changed

            self.post_process = pp_gen

        return ParserPipelineBundle(config=p_config, pipeline=tuple(pipe))

    def no_post_process(self, _):
        return False


@bindable
class ModuleNameParserBase(FrameSectionParser):
    TRACKER_CAPACITY = 1024
    AVG_NAME_LEN = 64

    def __init__(self):

        self.tracker_state = UnifiedParserState(
            modules=ModuleTrackerState(
                # Scalars (wrapped in arrays so they are mutable by reference in NJIT)
                count=np.zeros(1, dtypes.ID_TYPE),
                bytes_cursor=np.zeros(1, dtypes.OFFSET_TYPE),
                # Metadata buffers for unresolved names
                starts=np.empty(self.TRACKER_CAPACITY, dtypes.OFFSET_TYPE),
                lengths=np.empty(self.TRACKER_CAPACITY, dtypes.LEN_TYPE),
                hashes=np.zeros(self.TRACKER_CAPACITY, dtypes.HASH_TYPE),
                # The raw byte scratchpad
                name_bytes=np.empty(self.TRACKER_CAPACITY * self.AVG_NAME_LEN, dtype=dtypes.BYTE),
            )
        )

    def post_process(self, batch: PooledLogBatch) -> bool:
        state = self.tracker_state.modules
        unresolved_count = state.count[0]

        if unresolved_count == 0:
            return False

        # 1. Snapshot registry size before lookups
        registry = self.shared.id_registry.modules_table
        initial_count = registry.count  # Assuming .count represents registered items

        active_modules = batch.bundle.modules[: batch.size]
        get_module = self.local.device_id.get_module

        for i in range(unresolved_count):
            start = state.starts[i]
            length = state.lengths[i]

            # Extract the raw bytes and decode
            name_bytes = state.name_bytes[start : start + length]
            module_name_str = name_bytes.tobytes().decode("ascii")

            # get_module handles the discovery/registration logic
            try:
                mod_id = get_module(module_name_str).id
            except Exception:
                import traceback

                traceback.print_exc()
                mod_id = get_module("unknown").id

            # 2. Vectorized Swap: Replace the placeholder ID with the real one
            # Using temp_id = BASE + i ensures we only swap the specific
            # instance parsed in this chunk.
            temp_id = MODULE_TEMP_ID_BASE + i
            active_modules[active_modules == temp_id] = mod_id

        # 3. Reset State for the next chunk
        state.count[0] = 0
        state.bytes_cursor[0] = 0

        # 4. Compare registry counts
        # If the count is higher, we discovered new modules and need to re-bundle
        return registry.count > initial_count


@FrameSectionParserFactory.register("module_name")
class ModuleNameParser(ModuleNameParserBase):
    def __init__(self):
        super().__init__()


@configuration_property(
    "max_length",
    type="integer",
    title="Module name maximum length",
    required=True,
    default=0,
)
@FrameSectionParserFactory.register("module_name_fixed_width")
class FixedWidthModuleNameParser(ModuleNameParserBase):
    """This parser extracts names from fixed-width fields by scanning for double-space or tab terminators. It is designed for log formats where module names are left-aligned in a fixed-width column, and shorter names are padded with spaces."""

    max_length: int

    def __init__(self):
        super().__init__()

    def bundle(self):
        # 1. Build the immutable config snapshot
        # We pass only the search width and the current module registry

        config = UnifiedParserConfig(
            string_table=self.shared.id_registry.modules_table.bundle(),
            module_config=DynamicWidthConfig(max_length=self.max_length),
        )

        # 2. Return the universal 3-tuple: (Function, Mutable State, Immutable Config)
        # self.tracker_state is the flattened state initialized in the base class
        return ParserID.MOD_FIXED_WIDTH, self.tracker_state, config


@configuration_property(
    "max_length",
    type="integer",
    title="Module name maximum length",
    required=True,
    default=64,
)
@configuration_property(
    "max_depth",
    type="integer",
    title="Module name depth",
    required=True,
    default=8,
)
@configuration_property(
    "enable_brackets",
    type="boolean",
    title="Bracketed module names",
    required=False,
    default=False,
)
@configuration_property(
    "enable_dot_separator",
    type="boolean",
    title="Colon-space-dot separator for submodules",
    required=False,
    default=False,
)
@FrameSectionParserFactory.register("module_name_normalizer")
class ModuleNameNormalizer(ModuleNameParserBase):
    """This parser extracts module names from variable-width fields by scanning for common delimiters (spaces, tabs, brackets) and normalizing them. It is designed for log formats where module names may be of varying lengths and may include hierarchical components separated by dots or enclosed in brackets."""

    max_length: int
    max_depth: int
    enable_brackets: bool
    enable_dot_separator: bool

    def __init__(self):
        super().__init__()

    def bundle(self):
        # 1. Build the IMMUTABLE config snapshot
        # Note: 'tracker' is removed from here.
        config = UnifiedParserConfig(
            string_table=self.shared.id_registry.modules_table.bundle(),
            module_config=DynamicWidthConfig(
                max_length=self.max_length,
                max_depth=self.max_depth,
                enable_brackets=self.enable_brackets,
                enable_dot_separator=self.enable_dot_separator,
            ),
        )

        # 2. Return the universal 3-tuple: (Function, Mutable State, Immutable Config)
        return ParserID.MOD_DYNAMIC_SM, self.tracker_state, config


@FrameSectionParserFactory.register("timestamp")
class TimestampParser(FrameSectionParser):
    def __init__(self):
        super().__init__()
        self.state = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        sync_state = getattr(self.local, "sync_state", UnusedSyncState)
        self.state = UnifiedParserState(timestamp=TimeParserState(sync=sync_state))

        utc_offset_seconds = get_local_utc_offset_seconds()

        self.state.timestamp.utc_offset[0] = dtypes.TS_TYPE(utc_offset_seconds)

        return changed


#
# @FrameSectionParserFactory.register("log_level")
# class LevelParser(FrameSectionParser):
#     pass


@configuration_property(
    "count",
    type="integer",
    title="Number of words to skip",
    required=True,
    default=1,
)
@FrameSectionParserFactory.register("skip_words")
class SkipWordsParser(FrameSectionParser):
    """
    Structural parser that advances the cursor past a specific number of words.
    Useful for skipping irrelevant columns or 'junk' data in log lines.
    """

    count: int

    def __init__(self):
        super().__init__()

    def bundle(self):
        # 1. Prepare the immutable config with the skip count
        # config = SkipWordsConfig(count=self.count)
        config = UnifiedParserConfig(module_config=DynamicWidthConfig(max_length=self.count))
        # 2. Return the universal 3-tuple
        # We use EMPTY_STATE because we aren't extracting any module IDs
        return ParserID.SKIP_WORDS, EmptyUnifiedParserState, config


@FrameSectionParserFactory.register("timestamp_zephyr_uptime_formatted")
class ZephyrUptimeFormattedParser(TimestampParser):
    def __init__(self):
        super().__init__()
        self._bundle = None

    def apply_config(self, config: dict):
        changed = super().apply_config(config)

        self._bundle = ParserID.TS_ZEPHYR_UPTIME_FORMATTED, self.state, EmptyUnifiedParserConfig

        return changed

    def bundle(self):
        return self._bundle

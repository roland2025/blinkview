# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from math import factorial
from threading import Lock

from .BaseBindableConfigurable import BaseBindableConfigurable
from .base_configurable import BaseConfigurable, configuration_property
from .central_storage import CentralFactory
from .constants import SysCat
from .device_identity import DeviceIdentity, ModuleIdentity
from .log_row import LogRow
from .logger import SystemLogger, PrintLogger
from .registry import Registry
from .reorder_buffer import ReorderFactory
from .system_context import SystemContext
from ..io.BaseReader import BaseReader, DeviceFactory
from ..parsers.parser import ParserThread, ParserFactory, BaseParser
from ..utils.level_map import LogLevel
from typing import Dict, Type
import importlib
from types import SimpleNamespace


class PipelineManager(BaseBindableConfigurable):
    def __init__(self):
        super().__init__()
        self.lock = Lock()

        # Maps device_name -> DevicePipeline
        self.pipelines: Dict[str, BaseParser] = {}

        self.needs_delayed_init = True  # Flag to indicate if delayed initialization is needed

    # ==========================================
    # PIPELINE CONSTRUCTION
    # ==========================================

    def apply_config(self, config: dict) -> bool:
        super().apply_config(config)

        for item_id, item_config in config.items():
            item = self.pipelines.get(item_id)
            if item is None:
                try:
                    print(f"Creating new pipeline: '{item_id}'")

                    name = item_config.get("name", item_id)
                    self.logger.info(f"Creating new pipeline: '{name}' ({item_id}) config: {item_config}")

                    device_id = self.shared.id_registry.get_device(name)

                    local_ctx = SimpleNamespace(
                        get_logger=self.shared.registry.logger_creator("parser", device_id.name),
                        device_id=device_id
                    )
                    item = self.shared.factories.build("parser", item_config, self.shared, local_ctx)

                    self.pipelines[item_id] = item
                    self.shared.registry.config.subscribe(f"/pipelines/{item_id}", item)

                    if not self.needs_delayed_init:
                        self.apply_target(item_id, item)  # Apply targets immediately if delayed init is not needed
                        # start thread
                        item.start()
                    #
                    # sources = [item.sources_] if isinstance(item.sources_, str) else item.sources_
                    # for source in sources:
                    #     self.logger.debug(f"Pipeline '{item_id}' has source: {source}")
                    #     self.shared.registry.get_source(source).subscribe(item)

                    self.subscribe(name, item)
                except Exception as e:
                    self.logger.error(f"Failed to create pipeline '{item_id}'", e)

        self.needs_delayed_init = False  # Mark that delayed initialization is no longer needed
        return True

    def apply_targets(self):
        for item_id, item in self.pipelines.items():
            self.apply_target(item_id, item)

    def apply_target(self, item_id, item):
        print(f"Applying targets for pipeline '{item_id}'")
        if hasattr(item, "sources_"):
            print(f"Pipeline '{item_id}' has sources: {item.sources_}")
            # check if its a list or a single string
            self.logger.warn(f"Source '{item_id}' has sources: {item.sources_}")
            sources = [item.sources_] if isinstance(item.sources_, str) else item.sources_
            for source in sources:
                print(f"Source '{item_id}' has source: {source}")
                self.logger.debug(f"Source '{item_id}' has source: {source}")
                self.shared.registry.get_reference_target(source).subscribe(item)

        if hasattr(item, "targets_"):
            self.logger.warn(f"Applying targets for source '{item_id}': {item.targets_}")
            targets = [item.targets_] if isinstance(item.targets_, str) else item.targets_

            for target in targets:
                target_ref = self.shared.registry.get_reference_target(target)
                self.logger.debug(f"Source '{item_id}' has target: {target}")
                item.subscribe(target_ref)

    def subscribe(self, name_debug, ref, pipeline = None):
        if ref is None:
            return

        # 1. Map the fixed infrastructure
        mapped_targets = {
            SysCat.REORDER: self.shared.registry.reorder,
            SysCat.STORAGE: self.shared.registry.central
        }
        mapped_sources = {
            # SysCat.REORDER: self.shared.registry.reorder,
            # SysCat.STORAGE: self.shared.registry.central
        }

        if pipeline is not None:
            # 2. Map the device-specific pipeline dynamically
            # Check Reader capabilities
            if pipeline.reader is not None:
                if hasattr(pipeline.reader, "put"):
                    mapped_targets[SysCat.DEVICE] = pipeline.reader
                if hasattr(pipeline.reader, "subscribe"):
                    mapped_sources[SysCat.DEVICE] = pipeline.reader

            if pipeline.parser is not None:
                # Check Parser capabilities (Fixed the reader-assignment bug)
                if hasattr(pipeline.parser, "put"):
                    mapped_targets[SysCat.PARSER] = pipeline.parser
                if hasattr(pipeline.parser, "subscribe"):
                    mapped_sources[SysCat.PARSER] = pipeline.parser

        # 3. Connect Targets (Where 'ref' sends data TO)
        for target_key in getattr(ref, "targets", []):
            target_obj = mapped_targets.get(target_key)
            if target_obj:
                # If the component provides its own subscribe method, use it
                if hasattr(ref, "subscribe"):
                    ref.subscribe(target_obj)
                    # Otherwise, if it's a raw queue-like object, we might need a different link
                    self.logger.debug(
                        f"[{name_debug}] Linked '{ref.__class__.__name__}' -> '{target_obj.__class__.__name__}' [{ref.__class__.__module__}.{ref.__class__.__name__} -> {target_obj.__class__.__module__}.{target_obj.__class__.__name__}]")
                    break

        # 4. Connect Sources (Where 'ref' gets data FROM)
        for source_key in getattr(ref, "sources", []):
            source_obj = mapped_sources.get(source_key)
            if source_obj and hasattr(source_obj, "subscribe"):
                source_obj.subscribe(ref)
                self.logger.debug(
                    f"[{name_debug}] Linked '{source_obj.__class__.__name__}' -> '{ref.__class__.__name__}' [{source_obj.__class__.__module__}.{source_obj.__class__.__name__} -> {ref.__class__.__module__}.{ref.__class__.__name__}]")
                break

    def start(self):
        with self.lock:
            for pipeline in self.pipelines.values():
                pipeline.start()

    def stop(self):
        with self.lock:
            for pipeline in self.pipelines.values():
                pipeline.stop()

    def get_schema(self, name: str):
        try:
            return self.pipelines[name].get_config_schema()
        except KeyError:
            return self.shared.factories.get_base_schema("parser")

    def get(self, id_: str):
        print(f"PipelineManager: Retrieving pipeline with ID '{id_}' all: {list(self.pipelines.keys())}")
        return self.pipelines.get(id_)

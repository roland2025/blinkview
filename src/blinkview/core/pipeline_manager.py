# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from types import SimpleNamespace
from typing import Dict, Type

from ..io.BaseReader import BaseReader, DeviceFactory
from ..parsers.parser import BaseParser, ParserFactory, ParserThread
from ..utils.log_level import LogLevel
from .bindable import bindable
from .central_storage import CentralFactory
from .configurable import configurable, configuration_property
from .constants import SysCat
from .device_identity import DeviceIdentity, ModuleIdentity
from .log_row import LogRow
from .logger import PrintLogger, SystemLogger
from .registry import Registry
from .reorder_buffer import ReorderFactory
from .system_context import SystemContext


@configurable
@bindable
class PipelineManager:
    def __init__(self):
        self.lock = Lock()

        # Maps device_name -> DevicePipeline
        self.pipelines: Dict[str, BaseParser] = {}

        self.needs_delayed_init = True  # Flag to indicate if delayed initialization is needed

    # ==========================================
    # PIPELINE CONSTRUCTION
    # ==========================================

    def apply_config(self, config: dict) -> bool:
        changed = self.apply_base_config(config)

        registry = self.shared.registry
        factories = self.shared.factories

        # --- HANDLE REMOVALS ---
        current_ids = set(self.pipelines.keys())
        new_ids = set(config.keys())

        for item_id in current_ids - new_ids:
            self.logger.info(f"Removing deleted pipeline: '{item_id}'")
            item = self.pipelines.pop(item_id)

            # Stop the thread and sever all pub/sub links
            item.stop()
            if hasattr(item, "clear_all_links"):
                item.clear_all_links()

            # Unsubscribe from config updates
            if hasattr(registry.config, "unsubscribe"):
                registry.config.unsubscribe(f"/pipelines/{item_id}", item)

        # --- HANDLE ADDITIONS AND UPDATES ---
        for item_id, item_config in config.items():
            try:
                item = self.pipelines.get(item_id)

                name = item_config.get("name", item_id)
                if item is None:
                    # Logic for creating a brand new pipeline
                    self.logger.info(f"Creating new pipeline: '{name}' ({item_id})")

                    device_id = self.shared.id_registry.get_device(name)
                    local_ctx = SimpleNamespace(
                        get_logger=registry.logger_creator("parser", device_id.name),
                        device_id=device_id,
                    )

                    item = factories.build("parser", item_config, self.shared, local_ctx)
                    self.pipelines[item_id] = item
                    item.reference_id = item_id
                    # Register for individual config updates
                    registry.config.subscribe(f"/pipelines/{item_id}", item)

                    if not self.needs_delayed_init:
                        self.apply_target(item_id, item)
                        item.start()

                    self.subscribe(name, item)

                    print(f"[PipelineManager] apply_config: {self.pipelines.keys()}")
                else:
                    # Update existing pipeline and check for changes

                    old_sources = self._get_link_set(item, "sources_")
                    old_targets = self._get_link_set(item, "targets_")
                    config_changed = item.apply_config(item_config)

                    if config_changed:
                        new_sources = self._get_link_set(item, "sources_")
                        new_targets = self._get_link_set(item, "targets_")

                        self.logger.info(f"Pipeline '{item_id}' config changed; rebuilding topology.")

                        # Sever old links so we don't double-subscribe or leak data
                        # Re-bind the source/target links based on the updated config
                        # 3. Reconcile Sources (Upstream)
                        # Remove sources no longer present
                        for s_id in old_sources - new_sources:
                            upstream = registry.get_reference_target(s_id)
                            if upstream:
                                upstream.unsubscribe(item)

                        # Add new sources
                        for s_id in new_sources - old_sources:
                            upstream = registry.get_reference_target(s_id)
                            if upstream:
                                upstream.subscribe(item)

                        # 4. Reconcile Targets (Downstream)
                        # Remove targets no longer present
                        for t_id in old_targets - new_targets:
                            downstream = registry.get_reference_target(t_id)
                            if downstream:
                                item.unsubscribe(downstream)

                        # Add new targets
                        for t_id in new_targets - old_targets:
                            downstream = registry.get_reference_target(t_id)
                            if downstream:
                                item.subscribe(downstream)

                        # Handle potential thread restart if specific fields changed
                        if getattr(item, "thread_needs_restart", False):
                            item.restart()

            except Exception as e:
                self.logger.exception(f"Failed to process pipeline '{item_id}'", e)

        # ---FINALIZATION ---
        if not self.needs_delayed_init:
            # Ensure any new or updated items that should be running are started
            self.start()

        self.needs_delayed_init = False
        return changed

    def _get_link_set(self, item, attr_name: str) -> set:
        """Helper to normalize sources_/targets_ into a set of strings."""
        val = getattr(item, attr_name, [])
        if isinstance(val, str):
            return {val}
        return set(val) if val else set()

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

    def subscribe(self, name_debug, ref, pipeline=None):
        if ref is None:
            return

        # Map the fixed infrastructure
        mapped_targets = {}
        mapped_sources = {}
        is_reorderer = self.shared.registry.reorder is not None and self.shared.registry.reorder.enabled
        if is_reorderer:
            mapped_targets[SysCat.REORDER] = self.shared.registry.reorder
            mapped_sources[SysCat.REORDER] = self.shared.registry.reorder

        mapped_targets[SysCat.STORAGE] = self.shared.registry.central
        mapped_sources[SysCat.STORAGE] = self.shared.registry.central

        if pipeline is not None:
            # Map the device-specific pipeline dynamically
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

        # Connect Targets (Where 'ref' sends data TO)
        for target_key in getattr(ref, "targets", []):
            target_obj = mapped_targets.get(target_key)
            if target_obj:
                # If the component provides its own subscribe method, use it
                if hasattr(ref, "subscribe"):
                    ref.subscribe(target_obj)
                    # Otherwise, if it's a raw queue-like object, we might need a different link
                    self.logger.debug(
                        f"[{name_debug}] Linked '{ref.__class__.__name__}' -> '{target_obj.__class__.__name__}' [{ref.__class__.__module__}.{ref.__class__.__name__} -> {target_obj.__class__.__module__}.{target_obj.__class__.__name__}]"
                    )
                    break

        # Connect Sources (Where 'ref' gets data FROM)
        print(f"sources: {getattr(ref, 'sources', [])}")
        for source_key in getattr(ref, "sources", []):
            print(f"{name_debug}: {source_key}")
            source_obj = mapped_sources.get(source_key)
            if source_obj and hasattr(source_obj, "subscribe"):
                source_obj.subscribe(ref)
                self.logger.debug(
                    f"[{name_debug}] Linked '{source_obj.__class__.__name__}' -> '{ref.__class__.__name__}' [{source_obj.__class__.__module__}.{source_obj.__class__.__name__} -> {ref.__class__.__module__}.{ref.__class__.__name__}]"
                )
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

        print(f"[PipelineManager] get_schema: {self.pipelines.keys()}")
        try:
            return self.pipelines[name].get_config_schema()
        except KeyError:
            print(f"[DynamicConfigWidget] No schema found for pipeline '{name}'")
            return self.shared.factories.get_base_schema("parser")

    def get(self, id_: str):
        print(f"PipelineManager: Retrieving pipeline with ID '{id_}' all: {list(self.pipelines.keys())}")
        return self.pipelines.get(id_)

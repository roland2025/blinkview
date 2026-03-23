# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from types import SimpleNamespace
from typing import TYPE_CHECKING

from blinkview.core.BaseBindableConfigurable import BaseBindableConfigurable
from blinkview.core.base_configurable import BaseConfigurable

if TYPE_CHECKING:
    from blinkview import Registry
    from blinkview.core.factory_registry import FactoryRegistry


class SourcesManager(BaseBindableConfigurable):

    def __init__(self):
        super().__init__()
        self.lock = Lock()

        self.sources = {}  # { source_id: source_instance }

        self.needs_delayed_init = True  # Flag to indicate if delayed initialization is needed

    def apply_config(self, config: dict) -> bool:
        super().apply_config(config)

        registry = self.shared.registry
        system_ctx = registry.system_ctx
        factories = self.shared.factories

        for item_id, source_config in config.items():
            try:
                item = self.sources.get(item_id)
                if item is None:
                    print(f"Creating new source: '{item_id}'")
                    local_ctx = SimpleNamespace(
                        get_logger=registry.logger_creator("source", source_config.get("name", item_id)),
                        push_log=registry.reorder.put if registry.reorder else registry.central.put,
                        logging_id=item_id,
                    )
                    item = factories.build("source", source_config, system_ctx, local_ctx)

                    self.sources[item_id] = item
                    registry.config.subscribe(f"/sources/{item_id}", item)

                    if not self.needs_delayed_init:
                        self.apply_target(item_id, item)  # Apply targets immediately if delayed init is not needed
                        self.start()

            except Exception as e:
                print(f"Failed to create source: '{item_id}'")
                self.logger.error(f"Failed to create source: '{item_id}'", exc=e)

        self.needs_delayed_init = False  # Mark that delayed initialization is no longer needed

        return True

    def apply_targets(self):
        for item_id, item in self.sources.items():
            self.apply_target(item_id, item)

    def apply_target(self, item_id, item):
        print(f"Applying targets for source '{item_id}'")
        if hasattr(item, "sources_"):
            # check if its a list or a single string
            self.logger.warn(f"Source '{item_id}' has sources: {item.sources_}")
            sources = [item.sources_] if isinstance(item.sources_, str) else item.sources_
            for source in sources:
                self.logger.debug(f"Source '{item_id}' has source: {source}")
                self.shared.registry.get_reference_target(source).subscribe(item)

        if hasattr(item, "targets_"):
            self.logger.warn(f"Applying targets for source '{item_id}': {item.targets_}")
            targets = [item.targets_] if isinstance(item.targets_, str) else item.targets_

            for target in targets:
                target_ref = self.shared.registry.get_reference_target(target)
                self.logger.debug(f"Source '{item_id}' has target: {target}")
                item.subscribe(target_ref)

    def get_schema(self, name: str):
        try:
            return self.sources[name].get_config_schema()
        except KeyError:
            return self.shared.factories.get_base_schema("source")

    def start(self):
        with self.lock:
            for pipeline in self.sources.values():
                pipeline.start()

    def stop(self):
        with self.lock:
            for pipeline in self.sources.values():
                pipeline.stop()

    def get(self, source_id: str):
        return self.sources.get(source_id)

    def send_command(self, source_id: str, command: str):
        # find source
        source = self.sources.get(source_id)
        if source is None:
            return
        # if source has send_data
        if hasattr(source, "send_data"):
            source.send_data(command.encode())

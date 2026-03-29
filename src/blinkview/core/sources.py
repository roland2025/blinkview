# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from threading import Lock
from types import SimpleNamespace
from typing import TYPE_CHECKING

from blinkview.core.bindable import bindable
from blinkview.core.configurable import configurable


@bindable
@configurable
class SourcesManager:
    def __init__(self):
        self.lock = Lock()

        self.sources = {}  # { source_id: source_instance }

        self.needs_delayed_init = True  # Flag to indicate if delayed initialization is needed

        print(f"[SourcesManager] __init__ done")

    def apply_config(self, config: dict) -> bool:
        print(f"[SourcesManager] Applying config: {config}")
        changed = self.apply_base_config(config)

        registry = self.shared.registry
        system_ctx = registry.system_ctx
        factories = self.shared.factories

        # --- HANDLE REMOVALS ---
        current_ids = set(self.sources.keys())
        new_ids = set(config.keys())

        for item_id in current_ids - new_ids:
            print(f"Removing deleted source: '{item_id}'")
            item = self.sources.pop(item_id)
            item.stop()

            # Clean up pub/sub connections
            if hasattr(item, "clear_all_links"):
                item.clear_all_links()

            # Clean up config subscription
            if hasattr(registry.config, "unsubscribe"):
                registry.config.unsubscribe(f"/sources/{item_id}", item)

        # --- Handle Additions and Updates ---
        for item_id, source_config in config.items():
            try:
                item = self.sources.get(item_id)

                if item is None:
                    # Logic for creating a brand new source
                    local_ctx = SimpleNamespace(
                        get_logger=registry.logger_creator("source", source_config.get("name", item_id)),
                        push_log=registry.reorder.put if registry.reorder else registry.central.put,
                        logging_id=item_id,
                    )
                    item = factories.build("source", source_config, system_ctx, local_ctx)
                    item.reference_id = item_id
                    self.sources[item_id] = item
                    registry.config.subscribe(f"/sources/{item_id}", item)

                    if not self.needs_delayed_init:
                        self.apply_target(item_id, item)
                else:
                    # Update existing source and check if configuration actually changed
                    old_sources = self._get_link_set(item, "sources_")
                    old_targets = self._get_link_set(item, "targets_")

                    config_changed = item.apply_config(source_config)

                    if config_changed:
                        new_sources = self._get_link_set(item, "sources_")
                        new_targets = self._get_link_set(item, "targets_")

                        if self.logger:
                            self.logger.info(f"Source '{item_id}' config changed; rebuilding topology.")

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

                        # Check if the daemon requires a thread restart (from BaseDaemon logic)
                        if getattr(item, "thread_needs_restart", False):
                            item.restart()

            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to process source '{item_id}'", exc=e)
        # Finalize initialization
        if not self.needs_delayed_init:
            self.start()

        self.needs_delayed_init = False  # Mark that delayed initialization is no longer needed

        return changed

    def _get_link_set(self, item, attr_name: str) -> set:
        """Helper to normalize sources_/targets_ into a set of strings."""
        val = getattr(item, attr_name, [])
        if isinstance(val, str):
            return {val}
        return set(val) if val else set()

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

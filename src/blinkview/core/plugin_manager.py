# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import importlib

from blinkview.core.base_configurable import BaseConfigurable, configuration_property


@configuration_property(
    "enabled",
    type="boolean",
    default=False,
    required=True,
    description="Global plugin system toggle",
)
@configuration_property(
    "modules",
    type="object",
    default={},
    required=True,
    # --- NEW: Define the blueprint for dynamic keys ---
    additionalProperties={
        "type": "object",
        "default": {"enabled": True},  # What it defaults to when clicked
        "required": ["enabled"],
        "properties": {"enabled": {"type": "boolean", "title": "Enable Module"}},
    },
)
class PluginManager(BaseConfigurable):
    __doc__ = "Manages the lifecycle of dynamic plugin modules. Matches the 'plugins' key in the master configuration."

    enabled: bool
    modules: dict

    def __init__(self, registry, logger):
        super().__init__()
        self.registry = registry
        self.active_plugins = {}  # { module_path: plugin_instance }
        self.logger = logger

        # Subscribe to ONLY the plugins section
        self.registry.config.subscribe("/plugins", self)

    def apply_config(self, config: dict) -> bool:

        changed = super().apply_config(config)

        # Start modules that are newly enabled
        for path, mod_cfg in self.modules.items():
            if mod_cfg.get("enabled") and path not in self.active_plugins:
                self._start_plugin(path)

        return changed

    def _start_plugin(self, module_path: str):
        """Dynamically imports and instantiates a plugin."""

        try:
            # This is the magic line that executes the external file
            self.logger.info(f"Loading plugin module: '{module_path}'...")
            importlib.import_module(module_path)

            self.logger.info(f"Successfully loaded plugin: '{module_path}'")

        except ImportError as e:
            self.logger.error(f"Plugin Load Error.", e)

        except Exception as e:
            self.logger.error(f"Unexpected error while loading plugin '{module_path}'.", e)

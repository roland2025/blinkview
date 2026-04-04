# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from dataclasses import dataclass
from typing import TYPE_CHECKING, Callable

from qtpy.QtCore import QObject

from blinkview.ui.log_filter_index_manager import LogFilterIndexManager
from blinkview.ui.native_dark_mode import set_native_dark_mode

if TYPE_CHECKING:
    from blinkview.core.logger import SystemLogger
    from blinkview.core.settings_manager import SettingsManager
    from blinkview.ui.utils.config_node_manager import ConfigNodeManager
    from blinkview.ui.widgets.config.style_config import StyleConfig


class GUIContext(QObject):
    """
    The 'Single Source of Truth' for the UI.
    Contains models, registries, and the master update heartbeat.
    """

    def __init__(self, parent=None):
        super().__init__(parent)
        self.registry = None
        self.id_registry = None
        self.settings: "SettingsManager" = None

        self.telemetry_model = None
        self.module_filter_model = None
        self.theme: "StyleConfig" = None

        self.index_manager = LogFilterIndexManager(gui_context=self, parent=self)

        # Factory function for creating widgets with context (cls_name, name, as_window=False, **kwargs)
        self.create_widget = None

        self.config_manager: "ConfigNodeManager" = None

        self.gui_config_manager: "ConfigNodeManager" = None

        self.register_log_target = None
        self.deregister_log_target = None

        self.is_shutting_down = False

        self.reattach_tab = None  # Placeholder for the function to reattach a detached tab back to the main window

        self.gui_state = None

        self.gui_config = None

        self.updatable = []

        self.set_update_version = None

        self.logger: "SystemLogger" = None

        # Central Heartbeat: Drives all 30fps UI animations/updates
        # self.update_timer = QTimer(self)
        # self.update_timer.timeout.connect(self._on_heartbeat)
        #
        # # Match the theme's desired refresh rate (e.g., 33ms for ~30fps)
        # self.update_timer.start(33)

    def set_registry(self, registry):
        self.registry = registry
        self.id_registry = registry.id_registry
        self.settings = registry.system_ctx.settings

    def set_telemetry_model(self, telemetry_model):
        self.telemetry_model = telemetry_model

    def set_theme(self, theme: "StyleConfig"):
        self.theme = theme

    def set_widget_factory(self, factory_func):
        self.create_widget = factory_func

    def set_config_manager(self, config_manager: "ConfigNodeManager"):
        self.config_manager = config_manager

    def set_gui_config_manager(self, gui_config_manager: "ConfigNodeManager"):
        self.gui_config_manager = gui_config_manager

    def set_register_log_target(self, log_target_fn):
        self.register_log_target = log_target_fn

    def set_deregister_log_target(self, log_target_fn):
        self.deregister_log_target = log_target_fn

    def set_reattach_tab(self, reattach_fn):
        self.reattach_tab = reattach_fn

    def set_module_filter_model(self, module_filter_model):
        self.module_filter_model = module_filter_model

    def on_heartbeat(self):
        """Dispatches the update signal to slow sync components like the TelemetryModel."""
        self.telemetry_model.sync_registry()
        self.module_filter_model.sync_registry()

    def on_update(self):
        """Dispatches the update signal to all registered views for a fast sync."""
        self.telemetry_model.apply_updates()
        if self.updatable:
            for updatable in self.updatable:
                updatable.apply_updates()

    def add_updatable(self, updatable):
        """Registers a view/component to receive update signals."""
        self.updatable.append(updatable)

    def remove_updatable(self, updatable):
        """Unregisters a view/component from receiving update signals."""
        if updatable in self.updatable:
            self.updatable.remove(updatable)

    #
    # def create_log_filter(self, allowed_device=None, excluded_device=None, module=None):
    #     """Factory method to create a pre-configured LogFilter."""
    #     from blinkview.utils.log_filter import LogFilter
    #     return LogFilter(self.id_registry, allowed_device, excluded_device, filtered_module=module)

    def set_gui_state_handler(self, ui_state):
        self.gui_state = ui_state

    def set_gui_config_handler(self, gui_config):
        self.gui_config = gui_config

    def set_logger(self, logger):
        self.logger = logger

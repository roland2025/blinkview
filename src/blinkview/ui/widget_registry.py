# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Dict, List, Tuple

# (name, widget class) pairs, populated by every @register_widget_factory call seen so far.
_WIDGET_REGISTRATIONS: List[Tuple[str, type]] = []


def register_widget_factory(name: str):
    """Decorator: register a widget class under `name` in the global registration list. Apply
    directly on the widget class definition (e.g. @register_widget_factory(WidgetName.LOG_VIEWER)
    on LogViewerWidget). Actual assembly into BlinkMainWindow.widget_factories happens later via
    build_widget_factory_map() - same "module must be imported before use" caveat as
    register_warmup/register_factory_category."""

    def decorator(widget_cls):
        _WIDGET_REGISTRATIONS.append((name, widget_cls))
        return widget_cls

    return decorator


def import_registerable_widgets():
    """Imports every module that defines a registered widget - the one place that lists them.
    Each import fires the module's @register_widget_factory(...) and, for the ones with Numba
    kernels, its @register_warmup callback (module_filter_table's comes in through the two log
    viewers), so BlinkMainWindow and `blink setup` (ui/setup.py) get the same widgets and compile
    the same kernels. A plain function, not top-level imports, for the same reason as
    core/registry.py's _import_registerable_modules(): nothing here is referenced by name, so an
    "optimize imports" pass would otherwise be free to drop it."""
    from blinkview.ui.widgets import TelemetryWatch as TelemetryWatch
    from blinkview.ui.widgets import log_table_viewer as log_table_viewer
    from blinkview.ui.widgets import log_viewer as log_viewer
    from blinkview.ui.widgets import plotter as plotter
    from blinkview.ui.widgets import telemetry_table as telemetry_table
    from blinkview.ui.widgets import update_widget as update_widget
    from blinkview.ui.widgets.config import dynamic_config as dynamic_config


def build_widget_factory_map() -> Dict[str, type]:
    """Builds a fresh {name: widget class} dict from every @register_widget_factory call seen
    so far, for BlinkMainWindow.widget_factories."""
    widget_factories: Dict[str, type] = {}
    for name, widget_cls in _WIDGET_REGISTRATIONS:
        if name in widget_factories:
            raise ValueError(f"Widget name '{name}' is already registered.")
        widget_factories[name] = widget_cls
    return widget_factories

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


def build_widget_factory_map() -> Dict[str, type]:
    """Builds a fresh {name: widget class} dict from every @register_widget_factory call seen
    so far, for BlinkMainWindow.widget_factories."""
    widget_factories: Dict[str, type] = {}
    for name, widget_cls in _WIDGET_REGISTRATIONS:
        if name in widget_factories:
            raise ValueError(f"Widget name '{name}' is already registered.")
        widget_factories[name] = widget_cls
    return widget_factories

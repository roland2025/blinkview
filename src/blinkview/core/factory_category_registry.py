# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import TYPE_CHECKING, List, Tuple

if TYPE_CHECKING:
    from blinkview.core.factory_registry import FactoryRegistry

# (category, Factory class) pairs, populated by every @register_factory_category call seen so far.
_FACTORY_REGISTRATIONS: List[Tuple[str, type]] = []


def register_factory_category(category: str):
    """Decorator: register a Factory class under `category` in the global registration list.
    Apply directly on the Factory class definition (e.g. @register_factory_category(
    FactoryCategory.REORDER) on ReorderFactory). Actual registration into a live FactoryRegistry
    happens later via build_system_factory_registry() - same "module must be imported before use"
    caveat as register_warmup."""

    def decorator(factory_cls):
        _FACTORY_REGISTRATIONS.append((category, factory_cls))
        return factory_cls

    return decorator


def build_system_factory_registry() -> "FactoryRegistry":
    """Builds a fresh FactoryRegistry populated from every @register_factory_category call seen
    so far. Reuses FactoryRegistry.register()'s existing normalization/duplicate-guard as-is."""
    from blinkview.core.factory_registry import FactoryRegistry

    registry = FactoryRegistry()
    for category, factory_cls in _FACTORY_REGISTRATIONS:
        registry.register(category, factory_cls)
    return registry

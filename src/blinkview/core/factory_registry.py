# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from .factory import BaseFactory


class FactoryRegistry:
    """Central hub for building all system components from configuration."""

    def __init__(self):
        # Maps a string category (e.g., "subscriber") to its corresponding Factory class
        self._factories: dict[str, 'BaseFactory'] = {}

    def register(self, category: str, factory_class: type['BaseFactory']):
        """Registers a factory class under a specific category."""
        category = category.lower()  # Normalize category names to lowercase
        if category in self._factories:
            raise ValueError(f"Factory category '{category}' is already registered.")
        self._factories[category] = factory_class

    def build(self, category: str, config: dict = None, system_ctx: Any = None, local_ctx: Any = None, **kwargs) -> Any:
        category = category.lower()  # Normalize category names to lowercase

        """Routes the build request to the correct underlying factory."""
        factory = self._factories.get(category)
        if not factory:
            raise KeyError(
                f"Cannot build '{category}'. Known categories are: {list(self._factories.keys())}"
            )

        # Delegates the exact signature you provided to the specific factory
        return factory.build(config, system_ctx, local_ctx, **kwargs)

    def get_factory(self, category: str) -> 'BaseFactory':
        """Returns the factory class itself if you need direct access."""
        return self._factories.get(category.lower())

    def get_category_types(self, category: str) -> list[str]:
        """Returns a list of available types for a given category."""
        category = category.lower()  # Normalize category names to lowercase
        factory = self.get_factory(category)
        if not factory:
            raise KeyError(
                f"Cannot get types for '{category}'. Known categories are: {list(self._factories.keys())}"
            )
        return factory.get_available_types()

    def get_produced_type(self, category: str) -> type:
        """Returns the type of component produced by the factory in this category."""
        factory = self.get_factory(category)
        if not factory:
            raise KeyError(
                f"Cannot get produced type for '{category}'. Known categories are: {list(self._factories.keys())}"
            )
        return factory.produces_type

    def get_schema(self, category: str, type_name: str) -> dict:
        """Returns the configuration schema for a specific type within a category."""
        factory = self.get_factory(category)
        if not factory:
            raise KeyError(
                f"Cannot get schema for '{category}'. Known categories are: {list(self._factories.keys())}"
            )
        return factory.get_schema(type_name)

    def get_base_schema(self, category: str) -> dict:
        """Returns the base configuration schema for a given category."""
        target_type = self.get_produced_type(category)
        if not target_type:
            raise KeyError(
                f"Cannot get base schema for '{category}'. Known categories are: {list(self._factories.keys())}"
            )
        return target_type.get_config_schema()

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import importlib
import inspect
from types import SimpleNamespace
from typing import Callable, Dict, Generic, TypeVar, get_args, get_origin

from blinkview.core.system_context import SystemContext

# Define a generic type variable so the IDE knows what this factory produces
T = TypeVar("T")


class BaseFactory(Generic[T]):
    """
    A generic base class for all component factories.
    Automatically provisions an isolated registry for every subclass.
    """

    # Define a class attribute to hold the resolved type (T)
    produces_type: type = None
    # We don't define _registry here, because subclasses would share it.

    @classmethod
    def __init_subclass__(cls, **kwargs):
        """This runs automatically whenever a class inherits from BaseFactory."""
        super().__init_subclass__(**kwargs)
        # Give every new subclass its own isolated dictionary
        cls._registry: Dict[str, Callable[[], T]] = {}

        # Extract the generic type argument from __orig_bases__
        if hasattr(cls, "__orig_bases__"):
            for base in cls.__orig_bases__:
                # Check if this base is BaseFactory[...]
                if get_origin(base) is BaseFactory:
                    args = get_args(base)
                    if args:
                        # args[0] is the actual class passed into [T]
                        cls.produces_type = args[0]
                        break

    @classmethod
    def register(cls, name: str):
        """Decorator to register a new component builder."""
        name = name.lower()  # Normalize to lowercase for consistent lookups

        def wrapper(builder_func: Callable[[], T]):
            if name in cls._registry:
                # Fail fast if someone accidentally re-uses a name
                raise KeyError(f"Component '{name}' is already registered in {cls.__name__}.")

            cls._registry[name] = builder_func
            return builder_func

        return wrapper

    @staticmethod
    def load_plugin(module_name: str):
        """
        Dynamically imports a module.
        Made static because plugins might register components across multiple factories.
        """
        try:
            importlib.import_module(module_name)
        except ImportError as e:
            raise ImportError(f"Failed to load plugin '{module_name}': {e}")

    @classmethod
    def build(
        cls, config: dict = None, system_ctx: SystemContext = None, instance_ctx: SimpleNamespace = None, **kwargs
    ) -> T:
        """
        Builds a component directly from its configuration dictionary.
        Expects a 'type' key to exist within the config.
        """
        # Extract the name from config
        name = config.get("type")

        if not name:
            raise ValueError(f"Config for {cls.__name__} is missing a 'type'Received: {config}")
        name = name.lower()  # Normalize to lowercase for consistent lookups

        builder = cls._registry.get(name)
        if not builder:
            raise ValueError(f"Unknown component '{name}'. Available: {list(cls._registry.keys())}")

        # SMART INJECTION: Introspect what the plugin actually wants
        sig = inspect.signature(builder)

        # Check if the plugin has a catch-all **kwargs
        has_catch_all = any(p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())

        if has_catch_all:
            # If they have **kwargs, they can take everything
            safe_kwargs = kwargs
        else:
            # Otherwise, strictly filter the dictionary to match their __init__
            safe_kwargs = {k: v for k, v in kwargs.items() if k in sig.parameters}

        # INSTANTIATION: Perfectly safe, no TypeErrors
        instance = builder(**safe_kwargs)

        if hasattr(instance, "bind_system"):
            instance.bind_system(system_ctx, instance_ctx)

        # APPLY RULES
        if config is not None and hasattr(instance, "apply_config"):
            if hasattr(instance, "hydrate_config"):
                config = instance.hydrate_config(config)
            instance.apply_config(config)

        # FINALIZE / BAKE
        if hasattr(instance, "bake"):
            instance.bake()

        return instance

    @classmethod
    def get_available_types(cls) -> list:
        """Returns a list of all registered component types."""
        # create a key, description pair for each registered component
        available = []
        for name in cls._registry.keys():
            builder = cls._registry[name]
            doc = builder.__doc__.strip() if builder.__doc__ else "No description"
            # print(f"get_available_types - {name}: {doc}")
            available.append((name, doc))

        return available

    @classmethod
    def get_schema(cls, type_name: str) -> dict:
        """Returns the configuration schema for a specific component type."""
        builder = cls._registry.get(type_name.lower())
        if not builder:
            raise ValueError(f"Unknown component '{type_name}'.")

        if hasattr(builder, "get_config_schema"):
            return builder.get_config_schema()
        else:
            return {}  # Return an empty schema if not defined

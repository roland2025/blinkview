# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from copy import deepcopy


def configurable(cls):
    """
    Class decorator that injects dynamic configuration abilities into a class,
    removing the need for a BaseConfigurable inheritance hierarchy.
    """
    # Ensure the class has its properties dict
    if "CONFIG_PROPERTIES" not in cls.__dict__:
        cls.CONFIG_PROPERTIES = {}

    # Intercept and wrap the original __init__ to run our config setup
    original_init = cls.__init__

    def new_init(self, *args, **kwargs):
        self._config_callbacks = {}

        # Automatically apply defaults to the instance
        schema = self.__class__.get_config_schema()
        for key, prop in schema.get("properties", {}).items():
            if "default" in prop:
                setattr(self, key, prop["default"])

        # Register @on_config_change callbacks
        for attr_name in dir(self):
            try:
                attr = getattr(self, attr_name)
                if callable(attr) and hasattr(attr, "_config_triggers"):
                    for prop in attr._config_triggers:
                        self._config_callbacks.setdefault(prop, []).append(attr)
            except Exception:
                pass  # Ignore properties/attributes that throw errors on access

        # Execute the original __init__ of the decorated class
        original_init(self, *args, **kwargs)

    # Replace the class's init with our wrapped one
    cls.__init__ = new_init

    # Define the methods we want to inject
    @classmethod
    def get_config_schema(cls_) -> dict:
        properties = {}
        required_keys = []

        for base in reversed(cls_.__mro__):
            local_props = base.__dict__.get("CONFIG_PROPERTIES", {})
            if local_props:
                properties.update(deepcopy(local_props))

        for key, prop_schema in properties.items():
            req_val = prop_schema.get("required", False)
            if isinstance(req_val, bool):
                if req_val and key not in required_keys:
                    required_keys.append(key)

        final_properties = deepcopy(properties)
        for key in final_properties:
            if isinstance(final_properties[key].get("required"), bool):
                del final_properties[key]["required"]

        schema = {
            "type": "object",
            "title": f"{cls_.__name__} Configuration",
            "description": cls_.__doc__ or "",
            "properties": final_properties,
        }

        if required_keys:
            schema["required"] = required_keys

        if hasattr(cls_, "CONFIG_FACTORY_CATEGORY"):
            schema["_factory"] = getattr(cls_, "CONFIG_FACTORY_CATEGORY")

        return schema

    @classmethod
    def get_config_keys(cls_) -> tuple:
        return tuple(cls_.get_config_schema()["properties"].keys())

    @classmethod
    def hydrate_config(cls_, current_config: dict) -> dict:
        schema = cls_.get_config_schema()
        return cls_._hydrate_recursive(schema, deepcopy(current_config))

    @classmethod
    def _hydrate_recursive(cls_, schema: dict, data: any) -> any:
        prop_type = schema.get("type")

        if prop_type == "object":
            is_required = schema.get("required") is True
            has_default = "default" in schema
            target_type = schema.get("_factory_default")

            if data is None and not is_required and not has_default and not target_type:
                return None

            if not isinstance(data, dict):
                data = deepcopy(schema.get("default", {}))

            if target_type and "type" not in data:
                data["type"] = target_type

            properties = schema.get("properties", {})
            for key, prop_schema in properties.items():
                child_data = data.get(key)
                if child_data is None and not prop_schema.get("required") is True and "default" not in prop_schema:
                    continue
                data[key] = cls_._hydrate_recursive(prop_schema, child_data)

            return data

        elif prop_type == "array":
            if not isinstance(data, list):
                return deepcopy(schema.get("default", [])) if "default" in schema else None

            item_schema = schema.get("items")
            if item_schema and item_schema.get("type") == "object":
                return [cls_._hydrate_recursive(item_schema, item) for item in data]
            return data

        else:
            if data is None and "default" in schema:
                return deepcopy(schema.get("default"))
            return data

    def apply_base_config(self, config: dict) -> bool:
        valid_keys = self.get_config_keys()
        changed = False

        for key, value in config.items():
            if key in valid_keys:
                old_value = getattr(self, key, object())

                if old_value != value:
                    setattr(self, key, value)
                    changed = True

                    if hasattr(self, "_config_callbacks") and key in self._config_callbacks:
                        for callback in self._config_callbacks[key]:
                            callback(value, old_value)

        return changed

    # 4. Bind the injected methods to the target class
    cls.get_config_schema = get_config_schema
    cls.get_config_keys = get_config_keys
    cls.hydrate_config = hydrate_config
    cls._hydrate_recursive = _hydrate_recursive
    cls.apply_base_config = apply_base_config

    if "apply_config" not in cls.__dict__:
        cls.apply_config = apply_base_config

    return cls


def configuration_property(name: str, **schema_kwargs):
    """
    Adds a single configuration property to the class schema.
    Stacked decorators execute bottom-up, so we carefully prepend
    properties to maintain top-to-bottom visual order.
    """

    def wrapper(cls):
        # Check if this specific class already has its OWN properties dict.
        # We use __dict__ so we don't accidentally modify a parent class's dictionary!
        if "CONFIG_PROPERTIES" not in cls.__dict__:
            cls.CONFIG_PROPERTIES = {}

        # To counteract bottom-up execution, we put the NEW property first,
        # then append whatever properties were added by decorators below this one.
        ordered_props = {name: schema_kwargs}
        ordered_props.update(cls.CONFIG_PROPERTIES)

        # Save it back to the class
        cls.CONFIG_PROPERTIES = ordered_props

        if not hasattr(cls, "get_config_schema"):
            cls = configurable(cls)

        return cls

    return wrapper


def configuration_factory(category_name: str):
    """
    Marks this class as a factory base. The UI will use this to generate
    a dynamic 'type' dropdown, but it will NOT be saved in the JSON payload.
    """

    def wrapper(cls):
        # Save it as a private class attribute instead of a property
        cls.CONFIG_FACTORY_CATEGORY = category_name
        return cls

    return wrapper


def on_config_change(*properties):
    """
    Decorator to mark a method as a callback for when specific configuration properties change.
    Example: @on_config_change("baudrate", "port")
    """

    def wrapper(func):
        func._config_triggers = properties
        return func

    return wrapper


def override_property(name: str, **schema_overrides):
    def wrapper(cls):
        # Ensure we don't double-wrap or miss properties
        if "CONFIG_PROPERTIES" not in cls.__dict__:
            cls.CONFIG_PROPERTIES = {}

        if not hasattr(cls, "get_config_schema"):
            cls = configurable(cls)

        existing_prop = {}
        # Search parentage for the original property
        for base in cls.__mro__:
            if base is cls:
                continue
            # Find the property in the closest parent's local dict
            parent_props = base.__dict__.get("CONFIG_PROPERTIES", {})
            if name in parent_props:
                existing_prop = deepcopy(parent_props[name])
                break

        if not existing_prop:
            raise ValueError(f"Property '{name}' not found in parent hierarchy.")

        existing_prop.update(schema_overrides)

        # Prepend to maintain order
        new_props = {name: existing_prop}
        new_props.update(cls.CONFIG_PROPERTIES)
        cls.CONFIG_PROPERTIES = new_props

        return cls

    return wrapper

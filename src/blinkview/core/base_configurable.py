# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from copy import deepcopy


def configuration_property(name: str, **schema_kwargs):
    """
    Adds a single configuration property to the class schema.
    Stacked decorators execute bottom-up, so we carefully prepend
    properties to maintain top-to-bottom visual order.
    """

    def wrapper(cls):
        # 1. Check if this specific class already has its OWN properties dict.
        # We use __dict__ so we don't accidentally modify a parent class's dictionary!
        if 'CONFIG_PROPERTIES' not in cls.__dict__:
            cls.CONFIG_PROPERTIES = {}

        # 2. To counteract bottom-up execution, we put the NEW property first,
        # then append whatever properties were added by decorators below this one.
        ordered_props = {name: schema_kwargs}
        ordered_props.update(cls.CONFIG_PROPERTIES)

        # 3. Save it back to the class
        cls.CONFIG_PROPERTIES = ordered_props

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
    """
    Overrides specific keys of a configuration property defined in a parent class.
    Example: @override_property("printable", hidden=True, default="new_default")
    """

    def wrapper(cls):
        if 'CONFIG_PROPERTIES' not in cls.__dict__:
            cls.CONFIG_PROPERTIES = {}

        # 1. Search the class hierarchy (MRO) to find the original property
        existing_prop = {}
        from copy import deepcopy

        for base in cls.__mro__:
            if hasattr(base, 'CONFIG_PROPERTIES') and name in base.CONFIG_PROPERTIES:
                # We found it! Deepcopy it so we don't accidentally mutate the parent class.
                existing_prop = deepcopy(base.CONFIG_PROPERTIES[name])
                break

        if not existing_prop:
            raise ValueError(
                f"Cannot override property '{name}'. It was not found in "
                f"'{cls.__name__}' or any of its parent classes."
            )

        # 2. Apply the overrides on top of the parent's schema
        existing_prop.update(schema_overrides)

        # 3. Store it in the child's property dictionary (respecting top-down order)
        ordered_props = {name: existing_prop}
        ordered_props.update(cls.CONFIG_PROPERTIES)
        cls.CONFIG_PROPERTIES = ordered_props

        return cls

    return wrapper


class BaseConfigurable:
    # Child classes will define their own properties here
    CONFIG_PROPERTIES = {}

    def __init__(self):
        self._config_callbacks = {}

        # 1. Automatically apply defaults to the instance!
        schema = self.get_config_schema()
        for key, prop in schema.get("properties", {}).items():
            if "default" in prop:
                setattr(self, key, prop["default"])

        for attr_name in dir(self):
            try:
                attr = getattr(self, attr_name)
                if callable(attr) and hasattr(attr, '_config_triggers'):
                    for prop in attr._config_triggers:
                        self._config_callbacks.setdefault(prop, []).append(attr)
            except Exception:
                pass  # Ignore properties/attributes that throw errors on access

    @classmethod
    def get_config_schema(cls) -> dict:
        properties = {}
        required_keys = []

        # 1. Iterate through parents in reverse (MRO) to build the full property dict
        for base in reversed(cls.__mro__):
            if hasattr(base, 'CONFIG_PROPERTIES'):
                # Use deepcopy to avoid mutating parent class dictionaries
                properties.update(deepcopy(base.CONFIG_PROPERTIES))

        # 2. Build the 'required' list based on the merged properties
        # IMPORTANT: Do NOT use .pop() here, as it removes data needed for hydration
        for key, prop_schema in properties.items():
            req_val = prop_schema.get("required", False)

            if isinstance(req_val, bool):
                if req_val:
                    if key not in required_keys:
                        required_keys.append(key)
                # Keep the schema clean for JSON validation,
                # but don't delete the whole property!
                # We can handle the bool flag by making a shallow copy for the final schema

        # 3. Clean up boolean 'required' flags for standard JSON Schema compliance
        final_properties = deepcopy(properties)
        for key in final_properties:
            if isinstance(final_properties[key].get("required"), bool):
                del final_properties[key]["required"]

        # 4. Build the final schema
        schema = {
            "type": "object",
            "title": f"{cls.__name__} Configuration",
            "description": cls.__doc__ or "",
            "properties": final_properties
        }

        if required_keys:
            schema["required"] = required_keys

        if hasattr(cls, 'CONFIG_FACTORY_CATEGORY'):
            schema["_factory"] = getattr(cls, 'CONFIG_FACTORY_CATEGORY')

        return schema

    @classmethod
    def get_config_keys(cls) -> tuple:
        """Dynamically returns the tuple of valid keys based on the schema."""
        return tuple(cls.get_config_schema()["properties"].keys())

    def apply_config(self, config: dict) -> bool:
        # Use our dynamically generated keys
        valid_keys = self.get_config_keys()
        changed = False

        for key, value in config.items():
            if key in valid_keys:
                old_value = getattr(self, key, object())

                if old_value != value:
                    setattr(self, key, value)
                    changed = True

                    # --- NEW: Trigger registered callbacks ---
                    if hasattr(self, '_config_callbacks') and key in self._config_callbacks:
                        for callback in self._config_callbacks[key]:
                            # Pass old and new values to the registered method
                            callback(value, old_value)

        return changed

    @classmethod
    def hydrate_config(cls, current_config: dict) -> dict:
        """
        Entry point for hydration.
        Uses the class schema to recursively fill in missing defaults.
        """
        schema = cls.get_config_schema()
        # Pass the whole config to the recursive engine
        return cls._hydrate_recursive(schema, deepcopy(current_config))

    @classmethod
    def _hydrate_recursive(cls, schema: dict, data: any) -> any:
        """
        Recursive engine that handles Objects, Arrays, and Primitive defaults.
        """
        prop_type = schema.get("type")

        # --- CASE 1: OBJECT HYDRATION ---
        if prop_type == "object":
            # Determine if we SHOULD create this object
            is_required = schema.get("required") is True
            has_default = "default" in schema

            factory_default = schema.get("_factory_default")

            target_type = factory_default

            # If it doesn't exist, and isn't required/defaulted, don't create it
            if data is None and not is_required and not has_default and not target_type:
                return None

            if not isinstance(data, dict):
                data = deepcopy(schema.get("default", {}))

            if target_type and "type" not in data:
                data["type"] = target_type

            properties = schema.get("properties", {})
            for key, prop_schema in properties.items():
                # We only hydrate children if the parent 'data' dict actually exists
                # or if the child itself is required.
                child_data = data.get(key)

                # Check if we should skip optional child objects to avoid empty {}
                if child_data is None and not prop_schema.get("required") is True and "default" not in prop_schema:
                    continue

                # Recurse and assign
                data[key] = cls._hydrate_recursive(prop_schema, child_data)

            return data

        # --- CASE 2: ARRAY HYDRATION ---
        elif prop_type == "array":
            if not isinstance(data, list):
                return deepcopy(schema.get("default", [])) if "default" in schema else None

            item_schema = schema.get("items")
            if item_schema and item_schema.get("type") == "object":
                return [cls._hydrate_recursive(item_schema, item) for item in data]
            return data

        # --- CASE 3: PRIMITIVES ---
        else:
            if data is None and "default" in schema:
                return deepcopy(schema.get("default"))
            return data

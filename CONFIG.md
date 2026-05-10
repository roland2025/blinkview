# Decorator and Dynamic JSON Configuration System in Blinkview

The configuration system in Blinkview uses a dynamic JSON configuration approach powered by Python class decorators. This system automatically generates JSON Schemas from Python classes, provides dynamic UI widgets for configuration editing (`DynamicConfigWidget`), and handles recursive hydration of configuration data, avoiding the need for a complex inheritance hierarchy.

## Core Concepts

The configuration system relies on several key decorators defined in `blinkview.core.configurable`:

1.  `@configurable`: The main class decorator. It injects configuration management methods and lifecycle hooks into the target class.
2.  `@configuration_property`: Adds a property to the class's configuration schema.
3.  `@configuration_factory`: Marks a class as a base for a "factory" category, allowing polymorphic configuration (e.g., choosing different types of plugins/modules dynamically).
4.  `@on_config_change`: Marks a method to be invoked automatically when specific configuration properties are updated.

## How `@configurable` Works

When a class is decorated with `@configurable`, the decorator modifies the class's `__init__` method to:
*   Automatically apply default values defined in the schema properties to the instance.
*   Scan for and register callback methods decorated with `@on_config_change`.

It also injects several class and instance methods:
*   `get_config_schema()`: Generates a standard JSON Schema object representing the class's configuration properties.
*   `hydrate_config(current_config)`: Takes a partial or complete configuration dictionary, applies defaults, and recursively builds the full configuration object based on the schema.
*   `apply_base_config(config)`: Updates the instance's attributes based on the provided configuration dictionary and triggers any relevant `@on_config_change` callbacks for values that actually changed.

## Using `@configuration_property`

This decorator is used to define the schema for individual properties using JSON Schema conventions. Stacked decorators execute bottom-up in Python, but `configuration_property` is designed to safely prepend properties to maintain top-to-bottom visual ordering in the generated schema and UI.

### Example: Basic Properties

```python
from blinkview.core.configurable import configurable, configuration_property

@configurable
@configuration_property("host", type="string", default="127.0.0.1", title="Server Host", description="The IP address to bind to.")
@configuration_property("port", type="integer", default=8080, title="Server Port", required=True)
class MyServerConfig:
    def __init__(self):
        # Attributes are automatically initialized with defaults!
        # self.host == "127.0.0.1"
        # self.port == 8080
        pass
```

*Note: If `required=True` is NOT specified, the `DynamicConfigWidget` UI will automatically render the field with an "Override" checkbox. The value is only included in the JSON payload if the user checks the box.*

## Reacting to Changes with `@on_config_change`

You can define methods that automatically execute when specific configuration properties are modified via `apply_base_config()` (or `apply_config()`).

```python
from blinkview.core.configurable import configurable, configuration_property, on_config_change

@configurable
@configuration_property("baudrate", type="integer", default=115200)
@configuration_property("port", type="string", required=True)
class SerialDevice:
    
    @on_config_change("baudrate", "port")
    def on_connection_settings_changed(self, new_value, old_value):
        print(f"Connection setting changed from {old_value} to {new_value}")
        # Safely re-initialize or reconnect the serial port here
```

## Polymorphic Configurations with `@configuration_factory`

Sometimes a configuration requires a property that can be one of several different classes (e.g., different types of Data Sources or Codecs). The system supports this natively using factory categories.

### Step 1: Define the Base Category

First, define the base class (or interface) and mark it with a category name using `@configuration_factory`:

```python
from blinkview.core.configurable import configuration_factory, configurable

@configurable
@configuration_factory("data_source_type")
class BaseDataSource:
    """Base class for all data sources."""
    pass
```

### Step 2: Implement the Types

Next, implement the specific types. They must also be decorated with `@configurable`. The system uses the class's docstring for the UI description.

```python
from blinkview.core.configurable import configurable, configuration_property

@configurable
@configuration_property("file_path", type="string", required=True, title="Path to File")
class FileDataSource(BaseDataSource):
    """Reads telemetry data from a local file."""
    pass

@configurable
@configuration_property("ip_address", type="string", required=True, title="IP Address")
class NetworkDataSource(BaseDataSource):
    """Reads telemetry data from the network."""
    pass
```

### Step 3: Reference the Factory in a Schema

When referencing this polymorphic type in another class's configuration, use the `_factory` schema extension:

```python
@configurable
@configuration_property(
    "source", 
    type="object", 
    _factory="data_source_type",  # Links to the factory category
    _factory_default="file_data_source", # Optional default type
    title="Data Source Configuration",
    description="Select and configure the primary data source."
)
class AppConfig:
    pass
```

### How the UI handles Factories
The `DynamicConfigWidget` detects the `_factory` keyword. It automatically renders a "Type" dropdown containing the available options (e.g., "File Data Source", "Network Data Source"). When the user changes this dropdown, the UI dynamically clears and re-renders the sub-form to match the schema of the newly selected class.

## Advanced Schema Features in UI

The `DynamicConfigWidget` supports rendering complex JSON Schema structures:

*   **Dynamic Dictionaries:** Use `additionalProperties` to allow arbitrary user-defined string keys. The UI will render "Add Item" buttons and editable key fields.
    ```python
    @configuration_property("custom_tags", type="object", additionalProperties={"type": "string"})
    ```
*   **Complex Arrays:** Define arrays of objects or factory types using `items`. The UI provides "Up", "Down", and "Remove" controls for reordering list items.
    ```python
    @configuration_property(
        "pipeline_stages", 
        type="array", 
        items={"type": "object", "_factory": "pipeline_stage_type"}
    )
    ```

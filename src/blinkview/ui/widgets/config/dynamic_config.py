# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import jsonpatch
from copy import deepcopy

from PySide6.QtWidgets import (
    QScrollArea,
    QGroupBox, QCheckBox,
    QLineEdit, QGraphicsOpacityEffect, QFrame, QSizePolicy, QLabel
)
from PySide6.QtCore import Signal, Qt, QTimer

from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QFormLayout, QPushButton, QHBoxLayout, QMessageBox
)

from blinkview.ui.utils.config_node import ConfigNode
from blinkview.ui.widgets.config_widget_factory import WidgetFactory


class DynamicConfigWidget(QWidget):
    # 1. Define the signal that will broadcast the final JSON dictionary
    # config_applied = Signal(dict)

    signal_unregister = Signal(object)

    def __init__(self, gui_context, path, drop_keys, editable, tab_name, child_name=None):
        super().__init__()
        self.gui_context = gui_context

        self.tab_name = tab_name

        self.tab_params = {
            "path": path,
            "drop_keys": drop_keys,
            "editable": editable,
        }

        if child_name:
            self.tab_params["child_name"] = child_name

        self.node = self.gui_context.config_manager.create_node(path, child_name, drop_keys, editable)
        self.node.signal_received.connect(self.update_config_schema)

        # self.original_schema = deepcopy(schema or {})
        # self.schema = deepcopy(self.original_schema)
        # self.current_config = deepcopy(current_config or {})

        self.original_schema = {}
        self.schema = {}
        self.current_config = {}

        self._widget_registry = {}

        self.main_layout = QVBoxLayout(self)

        # 2. Add a Scroll Area to handle overflowing content
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)  # Allows the inner widget to expand
        self.scroll_area.setFrameShape(QFrame.NoFrame)  # Removes the ugly default border

        # Wrapper widget to handle centering the form inside the scroll area
        self.scroll_content = QWidget()
        self.scroll_content.setObjectName("ScrollContent")  # Added for clean debugging
        self.scroll_layout = QVBoxLayout(self.scroll_content)

        self.form_container = QWidget()
        self.form_container.setObjectName("FormContainer")  # Added for clean debugging

        self.form_container.setMaximumWidth(800)  # Optional: Limit the width for better readability on large screens

        self.form_layout = QFormLayout(self.form_container)
        self.form_layout.setFormAlignment(Qt.AlignLeft | Qt.AlignTop)
        self.form_layout.setFieldGrowthPolicy(QFormLayout.AllNonFixedFieldsGrow)

        # Optional: Add a little extra spacing between the labels and the inputs
        self.form_layout.setHorizontalSpacing(20)

        # 2. CRITICAL FIX: Bind the alignment directly to the widget as it gets added
        self.scroll_layout.addWidget(self.form_container)
        # self.scroll_layout.addStretch(1)  # This pushes the form_container to the top

        self.scroll_area.setWidget(self.scroll_content)
        self.main_layout.insertWidget(0, self.scroll_area)

        # self.main_layout.addStretch()
        # Build the dynamic UI
        self._build_ui(self.schema, self.current_config, self.form_layout, self._widget_registry)

        # 3. Add the button row at the bottom
        self.button_layout = QHBoxLayout()
        self.button_layout.addStretch()  # Pushes the button to the right

        self.btn_revert = QPushButton("Revert Changes")
        self.btn_revert.setEnabled(False)
        self.btn_revert.setStyleSheet("""
                    QPushButton {
                        background-color: #6c757d; /* Gray */
                        color: white; 
                        font-weight: bold; 
                        padding: 6px 12px;
                        border-radius: 4px;
                        border: none;
                    }
                    QPushButton:disabled {
                        background-color: #444444;
                        color: #888888;
                    }
                    QPushButton:hover:!disabled {
                        background-color: #5a6268;
                    }
                """)

        self.btn_apply = QPushButton("Apply Configuration")
        self.btn_apply.setEnabled(False)
        self.btn_apply.setStyleSheet("""
                    QPushButton {
                        background-color: #0d6efd; /* Blue */
                        color: white; 
                        font-weight: bold; 
                        padding: 6px 12px;
                        border-radius: 4px;
                        border: none;
                    }
                    QPushButton:disabled {
                        background-color: #444444; 
                        color: #888888;          
                    }
                    QPushButton:hover:!disabled {
                        background-color: #0b5ed7; 
                    }
                """)

        if False:
            # Using Object Names prevents the red border from cascading to every child widget!
            self.setStyleSheet(self.styleSheet() + """
                        QWidget#ScrollContent { background-color: #88fafa88; border: 2px solid red; }
                        QWidget#FormContainer { background-color: #fafffa; border: 2px dashed green; }
                        QGroupBox { border: 2px solid purple; margin-top: 10px; padding-top: 10px; }
                        QLineEdit, QComboBox, QSpinBox, QDoubleSpinBox, QPlainTextEdit, QListWidget {
                            border: 1px solid blue;
                        }
                    """)

        # Add them in order: Revert on the left, Apply on the right
        self.button_layout.addWidget(self.btn_revert)
        self.button_layout.addWidget(self.btn_apply)
        self.main_layout.addLayout(self.button_layout)

        # 4. Connect the click events
        self.applying_config = False
        self.btn_apply.clicked.connect(self._on_apply_clicked)
        self.btn_revert.clicked.connect(self._on_revert_clicked)

        self.node.fetch()

    def closeEvent(self, event):
        """Called automatically when the widget is instructed to close."""
        print(f"[Widget] Closing. Deregistering node for {self.node.active_path}")

        # 1. Clean up the backend connection
        self.node.signal_received.disconnect(self.update_config_schema)
        self.node.deregister()

        self.signal_unregister.emit(self)  # Notify any listeners that this widget is closing

        # 2. Accept the event so Qt proceeds with destroying the widget
        event.accept()

    def _check_for_changes(self, *_):
        """Compares current UI state to the loaded config and toggles the buttons."""
        if self.applying_config:
            return

        current_ui_state = self.get_config()
        is_changed = (current_ui_state != self.current_config)

        # Toggle both buttons together
        self.btn_apply.setEnabled(is_changed)
        self.btn_revert.setEnabled(is_changed)

    def _on_apply_clicked(self):
        if self.applying_config:
            QMessageBox.warning(self, "Please Wait", "Configuration is already being applied. Please wait.")
            return

        is_valid, error_msg = self.validate_current()

        if not is_valid:
            QMessageBox.critical(self, "Validation Error", f"Cannot apply configuration:\n\n{error_msg}")
            return

        self.applying_config = True

        previous_config = self.current_config
        current_config = self.get_config()

        # --- NEW: Generate RFC 6902 JSON Patch ---
        patch = jsonpatch.make_patch(previous_config, current_config).patch

        if not patch:
            self.applying_config = False
            return  # Safety catch: Nothing actually changed

        print(f"[DynamicConfigWidget] Generating JSON Patch:\n{patch}")

        # Send the patch and the new full state
        self.node.send(patch=patch)

        self.btn_apply.setText("Applying... Please wait.")
        self.btn_apply.setEnabled(False)

        QTimer.singleShot(5000, self._apply_timeout)

    def _on_revert_clicked(self):
        """Discards UI changes and restores the baseline backend configuration."""

        # 1. Restore the schema to its pristine original state
        self.schema = deepcopy(self.original_schema)

        # 2. Clear the UI completely
        self._clear_layout(self.form_layout)
        self._widget_registry.clear()

        # 3. Rebuild the UI using the unmodified baseline config
        self._build_ui(self.schema, self.current_config, self.form_layout, self._widget_registry)

        # 4. Disable both buttons since we are back to neutral
        self.btn_apply.setEnabled(False)
        self.btn_revert.setEnabled(False)

    def _build_ui(self, schema_node: dict, data: dict, layout: QFormLayout, registry: dict, current_path: list = None):
        """Main dispatcher: Prepares the schema, then routes each property to the correct UI builder."""
        current_path = current_path or []

        if not current_path:  # Only true for the very first root call!
            description = schema_node.get("description", "").strip()
            if description:
                desc_label = QLabel(description)
                desc_label.setWordWrap(True)
                desc_label.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Minimum)

                # Slightly larger text (12px) and a bigger bottom margin for the main title
                desc_label.setStyleSheet("color: #aaa; font-size: 12px; font-style: italic; margin-bottom: 12px;")

                # addRow with one widget spans the entire top of the form
                layout.addRow(desc_label)

        # 1. Prepare the schema (Inject factory types if necessary)
        self._inject_factory_schema(schema_node, data, registry)

        properties = schema_node.get("properties", {})
        required_keys = schema_node.get("required", [])

        def get_sort_priority(k):
            item_schema = properties.get(k, {})

            # 1. Hardcoded essentials always go to the very top
            if k == "enabled":
                return -100
            if k == "type":
                return -99

            # 2. Explicit Override (User-defined order)
            if "ui_order" in item_schema:
                return item_schema["ui_order"]

            # 3. Default fallback logic
            if k in required_keys:
                return 0

            return 20  # Optional fields drop to the bottom

        sorted_keys = sorted(properties.keys(), key=lambda k: (get_sort_priority(k), k))

        # 2. Dispatch each property to its specific builder
        for key in sorted_keys:
            prop_schema = properties[key]
            value = data.get(key)

            # Autofill missing defaults
            if value is None and "default" in prop_schema:
                value = prop_schema["default"]

            # Handle hidden fields immediately
            if prop_schema.get("hidden", False):
                registry[key] = {"type": "hidden", "value": value}
                continue

            prop_type = prop_schema.get("type", "string")
            is_complex_array = prop_type == "array" and (
                    prop_schema.get("items", {}).get("type") == "object" or "_factory" in prop_schema.get("items", {})
            )

            # Route to the correct builder method!
            if is_complex_array:
                self._build_complex_array(key, prop_schema, value, data, layout, registry, current_path, required_keys)
            elif "additionalProperties" in prop_schema:
                self._build_dynamic_dict(key, prop_schema, value, data, layout, registry, current_path, required_keys)
            elif prop_type == "object":
                self._build_static_object(key, prop_schema, value, layout, registry, current_path, required_keys)
            else:
                self._build_primitive(key, prop_schema, value, data, layout, registry, required_keys)

    def _inject_factory_schema(self, schema_node: dict, data: dict, registry: dict):
        """Handles pulling factory schemas and injecting the 'type' dropdown."""
        if "properties" not in schema_node:
            schema_node["properties"] = {}

        properties = schema_node["properties"]
        category = schema_node.get("_factory") or properties.get("_factory", {}).get("default")

        if "required" in schema_node and "_factory" in schema_node["required"]:
            schema_node["required"].remove("_factory")

        if not category:
            return

        factory_choices = self.node.factory_types(category)
        if not factory_choices:
            return

        # --- NEW: Extract our custom default flag ---
        default_type = schema_node.get("_factory_default")
        is_hidden = schema_node.get("_factory_dropdown_hidden", False)

        if is_hidden or len(factory_choices) == 1:
            # Use existing data, fallback to default, or first choice
            selected_type = data.get("type") or default_type or factory_choices[0][0]
            registry["type"] = {"type": "hidden", "value": selected_type}
            data["type"] = selected_type
        else:
            type_prop = {
                "type": "string", "title": "Type",
                "enum": [choice[0] for choice in factory_choices],
                "enum_descriptions": [choice[0].replace("_", " ").title() for choice in factory_choices],
                "enum_tooltips": [choice[1] for choice in factory_choices],
                "_is_factory_trigger": True
            }

            if default_type:
                type_prop["default"] = default_type

            properties["type"] = type_prop

            if "required" not in schema_node:
                schema_node["required"] = []
            if "type" not in schema_node["required"]:
                schema_node["required"].append("type")

        # --- NEW: Smart fallback for loading the sub-schema ---
        current_type = data.get("type")
        if not current_type:
            current_type = default_type
        if not current_type and factory_choices:
            current_type = factory_choices[0][0]

        if current_type:
            sub_schema = self.node.factory_schema(category, current_type)
            # print(f"[DynamicConfigWidget] Injecting factory schema for category '{category}', type '{current_type}':\n{json.dumps(sub_schema, indent=4)}")
            if sub_schema:
                sub_desc = sub_schema.get("description")
                if sub_desc and "type" in properties:
                    properties["type"]["description"] = sub_desc
                # ==========================================================

                if "properties" in sub_schema:
                    for k, v in sub_schema["properties"].items():
                        properties.setdefault(k, v)
                if "required" in sub_schema:
                    merged_req = set(schema_node.get("required", [])) | set(sub_schema["required"])
                    schema_node["required"] = list(merged_req)

    def _build_static_object(self, key, prop_schema, value, layout, registry, current_path, required_keys):
        """Builds a standard nested dictionary with known properties."""
        is_required = key in required_keys
        title = prop_schema.get("title", key.replace("_", " ").title())
        description = prop_schema.get("description", "")
        has_value = value is not None

        group_box, group_layout = self._create_group_box(title, description, is_required, has_value)

        child_registry = {}
        registry[key] = {
            "type": "object",
            "registry": child_registry,
            "container": group_box,
            "is_required": is_required
        }

        child_data = value if isinstance(value, dict) else {}

        # Recurse back to the main dispatcher for the children
        self._build_ui(prop_schema, child_data, group_layout, child_registry, current_path + [key])
        layout.addRow(group_box)

    def __build_static_object(self, key, prop_schema, value, layout, registry, current_path, required_keys):
        """Builds a standard nested dictionary with known properties. Hides if empty."""
        is_required = key in required_keys
        title = prop_schema.get("title", key.replace("_", " ").title())
        description = prop_schema.get("description", "")
        has_value = value is not None

        # --- NEW: Check if there is actually anything to render ---
        # We check 'properties' and also consider if the factory might have injected any
        has_children = bool(prop_schema.get("properties"))

        # If the object is empty and not checkable (optional), don't show it at all
        if not has_children and is_required:
            # Register it so get_config() still sees the empty object/default
            child_registry = {}
            registry[key] = {
                "type": "object",
                "registry": child_registry,
                "container": None,  # No widget
                "is_required": is_required
            }
            return

        # Standard creation if properties exist
        group_box, group_layout = self._create_group_box(title, description, is_required, has_value)

        child_registry = {}
        registry[key] = {
            "type": "object",
            "registry": child_registry,
            "container": group_box,
            "is_required": is_required
        }

        child_data = value if isinstance(value, dict) else {}

        # Recurse back to the main dispatcher
        self._build_ui(prop_schema, child_data, group_layout, child_registry, current_path + [key])

        # Only add to the layout if the group box actually contains something
        # (or if it's an optional toggle the user might want to enable/disable)
        if has_children or not is_required:
            layout.addRow(group_box)
        else:
            group_box.deleteLater()

    def _build_dynamic_dict(self, key, prop_schema, value, data, layout, registry, current_path, required_keys):
        """Builds a dynamic dictionary where users can add arbitrary string keys."""
        is_required = key in required_keys
        title = prop_schema.get("title", key.replace("_", " ").title())
        description = prop_schema.get("description", "")
        has_value = key in data

        group_box, group_layout = self._create_group_box(title, description, is_required, has_value)

        child_registry = {}
        registry[key] = {
            "type": "object",
            "registry": child_registry,
            "container": group_box,
            "is_required": is_required
        }

        child_data = value if isinstance(value, dict) else {}
        sub_schema = prop_schema["additionalProperties"]
        node_path = current_path + [key]

        # 1. Render all existing dynamic keys
        for dyn_key, dyn_val in child_data.items():
            # Create a header row for the editable key + delete button
            header_widget = QWidget()
            header_layout = QHBoxLayout(header_widget)
            header_layout.setContentsMargins(0, 0, 0, 0)

            key_editor = QLineEdit(dyn_key)
            key_editor.setPlaceholderText("Key Name")
            key_editor.setFixedWidth(150)
            key_editor.textChanged.connect(self._check_for_changes)

            btn_delete = QPushButton("✕ Remove")
            btn_delete.setFixedWidth(80)
            btn_delete.setStyleSheet("color: #dc3545; border: 1px solid #dc3545; border-radius: 4px;")

            header_layout.addWidget(key_editor)
            header_layout.addStretch()
            header_layout.addWidget(btn_delete)
            group_layout.addRow(header_widget)

            # --- VALUE RENDERING ---
            if sub_schema.get("type") == "object":
                # CASE A: Value is an Object (like your 'modules' example)
                # We create a sub-registry for the object's properties
                val_registry = {}
                child_registry[dyn_key] = {
                    "type": "dynamic_pair",
                    "key_widget": key_editor,
                    "val_type": "object",
                    "registry": val_registry
                }
                # Recurse to build the inner properties (like "enabled")
                self._build_ui(sub_schema, dyn_val or {}, group_layout, val_registry, node_path + [dyn_key])
            else:
                # CASE B: Value is a Primitive (like your 'LevelMap' example)
                val_widget = WidgetFactory.build_widget(sub_schema, dyn_val, self.node)
                WidgetFactory.connect_signals(val_widget, self._check_for_changes)

                child_registry[dyn_key] = {
                    "type": "dynamic_pair",
                    "key_widget": key_editor,
                    "val_type": "primitive",
                    "widget": val_widget
                }
                group_layout.addRow("Value:", val_widget)

            def on_delete(_=False, k=dyn_key):
                state = self.get_config()
                target = state
                for p in node_path: target = target.get(p, {})
                if k in target:
                    del target[k]
                    self.schema = deepcopy(self.original_schema)
                    self._clear_layout(self.form_layout)
                    self._widget_registry.clear()
                    self._build_ui(self.schema, state, self.form_layout, self._widget_registry)
                    self._check_for_changes()

            btn_delete.clicked.connect(on_delete)
            # Add a separator line for visual clarity between dynamic items
            line = QFrame()
            line.setFrameShape(QFrame.HLine)
            line.setFrameShadow(QFrame.Sunken)
            line.setStyleSheet("color: #444;")
            group_layout.addRow(line)

        # 2. Add the "New Key" input row
        add_layout = QHBoxLayout()
        new_key_input = QLineEdit()
        new_key_input.setPlaceholderText("Enter new item name...")
        btn_add = QPushButton("Add")
        add_layout.addWidget(new_key_input)
        add_layout.addWidget(btn_add)
        group_layout.addRow(add_layout)

        def on_add(_=False, path=node_path, input_widget=new_key_input, schema_template=sub_schema):
            new_key = input_widget.text().strip()
            if not new_key:
                return

            state = self.get_config()
            target = state
            for p in path:
                target = target.setdefault(p, {})

            if new_key not in target:
                target[new_key] = deepcopy(schema_template.get("default", {}))
                self.schema = deepcopy(self.original_schema)
                self._clear_layout(self.form_layout)
                self._widget_registry.clear()
                self._build_ui(self.schema, state, self.form_layout, self._widget_registry)
                self._check_for_changes()

        btn_add.clicked.connect(on_add)
        layout.addRow(group_box)

    def _build_complex_array(self, key, prop_schema, value, data, layout, registry, current_path, required_keys):
        """Builds an array of objects or factory plugins."""
        is_required = key in required_keys
        title = prop_schema.get("title", key.replace("_", " ").title())
        description = prop_schema.get("description", "")
        has_value = key in data

        group_box, group_layout = self._create_group_box(title, description, is_required, has_value)

        child_registry = {}
        registry[key] = {
            "type": "complex_array",
            "registry": child_registry,
            "container": group_box,
            "is_required": is_required
        }

        items_schema = prop_schema.get("items", {})
        child_data = value if isinstance(value, list) else []
        node_path = current_path + [key]

        # 1. Render all existing array items
        for idx, item_val in enumerate(child_data):
            idx_str = str(idx)
            item_schema = deepcopy(items_schema)

            dyn_schema = {"properties": {idx_str: item_schema}, "required": [idx_str]}
            dyn_schema["properties"][idx_str]["title"] = f"Item {idx + 1}"

            self._build_ui(dyn_schema, {idx_str: item_val}, group_layout, child_registry, node_path)

            # ==========================================================
            # --- NEW: Array Reordering Controls ---
            # ==========================================================

            control_layout = QHBoxLayout()
            control_layout.addStretch()  # Push all buttons to the right side

            nav_button_style = """
                            QPushButton {
                                border: none; 
                                color: #0d6efd; /* Bright blue when active */
                                text-decoration: underline; 
                                margin-right: 15px;
                            }
                            QPushButton:disabled {
                                color: #6c757d; /* Muted gray when disabled */
                                text-decoration: none; /* Drop the underline to make it look flatter */
                            }
                        """

            # --- Move Up Button ---
            btn_up = QPushButton("▲ Up")
            btn_up.setCursor(Qt.PointingHandCursor)
            btn_up.setEnabled(idx > 0)  # Disable for the very first item
            btn_up.setStyleSheet(nav_button_style)

            def on_move_up(_=False, i=idx):
                state = self.get_config()
                target = state
                for p in node_path:
                    target = target.get(p, [])

                if 0 < i < len(target):
                    # Swap the items in the python list
                    target[i - 1], target[i] = target[i], target[i - 1]

                    self.schema = deepcopy(self.original_schema)
                    self._clear_layout(self.form_layout)
                    self._widget_registry.clear()
                    self._build_ui(self.schema, state, self.form_layout, self._widget_registry)
                    self._check_for_changes()

            btn_up.clicked.connect(on_move_up)

            # --- Move Down Button ---
            btn_down = QPushButton("▼ Down")
            btn_down.setCursor(Qt.PointingHandCursor)
            btn_down.setEnabled(idx < len(child_data) - 1)  # Disable for the very last item
            btn_down.setStyleSheet(nav_button_style)

            def on_move_down(_=False, i=idx):
                state = self.get_config()
                target = state
                for p in node_path:
                    target = target.get(p, [])

                if 0 <= i < len(target) - 1:
                    # Swap the items in the python list
                    target[i + 1], target[i] = target[i], target[i + 1]

                    self.schema = deepcopy(self.original_schema)
                    self._clear_layout(self.form_layout)
                    self._widget_registry.clear()
                    self._build_ui(self.schema, state, self.form_layout, self._widget_registry)
                    self._check_for_changes()

            btn_down.clicked.connect(on_move_down)

            # --- Remove Button ---
            btn_delete = QPushButton(f"Remove Item {idx + 1}")
            btn_delete.setStyleSheet("color: #dc3545; border: none; text-decoration: underline;")
            btn_delete.setCursor(Qt.PointingHandCursor)

            def on_delete_arr(_=False, i=idx):
                state = self.get_config()
                target = state
                for p in node_path:
                    target = target.get(p, [])

                if i < len(target):
                    target.pop(i)
                    self.schema = deepcopy(self.original_schema)
                    self._clear_layout(self.form_layout)
                    self._widget_registry.clear()
                    self._build_ui(self.schema, state, self.form_layout, self._widget_registry)
                    self._check_for_changes()

            btn_delete.clicked.connect(on_delete_arr)

            # Pack them into the layout
            control_layout.addWidget(btn_up)
            control_layout.addWidget(btn_down)
            control_layout.addWidget(btn_delete)

            # Use an empty label string so the layout spans both columns in the form!
            group_layout.addRow("", control_layout)
            # ==========================================================

        # 2. Add "Add Item" button
        btn_add = QPushButton("Add Item")

        def on_add_arr(_=False, path=node_path, schema_template=items_schema):
            state = self.get_config()
            target = state
            for p in path[:-1]:
                target = target.setdefault(p, {})

            arr = target.get(path[-1])
            if not isinstance(arr, list):
                arr = []
                target[path[-1]] = arr

            arr.append(deepcopy(schema_template.get("default", {})))

            self.schema = deepcopy(self.original_schema)
            self._clear_layout(self.form_layout)
            self._widget_registry.clear()
            self._build_ui(self.schema, state, self.form_layout, self._widget_registry)
            self._check_for_changes()

        btn_add.clicked.connect(on_add_arr)
        group_layout.addRow(btn_add)
        layout.addRow(group_box)

    def _create_group_box(self, title: str, description: str, is_required: bool, has_value: bool):
        """Helper method to construct standardized, nested group boxes with descriptions."""

        group_box = QGroupBox(title)
        group_main_layout = QVBoxLayout(group_box)

        if description:
            desc_label = QLabel(description)
            desc_label.setWordWrap(True)
            desc_label.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Minimum)
            desc_label.setStyleSheet("color: #888; font-size: 11px; font-style: italic; margin-bottom: 6px;")
            group_main_layout.addWidget(desc_label)

        group_layout = QFormLayout()
        group_layout.setContentsMargins(0, 0, 0, 0)  # Keep it flush
        group_main_layout.addLayout(group_layout)

        if not is_required:
            group_box.setCheckable(True)
            group_box.setChecked(has_value)
            group_box.toggled.connect(self._check_for_changes)

        return group_box, group_layout

    def _build_primitive(self, key, prop_schema, value, data, layout, registry, required_keys):
        """Builds standard inputs (strings, ints, checkboxes) and handles overrides."""
        is_required = key in required_keys
        title = prop_schema.get("title", key.replace("_", " ").title())
        description = prop_schema.get("description", "")
        has_value = key in data

        widget = WidgetFactory.build_widget(
            schema=prop_schema,
            value=value,
            node_context=self.node,
            factory_callback=self._on_factory_type_changed
        )
        if not widget:
            return

        WidgetFactory.connect_signals(widget, self._check_for_changes)

        field_layout = QVBoxLayout()
        field_layout.setContentsMargins(0, 0, 0, 0)
        field_layout.setSpacing(2)
        field_layout.addWidget(widget)

        if description:
            desc_label = QLabel(description)
            desc_label.setWordWrap(True)
            desc_label.setStyleSheet("color: #888; font-size: 11px; font-style: italic;")

            # --- FIX 2: Force Qt to respect the width/height of wrapped text ---
            desc_label.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.Minimum)

            field_layout.addWidget(desc_label)
        field_layout.addStretch()
        if is_required:
            registry[key] = {"type": "primitive", "widget": widget, "is_required": True}
            layout.addRow(title, field_layout)
        else:
            opt_layout = QHBoxLayout()
            opt_layout.setContentsMargins(0, 0, 0, 0)
            opt_layout.setSpacing(10)

            toggle_cb = QCheckBox("Override")
            toggle_cb.setChecked(has_value)
            toggle_cb.setStyleSheet("color: #888; font-size: 11px;")
            toggle_cb.toggled.connect(self._check_for_changes)

            opt_layout.addLayout(field_layout, 1)

            # --- FIX 1: Add the checkbox exactly ONCE with proper arguments ---
            opt_layout.addWidget(toggle_cb, 0, Qt.AlignTop)

            def update_visuals(enabled, w=widget):
                w.setEnabled(enabled)
                opacity = 1.0 if enabled else 0.4
                effect = QGraphicsOpacityEffect(w)
                effect.setOpacity(opacity)
                w.setGraphicsEffect(effect)

            toggle_cb.toggled.connect(update_visuals)
            update_visuals(has_value)

            registry[key] = {
                "type": "primitive",
                "widget": widget,
                "is_required": False,
                "toggle": toggle_cb
            }
            layout.addRow(title, opt_layout)

    def _apply_timeout(self):
        if self.applying_config:
            self.btn_apply.setText("Apply Configuration")
            self.btn_apply.setEnabled(True)
            self.applying_config = False

    def get_config(self) -> dict:
        return self._extract_data(self._widget_registry)

    def _extract_data(self, registry: dict) -> dict:
        """
        Recursively extracts the underlying Python dictionary from the UI registry.
        Handles standard objects, complex arrays, and editable dynamic pairs.
        """
        result = {}
        for key, node in registry.items():
            # 1. Handle Hidden Fields (Factory types, etc.)
            if node["type"] == "hidden":
                if node["value"] is not None:
                    result[key] = node["value"]
                continue

            # 2. Handle Optional Fields (Skips if the user unchecked the 'Override' or 'GroupBox')
            if not node.get("is_required", True):
                if node["type"] == "object" and not node["container"].isChecked():
                    continue
                elif node["type"] == "primitive" and not node["toggle"].isChecked():
                    continue

            # 3. CASE: Editable Dynamic Pairs (Dictionaries with user-defined keys)
            if node["type"] == "dynamic_pair":
                # Extract the actual key from the QLineEdit
                actual_key = node["key_widget"].text().strip()
                if not actual_key:
                    continue

                # Extract the value based on its complexity
                if node.get("val_type") == "object":
                    # Value is a nested object (e.g., your 'modules' setup)
                    result[actual_key] = self._extract_data(node["registry"])
                else:
                    # Value is a single widget (e.g., your 'LevelMap' setup)
                    val_widget = node.get("val_widget") or node.get("widget")
                    result[actual_key] = WidgetFactory.extract_value(val_widget)

            # 4. CASE: Static Nested Objects
            elif node["type"] == "object":
                result[key] = self._extract_data(node["registry"])

            # 5. CASE: Complex Arrays (Lists of objects/factories)
            elif node["type"] == "complex_array":
                extracted_dict = self._extract_data(node["registry"])
                # Sort keys numerically (0, 1, 2...) to preserve order, then cast to list
                sorted_keys = sorted(extracted_dict.keys(), key=int)
                result[key] = [extracted_dict[k] for k in sorted_keys]

            # 6. CASE: Standard Primitives (Strings, Numbers, Booleans)
            else:
                widget = node.get("widget")
                if widget:
                    result[key] = WidgetFactory.extract_value(widget)

        return result

    def validate_current(self) -> tuple[bool, str]:
        """Validates the current UI state against the JSON Schema."""
        from jsonschema import validate
        from jsonschema.exceptions import ValidationError
        from copy import deepcopy

        data = self.get_config()

        # 1. Create a temporary schema for validation
        validation_schema = deepcopy(self.schema)

        # 2. Strip 'enum' constraints from any field that allows custom typing
        def sanitize_schema(node):
            if isinstance(node, dict):
                if node.get("_allow_custom", False) and "enum" in node:
                    del node["enum"]  # Remove strict enum check!
                for val in node.values():
                    sanitize_schema(val)
            elif isinstance(node, list):
                for item in node:
                    sanitize_schema(item)

        sanitize_schema(validation_schema)

        try:
            # 3. Validate against the sanitized schema
            validate(instance=data, schema=validation_schema)
            return True, "Configuration is valid."
        except ValidationError as e:
            return False, e.message

    def update_config_schema(self, config_: dict, schema_: dict):
        if self.applying_config:
            self.applying_config = False
            self.btn_apply.setText("Apply Configuration")
            if config_ == self.current_config:
                return

        self.original_schema = deepcopy(schema_)
        self.schema = deepcopy(schema_)
        self.current_config = deepcopy(config_)

        self._clear_layout(self.form_layout)
        self._widget_registry.clear()

        self._build_ui(self.schema, self.current_config, self.form_layout, self._widget_registry)

        # --- NEW: Ensure button is disabled on fresh load ---
        self.btn_apply.setEnabled(False)

    def _clear_layout(self, _=None):
        """Safely and instantly destroys the old UI by replacing its container."""
        self.scroll_layout.removeWidget(self.form_container)
        self.form_container.deleteLater()

        self.form_container = QWidget()
        self.form_container.setMaximumWidth(800)

        # Re-apply the size policy
        self.form_container.setSizePolicy(QSizePolicy.Maximum, QSizePolicy.MinimumExpanding)

        self.form_layout = QFormLayout(self.form_container)

        # Re-apply the alignment flag
        self.scroll_layout.addWidget(self.form_container, alignment=Qt.AlignTop | Qt.AlignHCenter)

    def _on_factory_type_changed(self):
        """Called immediately when ANY factory 'type' dropdown changes."""
        # 1. Grab the entire current state exactly as it is right now
        current_state = self.get_config()

        # 2. Reset the schema to the pristine baseline so all factories re-evaluate
        self.schema = deepcopy(self.original_schema)

        # 3. Nuke the UI
        self._clear_layout(self.form_layout)
        self._widget_registry.clear()

        # 4. Rebuild. The _build_ui method will naturally ignore any orphaned data
        # (like old properties from the previous factory type) because they won't
        # exist in the freshly injected sub-schema!
        self._build_ui(self.schema, current_state, self.form_layout, self._widget_registry)

        # 5. Check if the Apply button needs to light up
        self._check_for_changes()


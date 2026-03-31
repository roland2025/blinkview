# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from pathlib import Path

from qtpy.QtCore import QRegularExpression, Qt
from qtpy.QtWidgets import (
    QCheckBox,
    QComboBox,
    QDoubleSpinBox,
    QFileDialog,
    QHBoxLayout,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QPlainTextEdit,
    QPushButton,
    QSpinBox,
    QWidget,
)


def get_portable_path(absolute_path: str, max_up_levels: int = 2) -> str:
    """
    Converts to a relative path even if it involves parent directories (..).
    Only returns relative if the number of 'up' steps is <= max_up_levels.
    """
    try:
        target = Path(absolute_path).resolve()
        anchor = Path.cwd().resolve()

        # Calculate the relative path (this works even for parents)
        # On Python 3.12+ you can use target.relative_to(anchor, walk_up=True)
        # For older versions, we use os.path.relpath which is very robust
        import os

        rel_path_str = os.path.relpath(target, anchor)

        # Count how many times we go 'up' (..)
        up_count = rel_path_str.count(".." + os.sep) or (1 if rel_path_str.startswith("..") else 0)

        if up_count <= max_up_levels:
            # Normalize to forward slashes for cross-platform JSON compatibility
            return rel_path_str.replace(os.sep, "/")

    except (ValueError, RuntimeError, TypeError):
        pass

    return str(absolute_path).replace(os.sep, "/")


class WidgetFactory:
    """A stateless factory for generating PySide6 input widgets from JSON Schema."""

    @staticmethod
    def build_widget(schema: dict, value, node_context=None, factory_callback=None) -> QWidget:
        """Main dispatcher: Routes to specialized static builders."""

        # Handle Constants first
        if "const" in schema:
            return WidgetFactory.build_const_widget(schema)

        # Pre-process Single String References
        # This converts a reference into an 'enum' so the next block can handle it
        if "_reference" in schema and schema.get("type", "string") == "string" and node_context:
            try:
                ref_path = schema["_reference"]
                available_key_values = node_context.manager.get_reference_values(ref_path)

                # Only inject if we actually got results
                if available_key_values:
                    schema["enum"] = [kv[0] for kv in available_key_values]
                    schema["enum_descriptions"] = [str(kv[1]) if kv[1] else str(kv[0]) for kv in available_key_values]
            except Exception as e:
                print(f"[UI Warning] Could not fetch keys for reference {schema.get('_reference')}: {e}")

        # Handle Enums (This now catches both manual enums like 'url' AND converted references)
        if "enum" in schema:
            return WidgetFactory.build_enum_widget(schema, value, factory_callback)

        # Standard Type Routing
        match schema.get("type", "string"):
            case "string" if schema.get("ui_type") == "file":
                return WidgetFactory.build_file_selector(schema, value)

            case "string" if schema.get("ui_type") == "directory":
                # Ready for when you want to add folder selection!
                return WidgetFactory.build_directory_selector(schema, value)

            case "array":
                return WidgetFactory.build_array_widget(schema, value, node_context)

            case "boolean":
                return WidgetFactory.build_boolean_widget(schema, value)

            case "integer":
                return WidgetFactory.build_integer_widget(schema, value)

            case "number":
                return WidgetFactory.build_number_widget(schema, value)

            case "string":
                return WidgetFactory.build_string_widget(schema, value)

            case _:
                # Fallback for unknown types
                return WidgetFactory.build_string_widget(schema, value)

    @staticmethod
    def build_const_widget(schema: dict) -> QWidget:
        widget = QLineEdit()
        widget.setText(str(schema["const"]))
        widget.setReadOnly(True)
        widget.setStyleSheet("color: #666; background-color: #f0f0f0;")
        widget.setToolTip("This value is constant and cannot be modified.")
        return widget

    @staticmethod
    def build_enum_widget(schema: dict, value, factory_callback=None) -> QWidget:

        widget = QComboBox()
        enums = schema.get("enum", [])
        descriptions = schema.get("enum_descriptions", enums)

        # --- Grab the tooltips array (if it exists) ---
        tooltips = schema.get("enum_tooltips", [])

        is_custom_allowed = schema.get("_allow_custom", False)

        for i, item in enumerate(enums):
            desc = descriptions[i] if i < len(descriptions) else str(item)
            widget.addItem(str(desc), userData=item)

            # --- Apply the rich tooltip if available, otherwise fallback to the value ---
            if i < len(tooltips) and tooltips[i]:
                widget.setItemData(i, str(tooltips[i]), Qt.ToolTipRole)
            else:
                widget.setItemData(i, f"Value: {item}", Qt.ToolTipRole)

        if is_custom_allowed:
            widget.setEditable(True)
            widget.setInsertPolicy(QComboBox.NoInsert)

        actual_value = value if value is not None else schema.get("default")

        if actual_value is not None:
            if actual_value in enums:
                widget.setCurrentIndex(enums.index(actual_value))
            elif is_custom_allowed:
                widget.setCurrentText(str(actual_value))

        if schema.get("_is_factory_trigger") and factory_callback:
            widget.currentIndexChanged.connect(factory_callback)

        return widget

    @staticmethod
    def build_array_widget(schema: dict, value, node_context=None) -> QWidget:
        items_schema = schema.get("items", {})
        ref_path = items_schema.get("_reference")
        actual_value = value if value is not None else schema.get("default", [])

        if ref_path and node_context:
            widget = QListWidget()
            widget.setMaximumHeight(100)
            widget.setStyleSheet("QListWidget { border: 1px solid #ccc; border-radius: 4px; }")
            try:
                available_key_values = node_context.manager.get_reference_values(ref_path)

                # --- Iterate over both key (k) and value (v) ---
                for k, v in available_key_values:
                    # Show the value if it exists, otherwise fallback to showing the key
                    display_text = str(v) if v else str(k)

                    item = QListWidgetItem(display_text)

                    # --- Hide the actual key inside the item ---
                    item.setData(Qt.UserRole, k)

                    item.setFlags(item.flags() | Qt.ItemIsUserCheckable)
                    item.setCheckState(Qt.Checked if k in actual_value else Qt.Unchecked)
                    widget.addItem(item)
                return widget
            except Exception as e:
                print(f"[UI Warning] Could not fetch keys for reference {ref_path}: {e}")

        widget = QPlainTextEdit()
        widget.setToolTip("Enter one item per line.")
        widget.setMaximumHeight(100)
        if isinstance(actual_value, list):
            widget.setPlainText("\n".join(str(v) for v in actual_value))
        return widget

    @staticmethod
    def build_boolean_widget(schema: dict, value) -> QWidget:
        widget = QComboBox()
        widget.addItem("True", userData=True)
        widget.addItem("False", userData=False)
        actual_value = value if value is not None else schema.get("default", False)
        widget.setCurrentIndex(0 if actual_value else 1)
        return widget

    @staticmethod
    def build_integer_widget(schema: dict, value) -> QWidget:
        widget = QSpinBox()
        widget.setRange(schema.get("minimum", -2147483648), schema.get("maximum", 2147483647))
        widget.setValue(int(value if value is not None else schema.get("default", 0)))
        return widget

    @staticmethod
    def build_number_widget(schema: dict, value) -> QWidget:
        widget = QDoubleSpinBox()
        widget.setDecimals(schema.get("multipleOf", 3))
        widget.setValue(float(value if value is not None else schema.get("default", 0.0)))
        return widget

    @staticmethod
    def build_string_widget(schema: dict, value) -> QWidget:
        widget = QLineEdit()
        widget.setText(str(value if value is not None else schema.get("default", "")))

        if "pattern" in schema:
            regex_str = schema["pattern"]
            regex = QRegularExpression(regex_str)
            widget.setToolTip(f"Required format: {regex_str}")

            def validate_input():
                text = widget.text()
                if not text and not schema.get("required", False):
                    widget.setStyleSheet("")
                    return

                match = regex.match(text)
                if match.hasMatch() and match.capturedLength() == len(text):
                    widget.setStyleSheet("")
                else:
                    widget.setStyleSheet("background-color: #fff1f0; color: black; border: 1px solid #ffa39e;")

            widget.textChanged.connect(validate_input)
            validate_input()

        return widget

    @staticmethod
    def extract_value(widget):
        """Extracts the underlying Python value from a generated PySide6 widget."""
        widget = getattr(widget, "_data_widget", widget)

        if isinstance(widget, QComboBox):
            # ==========================================================
            # --- Safely extract custom text from editable boxes ---
            # ==========================================================
            if widget.isEditable():
                current_text = widget.currentText()

                # Check if what they typed perfectly matches one of our friendly descriptions
                exact_match_idx = widget.findText(current_text)

                if exact_match_idx >= 0:
                    # They selected an item from the list (or typed its exact name)
                    return widget.itemData(exact_match_idx)
                else:
                    # They typed a custom string that isn't in the list (e.g., a socket URL)
                    return current_text
            else:
                # Standard locked dropdown, safely use currentData
                return widget.currentData()

        elif isinstance(widget, QCheckBox):
            return widget.isChecked()

        elif isinstance(widget, (QSpinBox, QDoubleSpinBox)):
            return widget.value()

        elif isinstance(widget, QLineEdit):
            return widget.text()

        elif isinstance(widget, QListWidget):
            checked_items = []
            for i in range(widget.count()):
                item = widget.item(i)
                if item.checkState() == Qt.Checked:
                    # Pull the hidden key we stored in Qt.UserRole
                    hidden_key = item.data(Qt.UserRole)
                    checked_items.append(hidden_key)
            return checked_items

        elif isinstance(widget, QPlainTextEdit):
            raw_text = widget.toPlainText()
            return [line.strip() for line in raw_text.split("\n") if line.strip()]

        return None

    @staticmethod
    def connect_signals(widget, callback):
        """Wires up standard Qt signals to a provided callback function."""
        widget = getattr(widget, "_data_widget", widget)

        if isinstance(widget, QComboBox):
            widget.currentIndexChanged.connect(callback)
            # --- Also listen for typing in editable combo boxes! ---
            if widget.isEditable():
                widget.editTextChanged.connect(callback)

        elif isinstance(widget, QCheckBox):
            widget.toggled.connect(callback)

        elif isinstance(widget, (QSpinBox, QDoubleSpinBox)):
            widget.valueChanged.connect(callback)

        elif isinstance(widget, QLineEdit):
            widget.textChanged.connect(callback)

        elif isinstance(widget, QPlainTextEdit):
            widget.textChanged.connect(callback)

        elif isinstance(widget, QListWidget):
            widget.itemChanged.connect(callback)

    @staticmethod
    def build_file_selector(schema: dict, value) -> QWidget:
        container = QWidget()
        layout = QHBoxLayout(container)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(5)

        line_edit = QLineEdit()
        line_edit.setText(str(value if value is not None else schema.get("default", "")))
        line_edit.setPlaceholderText("Select a file path...")

        btn_browse = QPushButton("Browse...")

        def open_dialog():
            # Support a custom file filter, e.g., "CAN Database (*.dbc)"
            file_filter = schema.get("ui_file_filter", "All Files (*)")
            title = schema.get("title", "Select File")

            # getOpenFileName returns (path, selected_filter)
            abs_path, _ = QFileDialog.getOpenFileName(container, title, line_edit.text(), file_filter)

            if abs_path:
                # Use pathlib to make it portable before showing it in the UI
                portable_path = get_portable_path(abs_path)
                line_edit.setText(portable_path)

        btn_browse.clicked.connect(open_dialog)

        layout.addWidget(line_edit, 1)  # Expandable field
        layout.addWidget(btn_browse, 0)  # Fixed size button

        # Attach the line_edit so extract_value and connect_signals know where the data lives
        container._data_widget = line_edit
        return container

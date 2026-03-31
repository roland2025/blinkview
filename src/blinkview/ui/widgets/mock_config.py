# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
import sys
from copy import deepcopy

from qtpy.QtCore import QObject, QTimer, Signal
from qtpy.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget

from blinkview.ui.widgets.config.dynamic_config import DynamicConfigWidget


class MockManager:
    """Fakes the backend Configuration Manager to provide reference lookups."""

    def get_reference_values(self, ref_path: str) -> list[tuple[str, str]]:
        if ref_path == "/sources":
            return [
                ("src_cam_1", "Main Camera (1080p)"),
                ("src_lidar", "Roof LiDAR Array"),
                ("src_gps", "RTK GPS Module"),
            ]
        return [("default_1", "Default item")]


class MockConfigNode(QObject):
    """Simulates the backend connection for the UI without needing the Registry."""

    signal_received = Signal(dict, dict)

    def __init__(self, active_path: str):
        super().__init__()
        self.active_path = active_path
        self.current_schema = {}
        self.current_config = {}
        # --- Fake the manager so references work! ---
        self.manager = MockManager()

    def deregister(self):
        print(f"[MockNode] UI closed. Deregistering {self.active_path}...")

    # --- Fake the factory methods! ---
    def factory_types(self, category: str):
        if category == "processor":
            return [("filter", "Filter Plugin"), ("transform", "Transform Plugin")]
        return []

    def factory_schema(self, category: str, current_type: str):
        if current_type == "filter":
            return {
                "description": "Drops data points that fall below the threshold.",
                "properties": {
                    "threshold": {"type": "integer", "default": 50, "description": "Minimum acceptable value."}
                },
                "required": ["threshold"],
            }
        if current_type == "transform":
            return {
                "description": "Multiplies incoming numerical data by a scale factor.",
                "properties": {"scale": {"type": "number", "default": 1.5, "description": "Multiplier factor."}},
            }
        return {}

    # --- Update the signature to match the JSON Patch call ---
    def send(self, patch: list = None, **kwargs):
        print(f"\n[MockNode] Received update for {self.active_path}!")
        print(f"[MockNode] JSON Patch Received:\n{json.dumps(patch, indent=4)}")

        # Simulate the backend delay (e.g. saving to disk/DB)
        def simulate_backend_response():
            print("[MockNode] Simulating backend broadcast back to UI...")
            # Apply the patch to our mock state to simulate a real backend
            if patch:
                import jsonpatch

                self.current_config = jsonpatch.apply_patch(self.current_config, patch)
            self.signal_received.emit(self.current_config, self.current_schema)

        QTimer.singleShot(800, simulate_backend_response)


# ==========================================================
# --- UPGRADED SCHEMA: Shows off all the new UI Features ---
# ==========================================================
TEST_SCHEMA = {
    "type": "object",
    "title": "Advanced Sensor Configuration",
    "description": "Main configuration for the sensor array. Use this menu to bind data sources, set metadata, and define the processing pipeline.",
    "properties": {
        "enabled": {"type": "boolean", "title": "Device Enabled", "default": True},
        "port": {
            "type": "string",
            "title": "Connection String",
            "enum": ["COM1", "COM2", "COM3"],
            "_allow_custom": True,  # Enables custom typing!
            "description": "Select a detected COM port, or type a custom Socket URL.",
        },
        "sources_": {
            "type": "array",
            "title": "Bound Data Sources",
            "description": "Select which hardware sources this module should read from.",
            "items": {"type": "string", "_reference": "/sources"},
            "default": [],
        },
        "metadata": {
            "type": "object",
            "title": "Custom Metadata",
            "description": "Dynamically add custom key-value tags to attach to this sensor.",
            "additionalProperties": {"type": "string", "default": ""},
        },
        "steps": {
            "type": "array",
            "title": "Processing Steps",
            "description": "An ordered, sequential list of transformations to apply to the incoming data.",
            "items": {"type": "object", "_factory": "processor"},
        },
    },
    "required": ["enabled", "port"],
}

TEST_CONFIG = {
    "enabled": True,
    "port": "socket://192.168.1.50:9000",  # Custom typed URL!
    "sources_": ["src_lidar", "src_gps"],  # References!
    "metadata": {"location": "Front Bumper", "technician": "Alice"},
    "steps": [{"type": "filter", "threshold": 80}, {"type": "transform", "scale": 2.5}],
}


class DemoWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DynamicConfigWidget Standalone Test")
        self.resize(700, 900)

        # Create the Mock Node
        self.mock_node = MockConfigNode("/devices/test_device")
        self.mock_node.current_schema = deepcopy(TEST_SCHEMA)
        self.mock_node.current_config = deepcopy(TEST_CONFIG)

        # Create the UI
        self.central_widget = QWidget()
        self.layout = QVBoxLayout(self.central_widget)

        # Instantiate your widget
        self.config_widget = DynamicConfigWidget(self.mock_node, TEST_SCHEMA, TEST_CONFIG)
        self.layout.addWidget(self.config_widget)

        # Add a debug button to simulate an external update
        self.btn_simulate_external = QPushButton("Simulate Update from Another User/Backend")
        self.btn_simulate_external.clicked.connect(self.simulate_external_update)
        self.layout.addWidget(self.btn_simulate_external)

        self.setCentralWidget(self.central_widget)

    def simulate_external_update(self):
        """Simulates what happens if the backend updates the config while you are looking at it."""
        modified_config = deepcopy(self.config_widget.current_config)
        modified_config["port"] = "COM99"  # Force a change

        # Update the mock node's internal state and fire the signal
        self.mock_node.current_config = modified_config
        self.mock_node.signal_received.emit(modified_config, self.mock_node.current_schema)


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # Clean dark/light theme fusion styling
    app.setStyle("Fusion")

    window = DemoWindow()
    window.show()
    sys.exit(app.exec())

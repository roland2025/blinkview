# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import json
from PySide6.QtCore import QObject, Signal, QTimer


class MockManager:
    """Fakes the backend Configuration Manager to provide reference lookups."""

    def get_reference_values(self, ref_path: str) -> list[tuple[str, str]]:
        if ref_path == "/sources":
            return [
                ("src_cam_1", "Main Camera (1080p)"),
                ("src_lidar", "Roof LiDAR Array"),
                ("src_gps", "RTK GPS Module")
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
        self.manager = MockManager()

    def deregister(self):
        print(f"[MockNode] UI closed. Deregistering {self.active_path}...")

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
                "required": ["threshold"]
            }
        if current_type == "transform":
            return {
                "description": "Multiplies incoming numerical data by a scale factor.",
                "properties": {
                    "scale": {"type": "number", "default": 1.5, "description": "Multiplier factor."}
                }
            }
        return {}

    def send(self, patch: list = None, **kwargs):
        print(f"\n[MockNode] Received update for {self.active_path}!")
        print(f"[MockNode] JSON Patch Received:\n{json.dumps(patch, indent=4)}")

        def simulate_backend_response():
            print("[MockNode] Simulating backend broadcast back to UI...")
            if patch:
                import jsonpatch
                self.current_config = jsonpatch.apply_patch(self.current_config, patch)
            self.signal_received.emit(self.current_config, self.current_schema)

        QTimer.singleShot(800, simulate_backend_response)


# ==========================================================
# --- TEST DATA ---
# ==========================================================
TEST_SCHEMA = {
    "type": "object",
    "title": "Advanced Sensor Configuration",
    "description": "Main configuration for the sensor array. Use this menu to bind data sources, set metadata, and define the processing pipeline.",
    "properties": {
        "enabled": {
            "type": "boolean",
            "title": "Device Enabled",
            "default": True
        },
        "port": {
            "type": "string",
            "title": "Connection String",
            "enum": ["COM1", "COM2", "COM3"],
            "_allow_custom": True,  # Enables custom typing!
            "description": "Select a detected COM port, or type a custom Socket URL."
        },
        "sources_": {
            "type": "array",
            "title": "Bound Data Sources",
            "description": "Select which hardware sources this module should read from.",
            "items": {"type": "string", "_reference": "/sources"},
            "default": []
        },
        "metadata": {
            "type": "object",
            "title": "Custom Metadata",
            "description": "Dynamically add custom key-value tags to attach to this sensor.",
            "additionalProperties": {
                "type": "string",
                "default": ""
            }
        },
        "steps": {
            "type": "array",
            "title": "Processing Steps",
            "description": "An ordered, sequential list of transformations to apply to the incoming data.",
            "items": {
                "type": "object",
                "_factory": "processor"
            }
        }
    },
    "required": ["enabled", "port"]
}

TEST_CONFIG = {
    "enabled": True,
    "port": "socket://192.168.1.50:9000",  # Custom typed URL!
    "sources_": ["src_lidar", "src_gps"],  # References!
    "metadata": {
        "location": "Front Bumper",
        "technician": "Alice"
    },
    "steps": [
        {
            "type": "filter",
            "threshold": 80
        },
        {
            "type": "transform",
            "scale": 2.5
        }
    ]
}

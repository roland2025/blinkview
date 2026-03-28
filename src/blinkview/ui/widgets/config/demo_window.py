# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import sys
from copy import deepcopy

from PySide6.QtWidgets import QApplication, QMainWindow, QPushButton, QVBoxLayout, QWidget

from .dynamic_config import DynamicConfigWidget

# Import your real UI components
# Import your fake backend and test data
from .mock_backend import TEST_CONFIG, TEST_SCHEMA, MockConfigNode


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

        self.config_widget = DynamicConfigWidget(self.mock_node, TEST_SCHEMA, TEST_CONFIG)
        self.layout.addWidget(self.config_widget)

        # Add a debug button
        self.btn_simulate_external = QPushButton("Simulate Update from Another User/Backend")
        self.btn_simulate_external.clicked.connect(self.simulate_external_update)
        self.layout.addWidget(self.btn_simulate_external)

        self.setCentralWidget(self.central_widget)

    def simulate_external_update(self):
        modified_config = deepcopy(self.config_widget.current_config)
        modified_config["port"] = "COM99"
        self.mock_node.current_config = modified_config
        self.mock_node.signal_received.emit(modified_config, self.mock_node.current_schema)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    app.setStyle("Fusion")
    window = DemoWindow()
    window.show()
    sys.exit(app.exec())

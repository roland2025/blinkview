# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pyqtgraph as pg
from pyqtgraph import GraphicsLayoutWidget
import numpy as np
from PySide6.QtWidgets import QWidget, QVBoxLayout, QToolBar
from PySide6.QtCore import QTimer

from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from blinkview.ui.gui_context import GUIContext
from blinkview.utils.log_filter import LogFilter


class TelemetryPlotter(QWidget):
    def __init__(self, gui_context, tab_name, module: str, max_points=10000, parent=None):
        super().__init__(parent)
        self.gui_context = gui_context
        self.tab_name = tab_name
        self.max_points = max_points

        # Toggle State
        self.is_split = False

        print(f"[TelemetryPlotter] resolving module '{module}'")
        self.filtered_module = self._resolve_module(module)
        print(f"[TelemetryPlotter] resolved module '{module}' -> '{self.filtered_module}'")

        # Main Layout
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)  # Keep toolbar flush with the graph

        # Toolbar Setup
        self.toolbar = QToolBar()
        self.toolbar.setMovable(False)  # Keep it locked at the top
        self.layout.addWidget(self.toolbar)

        # Add Split Toggle Action
        # Making it checkable gives the user visual feedback (button stays pressed)
        self.split_action = self.toolbar.addAction("Split View")
        self.split_action.setCheckable(True)
        self.split_action.triggered.connect(self.set_split_mode)

        # Optional: Add a separator or clear button
        self.toolbar.addSeparator()
        self.toolbar.addAction("Clear", self.clear)

        # Setup Graphics Layout (allows multiple plots in one widget)
        # This replaces PlotWidget
        self.graph_view = pg.GraphicsLayoutWidget()
        self.layout.addWidget(self.graph_view)

        # Buffers & Tracking
        self.plots = []  # List of pg.PlotItem (the actual graph boxes)
        self.curves = []  # List of pg.PlotDataItem (the lines)
        self.x_data = np.zeros(self.max_points)
        self.y_data = None  # Initialized in _init_channels as np.zeros((max_points, num_vals))
        self.ptr = 0
        self.num_channels = 0

        # Registration & Data Loading
        self.gui_context.register_log_target(self)
        self.set_split_mode(self.is_split)
        self.load_history()

    def _init_channels(self, num_channels):
        """Initializes data buffers and triggers the UI build."""
        self.num_channels = num_channels
        self.y_data = np.zeros((self.max_points, num_channels))

        # Delegate the creation of PlotItems and PlotDataItems to the split logic
        self.set_split_mode(self.is_split)

    def process_log_batch(self, batch: list[LogRow], load_history=False):
        target_mod = self.filtered_module

        # 1. Extraction: Get timestamp and the WHOLE list of values
        extracted = [
            (row.timestamp, row.get_values())
            for row in batch
            if row.module == target_mod and row.get_values()
        ]

        if not extracted:
            return

        # 2. Setup channels on first valid data
        first_vals = extracted[0][1]
        if self.y_data is None:
            self._init_channels(len(first_vals))

        # Convert to numpy
        new_times = np.array([t for t, v in extracted], dtype=float)
        # Ensure all rows have the same number of columns as our buffer
        new_values = np.array([v[:self.num_channels] for t, v in extracted], dtype=float)
        num_new = new_times.size

        # 3. Buffer Management (2D Rolling)
        if self.ptr + num_new <= self.max_points:
            self.x_data[self.ptr: self.ptr + num_new] = new_times
            self.y_data[self.ptr: self.ptr + num_new, :] = new_values
            self.ptr += num_new
        else:
            shift = (self.ptr + num_new) - self.max_points
            self.x_data[:-shift] = self.x_data[shift:]
            self.y_data[:-shift, :] = self.y_data[shift:, :]

            self.x_data[-num_new:] = new_times
            self.y_data[-num_new:, :] = new_values
            self.ptr = self.max_points

        # 4. Visual Update
        # x_slice = self.x_data[:self.ptr]
        # for i, curve in enumerate(self.curves):
        #     curve.setData(x_slice, self.y_data[:self.ptr, i])

        self._update_plots()

    def clear(self):
        self.x_data.fill(0)
        if self.y_data is not None:
            self.y_data.fill(0)
        self.ptr = 0
        for curve in self.curves:
            curve.setData([], [])

    def _resolve_module(self, mod_identifier):
        if not mod_identifier or not isinstance(mod_identifier, str): return None
        try:
            dev_name, mod_name = mod_identifier.split('.', 1)
            # Use id_registry from gui_context as established earlier
            return self.gui_context.id_registry.get_device(dev_name).get_module(mod_name)
        except Exception:
            return None

    def load_history(self):
        """Special one-time call for the initial historical load."""
        print(f"Loading history for '{self.filtered_module}'")
        try:
            log_filter = LogFilter(self.gui_context.id_registry, filtered_module=self.filtered_module)
            history = self.gui_context.registry.central.get_rows(
                log_filter,
                total=self.max_points
            )
            # print(history)
            if not history:
                return
            print(f"Loaded {len(history)} log entries for module '{history[0].module}' during initialization.")
            self.process_log_batch(history, load_history=True)
        finally:
            pass

    def get_state(self):
        return {"module": self.filtered_module.name_with_device()}

    def set_split_mode(self, split: bool):
        self.is_split = split
        if self.num_channels == 0:
            return

        visibility_states = [c.isVisible() for c in self.curves]
        if not visibility_states and self.num_channels > 0:
            visibility_states = [True] * self.num_channels

        # 1. Clear the current layout and curve list
        self.graph_view.clear()
        self.plots = []
        self.curves = []

        colors = ['y', 'g', 'c', 'm', 'r', 'b']

        if not self.is_split:
            # COMBINED: One plot, all curves inside
            p = self.graph_view.addPlot(axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
            p.addLegend()
            self.plots.append(p)
            for i in range(self.num_channels):
                curve = p.plot(pen=colors[i % len(colors)], name=f"Val {i}")
                # Re-apply visibility
                if i < len(visibility_states):
                    curve.setVisible(visibility_states[i])
                self.curves.append(curve)
        else:
            # SPLIT: Multiple plots, one curve each
            for i in range(self.num_channels):
                # addPlot(row, col) - putting each on a new row
                p = self.graph_view.addPlot(row=i, col=0, axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
                # Sync x-axes so zooming one zooms all
                if i > 0:
                    p.setXLink(self.plots[0])

                self.plots.append(p)
                curve = p.plot(pen=colors[i % len(colors)], name=f"Val {i}")
                # Re-apply visibility
                if i < len(visibility_states):
                    curve.setVisible(visibility_states[i])
                    # If hidden, you might also want to hide the whole plot box in split mode
                    p.setVisible(visibility_states[i])
                self.curves.append(curve)

        # Trigger a redraw of existing data
        self._update_plots()

    def _update_plots(self):
        """Helper to push buffer data to the curves."""
        if self.ptr == 0: return
        x_slice = self.x_data[:self.ptr]
        for i, curve in enumerate(self.curves):
            curve.setData(x_slice, self.y_data[:self.ptr, i])

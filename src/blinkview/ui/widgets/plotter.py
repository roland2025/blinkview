# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo
from dataclasses import dataclass
from typing import List, Optional

import pyqtgraph as pg
from PySide6.QtGui import QAction
from pyqtgraph import GraphicsLayoutWidget
import numpy as np
from PySide6.QtWidgets import QWidget, QVBoxLayout, QToolBar, QMenu
from PySide6.QtCore import QTimer

from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from blinkview.ui.gui_context import GUIContext
from blinkview.utils.log_filter import LogFilter


@dataclass
class SeriesContainer:
    """The permanent record of a data channel."""
    index: int
    name: str
    color: str
    visible: bool = True
    # These are ephemeral; they get replaced when the UI layout changes
    curve: Optional[pg.PlotDataItem] = None
    plot_item: Optional[pg.PlotItem] = None


class TelemetryPlotter(QWidget):
    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)
        self.gui_context: GUIContext = gui_context
        self.max_points = self.gui_context.settings.get('plot.max_points', 10000)

        self.tab_name: str = ""
        self.is_split: bool = False
        self.module: Optional[ModuleIdentity] = None

        # Buffers
        self.x_data = np.zeros(self.max_points)
        self.y_data = None
        self.ptr = 0

        # New Single Source of Truth for Series
        self.series_list: List[SeriesContainer] = []

        self._set_defaults()

        if state:
            self.restore(state)

        # UI Setup
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        self.toolbar = QToolBar()
        self.layout.addWidget(self.toolbar)

        self.split_action = self.toolbar.addAction("Split View")
        self.split_action.setCheckable(True)
        self.split_action.setChecked(self.is_split)
        self.split_action.triggered.connect(self.set_split_mode)

        self.channel_btn = self.toolbar.addAction("Channels")
        self.channel_btn.triggered.connect(self._show_channel_menu)

        self.toolbar.addSeparator()
        self.toolbar.addAction("Clear", self.clear)

        self.graph_view = pg.GraphicsLayoutWidget()
        self.layout.addWidget(self.graph_view)

        self.gui_context.register_log_target(self)
        self.load_history()

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__
        self.is_split = False

    def restore(self, state: dict):
        print(f"[TelemetryPlotter] restoring state '{state}'")
        self.tab_name = state.get("tab_name", self.tab_name)
        self.is_split = state.get("is_split", self.is_split)

        module_name = state["module"]

        print(f"[TelemetryPlotter] resolving module '{module_name}'")
        self.module = self._resolve_module(state["module"])
        print(f"[TelemetryPlotter] resolved module '{module_name}' -> '{self.module}'")

        self.clear()
        series = state.get("series", [])
        for s in series:
            self.series_list.append(SeriesContainer(s["index"], s["name"], s["color"], s["visible"]))

    def _init_channels(self, num_channels: int):
        """Called exactly once when data first arrives."""
        self.y_data = np.zeros((self.max_points, num_channels))

        if not self.series_list:
            colors = ['y', 'g', 'c', 'm', 'r', 'b']
            for i in range(num_channels):
                self.series_list.append(SeriesContainer(
                    index=i,
                    name=f"{self.module.short_name} {i}" if num_channels > 1 else self.module.short_name,
                    color=colors[i % len(colors)],
                    visible=True  # Default to visible
                ))

        # Now build the UI for the first time
        self.set_split_mode(self.is_split)

    @property
    def num_channels(self) -> int:
        """Dynamically get channel count from the buffer shape."""
        return self.y_data.shape[1] if self.y_data is not None else 0

    def process_log_batch(self, batch: list[LogRow], load_history=False):
        target_mod = self.module

        # 1. Extraction: Get timestamp and the WHOLE list of values
        extracted = [
            (row.timestamp, row.get_values())
            for row in batch
            if row.module == target_mod and row.get_values()
        ]

        if not extracted:
            return

        # 2. Setup channels on first valid data
        if self.y_data is None:
            first_vals = extracted[0][1]
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
        print(f"Loading history for '{self.module}'")
        try:
            log_filter = LogFilter(self.gui_context.id_registry, filtered_module=self.module)
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
        series = []
        for s in self.series_list:
            series.append({
                "index": s.index,
                "name": s.name,
                "color": s.color,
                "visible": s.visible,
            })
        return {
            "module": self.module.name_with_device(),
            "is_split": self.is_split,
            "series": series,
        }

    def set_split_mode(self, split: bool):
        self.is_split = split
        if not self.series_list:
            return

        # 1. Clear the old UI objects from the view
        self.graph_view.clear()

        # 2. Setup the new Layout
        shared_plot = None
        legend = None
        if not self.is_split:
            shared_plot = self.graph_view.addPlot(axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
            if self.num_channels > 1:
                legend = shared_plot.addLegend()

        # 3. Re-link existing containers to new UI widgets
        for i, s in enumerate(self.series_list):
            if self.is_split:
                p = self.graph_view.addPlot(row=i, col=0, axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
                if i > 0:
                    p.setXLink(self.series_list[0].plot_item)
                s.plot_item = p
            else:
                s.plot_item = shared_plot

            # Create new curve and store it back in the persistent container
            s.curve = s.plot_item.plot(pen=s.color, name=s.name)

            # Re-apply the container's visibility state to the new objects
            s.curve.setVisible(s.visible)
            if self.is_split:
                s.plot_item.setVisible(s.visible)

        if legend:
            self._setup_legend_callbacks(legend)

        self._update_plots()

    def _update_plots(self):
        """Pushes data only to containers that currently have a valid curve."""
        if self.ptr == 0:
            return

        x_slice = self.x_data[:self.ptr]
        y_slice = self.y_data[:self.ptr]

        for s in self.series_list:
            # Check if container is visible AND has a valid UI handle
            if s.visible and s.curve is not None:
                s.curve.setData(x_slice, y_slice[:, s.index])

    def clear(self):
        self.x_data.fill(0)
        if self.y_data is not None:
            self.y_data.fill(0)
        self.ptr = 0
        for series in self.series_list:
            series.curve.setData([], [])

    def closeEvent(self, event):
        """Cleanup registration when the widget is closed."""
        self.gui_context.deregister_log_target(self)
        super().closeEvent(event)

    def _show_channel_menu(self):
        """Generates a popup menu to toggle specific series visibility."""
        menu = QMenu(self)
        for series in self.series_list:
            action = QAction(series.name, menu, checkable=True)
            action.setChecked(series.visible)
            # Use a lambda with default argument to capture current 'series'
            action.triggered.connect(lambda checked, s=series: self.toggle_series(s, checked))
            menu.addAction(action)

        # Position menu under the toolbar button
        button_widget = self.toolbar.widgetForAction(self.channel_btn)
        menu.exec(button_widget.mapToGlobal(button_widget.rect().bottomLeft()))

    def _setup_legend_callbacks(self, legend):
        """Connects legend clicks to our toggle_series logic."""
        for sample, label in legend.items:
            # The 'label' is a LabelItem; its 'item' is the underlying GraphicsWidget
            # We override the click event to trigger our custom logic
            for series in self.series_list:
                if series.name == label.text:
                    # Capture the series in a closure
                    label.mouseClickEvent = lambda ev, s=series: self.toggle_series(s, not s.visible)
                    sample.mouseClickEvent = lambda ev, s=series: self.toggle_series(s, not s.visible)

    def toggle_series(self, series: SeriesContainer, visible: bool):
        """The single source of truth for toggling visibility."""
        series.visible = visible

        if series.curve:
            series.curve.setVisible(visible)

        if self.is_split and series.plot_item:
            series.plot_item.setVisible(visible)

        # Force an auto-range so the plot area shrinks/grows to fit visible data
        if series.plot_item:
            series.plot_item.autoRange()

    def rename_channel(self, index: int, new_name: str):
        """Example of why persistence is great: you just update the container."""
        if 0 <= index < len(self.series_list):
            s = self.series_list[index]
            s.name = new_name
            # If the UI exists, update the legend/label immediately
            if s.curve:
                s.curve.opts['name'] = new_name
                # Note: pyqtgraph legends might need a refresh call here

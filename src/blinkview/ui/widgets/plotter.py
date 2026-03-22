# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from dataclasses import dataclass
from typing import List, Optional

import pyqtgraph as pg
from PySide6.QtGui import QAction, QColor
from pyqtgraph import GraphicsLayoutWidget, LinearRegionItem
import numpy as np
from PySide6.QtWidgets import QWidget, QVBoxLayout, QToolBar, QMenu, QLabel, QComboBox
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
    curve: Optional[pg.PlotDataItem] = None  # Main View
    overview_curve: Optional[pg.PlotDataItem] = None  # Overview View
    plot_item: Optional[pg.PlotItem] = None  # Main View Plot


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

        self.overview_plot: Optional[pg.PlotItem] = None
        self.region: Optional[LinearRegionItem] = None
        self.is_auto_scroll = True  # Keep window on the "last 10 mins"
        self._is_system_updating = False

        self.view_duration_text = "60s"  # seconds
        self.view_duration = 0

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
        self.toolbar.addWidget(QLabel(" Window: "))
        self.duration_combo = QComboBox()
        self.duration_combo.setEditable(True)
        self.duration_combo.addItems(["10s", "30s", "60s", "5m", "10m", "30m", "1h"])

        idx = self.duration_combo.findText(self.view_duration_text)
        if idx >= 0:
            self.duration_combo.setCurrentIndex(idx)
        else:
            # Fallback if someone changes the list but forgets the default
            self.duration_combo.setCurrentText(self.view_duration_text)

        self.duration_combo.currentTextChanged.connect(self._on_duration_changed)
        self.toolbar.addWidget(self.duration_combo)

        self.toolbar.addAction("Clear", self.clear)

        self.toolbar.addSeparator()

        # Add an Auto-Scroll toggle to the toolbar
        self.autoscroll_action = self.toolbar.addAction("Auto-Scroll: ON")
        self.autoscroll_action.setCheckable(True)
        self.autoscroll_action.setChecked(True)
        self.autoscroll_action.triggered.connect(self.set_autoscroll)

        self.graph_view = pg.GraphicsLayoutWidget()
        self.layout.addWidget(self.graph_view)

        self.gui_context.register_log_target(self)
        self.load_history()

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__
        self.is_split = False
        self.is_auto_scroll = True
        self.is_system_updating = True
        self.view_duration_text = "60s"
        self.view_duration = 60

    def restore(self, state: dict):
        print(f"[TelemetryPlotter] restoring state '{state}'")
        self.tab_name = state.get("tab_name", self.tab_name)
        self.is_split = state.get("is_split", self.is_split)

        self.view_duration_text = state.get("view_duration", self.view_duration_text)
        self.view_duration = self._parse_duration(self.view_duration_text)

        module_name = state["module"]

        print(f"[TelemetryPlotter] resolving module '{module_name}'")
        self.module = self._resolve_module(state["module"])
        print(f"[TelemetryPlotter] resolved module '{module_name}' -> '{self.module}'")

        self.clear()
        series = state.get("series", [])
        for i, s in enumerate(series):
            self.series_list.append(SeriesContainer(s["index"], s["name"], self.get_color(i).name(), s["visible"]))

    def _parse_duration(self, text: str) -> Optional[int]:
        """Parses strings like '10s', '5m', '2h' into total seconds."""
        text = text.lower().strip()
        # Regex to capture the number and the unit suffix
        match = re.match(r"^(\d*\.?\d+)\s*([smh]?)$", text)
        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2)

        if unit == 'm':
            return int(value * 60)
        elif unit == 'h':
            return int(value * 3600)
        else:  # Default to seconds if 's' or no unit provided
            return int(value)

    def _on_duration_changed(self, text: str):
        seconds = self._parse_duration(text)
        if seconds is None or seconds <= 0:
            return

        self.view_duration = seconds

        if self.ptr > 0:
            if self.is_auto_scroll:
                # Snap to the live edge
                now = self.x_data[self.ptr - 1]
                self._apply_view_range(now - self.view_duration, now)
            else:
                # Stay where we are, but expand/shrink the window to the left
                _, current_max_x = self.region.getRegion()
                self._apply_view_range(current_max_x - self.view_duration, current_max_x)

    def _init_channels(self, num_channels: int):
        """Called exactly once when data first arrives."""
        self.y_data = np.zeros((self.max_points, num_channels))

        if not self.series_list:
            for i in range(num_channels):
                self.series_list.append(SeriesContainer(
                    index=i,
                    name=f"{self.module.short_name} {i}" if num_channels > 1 else self.module.short_name,
                    color=self.get_color(i).name(),
                    visible=True  # Default to visible
                ))

        # Now build the UI for the first time
        self.set_split_mode(self.is_split)

    def get_color(self, i: int) -> QColor:
        return QColor.fromHsv((120 + i * 80) % 360, 255, 255)

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
            "view_duration": self.duration_combo.currentText(),
        }

    def set_split_mode(self, split: bool):
        self.is_split = split
        if not self.series_list:
            return

        self.graph_view.clear()

        # Setup Overview Plot with DateAxis
        self.overview_plot = self.graph_view.addPlot(
            row=0,
            col=0,
            # title="History Overview",
            axisItems={'bottom': pg.DateAxisItem(orientation='bottom')}
        )
        self.overview_plot.setMaximumHeight(120)

        # Setup Main Plots
        shared_plot = None
        legend = None
        if not self.is_split:
            shared_plot = self.graph_view.addPlot(row=1, col=0, axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
            if self.num_channels > 1:
                legend = shared_plot.addLegend()

        for i, s in enumerate(self.series_list):
            if self.is_split:
                p = self.graph_view.addPlot(row=i+1, col=0, axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
                if i > 0:
                    p.setXLink(self.series_list[0].plot_item)
                s.plot_item = p
            else:
                s.plot_item = shared_plot

            s.curve = s.plot_item.plot(pen=s.color, name=s.name)
            s.curve.setVisible(s.visible)
            if self.is_split:
                s.plot_item.setVisible(s.visible)

        if legend:
            self._setup_legend_callbacks(legend)


        # Setup Region with Smart Initialization
        current_now = self.x_data[self.ptr - 1] if self.ptr > 0 else 60
        start_region = current_now - self.view_duration

        self.region = LinearRegionItem([start_region, current_now])
        self.region.setZValue(10)
        self.overview_plot.addItem(self.region)

        for s in self.series_list:
            s.overview_curve = self.overview_plot.plot(pen=s.color)
            s.overview_curve.setVisible(s.visible)

        # --- SYNC LOGIC ---
        def update_main_from_region():
            # 1. Break the infinite feedback loop
            if self._is_system_updating:
                return

            minX, maxX = self.region.getRegion()

            # 2. Auto-Scroll Snap Logic
            if self.ptr > 0:
                latest_time = self.x_data[self.ptr - 1]
                snap_tolerance = self.view_duration * 0.05

                if maxX >= latest_time - snap_tolerance:
                    self.set_autoscroll(True)
                else:
                    self.set_autoscroll(False)

            # 3. Apply the range to all main plots
            for s in self.series_list:
                if s.plot_item:
                    s.plot_item.setXRange(minX, maxX, padding=0)

        def update_region_from_main(window, viewRange):
            # 1. Break the infinite feedback loop
            if self._is_system_updating:
                return

            rlow, rhigh = viewRange[0]

            # 2. Auto-Scroll Snap Logic
            if self.ptr > 0:
                latest_time = self.x_data[self.ptr - 1]
                snap_tolerance = self.view_duration * 0.05

                if rhigh >= latest_time - snap_tolerance:
                    self.set_autoscroll(True)
                else:
                    self.set_autoscroll(False)

            # 3. Safely update the overview region
            self._is_system_updating = True  # Lock
            self.region.setRegion([rlow, rhigh])  # Triggers update_main_from_region, which will safely return
            self._is_system_updating = False  # Unlock

        # Connect the signals once and leave them alone
        self.region.sigRegionChanged.connect(update_main_from_region)

        main_p = self.series_list[0].plot_item
        if main_p:
            main_p.sigRangeChanged.connect(update_region_from_main)

        self._update_plots()

    def _update_plots(self):
        if self.ptr == 0: return

        x_slice = self.x_data[:self.ptr]
        y_slice = self.y_data[:self.ptr]
        current_time = x_slice[-1]

        # ALWAYS update data so unhiding is instantaneous
        for s in self.series_list:
            if s.curve:
                s.curve.setData(x_slice, y_slice[:, s.index])
            if s.overview_curve:
                s.overview_curve.setData(x_slice, y_slice[:, s.index])

        # Handle the sliding window for Auto-Scroll
        if self.is_auto_scroll:
            self._apply_view_range(current_time - self.view_duration, current_time)

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

        # 1. Toggle main curve visibility
        if series.curve:
            series.curve.setVisible(visible)

        # 2. Toggle overview curve visibility (The small history lines)
        if series.overview_curve:
            series.overview_curve.setVisible(visible)

        # 3. Handle Split View Layout
        if self.is_split and series.plot_item:
            series.plot_item.setVisible(visible)

        # 4. Trigger a Y-axis rescale
        # If unhiding, we want the plot to jump to the data's scale immediately
        if visible and series.plot_item:
            series.plot_item.autoRange()

        if self.overview_plot:
            # We tell the overview plot to fit its view to currently visible items
            self.overview_plot.autoRange()

        # self.graph_view.ci.layout.activate()

    def rename_channel(self, index: int, new_name: str):
        """Example of why persistence is great: you just update the container."""
        if 0 <= index < len(self.series_list):
            s = self.series_list[index]
            s.name = new_name
            # If the UI exists, update the legend/label immediately
            if s.curve:
                s.curve.opts['name'] = new_name
                # Note: pyqtgraph legends might need a refresh call here

    def set_autoscroll(self, enabled: bool):
        """Updates the auto-scroll state and UI."""
        self.is_auto_scroll = enabled
        self.autoscroll_action.setChecked(enabled)
        self.autoscroll_action.setText(f"Auto-Scroll: {'ON' if enabled else 'OFF'}")

        # If turned back on, force a snap to the present immediately
        if enabled:
            self._update_plots()

    def _apply_view_range(self, start_time: float, end_time: float):
        """Safely updates both the region and the main plots without triggering feedback loops."""
        if not self.region:
            return

        self._is_system_updating = True
        try:
            # 1. Update the overview slider
            self.region.setRegion([start_time, end_time])

            # 2. Update the main plots (the bottom views)
            for s in self.series_list:
                if s.plot_item:
                    s.plot_item.setXRange(start_time, end_time, padding=0)
        finally:
            self._is_system_updating = False

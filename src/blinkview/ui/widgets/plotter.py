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
class ModuleBuffer:
    """Holds the rolling buffers for a specific module."""
    x_data: np.ndarray
    y_data: Optional[np.ndarray] = None
    ptr: int = 0
    num_channels: int = 0


@dataclass
class SeriesContainer:
    """The permanent record of a data channel."""
    module: ModuleIdentity  # NEW: Track which module this series belongs to
    index: int
    name: str
    color: str
    visible: bool = True
    curve: Optional[pg.PlotDataItem] = None
    overview_curve: Optional[pg.PlotDataItem] = None
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
        self.modules: List[ModuleIdentity] = []
        self.buffers: dict[ModuleIdentity, ModuleBuffer] = {}

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
        self.toolbar.addWidget(QLabel("Window:"))
        self.duration_combo = QComboBox()
        self.duration_combo.setMinimumWidth(60)
        self.duration_combo.setEditable(True)
        self.duration_combo.addItems(["10s", "30s", "60s", "5m", "10m", "30m", "1h"])

        idx = self.duration_combo.findText(self.view_duration_text)
        if idx >= 0:
            self.duration_combo.setCurrentIndex(idx)
        else:
            # Fallback if someone changes the list but forgets the default
            self.duration_combo.setCurrentText(self.view_duration_text)

        self.duration_combo.currentTextChanged.connect(self._on_duration_changed)

        self.toolbar.addAction("Clear", self.clear)

        self.toolbar.addSeparator()

        # Add an Auto-Scroll toggle to the toolbar
        self.autoscroll_action = self.toolbar.addAction("Auto-Scroll: ON")
        self.autoscroll_action.setCheckable(True)
        self.autoscroll_action.setChecked(True)
        self.autoscroll_action.triggered.connect(self.set_autoscroll)

        self.toolbar.addWidget(self.duration_combo)

        self.graph_view = pg.GraphicsLayoutWidget()
        self.layout.addWidget(self.graph_view)

        self.setAcceptDrops(True)

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

        # Restore multiple modules
        self.modules = self.gui_context.id_registry.resolve_modules(state.get("modules", []))

        self.clear()

        series = state.get("series", [])
        for i, s in enumerate(series):
            mod = self.gui_context.id_registry.resolve_module(s.get("module"))
            if mod:
                self.series_list.append(SeriesContainer(
                    module=mod,
                    index=s["index"],
                    name=s["name"],
                    color=self.get_color(i).name(),
                    visible=s["visible"]
                ))

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
        latest_now = self._get_latest_timestamp()  # Use the helper

        if latest_now > 0:  # Check if we actually have data
            if self.is_auto_scroll:
                # Snap to the live edge
                self._apply_view_range(latest_now - self.view_duration, latest_now)
            else:
                # Stay where we are, but expand/shrink the window to the left
                _, current_max_x = self.region.getRegion()
                self._apply_view_range(current_max_x - self.view_duration, current_max_x)

    def _init_module_channels(self, module: ModuleIdentity, num_channels: int):
        """Called exactly once per module when data first arrives."""
        buf = self.buffers[module]
        buf.y_data = np.zeros((self.max_points, num_channels))
        buf.num_channels = num_channels

        # If we didn't restore series from state, generate them now
        if not any(s.module == module for s in self.series_list):
            # Calculate a global offset for colors so modules don't look identical
            existing_series_count = len(self.series_list)
            for i in range(num_channels):
                self.series_list.append(SeriesContainer(
                    module=module,
                    index=i,
                    name=f"{module.short_name} {i}" if num_channels > 1 else module.short_name,
                    color=self.get_color(existing_series_count + i).name(),
                    visible=True
                ))

        # Re-trigger UI building
        self.set_split_mode(self.is_split)

    def get_color(self, i: int) -> QColor:
        return QColor.fromHsv((120 + i * 80) % 360, 255, 255)

    @property
    def total_series_count(self) -> int:
        return len(self.series_list)

    def process_log_batch(self, batch: list[LogRow], load_history=False):
        updated = False

        for module in self.modules:
            # Extraction per module
            extracted = [
                (row.timestamp, row.get_values())
                for row in batch
                if row.module == module and row.get_values()
            ]

            if not extracted:
                continue

            try:
                buf = self.buffers[module]
            except KeyError:
                buf = ModuleBuffer(x_data=np.zeros(self.max_points))
                self.buffers[module] = buf
                self._init_module_channels(module, len(extracted[0][1]))

            # Convert to numpy
            new_times = np.array([t for t, v in extracted], dtype=float)
            new_values = np.array([v[:buf.num_channels] for t, v in extracted], dtype=float)
            num_new = new_times.size

            # Buffer Management (2D Rolling)
            if buf.ptr + num_new <= self.max_points:
                buf.x_data[buf.ptr: buf.ptr + num_new] = new_times
                buf.y_data[buf.ptr: buf.ptr + num_new, :] = new_values
                buf.ptr += num_new
            else:
                shift = (buf.ptr + num_new) - self.max_points
                buf.x_data[:-shift] = buf.x_data[shift:]
                buf.y_data[:-shift, :] = buf.y_data[shift:, :]

                buf.x_data[-num_new:] = new_times
                buf.y_data[-num_new:, :] = new_values
                buf.ptr = self.max_points

            updated = True

        # Visual Update only if we ingested something
        if updated:
            self._update_plots()

    def _load_module_history(self, module: ModuleIdentity):
        """Helper to load history for a single module (used during drop)."""
        print(f"Loading history for newly dropped module: '{module}'")
        log_filter = LogFilter(self.gui_context.id_registry, filtered_module=module)
        history = self.gui_context.registry.central.get_rows(
            log_filter,
            total=self.max_points
        )
        if history:
            self.process_log_batch(history, load_history=True)

    def load_history(self):
        """Load history for all configured modules (used on startup)."""
        for module in self.modules:
            self._load_module_history(module)

    def get_state(self):
        series = []
        for s in self.series_list:
            series.append({
                "module": s.module.name_with_device(),
                "index": s.index,
                "name": s.name,
                "color": s.color,
                "visible": s.visible,
            })
        return {
            "modules": [m.name_with_device() for m in self.modules],
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
            shared_plot.setTitle(None)
            if self.total_series_count > 1:
                legend = shared_plot.addLegend()

        for i, s in enumerate(self.series_list):
            if self.is_split:
                p = self.graph_view.addPlot(row=i+1, col=0, axisItems={'bottom': pg.DateAxisItem(orientation='bottom')})
                p.setTitle(f'<span style="color: {s.color}; font-weight: bold;">{s.name}</span>')
                # p.setTitle(s.name)
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
        latest_now = self._get_latest_timestamp()
        current_now = latest_now if latest_now > 0 else 60
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

            # 3. Apply the range to all main plots
            for s in self.series_list:
                if s.plot_item:
                    s.plot_item.setXRange(minX, maxX, padding=0)

        def update_region_from_main(window, viewRange):
            # 1. Break the infinite feedback loop
            if self._is_system_updating:
                return

            rlow, rhigh = viewRange[0]

            # Safely update the overview region
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
        latest_time = 0.0

        for s in self.series_list:
            buf = self.buffers.get(s.module)
            if not buf or buf.ptr == 0:
                continue

            x_slice = buf.x_data[:buf.ptr]
            y_slice = buf.y_data[:buf.ptr]

            # Track the absolute latest time across all modules for auto-scrolling
            if x_slice[-1] > latest_time:
                latest_time = x_slice[-1]

            if s.curve:
                s.curve.setData(x_slice, y_slice[:, s.index])
            if s.overview_curve:
                s.overview_curve.setData(x_slice, y_slice[:, s.index])

        if self.is_auto_scroll and latest_time > 0:
            self._apply_view_range(latest_time - self.view_duration, latest_time)

    def clear(self):
        for buf in self.buffers.values():
            buf.x_data.fill(0)
            if buf.y_data is not None:
                buf.y_data.fill(0)
            buf.ptr = 0

        for series in self.series_list:
            if series.curve:
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

    def _get_latest_timestamp(self) -> float:
        """Finds the most recent timestamp across all active module buffers."""
        latest = 0.0
        for buf in self.buffers.values():
            if buf.ptr > 0:
                ts = buf.x_data[buf.ptr - 1]
                if ts > latest:
                    latest = ts
        return latest

    def dragEnterEvent(self, event):
        # We accept the drop if the mime data contains text
        # (Assuming your sidebar drags the module identifier as text)
        if event.mimeData().hasText():
            event.acceptProposedAction()

    def dropEvent(self, event):
        mod_identifier = event.mimeData().text()
        module = self.gui_context.id_registry.resolve_module(mod_identifier)

        if not module:
            print(f"Cannot resolve module: {mod_identifier}")
            return

        if module in self.modules:
            print(f"Module {module.short_name} already in plot.")
            return

        # 1. Add to our tracking list
        self.modules.append(module)

        # 2. Immediately try to load existing history for just this module
        self._load_module_history(module)

        # 3. Accept the action
        event.acceptProposedAction()

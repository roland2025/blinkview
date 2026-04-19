# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, List, Optional, Union

import numpy as np

from blinkview.core import dtypes
from blinkview.core.buffers import ModuleBuffer
from blinkview.core.dtypes import SEQ_NONE
from blinkview.core.numpy_log import (
    allocate_discovery_workspace,
    fetch_telemetry_arrays,
    get_telemetry_anchor,
)
from blinkview.ops.telemetry import minmax_downsample_inplace, slice_and_downsample_linear

if TYPE_CHECKING:
    import pyqtgraph as pg

from qtpy.QtGui import QAction, QColor
from qtpy.QtWidgets import QComboBox, QLabel, QMenu, QSizePolicy, QToolBar, QVBoxLayout, QWidget

from blinkview.core.device_identity import ModuleIdentity
from blinkview.ui.gui_context import GUIContext


@dataclass
class SeriesContainer:
    """The permanent record of a data channel."""

    module: ModuleIdentity  # NEW: Track which module this series belongs to
    index: int
    name: str
    color: str
    visible: bool = True
    curve: Optional["pg.PlotDataItem"] = None
    overview_curve: Optional["pg.PlotDataItem"] = None
    plot_item: Optional["pg.PlotItem"] = None
    last_seq: dtypes.SEQ_TYPE = SEQ_NONE

    main_x: List[np.ndarray] = field(default_factory=list)
    main_y: List[np.ndarray] = field(default_factory=list)
    ov_x: List[np.ndarray] = field(default_factory=list)
    ov_y: List[np.ndarray] = field(default_factory=list)
    buf_idx: int = 0  # Toggle between 0 and 1

    _last_t_min: float = 0.0
    _last_t_max: float = 0.0
    _last_bins: int = 0

    _last_y_min: float = 0.0
    _last_y_max: float = 0.0
    _has_y_range: bool = False

    def __post_init__(self):
        # Placeholder size—actual allocation usually happens
        # when max_points is known, but let's assume 50k
        for _ in range(2):
            self.main_x.append(np.zeros(50000, dtype=dtypes.PLOT_TS_TYPE))
            self.main_y.append(np.zeros(50000, dtype=dtypes.PLOT_VAL_TYPE))
            self.ov_x.append(np.zeros(10000, dtype=dtypes.PLOT_TS_TYPE))  # Overview is small
            self.ov_y.append(np.zeros(10000, dtype=dtypes.PLOT_VAL_TYPE))


class TelemetryPlotter(QWidget):
    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)

        import pyqtgraph as pg

        # Store references so other methods can use them easily
        self._pg = pg

        # pg.setConfigOptions(useOpenGL=True)
        pg.setConfigOptions(antialias=False)
        pg.setConfigOptions(enableExperimental=True)

        self.gui_context: GUIContext = gui_context

        self.logger = None
        self.logger_apply = None
        self.logger_update = None
        # self.logger = gui_context.logger.child(f"plotter_{id(self):x}")

        self.max_points = self.gui_context.settings.get("plot.max_points", 50000)

        self.tab_name: str = ""
        self.is_split: bool = False

        self.log_seq = SEQ_NONE
        self.latest_seq = SEQ_NONE

        self.plot_data_changed = False

        self._discovery_workspace = allocate_discovery_workspace()

        self._last_overview_update_ns = 0
        self._overview_update_interval_ns = 1_000_000_000  # in nanoseconds

        # Buffers
        self.modules: List[ModuleIdentity] = []
        self.buffers: dict[ModuleIdentity, ModuleBuffer] = {}

        # New Single Source of Truth for Series
        self.series_list: List[SeriesContainer] = []

        # Maps PlotItem instance -> dictionary of range state
        self.plot_range_states = {}

        self.overview_plot: Optional["pg.PlotItem"] = None
        self.region: Optional["pg.LinearRegionItem"] = None
        self.is_auto_scroll = True  # Keep window on the "last 10 mins"
        self._is_system_updating = False

        self.view_duration_text = "60s"  # seconds
        self.view_duration = 0
        self.show_overview = True

        self.update_freq_text = "1 Hz"
        self._update_interval_ns = self.gui_context.theme.ui_update_rate_ms * 1_000_000
        self._last_update_ns = 0

        self._last_region_update_ns = 0

        self._last_data_update_ns = 0
        self.data_update_interval_ns = 250_000_000

        self._set_defaults()

        if state:
            self.restore(state)

        # UI Setup
        self.layout = QVBoxLayout(self)
        self.layout.setContentsMargins(0, 0, 0, 0)
        self.layout.setSpacing(0)

        self.toolbar = QToolBar()
        self.layout.addWidget(self.toolbar)

        self.overview_action = self.toolbar.addAction("Overview")
        self.overview_action.setCheckable(True)
        self.overview_action.setChecked(self.show_overview)
        self.overview_action.triggered.connect(self.set_overview_visible)

        self.split_action = self.toolbar.addAction("Split View")
        self.split_action.setCheckable(True)
        self.split_action.setChecked(self.is_split)
        self.split_action.triggered.connect(self.set_split_mode)

        self.channel_btn = self.toolbar.addAction("Channels")
        self.channel_btn.triggered.connect(self._show_channel_menu)

        self.toolbar.addSeparator()
        self.toolbar.addAction("Reset view", self.reset_view)

        self.toolbar.addSeparator()

        # Add an Auto-Scroll toggle to the toolbar
        self.autoscroll_action = self.toolbar.addAction("Auto-Scroll: ON")
        self.autoscroll_action.setCheckable(True)
        self.autoscroll_action.setChecked(True)
        self.autoscroll_action.triggered.connect(self.set_autoscroll)

        # self.toolbar.addWidget(QLabel("Window:"))
        self.duration_combo = QComboBox()
        self.duration_combo.setMinimumWidth(60)
        self.duration_combo.setEditable(True)
        duration_items = ["0.1s", "0.5s", "1s", "2s", "5s", "10s", "30s", "60s", "5m", "10m", "30m", "1h"]
        self.duration_combo.addItems(reversed(duration_items))

        idx = self.duration_combo.findText(self.view_duration_text)
        if idx >= 0:
            self.duration_combo.setCurrentIndex(idx)
        else:
            # Fallback if someone changes the list but forgets the default
            self.duration_combo.setCurrentText(self.view_duration_text)

        self.duration_combo.currentTextChanged.connect(self._on_duration_changed)

        self.toolbar.addWidget(self.duration_combo)
        self.toolbar.addWidget(QLabel("@"))

        self.freq_combo = QComboBox()
        self.freq_combo.setMinimumWidth(65)
        freq_items = ["1 Hz", "2 Hz", "5 Hz", "10 Hz", "15 Hz", "20 Hz", "30 Hz", "60 Hz"]
        self.freq_combo.addItems(reversed(freq_items))

        # Initialize from state or default
        idx = self.freq_combo.findText(self.update_freq_text)
        self.freq_combo.setCurrentIndex(idx if idx >= 0 else 0)
        self.freq_combo.currentTextChanged.connect(self._on_freq_changed)

        self.toolbar.addWidget(self.freq_combo)

        spacer = QWidget()
        spacer.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.toolbar.addWidget(spacer)

        self.toolbar.addSeparator()

        self.toolbar.addAction("Clear", self.clear)

        self.graph_view = pg.GraphicsLayoutWidget()

        self.layout.addWidget(self.graph_view)

        self.setAcceptDrops(True)

        self.gui_context.add_updatable(self)

    def _set_defaults(self):
        self.tab_name = self.__class__.__name__
        self.is_split = False
        self.is_auto_scroll = True
        self.is_system_updating = True
        self.view_duration_text = "60s"
        self.view_duration = 60
        self.show_overview = True

        self.update_freq_text = "60 Hz"
        self._last_update_ns = 0

    def restore(self, state: dict):
        print(f"[TelemetryPlotter] restoring state '{state}'")
        self.tab_name = state.get("tab_name", self.tab_name)
        self.is_split = state.get("is_split", self.is_split)
        self.view_duration_text = state.get("view_duration", self.view_duration_text)
        self.view_duration = self._parse_duration(self.view_duration_text)
        self.show_overview = state.get("show_overview", True)

        self.update_freq_text = state.get("update_freq", "1 Hz")
        self._on_freq_changed(self.update_freq_text)

        # Restore multiple modules
        self.modules = self.gui_context.id_registry.resolve_modules(state.get("modules", []))

        if self.logger is None:
            module_name = self.modules[0].name if self.modules else "unknown"
            self.logger = self.gui_context.logger.child(f"plotter_{module_name}")
            self.logger_apply = self.logger.child("apply")
            self.logger_update = self.logger.child("update")

        self.clear()

        series = state.get("series", [])
        for i, s in enumerate(series):
            mod = self.gui_context.id_registry.resolve_module(s.get("module"))
            if mod:
                self.series_list.append(
                    SeriesContainer(
                        module=mod,
                        index=s["index"],
                        name=s["name"],
                        color=self.get_color(i).name(),
                        visible=s["visible"],
                    )
                )

    def _on_freq_changed(self, text: str):
        """Parses '30 Hz' into a nanosecond interval for the update gate."""
        match = re.search(r"(\d+)", text)
        if match:
            hz = int(match.group(1))
            self.update_freq_text = text
            self._update_interval_ns = 1_000_000_000 // hz

        self._update_axis_visibility()

    def reset_view(self):
        self.set_autoscroll(True)
        self.set_split_mode(self.is_split)

    def _parse_duration(self, text: str) -> Optional[float]:
        """Parses strings like '10s', '5m', '2h' into total seconds."""
        text = text.lower().strip()
        # Regex to capture the number and the unit suffix
        match = re.match(r"^(\d*\.?\d+)\s*([smh]?)$", text)
        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2)

        if unit == "m":
            return value * 60.0
        elif unit == "h":
            return value * 3600.0
        else:  # Default to seconds
            return value

    def _on_duration_changed(self, text: str):
        seconds = self._parse_duration(text)
        if seconds is None or seconds <= 0:
            return

        self.view_duration = seconds
        latest_now = self._get_latest_timestamp()

        if latest_now > 0:
            if self.is_auto_scroll:
                self._apply_view_range(latest_now - self.view_duration, latest_now)
            else:
                # --- FIX START ---
                # Safely get the current 'right edge' of the view
                if self.region:
                    _, current_max_x = self.region.getRegion()
                elif self.series_list and self.series_list[0].plot_item:
                    # Fallback: Get it directly from the plot's view range
                    _, current_max_x = self.series_list[0].plot_item.viewRange()[0]
                else:
                    current_max_x = latest_now
                # --- FIX END ---

                self._apply_view_range(current_max_x - self.view_duration, current_max_x)

            # Force redraw for the new duration
            self._update_plots()

    def _init_module_channels(self, module: ModuleIdentity, num_channels: int, anchor_seq: dtypes.SEQ_TYPE):
        """Called exactly once per module when data first arrives."""
        # 1. Handle the buffer creation here so it's centralized
        buf = ModuleBuffer(
            max_points=self.max_points,
            num_channels=num_channels,
            last_seq=anchor_seq,
        )
        self.buffers[module] = buf

        # 2. Setup the UI series list
        if not any(s.module == module for s in self.series_list):
            start_idx = len(self.series_list)
            for i in range(num_channels):
                self.series_list.append(
                    SeriesContainer(
                        module=module,
                        index=i,
                        name=f"{module.short_name} {i}" if num_channels > 1 else module.short_name,
                        color=self.get_color(start_idx + i).name(),
                        visible=True,
                    )
                )

        self.set_split_mode(self.is_split)
        return buf  # Return it so apply_updates can use it immediately

    def get_color(self, i: int) -> QColor:
        return QColor.fromHsv((120 + i * 80) % 360, 255, 255)

    @property
    def total_series_count(self) -> int:
        return len(self.series_list)

    def apply_updates(self, force: bool = False):
        registry = self.gui_context.registry
        now_ns_func = registry.now_ns
        now_ns = now_ns_func()

        # --- THROTTLE GATE ---
        # If we are significantly ahead of our target interval, bail early.
        # The 10ms deadzone ensures we don't miss a cycle due to tiny clock jitters.
        deadzone_ns = 10_000_000
        # BYPASS: Ignore the timer if a manual update was forced
        if not force and (now_ns - self._last_update_ns) < (self._update_interval_ns - deadzone_ns):
            return

        self._last_update_ns = now_ns

        updated = force
        log_pool = registry.central.log_pool
        array_pool = registry.system_ctx.array_pool

        # 1. Global High-Watermark (Quickest check to see if log moved at all)
        global_latest_seq = log_pool.latest_sequence()

        # Determine fetch throttle (0 delay if scrolling, 500ms if looking at history)
        fetch_throttle_ns = 0 if self.is_auto_scroll else 500_000_000

        max_points = self.max_points

        for module in self.modules:
            buf = self.buffers.get(module)

            # --- GATE 1: Visibility ---
            # If we have a buffer and series list, but nothing is visible, skip.
            # If buf is None, we must proceed to discovery.
            if buf is not None and len(self.series_list) > 0:
                if not any(s.visible for s in self.series_list if s.module == module):
                    continue

            # --- GATE 2: Sequence & Freshness Gate ---
            # Skip ONLY if we've fetched before AND the log hasn't advanced.
            if buf and buf.last_fetch_ns > 0 and buf.last_seq >= global_latest_seq:
                continue

            # --- GATE 3: History Throttling ---
            if buf and not self.is_auto_scroll:
                if (now_ns - buf.last_fetch_ns) < fetch_throttle_ns:
                    continue

            # Prepare for fetch/discovery
            current_module_seq = buf.last_seq if buf else self.log_seq
            target_cols = buf.num_channels if buf else 0

            # === PEEK LOGIC (Discovery) ===
            if target_cols == 0:
                anchor_seq, channels = get_telemetry_anchor(
                    log_pool, module.id, current_module_seq, self._discovery_workspace, max_points
                )
                if channels > 0:
                    # This initializes buf and updates self.buffers[module]
                    initial_watermark = dtypes.SEQ_TYPE(anchor_seq - 1) if anchor_seq > 0 else dtypes.SEQ_NONE
                    buf = self._init_module_channels(module, channels, initial_watermark)
                    target_cols = channels
                    current_module_seq = buf.last_seq
                else:
                    continue

            # === FETCH LOGIC ===
            with fetch_telemetry_arrays(
                array_pool, log_pool, module.id, current_module_seq, target_cols, buf.temp_floats, max_points
            ) as batch:
                # Advance the log watermark
                buf.last_seq = batch.watermark
                buf.last_fetch_ns = now_ns
                if buf.update(batch):
                    updated = True

        # 1. Handle Auto-Scroll (Moves the camera)

        # 2. Main Plot Update (Viewport clipped, high frequency)
        # We call this if data arrived OR if the view moved (Auto-scroll/Manual)

        graph_view = self.graph_view
        try:
            if self.is_auto_scroll:
                graph_view.setUpdatesEnabled(False)
                latency_offset = self.data_update_interval_ns
                # latency_offset = 1_000_000_000
                latest_time = (now_ns - latency_offset) / 1_000_000_000.0
                self._apply_view_range(latest_time - self.view_duration, latest_time, force=force)
                updated = True

            time_since_data = now_ns - self._last_data_update_ns
            if force or time_since_data >= self.data_update_interval_ns:
                # if updated:
                graph_view.setUpdatesEnabled(False)

                # if self.is_auto_scroll:
                #     latest_time = self._get_latest_timestamp()
                #     if latest_time > 0:
                #         self._apply_view_range(latest_time - self.view_duration, latest_time)
                self._update_plots()
                self._last_data_update_ns = now_ns

            # 3. Overview Update (Full buffer, low frequency)
            if self.show_overview:
                time_since_ov = now_ns - self._last_overview_update_ns

                # FORCE update if the main loop is slower than the overview timer
                # OR if the overview timer has naturally expired.
                is_ov_due = time_since_ov >= (self._overview_update_interval_ns - deadzone_ns * 3)
                main_is_slower = self._update_interval_ns >= self._overview_update_interval_ns

                if force or is_ov_due or main_is_slower:
                    has_new_data = any(buf.is_dirty_overview for buf in self.buffers.values())
                    # Only draw if there's actually something new to show
                    if force or has_new_data or self.is_auto_scroll:
                        graph_view.setUpdatesEnabled(False)
                        self._update_overview()
                        self._last_overview_update_ns = now_ns

                    for buf in self.buffers.values():
                        buf.is_dirty_overview = False
        finally:
            if not graph_view.updatesEnabled():
                graph_view.setUpdatesEnabled(True)

        # 4. Final Cleanup: Clear dirty flags only after both consumers had their turn
        for buf in self.buffers.values():
            buf.is_dirty = False

    def get_state(self):
        series = []
        for s in self.series_list:
            series.append(
                {
                    "module": s.module.name_with_device(),
                    "index": s.index,
                    "name": s.name,
                    "color": s.color,
                    "visible": s.visible,
                }
            )
        return {
            "modules": [m.name_with_device() for m in self.modules],
            "is_split": self.is_split,
            "series": series,
            "view_duration": self.duration_combo.currentText(),
            "show_overview": self.show_overview,
            "update_freq": self.update_freq_text,
        }

    def set_split_mode(self, split: bool):
        pg = self._pg
        self.is_split = split
        if not self.series_list:
            return

        saved_range = None
        if self.series_list and self.series_list[0].plot_item:
            saved_range = self.series_list[0].plot_item.viewRange()[0]

        self.plot_range_states.clear()

        # Clear the layout to start fresh
        self.graph_view.clear()

        # Track the vertical row index in the GraphicsLayout
        current_row = 0
        LEFT_AXIS_WIDTH = 50
        # --- 1. Setup Overview Plot (Conditional) ---
        if self.show_overview:
            self.overview_plot = self.graph_view.addPlot(
                row=current_row,
                col=0,
                axisItems={"bottom": pg.DateAxisItem(orientation="bottom")},
            )
            self.overview_plot.setMaximumHeight(200)
            self.overview_plot.enableAutoRange(axis="y", enable=False)
            self.overview_plot.getAxis("left").setWidth(LEFT_AXIS_WIDTH)

            # Setup Region/Slider
            latest_now = self._get_latest_timestamp()
            current_now = latest_now if latest_now > 0 else 60
            start_region = current_now - self.view_duration
            self.region = pg.LinearRegionItem([start_region, current_now])
            self.region.setZValue(10)
            self.overview_plot.addItem(self.region)

            # Connect signals for two-way syncing
            self.region.sigRegionChanged.connect(self._on_region_changed)

            current_row += 1
        else:
            # CRITICAL: Nullify references so other methods know to skip overview logic
            self.overview_plot = None
            self.region = None

        # --- 2. Setup Main Plots ---
        shared_plot = None
        legend = None

        if not self.is_split:
            # SINGLE PLOT MODE
            shared_plot = self.graph_view.addPlot(
                row=current_row, col=0, axisItems={"bottom": pg.DateAxisItem(orientation="bottom")}
            )

            shared_plot.showGrid(x=False, y=True)
            shared_plot.enableAutoRange(axis="y", enable=False)
            shared_plot.getAxis("left").setWidth(LEFT_AXIS_WIDTH)

            # shared_plot.getAxis("bottom").setStyle(showValues=False)
            # shared_plot.setAutoVisible(y=True)

            if self.total_series_count > 1:
                legend = shared_plot.addLegend()

            # Connect range changes to the overview slider if it exists
            shared_plot.sigRangeChangedManually.connect(self._on_main_plot_range_changed)

        # --- 3. Create Curves and Split Plots ---
        for i, s in enumerate(self.series_list):
            if self.is_split:
                # MULTI-PLOT MODE (One per channel)
                p = self.graph_view.addPlot(
                    row=current_row + i, col=0, axisItems={"bottom": pg.DateAxisItem(orientation="bottom")}
                )
                p.setTitle(f'<span style="color: {s.color}; font-weight: bold;">{s.name}</span>')
                p.enableAutoRange(axis="y", enable=False)
                p.showGrid(x=False, y=True)
                p.getAxis("left").setWidth(LEFT_AXIS_WIDTH)
                # p.getAxis("bottom").setStyle(showValues=False)
                # p.setAutoVisible(y=True)

                # Link all split plots to the same X-axis
                if i > 0:
                    p.setXLink(self.series_list[0].plot_item)

                # Only need to connect the first plot to the overview (since they are linked)
                if i == 0 or True:
                    p.sigRangeChangedManually.connect(self._on_main_plot_range_changed)

                s.plot_item = p

                p.setVisible(s.visible)
            else:
                s.plot_item = shared_plot

            # Create/Update Main Curves
            # s.curve = s.plot_item.plot(pen=s.color, name=s.name, clipToView=True, skipFiniteCheck=True, antialias=False)
            s.curve = pg.PlotCurveItem(pen=s.color, name=s.name, clipToView=True, skipFiniteCheck=True)
            s.plot_item.addItem(s.curve)
            s.curve.setVisible(s.visible)

            # Create/Update Overview Curves
            if self.show_overview:
                # s.overview_curve = self.overview_plot.plot(pen=s.color)
                s.overview_curve = pg.PlotCurveItem(pen=s.color)
                self.overview_plot.addItem(s.overview_curve)
                s.overview_curve.setVisible(s.visible)
            else:
                # CRITICAL: Ensure the update loop doesn't try to draw a non-existent curve
                s.overview_curve = None

        if saved_range and self.series_list and self.series_list[0].plot_item:
            self.series_list[0].plot_item.setXRange(saved_range[0], saved_range[1], padding=0)

            # Reset internal series trackers so they don't think they've
            # already drawn this range on the NEW curve objects
        for s in self.series_list:
            s._last_t_min = -1.0
            s._last_t_max = -1.0

        self._update_axis_visibility()

        # Finalize UI
        if legend:
            self._setup_legend_callbacks(legend)

        # Initial data push
        self.apply_updates(force=True)

    def _update_plots(self):
        series_list = self.series_list
        if not series_list:
            return

        plot_item_primary = series_list[0].plot_item
        if plot_item_primary is None:
            return

        view_range = plot_item_primary.viewRange()
        t_min, t_max = view_range[0]

        if self.is_auto_scroll:
            t_max += (self.data_update_interval_ns / 1_000_000_000.0) * 1.2

        num_bins = self._get_target_resolution(plot_item_primary)

        # 1. Track bounds per PlotItem for this frame
        # Format: { plot_object: [current_min, current_max, has_data_flag] }
        frame_bounds = {}

        self.graph_view.blockSignals(True)
        try:
            for module in self.modules:
                buf = self.buffers.get(module)
                if not buf or buf.size == 0:
                    continue

                bundle = buf.bundle()

                buf_newest_ts = buf.x_data[buf.head - 1] if buf.size > 0 else 0

                for s in series_list:
                    if s.module != module or not s.visible:
                        continue

                    view_moved = s._last_t_min != t_min or s._last_t_max != t_max or s._last_bins != num_bins
                    data_visible = self.is_auto_scroll or buf_newest_ts >= t_min

                    if view_moved or (buf.is_dirty and data_visible):
                        s.buf_idx = 1 - s.buf_idx
                        data_start = 0 if buf.size < self.max_points else buf.head

                        n, y_min, y_max = slice_and_downsample_linear(
                            bundle,
                            s.index,
                            s.main_x[s.buf_idx],
                            s.main_y[s.buf_idx],
                            t_min,
                            t_max,
                            num_bins,
                        )
                        s.curve.setData(s.main_x[s.buf_idx][:n], s.main_y[s.buf_idx][:n])

                        s._last_t_min, s._last_t_max, s._last_bins = t_min, t_max, num_bins

                        # AGGREGATION: Update the global bounds for this series' plot
                        if n > 0:
                            if s.plot_item not in frame_bounds:
                                frame_bounds[s.plot_item] = [y_min, y_max, True]
                            else:
                                b = frame_bounds[s.plot_item]
                                if y_min < b[0]:
                                    b[0] = y_min
                                if y_max > b[1]:
                                    b[1] = y_max

            # 2. Apply Hysteresis once per PlotItem using aggregated bounds
            for p_item, (y_min, y_max, _) in frame_bounds.items():
                self._apply_hysteresis_to_plot(p_item, y_min, y_max)

        finally:
            self.graph_view.blockSignals(False)

    def _update_overview(self):
        # Fixed resolution for the small strip
        series_list = self.series_list
        if not series_list:
            return

        # Ensure we have a plot to scale
        if not self.overview_plot:
            return

        ov_bins = self._get_target_resolution(self.overview_plot)
        buffers_get = self.buffers.get
        max_points = self.max_points

        # Aggregator for the overview's Y-axis
        ov_min = float("inf")
        ov_max = float("-inf")
        has_data = False

        for module in self.modules:
            buf = buffers_get(module)
            # We only skip the DRAW if not dirty, but we still need
            # the range if we're going to scale correctly.
            if not buf or buf.size == 0:
                continue

            data_start = 0 if buf.size < max_points else buf.head

            for s in series_list:
                if s.module != module or not s.overview_curve or not s.visible:
                    continue

                # Capture the n, y_min, y_max from Numba
                n_ov, y_min, y_max = minmax_downsample_inplace(
                    buf.x_data,
                    buf.x_data_int64,
                    buf.y_data,
                    s.index,
                    data_start,
                    buf.size,
                    s.ov_x[s.buf_idx],
                    s.ov_y[s.buf_idx],
                    ov_bins,
                )

                if n_ov > 0:
                    s.overview_curve.setData(s.ov_x[s.buf_idx][:n_ov], s.ov_y[s.buf_idx][:n_ov])

                    # Aggregate bounds
                    if y_min < ov_min:
                        ov_min = y_min
                    if y_max > ov_max:
                        ov_max = y_max
                    has_data = True

        # 🚀 Apply hysteresis to the Overview Plot
        if has_data:
            self._apply_hysteresis_to_plot(self.overview_plot, ov_min, ov_max)

        if self.region and self.series_list and self.series_list[0].plot_item:
            v_range = self.series_list[0].plot_item.viewRange()
            rlow, rhigh = v_range[0]

            self.region.blockSignals(True)
            self.region.setRegion([rlow, rhigh])
            self.region.blockSignals(False)

    def _apply_hysteresis_to_plot(self, plot_item, y_min, y_max):
        import math

        # --- SAFETY GUARD 1: Wait for UI Layout ---
        # If the widget hasn't been drawn yet (height is 0), calculating ticks
        # will cause a ZeroDivisionError in PyQtGraph. Bail out and let the
        # next update loop catch it.
        vb = plot_item.getViewBox()
        if vb.height() <= 1.0:
            return

        # --- SAFETY GUARD 2: Protect against bad data ---
        if math.isnan(y_min) or math.isnan(y_max) or math.isinf(y_min) or math.isinf(y_max):
            return

        # Get or create the state for this specific PlotItem
        state = self.plot_range_states.get(plot_item)
        if state is None:
            state = {"min": 0.0, "max": 0.0, "init": False}
            self.plot_range_states[plot_item] = state

        # 1. Calculate Ideal Range (5% padding)
        data_span = y_max - y_min
        padding = data_span * 0.05 if data_span > 0 else 1.0
        ideal_min = y_min - padding
        ideal_max = y_max + padding

        # 2. Hysteresis Decision
        should_update = False
        if not state["init"]:
            should_update = True
        else:
            # MANDATORY EXPAND: Data is leaving current view
            if y_min < state["min"] or y_max > state["max"]:
                should_update = True
            else:
                # LAZY SHRINK: Current data span is < 75% of visual window
                current_view_span = state["max"] - state["min"]
                if data_span < (current_view_span * 0.75):
                    should_update = True

        # 3. Apply
        if should_update:
            state["init"] = True
            state["min"] = ideal_min
            state["max"] = ideal_max
            plot_item.setYRange(ideal_min, ideal_max, padding=0, update=True)

    def _scroll_to_now(self):
        """Moves the window to the current system time, regardless of data arrival."""
        if not self.is_auto_scroll:
            return

        # Get current time in seconds (matching your data's timestamp format)
        # Based on your code, this seems to be the registry's now_ns / 1e9
        now_sec = self.gui_context.registry.now_ns() / 1e9

        # Optional: If you want the plot to 'wait' for the first piece of data
        # before it starts flying off into the future:
        # latest_data = self._get_latest_timestamp()
        # if latest_data == 0: return

        self._apply_view_range(now_sec - self.view_duration, now_sec)

    def clear(self):
        # 1. Fetch the absolute latest sequence from the pool
        log_pool = self.gui_context.registry.central.log_pool
        current_pool_tail = log_pool.latest_sequence()

        # 2. Synchronize the plotter's global pointer
        self.log_seq = current_pool_tail
        self.latest_seq = current_pool_tail

        # 3. Reset all individual module buffers to this new tail
        for buf in self.buffers.values():
            buf.last_seq = current_pool_tail
            buf.head = 0
            buf.size = 0
            buf.ptr = 0
            buf.is_dirty = True  # Force a redraw of the empty state

        # 4. Clear visual curves
        for series in self.series_list:
            if series.curve:
                series.curve.setData([], [])
            if series.overview_curve:
                series.overview_curve.setData([], [])

        self.plot_range_states.clear()

        # 5. Refresh both components
        self._update_plots()
        self._update_overview()

    def closeEvent(self, event):
        """Cleanup registration when the widget is closed."""
        self.gui_context.deregister_log_target(self)
        self.gui_context.remove_updatable(self)
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
        series.visible = visible

        if series.curve:
            series.curve.setVisible(visible)

        if series.overview_curve:
            series.overview_curve.setVisible(visible)

        if self.is_split and series.plot_item:
            series.plot_item.setVisible(visible)

        # Force a refresh of both to account for the new/hidden data
        self.apply_updates(force=True)

    def rename_channel(self, index: int, new_name: str):
        """Example of why persistence is great: you just update the container."""
        if 0 <= index < len(self.series_list):
            s = self.series_list[index]
            s.name = new_name
            # If the UI exists, update the legend/label immediately
            if s.curve:
                s.curve.opts["name"] = new_name
                # Note: pyqtgraph legends might need a refresh call here

    def set_autoscroll(self, enabled: bool):
        if self.is_auto_scroll == enabled:
            return

        self.is_auto_scroll = enabled
        self.autoscroll_action.setChecked(enabled)
        self.autoscroll_action.setText(f"Auto-Scroll: {'ON' if enabled else 'OFF'}")

        # Dynamically toggle labels based on mode
        self._update_axis_visibility()

        if enabled:
            self._update_plots()

    def _update_axis_visibility(self):
        """Shows labels only when paused (not auto-scrolling) to save CPU."""
        show_labels = (not self.is_auto_scroll) or self._update_interval_ns >= 250_000_000

        # 1. Update the main plots
        if self.series_list:
            # In split mode, we only ever want labels on the bottom-most plot anyway
            if self.is_split:
                for i, s in enumerate(self.series_list):
                    if s.plot_item:
                        is_bottom = i == len(self.series_list) - 1
                        # Show values only if it's the bottom plot AND we are paused
                        s.plot_item.getAxis("bottom").setStyle(showValues=(show_labels and is_bottom))
            else:
                # Single plot mode
                self.series_list[0].plot_item.getAxis("bottom").setStyle(showValues=show_labels)

        # 2. Update the overview plot (Usually keep labels on here for context, or hide if you're hardcore)
        if self.overview_plot:
            self.overview_plot.getAxis("bottom").setStyle(showValues=True)

    def _apply_view_range(self, start_time: float, end_time: float, force=False):
        """Safely updates both the region and the main plots without triggering feedback loops."""
        if end_time - start_time < 0.1:
            start_time = end_time - 0.1

        self._is_system_updating = True
        try:
            # 1. Update the overview slider ONLY if it exists
            if self.region:
                now_ns = self.gui_context.registry.now_ns()
                # 33ms is the "sweet spot" for visual smoothness vs CPU usage
                if force:  # or (now_ns - self._last_region_update_ns) > 950_000_000:
                    self.region.blockSignals(True)
                    self.region.setRegion([start_time, end_time])
                    self.region.blockSignals(False)
                    self._last_region_update_ns = now_ns

            # 2. ALWAYS update the main plots (the bottom views)
            # This ensures auto-scroll works even if the overview is hidden
            if self.series_list and self.series_list[0].plot_item:
                self.series_list[0].plot_item.setXRange(start_time, end_time, padding=0)
        finally:
            self._is_system_updating = False

    def _get_latest_timestamp(self) -> float:
        """Finds the most recent timestamp across all active module buffers."""
        latest = 0.0
        for buf in self.buffers.values():
            if buf.size > 0:
                ts = buf.x_data[buf.head - 1]
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

        # Add to our tracking list
        self.modules.append(module)

        # Accept the action
        event.acceptProposedAction()

    def _get_target_resolution(self, plot_item) -> int:
        """Returns the number of bins required based on pixel width."""
        try:
            # Get width in pixels. Fallback to 1920 if not yet rendered.
            width = plot_item.getViewBox().width()
            return int(width) if width > 0 else 1000
        except AttributeError:
            return 1000

    def set_overview_visible(self, visible: bool):
        """Toggles the visibility of the history overview plot."""
        self.show_overview = visible
        self.overview_action.setChecked(visible)
        # Re-triggering split mode will rebuild the GraphicsLayout without the overview row
        self.set_split_mode(self.is_split)

    def _on_region_changed(self):
        if self._is_system_updating or not self.region:
            return
        self.set_autoscroll(False)
        minX, maxX = self.region.getRegion()

        # FIX: Same here, only apply to the primary linked plot
        if self.series_list and self.series_list[0].plot_item:
            self.series_list[0].plot_item.setXRange(minX, maxX, padding=0)

        self._update_plots()  # Fills the new view range

    def _on_main_plot_range_changed(self):
        import math

        if self._is_system_updating:
            return
        try:
            # 1. Capture the new range
            v_range = self.series_list[0].plot_item.viewRange()
            rlow, rhigh = v_range[0]
            if math.isnan(rlow) or math.isnan(rhigh):
                return

            # 2. Stop auto-scroll because the user is manually interacting
            if self.is_auto_scroll:
                self.set_autoscroll(False)

            # 3. Sync the Overview Slider (ONLY if it exists)
            if self.region:
                self._is_system_updating = True
                try:
                    self.region.setRegion([rlow, rhigh])
                finally:
                    self._is_system_updating = False

            # 4. ALWAYS update the plots to fill the new X-range with data
            self._update_plots()

        except Exception as e:
            self.logger.error(f"Range change error: {e}")

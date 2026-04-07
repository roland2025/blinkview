# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional, Union

from blinkview.core.batch_queue import BatchQueue
from blinkview.core.batched_logrows import BatchedLogRows
from blinkview.core.numpy_log import fetch_telemetry_arrays, peek_channel_count
from blinkview.utils.log_level import LogLevel

if TYPE_CHECKING:
    import numpy as np
    import pyqtgraph as pg

from qtpy.QtGui import QAction, QColor
from qtpy.QtWidgets import QComboBox, QLabel, QMenu, QSizePolicy, QToolBar, QVBoxLayout, QWidget

from blinkview.core.device_identity import ModuleIdentity
from blinkview.core.log_row import LogRow
from blinkview.ui.gui_context import GUIContext
from blinkview.utils.log_filter import LogFilter


@dataclass
class ModuleBuffer:
    """Holds the rolling buffers for a specific module."""

    x_data: "np.ndarray"
    y_data: Optional["np.ndarray"] = None
    head: int = 0  # Where to insert the next batch of data
    size: int = 0  # How many valid points are in the buffer currently
    ptr: int = 0
    num_channels: int = 0
    buffer: BatchQueue = None


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
    last_seq: int = -1


class TelemetryPlotter(QWidget):
    def __init__(self, gui_context, state=None, parent=None):
        super().__init__(parent)

        import numpy as np
        import pyqtgraph as pg

        # Store references so other methods can use them easily
        self._np = np
        self._pg = pg

        # pg.setConfigOptions(useOpenGL=True)
        pg.setConfigOptions(antialias=False)

        self.gui_context: GUIContext = gui_context

        self.logger = gui_context.logger.child(f"plotter_{id(self):x}")

        self.max_points = self.gui_context.settings.get("plot.max_points", 50000)

        self.tab_name: str = ""
        self.is_split: bool = False

        self.log_seq = -1
        self.latest_seq = -1

        self.plot_data_changed = False

        # Buffers
        self.modules: List[ModuleIdentity] = []
        self.buffers: dict[ModuleIdentity, ModuleBuffer] = {}

        # New Single Source of Truth for Series
        self.series_list: List[SeriesContainer] = []

        self.overview_plot: Optional["pg.PlotItem"] = None
        self.region: Optional["pg.LinearRegionItem"] = None
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
        self.toolbar.addAction("Reset view", self.reset_view)

        self.toolbar.addSeparator()

        self.duration_combo = QComboBox()
        self.duration_combo.setMinimumWidth(60)
        self.duration_combo.setEditable(True)
        self.duration_combo.addItems(["0.1s", "0.5s", "1s", "10s", "30s", "60s", "5m", "10m", "30m", "1h"])

        idx = self.duration_combo.findText(self.view_duration_text)
        if idx >= 0:
            self.duration_combo.setCurrentIndex(idx)
        else:
            # Fallback if someone changes the list but forgets the default
            self.duration_combo.setCurrentText(self.view_duration_text)

        self.duration_combo.currentTextChanged.connect(self._on_duration_changed)

        self.toolbar.addSeparator()

        # Add an Auto-Scroll toggle to the toolbar
        self.autoscroll_action = self.toolbar.addAction("Auto-Scroll: ON")
        self.autoscroll_action.setCheckable(True)
        self.autoscroll_action.setChecked(True)
        self.autoscroll_action.triggered.connect(self.set_autoscroll)

        self.toolbar.addWidget(QLabel("Window:"))
        self.toolbar.addWidget(self.duration_combo)

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
                self.series_list.append(
                    SeriesContainer(
                        module=mod,
                        index=s["index"],
                        name=s["name"],
                        color=self.get_color(i).name(),
                        visible=s["visible"],
                    )
                )

    def reset_view(self):
        self.set_autoscroll(True)
        self.set_split_mode(self.is_split)

    def _parse_duration(self, text: str) -> Optional[int]:
        """Parses strings like '10s', '5m', '2h' into total seconds."""
        text = text.lower().strip()
        # Regex to capture the number and the unit suffix
        match = re.match(r"^(\d*\.?\d+)\s*([smh]?)$", text)
        if not match:
            return None

        value = float(match.group(1))
        unit = match.group(2)

        if unit == "m":
            return int(value * 60)
        elif unit == "h":
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
        np = self._np
        buf = self.buffers[module]

        # ALLOCATE 2x SIZE FOR MIRRORED BUFFER
        buf.y_data = np.zeros((self.max_points * 2, num_channels), order="F")
        buf.x_data = np.zeros(self.max_points * 2)
        buf.num_channels = num_channels

        if not any(s.module == module for s in self.series_list):
            existing_series_count = len(self.series_list)
            for i in range(num_channels):
                self.series_list.append(
                    SeriesContainer(
                        module=module,
                        index=i,
                        name=f"{module.short_name} {i}" if num_channels > 1 else module.short_name,
                        color=self.get_color(existing_series_count + i).name(),
                        visible=True,
                    )
                )

        self.set_split_mode(self.is_split)

    def get_color(self, i: int) -> QColor:
        return QColor.fromHsv((120 + i * 80) % 360, 255, 255)

    @property
    def total_series_count(self) -> int:
        return len(self.series_list)

    def update_batch(self, module: ModuleIdentity, batches):
        np = self._np
        _len = len
        updated = False

        for batch in batches:
            batch_len = len(batch)
            if batch_len == 0:
                continue

            # 1. Resolve buffer and target columns
            try:
                buf = self.buffers[module]
                target_cols = buf.num_channels
            except KeyError:
                # Find the first valid row to determine column count
                first_row = next((r for r in batch if r.module == module), None)
                if not first_row:
                    continue
                vals = first_row.get_values()
                if not vals:
                    continue

                target_cols = len(vals)
                buf = ModuleBuffer(x_data=np.zeros(0), buffer=BatchQueue(self.max_points))
                self.buffers[module] = buf
                self._init_module_channels(module, target_cols)

            # 2. Pre-allocate temporary numpy arrays for THIS batch
            temp_t = np.empty(batch_len, dtype=float)
            temp_v = np.empty((batch_len, target_cols), dtype=float)

            # 3. Fill the arrays directly (avoids building intermediate python lists/tuples)
            idx = 0
            for row in batch:
                if row.module == module:
                    vals = row.get_values()
                    if vals and len(vals) == target_cols:
                        temp_t[idx] = row.timestamp
                        temp_v[idx] = vals  # Numpy handles the list-to-C-array conversion instantly
                        idx += 1

            if idx == 0:
                continue

            # 4. Slice out only the valid data we collected
            new_times = temp_t[:idx]
            new_values = temp_v[:idx]
            num_new = idx

            if num_new >= self.max_points:
                # Keep only the latest data if batch is massive
                new_times = new_times[-self.max_points :]
                new_values = new_values[-self.max_points :, :]
                num_new = self.max_points

                # Write to primary half
                buf.x_data[: self.max_points] = new_times
                buf.y_data[: self.max_points, :] = new_values
                # Mirror to second half
                buf.x_data[self.max_points : 2 * self.max_points] = new_times
                buf.y_data[self.max_points : 2 * self.max_points, :] = new_values

                buf.head = 0
                buf.size = self.max_points
            else:
                end_idx = buf.head + num_new
                if end_idx <= self.max_points:
                    # Fits without wrapping primary half
                    buf.x_data[buf.head : end_idx] = new_times
                    buf.y_data[buf.head : end_idx, :] = new_values

                    # Mirror to second half
                    buf.x_data[buf.head + self.max_points : end_idx + self.max_points] = new_times
                    buf.y_data[buf.head + self.max_points : end_idx + self.max_points, :] = new_values
                else:
                    # Wraps the primary half
                    overflow = end_idx - self.max_points
                    first_part = num_new - overflow

                    # Fill end of primary and end of mirror
                    buf.x_data[buf.head : self.max_points] = new_times[:first_part]
                    buf.y_data[buf.head : self.max_points, :] = new_values[:first_part, :]
                    buf.x_data[buf.head + self.max_points : 2 * self.max_points] = new_times[:first_part]
                    buf.y_data[buf.head + self.max_points : 2 * self.max_points, :] = new_values[:first_part, :]

                    # Fill start of primary and start of mirror
                    buf.x_data[0:overflow] = new_times[first_part:]
                    buf.y_data[0:overflow, :] = new_values[first_part:, :]
                    buf.x_data[self.max_points : self.max_points + overflow] = new_times[first_part:]
                    buf.y_data[self.max_points : self.max_points + overflow, :] = new_values[first_part:, :]

                buf.head = (buf.head + num_new) % self.max_points
                buf.size = min(buf.size + num_new, self.max_points)

            updated = True

        return updated

    def __apply_updates(self):
        # Fetches entries from backend and updates screen
        now_ns = self.gui_context.registry.now_ns
        start_seq = self.latest_seq

        # Point to the new fast-path getter
        get_telemetry_batch = self.gui_context.registry.central.get_telemetry_batch

        updated = False

        start = now_ns()

        fetch_duration = 0

        for module in self.modules:
            fetch_start = now_ns()

            buf = self.buffers.get(module)
            target_cols = buf.num_channels if buf else 0

            # 1. Pull native numpy arrays straight from the central queue
            batch_container = get_telemetry_batch(module, start_seq, target_cols)

            if batch_container is not None:
                with batch_container as batch:
                    self.latest_seq = batch.latest_seq

                    if not buf:
                        buf = ModuleBuffer(
                            x_data=self._np.zeros(self.max_points * 2),
                            y_data=self._np.zeros((self.max_points * 2, batch.target_cols), order="F"),
                            buffer=BatchQueue(self.max_points),
                        )
                        self.buffers[module] = buf
                        self._init_module_channels(module, batch.target_cols)

                    # Read directly from the perfectly-sized views
                    self._insert_into_circular_buffer(module, batch.times, batch.values)
                    updated = True

            fetch_end = now_ns()
            fetch_duration += fetch_end - fetch_start

        draw_start = now_ns()
        if updated:
            self._update_plots()

        end = now_ns()
        draw_duration = end - draw_start
        # Calculate ratio, defaulting to 0.0 if fetch_duration is 0 to avoid crash
        ratio = draw_duration / fetch_duration if fetch_duration > 0 else 0.0

        self.logger.debug(
            f"updated={1 if updated else 0} "
            f"total={(end - start) / 1e6:.3f} ms "
            f"draw={draw_duration / 1e6:.3f} ms "
            f"fetch={fetch_duration / 1e6:.3f} ms "
            f"ratio={ratio:.3f}"
        )

    def apply_updates(self):
        np = self._np
        now_ns = self.gui_context.registry.now_ns
        start_seq = self.latest_seq
        updated = False
        start = now_ns()
        fetch_duration = 0

        log_pool = self.gui_context.registry.central.log_pool

        for module in self.modules:
            fetch_start = now_ns()

            buf = self.buffers.get(module)
            target_cols = buf.num_channels if buf else 0

            # === PEEK LOGIC ===
            if target_cols == 0:
                target_cols = peek_channel_count(log_pool, module.id, start_seq)

                # If we found data, initialize the buffer and series!
                if target_cols > 0:
                    buf = ModuleBuffer(
                        x_data=np.zeros(self.max_points * 2),
                        y_data=np.zeros((self.max_points * 2, target_cols), order="F"),
                        buffer=BatchQueue(self.max_points),
                    )
                    self.buffers[module] = buf
                    self._init_module_channels(module, target_cols)
                    print(f"[TelemetryPlotter] Schema discovered for {module}: {target_cols} channels")
                else:
                    # Still no numeric data for this module yet, skip to next module
                    continue

            # We now absolutely know target_cols > 0
            for new_times, new_values, max_seq in fetch_telemetry_arrays(log_pool, module.id, start_seq, target_cols):
                self.latest_seq = max(self.latest_seq, max_seq)
                self._insert_into_circular_buffer(module, new_times, new_values)
                updated = True

            fetch_end = now_ns()
            fetch_duration += fetch_end - fetch_start

        draw_start = now_ns()
        if updated:
            self._update_plots()

        end = now_ns()
        draw_duration = end - draw_start
        ratio = draw_duration / fetch_duration if fetch_duration > 0 else 0.0

        self.logger.debug(
            f"updated={1 if updated else 0} "
            f"total={(end - start) / 1e6:.3f} ms "
            f"draw={draw_duration / 1e6:.3f} ms "
            f"fetch={fetch_duration / 1e6:.3f} ms "
            f"ratio={ratio:.3f}"
        )

    def _insert_into_circular_buffer(self, module, new_times, new_values):
        """Inserts arrays directly into the Mirrored Ring Buffer."""
        buf = self.buffers[module]
        num_new = new_times.size

        if num_new >= self.max_points:
            # Massive batch: overwrite the whole buffer
            new_times = new_times[-self.max_points :]
            new_values = new_values[-self.max_points :, :]
            num_new = self.max_points

            buf.x_data[: self.max_points] = new_times
            buf.y_data[: self.max_points, :] = new_values
            # Mirror
            buf.x_data[self.max_points : 2 * self.max_points] = new_times
            buf.y_data[self.max_points : 2 * self.max_points, :] = new_values

            buf.head = 0
            buf.size = self.max_points
        else:
            end_idx = buf.head + num_new
            if end_idx <= self.max_points:
                # Clean fit
                buf.x_data[buf.head : end_idx] = new_times
                buf.y_data[buf.head : end_idx, :] = new_values
                # Mirror
                buf.x_data[buf.head + self.max_points : end_idx + self.max_points] = new_times
                buf.y_data[buf.head + self.max_points : end_idx + self.max_points, :] = new_values
            else:
                # Wraps around
                overflow = end_idx - self.max_points
                first_part = num_new - overflow

                # First chunk
                buf.x_data[buf.head : self.max_points] = new_times[:first_part]
                buf.y_data[buf.head : self.max_points, :] = new_values[:first_part, :]
                buf.x_data[buf.head + self.max_points : 2 * self.max_points] = new_times[:first_part]
                buf.y_data[buf.head + self.max_points : 2 * self.max_points, :] = new_values[:first_part, :]

                # Overflow chunk
                buf.x_data[0:overflow] = new_times[first_part:]
                buf.y_data[0:overflow, :] = new_values[first_part:, :]
                buf.x_data[self.max_points : self.max_points + overflow] = new_times[first_part:]
                buf.y_data[self.max_points : self.max_points + overflow, :] = new_values[first_part:, :]

            buf.head = (buf.head + num_new) % self.max_points
            buf.size = min(buf.size + num_new, self.max_points)

    def _load_module_history(self, module: ModuleIdentity):
        """Helper to load history for a single module (used during drop)."""
        print(f"Loading history for newly dropped module: '{module}'")
        log_filter = LogFilter(self.gui_context.id_registry, filtered_module=module)
        batches = self.gui_context.registry.central.get_batches(
            log_filter, total=self.max_points, start_seq=self.log_seq
        )
        if batches:
            if self.update_batch(module, batches):
                self._update_plots()

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
        }

    def set_split_mode(self, split: bool):
        pg = self._pg

        self.is_split = split
        if not self.series_list:
            return

        self.graph_view.clear()

        # Setup Overview Plot with DateAxis
        self.overview_plot = self.graph_view.addPlot(
            row=0,
            col=0,
            # title="History Overview",
            axisItems={"bottom": pg.DateAxisItem(orientation="bottom")},
        )
        self.overview_plot.setMaximumHeight(120)
        self.overview_plot.enableAutoRange(axis="y", enable=True)

        # Setup Main Plots
        shared_plot = None
        legend = None
        if not self.is_split:
            shared_plot = self.graph_view.addPlot(
                row=1, col=0, axisItems={"bottom": pg.DateAxisItem(orientation="bottom")}
            )
            shared_plot.setTitle(None)
            shared_plot.enableAutoRange(axis="y", enable=True)
            shared_plot.setAutoVisible(y=True)

            if self.total_series_count > 1:
                legend = shared_plot.addLegend()

        for i, s in enumerate(self.series_list):
            if self.is_split:
                p = self.graph_view.addPlot(
                    row=i + 1, col=0, axisItems={"bottom": pg.DateAxisItem(orientation="bottom")}
                )
                p.setTitle(f'<span style="color: {s.color}; font-weight: bold;">{s.name}</span>')
                p.enableAutoRange(axis="y", enable=True)
                p.setAutoVisible(y=True)
                # p.setTitle(s.name)
                if i > 0:
                    p.setXLink(self.series_list[0].plot_item)
                s.plot_item = p
            else:
                s.plot_item = shared_plot

            s.curve = s.plot_item.plot(
                pen=s.color,
                name=s.name,
                clipToView=True,  # Only draw what is inside the axes
                autoDownsample=False,  # Disable PyQtGraph's internal downsampler (use the Numpy stride trick instead)
                skipFiniteCheck=True,  # BYPASS: Assumes your array has no NaN or Inf values
                connect="all",  # BYPASS: Assumes the line is continuous (no gaps)
                antialias=False,  # BYPASS: Anti-aliasing requires heavy CPU smoothing
            )
            s.curve.setVisible(s.visible)
            if self.is_split:
                s.plot_item.setVisible(s.visible)

        if legend:
            self._setup_legend_callbacks(legend)

        # Setup Region with Smart Initialization
        latest_now = self._get_latest_timestamp()
        current_now = latest_now if latest_now > 0 else 60
        start_region = current_now - self.view_duration
        self.region = pg.LinearRegionItem([start_region, current_now])

        self.region.setZValue(10)
        self.overview_plot.addItem(self.region)

        for s in self.series_list:
            s.overview_curve = self.overview_plot.plot(pen=s.color)
            s.overview_curve.setVisible(s.visible)

        # --- SYNC LOGIC ---
        def update_main_from_region():
            # Break the infinite feedback loop
            if self._is_system_updating:
                return

            self.set_autoscroll(False)

            minX, maxX = self.region.getRegion()

            # Apply the range to all main plots
            for s in self.series_list:
                if s.plot_item:
                    s.plot_item.setXRange(minX, maxX, padding=0)

        def update_region_from_main(window, viewRange):
            import math

            rlow, rhigh = viewRange[0]

            # CRITICAL: Prevent Qt BSP Segfaults
            if math.isnan(rlow) or math.isnan(rhigh):
                return

            # Break the infinite feedback loop
            if self._is_system_updating:
                return

            self.set_autoscroll(False)

            rlow, rhigh = viewRange[0]

            # Safely update the overview region
            self._is_system_updating = True  # Lock
            self.region.setRegion([rlow, rhigh])  # Triggers update_main_from_region, which will safely return
            self._is_system_updating = False  # Unlock

        # Connect the signals once and leave them alone
        self.region.sigRegionChanged.connect(update_main_from_region)

        main_p = self.series_list[0].plot_item
        if main_p:
            main_p.sigRangeChangedManually.connect(update_region_from_main)

        self._update_plots()

    def processing_done(self):
        if self.plot_data_changed:
            self._update_plots()
            self.plot_data_changed = False

    def _update_plots(self):
        latest_time = 0.0
        try:
            for s in self.series_list:
                buf = self.buffers.get(s.module)
                if not buf or buf.size == 0:
                    continue

                if buf.size < self.max_points:
                    # Buffer hasn't wrapped yet, just read from 0 to head
                    x_ordered = buf.x_data[: buf.head]
                    y_ordered = buf.y_data[: buf.head, s.index]
                else:
                    # MAGIC: Zero-allocation contiguous slice!
                    # Because we mirrored the data, head to (head + max_points)
                    # is guaranteed to be chronologically sorted.
                    x_ordered = buf.x_data[buf.head : buf.head + self.max_points]
                    y_ordered = buf.y_data[buf.head : buf.head + self.max_points, s.index]

                if len(x_ordered) > 0 and x_ordered[-1] > latest_time:
                    latest_time = x_ordered[-1]

                if s.curve:
                    s.curve.setData(x_ordered, y_ordered)
                if s.overview_curve:
                    s.overview_curve.setData(x_ordered, y_ordered)

        except Exception as e:
            print(f"[{self.__class__.__name__}] _update_plots: {e}")

        if self.is_auto_scroll and latest_time > 0:
            self._apply_view_range(latest_time - self.view_duration, latest_time)

        if self.is_auto_scroll and latest_time > 0:
            self._apply_view_range(latest_time - self.view_duration, latest_time)

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
        # Reset the sequence tracker so we don't re-fetch old data
        # Note: self.latest_seq should stay at its current value so the
        # next 'apply_updates' only picks up logs generated AFTER this moment.
        self.log_seq = self.latest_seq

        for buf in self.buffers.values():
            # Reset the Mirrored Ring Buffer pointers
            buf.head = 0
            buf.size = 0
            buf.ptr = 0  # Reset legacy pointer too

            # # Optional: Wipe the actual arrays
            # buf.x_data.fill(0)
            # if buf.y_data is not None:
            #     buf.y_data.fill(0)

        # Clear the visual curves
        for series in self.series_list:
            if series.curve:
                series.curve.setData([], [])

            # CRITICAL: You were missing the overview curve reset!
            if series.overview_curve:
                series.overview_curve.setData([], [])

        # 5. Trigger a redraw to show the empty plots immediately
        self._update_plots()

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
        """The single source of truth for toggling visibility."""
        series.visible = visible

        # Toggle main curve visibility
        if series.curve:
            series.curve.setVisible(visible)

        # Toggle overview curve visibility (The small history lines)
        if series.overview_curve:
            series.overview_curve.setVisible(visible)

        # Handle Split View Layout
        if self.is_split and series.plot_item:
            series.plot_item.setVisible(visible)

        # Trigger a Y-axis rescale
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
                s.curve.opts["name"] = new_name
                # Note: pyqtgraph legends might need a refresh call here

    def set_autoscroll(self, enabled: bool):
        """Updates the auto-scroll state and UI."""
        # Optional optimization: only update if state actually changed
        if self.is_auto_scroll == enabled:
            return

        self.is_auto_scroll = enabled
        self.autoscroll_action.setChecked(enabled)
        self.autoscroll_action.setText(f"Auto-Scroll: {'ON' if enabled else 'OFF'}")

        if enabled:
            # When re-enabling, jump to the live edge immediately
            self._update_plots()

    def _apply_view_range(self, start_time: float, end_time: float):
        """Safely updates both the region and the main plots without triggering feedback loops."""
        if not self.region:
            return

        if end_time - start_time < 0.1:
            start_time = end_time - 0.1

        self._is_system_updating = True
        try:
            # Update the overview slider
            self.region.setRegion([start_time, end_time])

            # Update the main plots (the bottom views)
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

        # Add to our tracking list
        self.modules.append(module)

        # Immediately try to load existing history for just this module
        self._load_module_history(module)

        # Accept the action
        event.acceptProposedAction()

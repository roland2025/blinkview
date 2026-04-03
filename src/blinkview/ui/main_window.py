# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import signal
import sys
from pathlib import Path
from time import perf_counter
from typing import Optional

from PySide6.QtGui import QFont
from qtpy.QtCore import Qt, QTimer, Signal, Slot
from qtpy.QtGui import QAction, QIcon
from qtpy.QtWidgets import (
    QApplication,
    QComboBox,
    QDockWidget,
    QInputDialog,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMenu,
    QPushButton,
    QTabWidget,
    QToolBar,
    QToolButton,
    QVBoxLayout,
    QWidget,
)

from blinkview import __version__ as blinkview_version
from blinkview.core.batch_queue import BatchQueue
from blinkview.core.config_manager import ConfigManager
from blinkview.core.registry import Registry
from blinkview.core.task_manager import TaskManager
from blinkview.ui.cli_args import setup_gui_parser
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.native_dark_mode import set_native_dark_mode
from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.utils.in_development import set_as_in_development
from blinkview.ui.utils.ui_state_handler import UIStateHandler
from blinkview.ui.utils.update_checker import check_for_updates_silently
from blinkview.ui.utils.window_manager import WindowManager
from blinkview.ui.widgets.config.dynamic_config import DynamicConfigWidget
from blinkview.ui.widgets.config.style_config import StyleConfig
from blinkview.ui.widgets.device_sidebar import DeviceSidebarWidget
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.ui.widgets.module_filter_model import ModuleFilterModel
from blinkview.ui.widgets.pipelines_sidebar import PipelinesSidebarWidget
from blinkview.ui.widgets.plotter import TelemetryPlotter
from blinkview.ui.widgets.telemetry_model import TelemetryModel
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from blinkview.ui.widgets.TelemetryWatch import TelemetryWatch
from blinkview.ui.widgets.title_bar import TitleBar
from blinkview.ui.widgets.toast import ToastManager, ToastType
from blinkview.ui.widgets.update_widget import UpdateWidget, check_post_update
from blinkview.ui.windows.detached_tab_window import DetachedTabWindow
from blinkview.utils.used_modules import print_used_modules


class BlinkMainWindow(QMainWindow):
    def __init__(self, registry, set_update_version=None):
        super().__init__()
        self.resize(1280, 800)
        set_native_dark_mode(self)

        use_frameless = False  # Set to False to see the standard window frame (useful for debugging)

        self.gui_context = GUIContext()
        self.gui_context.set_register_log_target(self.register_log_target)
        self.gui_context.set_deregister_log_target(self.deregister_log_target)

        if set_update_version is not None:

            def set_update_and_close(ver):
                set_update_version(ver)
                self.close()

            self.gui_context.set_update_version = set_update_and_close

        self.gui_context.set_registry(registry)

        fm = self.gui_context.registry.file_manager
        # Standalone is indicated at the end only if necessary
        mode_suffix = " (Standalone)" if fm.standalone_mode else ""
        self.setWindowTitle(f"{fm.project_name} / {fm.profile_name} - BlinkView{mode_suffix} - {blinkview_version}")

        self.gui_context.registry.configure_system()

        self.gui_context.set_config_manager(ConfigNodeManager(self.gui_context))

        gui_config = ConfigManager(
            fm.get_config_path("gui"),
            fm.get_session_path("gui", suffix="autosave"),
            {"watches": {}},
        )

        self.gui_context.set_gui_config_handler(gui_config)
        self.gui_context.set_gui_config_manager(ConfigNodeManager(self.gui_context, gui_config))

        self.gui_context.set_widget_factory(self.create_widget)

        # Setup the Toolbar and Button
        self.toolbar = QToolBar("Main Toolbar")

        self.main_menu_btn = QToolButton()
        self.main_menu_btn.setText("Menu")
        self.main_menu_btn.setPopupMode(QToolButton.InstantPopup)

        # Initialize the menu and connect the "On the Fly" signal
        self.app_menu = QMenu(self)
        self.app_menu.aboutToShow.connect(self.populate_main_menu)
        self.main_menu_btn.setMenu(self.app_menu)
        self.toolbar.addWidget(self.main_menu_btn)

        self.btn_open_logs = QAction("Live Logs", self)
        self.btn_open_logs.triggered.connect(lambda _: self.create_widget("LogViewerWidget", "Live Logs"))
        self.toolbar.addAction(self.btn_open_logs)

        self.btn_open_system_logs = QAction("System Logs", self)
        self.btn_open_system_logs.triggered.connect(
            lambda _: self.create_widget("LogViewerWidget", "System Logs", params={"allowed_device": "SYSTEM"})
        )
        self.toolbar.addAction(self.btn_open_system_logs)

        # --- Telemetry Action ---
        self.btn_open_telemetry = QAction("Telemetry", self)
        # Use an icon if you have one, e.g., QIcon("chart.png")
        self.btn_open_telemetry.triggered.connect(lambda _: self.create_widget("TelemetryTable", "Live Telemetry"))
        self.toolbar.addAction(self.btn_open_telemetry)

        self.toolbar.addSeparator()

        self.watch_button = QToolButton()
        self.watch_button.setText("Watch")
        self.watch_button.clicked.connect(self.show_watch_menu)

        self.toolbar.addWidget(self.watch_button)

        # --- Sidebar Setup ---
        self.sources_dock = QDockWidget("Sources", self)
        self.sources_dock.setObjectName("SourcesDock")  # Required for state saving later
        self.sources_dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.sources_dock)

        self.pipelines_dock = QDockWidget("Pipelines", self)
        self.pipelines_dock.setObjectName("PipelinesDock")
        self.pipelines_dock.setAllowedAreas(Qt.LeftDockWidgetArea | Qt.RightDockWidgetArea)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.pipelines_dock)

        self.toolbar.addSeparator()

        # Sources Toggle
        self.action_view_sources = self.sources_dock.toggleViewAction()
        self.action_view_sources.setText("Sources")  # Or use an icon
        self.toolbar.addAction(self.action_view_sources)

        # Pipelines Toggle
        self.action_view_pipelines = self.pipelines_dock.toggleViewAction()
        self.action_view_pipelines.setText("Pipelines")
        self.toolbar.addAction(self.action_view_pipelines)

        self.toolbar.addSeparator()

        # Create the MPS Label
        self.mps_label = QLabel("0 msg/s")
        self.mps_label.setMinimumWidth(100)
        # Use a monospace font so the toolbar doesn't "jump" when numbers change
        self.mps_label.setFont(QFont("Consolas", 9) if sys.platform == "win32" else QFont("Monospace", 9))
        self.mps_label.setStyleSheet("color: #eee; margin-right: 10px;")  # Dim it slightly to look like a status
        self.toolbar.addWidget(self.mps_label)

        # Counters for calculation
        self._msg_counter = 0
        self._last_mps_time = perf_counter()

        # --- Set up the Central Tabbed Workspace ---
        self.central_tabs = QTabWidget()
        self.central_tabs.setTabsClosable(True)  # Allow users to close config tabs
        self.central_tabs.tabCloseRequested.connect(self.close_tab)

        # Optional: Make it look a bit more like a modern IDE
        self.central_tabs.setDocumentMode(True)

        self.central_tabs.tabBar().setContextMenuPolicy(Qt.CustomContextMenu)
        self.central_tabs.tabBar().customContextMenuRequested.connect(self.show_tab_context_menu)

        devices_config_node = self.gui_context.config_manager.create_node("/sources")
        self.device_sidebar = DeviceSidebarWidget(devices_config_node, gui_context=self.gui_context)
        self.sources_dock.setWidget(self.device_sidebar)

        pipelines_config_node = self.gui_context.config_manager.create_node("/pipelines")
        self.pipelines_sidebar = PipelinesSidebarWidget(pipelines_config_node, gui_context=self.gui_context)
        # self.pipelines_sidebar.device_added.connect(self.on_add_device)
        self.pipelines_dock.setWidget(self.pipelines_sidebar)

        # Keep a list so Python's garbage collector doesn't destroy our floating windows
        self.window_manager = WindowManager()

        if use_frameless:
            self.main_container = QWidget()
            self.main_layout = QVBoxLayout(self.main_container)
            self.main_layout.setContentsMargins(0, 0, 0, 0)
            self.main_layout.setSpacing(0)

            # Add Custom Title Bar
            self.title_bar = TitleBar(self)
            self.main_layout.addWidget(self.title_bar)

            # Set the container as the actual central widget

            # Wire the Hamburger Menu
            self.title_bar.menu_btn.clicked.connect(self.show_main_menu)

            self.setWindowFlags(Qt.FramelessWindowHint | Qt.Window)
            self.toolbar.setMovable(False)
            self.main_layout.addWidget(self.toolbar)

            self.main_layout.addWidget(self.central_tabs)

            self.setCentralWidget(self.main_container)
        else:
            self.addToolBar(self.toolbar)

            self.setCentralWidget(self.central_tabs)

        # Backend Integration
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put
        # self.timestamp_formatter = ConsoleTimestampFormatter()

        self.log_targets = []

        self.gui_context.registry.subscribe(self)

        # Signal Handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.last_poll_time = perf_counter()

        # UI Poller (Runs here, updates the log window)
        self.gui_context.set_theme(StyleConfig())

        self.fps_slow = 1
        self.timeout_slow = 1000 // self.fps_slow
        self.timeout_fast = self.gui_context.theme.ui_update_rate_ms
        self.timer_fast = QTimer(self)
        self.timer_fast.timeout.connect(self.poll_queue)

        self.gui_context.set_telemetry_model(TelemetryModel(gui_context=self.gui_context))

        self.gui_context.set_module_filter_model(ModuleFilterModel(gui_context=self.gui_context))

        # 1FPS Structure Syncer
        self.timer_slow = QTimer(self)
        self.timer_slow.timeout.connect(self.gui_context.on_heartbeat)

        self.widget_factories = {
            "LogViewerWidget": LogViewerWidget,
            "TelemetryTable": TelemetryTable,
            "DynamicConfigWidget": DynamicConfigWidget,
            "TelemetryPlotter": TelemetryPlotter,
            "TelemetryWatch": TelemetryWatch,
            "UpdateWidget": UpdateWidget,
        }

        self.gui_context.set_gui_state_handler(UIStateHandler(self))
        self.gui_context.registry.file_manager.set_gui_context(self.gui_context)

        self.gui_context.set_reattach_tab(self.reattach_tab)

        self.device_toolbars = {}
        self.sources_node = self.gui_context.config_manager.create_node("/sources")
        self.sources_node.on_update(self.sync_device_toolbars)

        self.watches_node = None

        print("[BlinkMainWindow] Initialization complete.")

        QTimer.singleShot(1000, lambda: check_for_updates_silently(self.gui_context, parent=self))

    def load_ui_state(self):
        self.gui_context.gui_state.load_ui_state(self.gui_context.registry.file_manager.get_config_path("gui_state"))

        # QTimer.singleShot(0, lambda: ToastManager.show("Something happened...", ToastType.INFO))
        # QTimer.singleShot(333, lambda: ToastManager.show("WAARNING...", ToastType.WARNING))
        # QTimer.singleShot(666, lambda: ToastManager.show("WHoop success...", ToastType.SUCCESS))
        # QTimer.singleShot(999, lambda: ToastManager.show("Attention error...", ToastType.ERROR))

        # delay the start of the registry, allows the windows to appear before doing anything heavy
        QTimer.singleShot(100, self.gui_context.registry.start)

        QTimer.singleShot(200, lambda: self.timer_slow.start(self.timeout_slow))  # 1 second

        QTimer.singleShot(300, lambda: self.timer_fast.start(self.timeout_fast))

    def register_log_target(self, target):
        """Adds a target that expects a 'process_log_batch(list)' method."""
        if target not in self.log_targets:
            self.log_targets.append(target)

    def deregister_log_target(self, target):
        if target in self.log_targets:
            print(f"[BlinkMainWindow] Deregistering log target: {hex(id(target))}")
            self.log_targets.remove(target)

    def show_main_menu(self):
        menu = QMenu(self)
        menu.addAction("New Project")
        menu.addAction("Settings")
        menu.addSeparator()
        menu.addAction("Exit", self.close)

        # Show menu below the hamburger button
        btn = self.title_bar.menu_btn
        menu.exec(btn.mapToGlobal(btn.rect().bottomLeft()))

    def fetch_system_schema(self, callback):
        """Simulates fetching the entire system schema for the settings page."""
        system_ctx = self.gui_context.registry.system_ctx

        def fetch():
            try:
                print(f"[Fetching] system schema")
                schema = self.gui_context.registry.config.get_by_path()
                print(f"[Fetching] system schema: {schema}")
                callback(schema)
            except Exception as e:
                print(f"[Fetching] error fetching system schema: {e}")

        system_ctx.tasks.run_task(fetch)

    # --- Core Tab Management Helpers ---

    def focus_tab_if_exists(self, tab_name):
        """Checks if a tab exists, focuses it if it does, and returns True."""
        for i in range(self.central_tabs.count()):
            if self.central_tabs.tabText(i) == tab_name:
                self.central_tabs.setCurrentIndex(i)
                return True
        return False

    def add_tab_focused(self, widget, tab_name):
        """Adds a new tab and immediately switches focus to it."""
        tab_index = self.central_tabs.addTab(widget, tab_name)
        self.central_tabs.setCurrentIndex(tab_index)

    def populate_main_menu(self):
        """Generates the main application menu dynamically."""
        menu = self.app_menu
        menu.clear()

        # Core Config Actions
        settings_act = menu.addAction("Settings")
        settings_act.triggered.connect(
            lambda: self.gui_context.config_manager.show(
                "/", "System", drop_keys=["plugins", "version", "pipelines", "sources"]
            )
        )

        plugins_act = menu.addAction("Plugins")
        plugins_act.triggered.connect(lambda: self.gui_context.config_manager.show("/plugins", "Plugins"))

        menu.addSeparator()

        # Dynamic Content: Context-Aware Actions
        # Example: Only show 'Close All Tabs' if there are tabs open
        # if self.central_tabs.count() > 0:
        #     close_all_act = menu.addAction("❌ Close All Tabs")
        #     close_all_act.triggered.connect(self.close_all_tabs)  # You'll need to implement this
        #     menu.addSeparator()

        # List Recently Opened Watches (Optional Fun Feature)
        # You can pull this from your gui_config
        # watches = self.watches_node.config or {}
        # if watches:
        #     recent_menu = menu.addMenu("Recent Watches")
        #     for wid, data in list(watches.items())[:5]:  # Show top 5
        #         name = data.get("tab_name", wid)
        #         act = recent_menu.addAction(name)
        #         act.triggered.connect(lambda checked=False, w=wid: self.open_watch(w))

        update_act = menu.addAction("Check for updates")
        update_act.triggered.connect(
            lambda: self.create_widget("UpdateWidget", "Updates", as_window=True, reattach_on_close=False)
        )

        # set_as_in_development(update_act, self)

        # Global Exit
        menu.addSeparator()
        exit_act = menu.addAction("Quit")
        exit_act.triggered.connect(self.close)

    def create_widget(self, cls_name, name, as_window=False, show=True, params=None, reattach_on_close=True):
        """Routes a string class name to the correct factory method."""

        # Prevent duplicate tabs using the helper
        if params is None:
            params = {}

        if params.get("tab_name") is None:
            params["tab_name"] = name

        if self.focus_tab_if_exists(name):
            return None

        if self.window_manager.raise_window(name):
            return None

        # window or tab doesnt exist, we need to create it

        print(f"[BlinkMainWindow] Opening widget: {cls_name} with name: {name} (as_window={as_window})")
        factory = self.widget_factories.get(cls_name)

        if not factory:
            print(f"Warning: Unknown widget class '{cls_name}'.")
            return None

        # Instantiate core widget
        widget = factory(self.gui_context, params)

        signal_destroy = getattr(widget, "signal_destroy", None)

        # Route to correct container
        if as_window:
            floating_win = DetachedTabWindow(self.gui_context, widget, name, reattach=reattach_on_close)

            if signal_destroy:
                signal_destroy.connect(lambda _: floating_win.force_destroy())

            self.window_manager.register(floating_win, widget)
            if show:
                floating_win.show()
            return floating_win
        else:
            self.add_tab_focused(widget, name)

            if signal_destroy:
                signal_destroy.connect(self.close_tab_by_widget)
            return widget

    def poll_queue(self):
        """Drains the queue, monitors UI lag, and yields to the event loop if budgeted time is exceeded."""
        try:
            # print("[BlinkMainWindow] Polling log queue...")
            current_time = perf_counter()
            drift_ms = (current_time - self.last_poll_time) * 1000
            self.last_poll_time = current_time
            get_nowait = self.input_queue.get_nowait
            log_targets = self.log_targets

            # If the gap is significantly larger than our ~16.6ms target, the UI is lagging
            if drift_ms > self.timeout_fast * 2:  # More than 2 frames late
                print(f"[UI Monitor] 🐌 Thread Lag Detected: {drift_ms:.1f}ms since last poll!")

            time_budget = (
                self.timeout_fast * 0.8 / 1000
            )  # Spend at most 80% of the frame time processing logs, converted to seconds
            batches_processed = 0

            while True:
                batch = get_nowait()
                if not batch:
                    break  # Queue is empty, all caught up!

                with batch:
                    self._msg_counter += batch.size
                    batches_processed += 1

                    # print(f"[BlinkMainWindow] Processing batch of {len(batch)} logs (Batch #{batches_processed})")

                    # Broadcast the batch to all targets
                    for target in log_targets:
                        try:
                            target.process_log_batch(batch)
                        except Exception as e:
                            print(f"[BlinkMainWindow] Target {target} failed to process batch: {e}")

                # Check if we've overstayed our welcome on the UI thread
                elapsed_processing = perf_counter() - current_time
                if elapsed_processing > time_budget:
                    print(
                        f"[UI Monitor] ⚠️ Oversaturated! Processed {batches_processed} batches in {elapsed_processing * 1000:.1f}ms. Yielding to event loop."
                    )
                    break  # Leave the remaining batches in the queue for the next timer tick

            elapsed_since_mps = current_time - self._last_mps_time
            if elapsed_since_mps >= 1.0:
                # Calculate throughput
                mps = int(self._msg_counter / elapsed_since_mps)

                # Formatting with thousands separator (e.g., 538,521)
                self.mps_label.setText(f"{mps:,} msg/s")

                # Reset counters
                self._msg_counter = 0
                self._last_mps_time = current_time

            self.gui_context.on_update()

        except Exception as e:
            print(f"[BlinkMainWindow] Error while polling queue: {e}")

    def _signal_handler(self, signum, frame):
        print(f"\n[BlinkView] Received signal {signum}. Initiating graceful shutdown...")
        self.close()

    def closeEvent(self, event):
        """Clean up all resources when the Main Window closes."""
        print("Closing BlinkView...")
        self.gui_context.is_shutting_down = True

        self.gui_context.registry.file_manager.save_gui()

        self.gui_context.registry.stop()
        self.timer_fast.stop()
        self.timer_slow.stop()

        self.window_manager.close_all()

        event.accept()

    @Slot(int)
    def close_tab(self, index: int):
        widget_to_close = self.central_tabs.widget(index)

        if widget_to_close:
            # DEREGISTER IT FIRST
            if hasattr(self, "deregister_log_target"):
                self.deregister_log_target(widget_to_close)

            # Then destroy it
            widget_to_close.close()
            widget_to_close.deleteLater()

        self.central_tabs.removeTab(index)

    def remove_tab_by_widget(self, widget: QWidget):
        """Finds the index of the widget and removes that tab."""
        index = self.central_tabs.indexOf(widget)
        if index != -1:
            print(f"[Main] Removing tab index {index} because widget requested destruction.")
            self.central_tabs.removeTab(index)
            widget.deleteLater()  # Explicitly clean up memory

    def close_tab_by_widget(self, widget: QWidget):
        """Bridge: Finds the tab index for a widget and calls the existing close_tab logic."""
        index = self.central_tabs.indexOf(widget)
        if index != -1:
            self.close_tab(index)
        else:
            # Fallback if the widget isn't in a tab (e.g., it's a window)
            widget.close()
            widget.deleteLater()

    def show_tab_context_menu(self, position):
        """Pops up a menu when the user right-clicks a specific tab."""
        tab_index = self.central_tabs.tabBar().tabAt(position)

        # If they right-clicked empty space on the tab bar, ignore it
        if tab_index < 0:
            return

        menu = QMenu(self)
        detach_action = menu.addAction("↗️ Detach to New Window")

        # Show the menu at the exact mouse coordinates
        global_pos = self.central_tabs.tabBar().mapToGlobal(position)
        action = menu.exec(global_pos)

        if action == detach_action:
            self.detach_tab(tab_index)

    def detach_tab(self, index: int):
        """Removes the widget from the tab and wraps it in a floating window."""
        self.central_tabs.setUpdatesEnabled(False)

        widget = self.central_tabs.widget(index)
        title = self.central_tabs.tabText(index)
        try:
            # Remove it from the tab layout (this does NOT destroy the widget)
            self.central_tabs.removeTab(index)
            widget.setParent(None)

            self.central_tabs.setUpdatesEnabled(True)
            # Wrap it and show it
            floating_win = DetachedTabWindow(self.gui_context, widget, title)
            self.window_manager.register(floating_win, widget)
            floating_win.show()
        finally:
            self.central_tabs.setUpdatesEnabled(True)

    def reattach_tab(self, widget, title: str):
        """Triggered by the floating window when it is closed."""
        # Add it back to the tab bar
        tab_index = self.central_tabs.addTab(widget, title)

        # Immediately focus the re-attached tab
        self.central_tabs.setCurrentIndex(tab_index)

    def sync_device_toolbars(self, sources_config: dict, schema: dict):
        """Creates or removes toolbars based on the current sources config."""

        # Remove toolbars for sources that no longer exist
        tracked_ids = list(self.device_toolbars.keys())
        for source_id in tracked_ids:
            config = sources_config.get(source_id)

            # If the source was deleted OR it's now disabled, kill the toolbar
            if not config or not config.get("enabled", False):
                toolbar = self.device_toolbars.pop(source_id)
                self.removeToolBar(toolbar)
                toolbar.deleteLater()
                print(f"[UI] Removed toolbar for: {source_id}")

        # Handle Additions (New in config AND toggled to Enabled)
        for source_id, config in sources_config.items():
            is_enabled = config.get("enabled", False)

            if is_enabled and source_id not in self.device_toolbars:
                self.create_device_control_toolbar(source_id, config.get("name", source_id))
                print(f"[UI] Created toolbar for: {source_id}")

    def show_watch_menu(self):
        """Triggered by the button click."""
        # Create the temporary menu
        menu = QMenu(self)
        menu.setAttribute(Qt.WA_DeleteOnClose)

        # Necessary only now: Create the node
        node_didnt_exist = self.watches_node is None
        if node_didnt_exist:
            self.watches_node = self.gui_context.gui_config_manager.create_node(
                "/watches", on_update=lambda config, schema: self._rebuild_menu(menu, config)
            )

        # Initial Build (likely shows "Loading..." the first time)
        self._rebuild_menu(menu, None if node_didnt_exist else self.watches_node.config)

        # Position and Show
        pos = self.watch_button.mapToGlobal(self.watch_button.rect().bottomLeft())
        menu.exec(pos)

        # Cleanup: When the menu closes, stop listening to updates to prevent
        # the menu from trying to update while it's being garbage collected.
        print("[UI] Watch menu closed")
        if node_didnt_exist:
            try:
                self.watches_node.signal_received.disconnect()
            except (RuntimeError, TypeError):
                pass

    def _rebuild_menu(self, menu: QMenu, config: Optional[dict]):
        print(f"[UI] Rebuilding menu for: {config}")
        """Wipes the menu and populates it with current data."""
        if not menu:
            return

        menu.clear()

        if config is None:
            # loading
            act = menu.addAction("Loading watches...")
            act.setEnabled(False)
            return

        if not config:
            # Check if we are still waiting on the first fetch
            act = menu.addAction("No saved watches")
            act.setEnabled(False)
        else:
            for watch_id, data in sorted(config.items()):
                name = data.get("name", f"Watch {watch_id}")
                action = menu.addAction(name)
                action.triggered.connect(lambda _, wid=watch_id: self.open_watch(wid))

        menu.addSeparator()
        menu.addAction("+ New Watch...").triggered.connect(lambda: self.open_watch(None))

    def open_watch(self, watch_id=None):
        """Opens a Telemetry Watch tab for the given watch name."""
        # watches = self.watches_node.config
        node = self.watches_node
        if watch_id is None:
            name, ok = QInputDialog.getText(self, "New Watch", "Enter a name for this watch:", text="New Watch")

            # If user clicks 'Cancel' or gives an empty string, abort creation
            if not ok or not name.strip():
                return

            watches = node.get_copy()
            watch_id, conf = TelemetryWatch.new_watch(name)
            watches[watch_id] = conf
            node.send_config(watches)

        else:
            conf = node.get(watch_id)

        name = conf.get("name", "Default")

        self.create_widget("TelemetryWatch", f"Watch {name}", params={"id": watch_id})

    def create_device_control_toolbar(self, source_id, device_name):
        """Generates a dedicated toolbar for a specific device with command history."""
        toolbar = QToolBar(f"Control: {device_name}")
        toolbar.setObjectName(f"toolbar_{source_id}")

        toolbar.addWidget(QLabel(f" <b>{device_name}:</b> "))

        # Create Editable ComboBox instead of LineEdit
        command_input = QComboBox()
        command_input.setEditable(True)
        command_input.setInsertPolicy(QComboBox.NoInsert)  # We'll handle insertion manually to control duplicates
        command_input.lineEdit().setPlaceholderText("Enter command...")
        command_input.setMinimumWidth(200)
        toolbar.addWidget(command_input)

        btn_send = QPushButton("Send")

        def handle_send():
            val = command_input.currentText().strip()
            if not val:
                return

            # Manage History Logic
            # Remove item if it exists to move it to the top (prevent duplicates)
            existing_index = command_input.findText(val)
            if existing_index >= 0:
                command_input.removeItem(existing_index)

            # Insert at the top
            command_input.insertItem(0, val)
            command_input.setCurrentIndex(0)

            # Optional: Limit history to 10 items
            if command_input.count() > 10:
                command_input.removeItem(10)

            # Execution Logic
            val_with_newline = f"{val}\n"
            try:
                tasks = self.gui_context.registry.system_ctx.tasks
                devices = self.gui_context.registry.sources
                tasks.run_task(devices.send_command, source_id, val_with_newline)
            except Exception as e:
                print(f"Error sending to '{device_name}': {e}")

            # Clear current text for next command
            command_input.setEditText("")

            # Connect signals

        command_input.lineEdit().returnPressed.connect(handle_send)
        btn_send.clicked.connect(handle_send)
        toolbar.addWidget(btn_send)

        self.addToolBar(Qt.TopToolBarArea, toolbar)
        self.device_toolbars[source_id] = toolbar


def run(args):
    if "QT_API" not in os.environ:
        os.environ["QT_API"] = "pyside6"

    install_version: Optional[str] = None  # version to install when closing app

    def set_update_version(ver):
        nonlocal install_version
        install_version = ver

    try:
        # Force Windows to show the custom icon in the taskbar
        if sys.platform == "win32":
            import ctypes

            try:
                myappid = f"ee.incubator.blinkview.{blinkview_version}"
                ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(myappid)
            except Exception:
                pass  # Fails gracefully on non-Windows systems

        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)
        app = QApplication(sys.argv)

        use_qdarktheme = True
        if use_qdarktheme:
            import qdarktheme

            qdarktheme.setup_theme("dark", corner_shape="sharp")
            custom_tooltips = """
            QToolTip {
                background-color: #1e1f22; /* Deep charcoal (PyCharm tooltip bg) */
                color: #bcbec4;            /* Soft light gray text */
                border: 1px solid #4e5157; /* Subtle border for definition */
                padding: 5px;              /* Breathe room */
                border-radius: 0px;        /* Sharp corners to match your 'sharp' setting */
            }
            """
            app.setStyleSheet(app.styleSheet() + custom_tooltips)
        else:
            app.setStyle("Fusion")

        # Set the global application icon
        app.setWindowIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icon.png")))

        registry = Registry(
            session_name=args.session,
            profile_name=args.profile,
            log_dir=args.logdir,
            config_path=args.config,
        )

        viewer = BlinkMainWindow(registry, set_update_version=set_update_version)
        viewer.setWindowOpacity(0)
        viewer.show()

        def finalize_ui_restore():
            # Because this runs AFTER app.exec() starts, Qt's geometry math will be flawless.
            viewer.load_ui_state()

            viewer.raise_()
            viewer.activateWindow()
            # Materialize the window in its perfect location
            viewer.setWindowOpacity(1.0)

        # Schedule the restoration to happen on the very first frame of the Event Loop
        QTimer.singleShot(50, finalize_ui_restore)
        exit_code = app.exec()

        # print_used_modules()

        sys.exit(exit_code)
    finally:
        if install_version is not None:
            from blinkview.utils.updater import Updater

            updater = Updater()
            updater.install(install_version)


def main():
    import argparse

    parser = argparse.ArgumentParser(description="BlinkView - A Real-Time Telemetry Visualization Tool")
    setup_gui_parser(parser)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

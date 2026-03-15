# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import sys

import signal
from pathlib import Path
from time import perf_counter

from PySide6.QtWidgets import QApplication, QToolButton
from PySide6.QtCore import Slot
from PySide6.QtGui import QIcon
from PySide6.QtWidgets import (
    QMainWindow
)

from PySide6.QtWidgets import (
    QDockWidget
)
from PySide6.QtGui import QAction
from PySide6.QtCore import Qt
from PySide6.QtCore import QTimer
from PySide6.QtWidgets import QTabWidget
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QToolBar, QMenu
)

from blinkview.ui.cli_args import setup_gui_parser
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.native_dark_mode import set_native_dark_mode
from blinkview.ui.utils.config_node import ConfigNode
from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.utils.ui_state_handler import UIStateHandler
from blinkview.ui.utils.window_manager import WindowManager
from blinkview.ui.widgets.config.dynamic_config import DynamicConfigWidget
from blinkview.ui.widgets.config.style_config import StyleConfig
from blinkview.ui.widgets.log_viewer import LogViewerWidget
from blinkview.ui.widgets.module_filter_model import ModuleFilterModel
from blinkview.ui.widgets.pipelines_sidebar import PipelinesSidebarWidget
from blinkview.ui.widgets.telemetry_model import TelemetryModel
from blinkview.ui.widgets.telemetry_table import TelemetryTable
from blinkview.ui.widgets.title_bar import TitleBar
from blinkview.ui.windows.detached_tab_window import DetachedTabWindow
from ..core.batch_queue import BatchQueue
from ..core.registry import Registry

from .widgets.device_sidebar import DeviceSidebarWidget


class BlinkMainWindow(QMainWindow):

    def __init__(self, session_name: str = None, project_name: str = None, log_dir: str | Path = None, config_path: str = None):
        super().__init__()
        self.setWindowTitle("BlinkView")
        self.resize(1280, 800)
        set_native_dark_mode(self)

        use_frameless = False  # Set to False to see the standard window frame (useful for debugging)

        # 1. Instantiate the secondary Log Window (but don't show it yet)
        # self.log_window = LogViewerWindow()

        self.gui_context = GUIContext()
        self.gui_context.set_register_log_target(self.register_log_target)
        self.gui_context.set_deregister_log_target(self.deregister_log_target)

        self.gui_context.set_registry(Registry(session_name=session_name, project_name=project_name, log_dir=log_dir, config_path=config_path))

        self.gui_context.registry.configure_system()

        self.gui_context.set_config_manager(ConfigNodeManager(self.gui_context))

        self.gui_context.set_widget_factory(self.create_widget)

        # 2. Setup the Toolbar and Button
        self.toolbar = QToolBar("Main Toolbar")

        self.btn_open_logs = QAction("Live Logs", self)
        self.btn_open_logs.triggered.connect(lambda _: self.create_widget("LogViewerWidget", "Live Logs"))
        self.toolbar.addAction(self.btn_open_logs)

        self.btn_open_system_logs = QAction("System Logs", self)
        self.btn_open_system_logs.triggered.connect(lambda _: self.create_widget("LogViewerWidget", "System Logs", allowed_device="SYSTEM"))
        self.toolbar.addAction(self.btn_open_system_logs)

        # --- Telemetry Action ---
        self.btn_open_telemetry = QAction("Telemetry", self)
        # Use an icon if you have one, e.g., QIcon("chart.png")
        self.btn_open_telemetry.triggered.connect(lambda _: self.create_widget("TelemetryTable", "Live Telemetry"))
        self.toolbar.addAction(self.btn_open_telemetry)

        self.toolbar.addSeparator()

        self.btn_show_settings = QAction("Settings", self)
        self.btn_show_settings.triggered.connect(
            lambda: self.gui_context.config_manager.show("/", "System", drop_keys=["plugins", "version", "pipelines", "sources"]))
        self.toolbar.addAction(self.btn_show_settings)

        self.btn_show_plugins = QAction("Plugins", self)
        self.btn_show_plugins.triggered.connect(
            lambda: self.gui_context.config_manager.show("/plugins", "Plugins"))
        self.toolbar.addAction(self.btn_show_plugins)

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

        # --- NEW: Set up the Central Tabbed Workspace ---
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
        devices_config_node.fetch()

        pipelines_config_node = self.gui_context.config_manager.create_node("/pipelines")
        self.pipelines_sidebar = PipelinesSidebarWidget(pipelines_config_node, gui_context=self.gui_context)
        # self.pipelines_sidebar.device_added.connect(self.on_add_device)
        self.pipelines_dock.setWidget(self.pipelines_sidebar)
        pipelines_config_node.fetch()

        # Keep a list so Python's garbage collector doesn't destroy our floating windows
        self.window_manager = WindowManager()

        if use_frameless:

            self.main_container = QWidget()
            self.main_layout = QVBoxLayout(self.main_container)
            self.main_layout.setContentsMargins(0, 0, 0, 0)
            self.main_layout.setSpacing(0)

            # 3. Add Custom Title Bar
            self.title_bar = TitleBar(self)
            self.main_layout.addWidget(self.title_bar)

            # 5. Set the container as the actual central widget

            # 6. Wire the Hamburger Menu
            self.title_bar.menu_btn.clicked.connect(self.show_main_menu)

            self.setWindowFlags(Qt.FramelessWindowHint | Qt.Window)
            self.toolbar.setMovable(False)
            self.main_layout.addWidget(self.toolbar)

            self.main_layout.addWidget(self.central_tabs)

            self.setCentralWidget(self.main_container)
        else:
            self.addToolBar(self.toolbar)

            self.setCentralWidget(self.central_tabs)

        # 3. Backend Integration
        self.input_queue = BatchQueue()
        self.put = self.input_queue.put
        # self.timestamp_formatter = ConsoleTimestampFormatter()

        self.log_targets = []

        self.gui_context.registry.subscribe(self)

        # 4. Signal Handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        self.last_poll_time = perf_counter()

        # 5. UI Poller (Runs here, updates the log window)
        # 60FPS Data Poller (Existing)
        self.gui_context.set_theme(StyleConfig())

        self.fps_slow = 1
        self.timeout_slow = 1000 // self.fps_slow
        self.timeout_fast = self.gui_context.theme.ui_update_rate_ms
        self.timer_fast = QTimer(self)
        self.timer_fast.timeout.connect(self.poll_queue)
        self.timer_fast.start(self.timeout_fast)

        self.gui_context.set_telemetry_model(TelemetryModel(gui_context=self.gui_context))

        self.gui_context.set_module_filter_model(ModuleFilterModel(gui_context=self.gui_context))

        # 1FPS Structure Syncer (New)
        self.timer_slow = QTimer(self)
        self.timer_slow.timeout.connect(self.gui_context.on_heartbeat)
        self.timer_slow.start(self.timeout_slow)  # 1 second

        self.widget_factories = {
            "LogViewerWidget": LogViewerWidget,
            "TelemetryTable": TelemetryTable,
            "DynamicConfigWidget": DynamicConfigWidget,
        }

        self.gui_context.set_gui_state_handler(UIStateHandler(self))
        self.gui_context.registry.file_manager.set_gui_context(self.gui_context)

        self.gui_context.registry.start()
        print("[BlinkMainWindow] Initialization complete.")

    def load_ui_state(self):
        self.gui_context.gui_state.load_ui_state(self.gui_context.registry.file_manager.get_config_path("gui_state"))

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
                schema = self.gui_context.registry.get_config_schema()
                print(f"[Fetching] system schema: {schema}")
                callback(schema)
            except Exception as e:
                print(f"[Fetching] error fetching system schema: {e}")

        system_ctx.tasks.run_task(fetch)

    # --- 1. Core Tab Management Helpers ---

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

    def create_widget(self, cls_name, name, as_window=False, **kwargs):
        """Routes a string class name to the correct factory method."""

        # 1. Prevent duplicate tabs using the helper
        if self.focus_tab_if_exists(name):
            return None

        if self.window_manager.raise_window(name):
            return None

        # window or tab doesnt exist, we need to create it

        print(f"[BlinkMainWindow] Opening widget: {cls_name} with name: {name} (as_window={as_window})")
        factory = self.widget_factories.get(cls_name)

        if not factory:
            print(f"⚠️ Warning: Unknown widget class '{cls_name}'.")
            return None

        # 2. Instantiate core widget
        widget = factory(self.gui_context, tab_name=name, **kwargs)

        # 3. Route to correct container
        if as_window:
            floating_win = DetachedTabWindow(self.gui_context, widget, name)
            self.window_manager.register(floating_win, widget)
            return floating_win
        else:
            self.add_tab_focused(widget, name)
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

            time_budget = self.timeout_fast * 0.8 / 1000  # Spend at most 80% of the frame time processing logs, converted to seconds
            batches_processed = 0

            while True:
                batch = get_nowait()
                if not batch:
                    break  # Queue is empty, all caught up!

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
                        f"[UI Monitor] ⚠️ Oversaturated! Processed {batches_processed} batches in {elapsed_processing * 1000:.1f}ms. Yielding to event loop.")
                    break  # Leave the remaining batches in the queue for the next timer tick

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
            if hasattr(self, 'deregister_log_target'):
                self.deregister_log_target(widget_to_close)

            # Then destroy it
            widget_to_close.close()
            widget_to_close.deleteLater()

        self.central_tabs.removeTab(index)

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
        widget = self.central_tabs.widget(index)
        title = self.central_tabs.tabText(index)

        # Remove it from the tab layout (this does NOT destroy the widget)
        self.central_tabs.removeTab(index)

        # Wrap it and show it
        floating_win = DetachedTabWindow(self.gui_context, widget, title)
        self.window_manager.register(floating_win, widget)
        floating_win.show()

    def reattach_tab(self, widget, title: str):
        """Triggered by the floating window when it is closed."""
        # Add it back to the tab bar
        tab_index = self.central_tabs.addTab(widget, title)

        # Immediately focus the re-attached tab
        self.central_tabs.setCurrentIndex(tab_index)


def run(args):
    # Force Windows to show the custom icon in the taskbar
    if sys.platform == "win32":
        import ctypes
        try:
            myappid = 'ee.incubator.blinkview.0.1.0'  # Arbitrary string
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
        app.setStyle('Fusion')

    # Set the global application icon
    app.setWindowIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icon.png")))

    # Pass the queue into the viewer
    viewer = BlinkMainWindow(config_path=args.config, session_name=args.session, project_name=args.project, log_dir=args.logdir)
    viewer.setWindowOpacity(0)
    viewer.show()

    def finalize_ui_restore():
        # Because this runs AFTER app.exec() starts, Qt's geometry math will be flawless.
        viewer.load_ui_state()

        viewer.raise_()
        viewer.activateWindow()
        # Materialize the window in its perfect location
        viewer.setWindowOpacity(1.0)

    # 4. Schedule the restoration to happen on the very first frame of the Event Loop
    QTimer.singleShot(50, finalize_ui_restore)

    sys.exit(app.exec())


def main():
    import argparse
    parser = argparse.ArgumentParser(description="BlinkView - A Real-Time Telemetry Visualization Tool")
    setup_gui_parser(parser)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

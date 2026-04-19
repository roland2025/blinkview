# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import os
import sys
from pathlib import Path
from typing import Optional

os.environ["NUMBA_DEBUG_CACHE"] = "1"


def run(args):
    from qtpy.QtCore import Qt, QTimer
    from qtpy.QtGui import QIcon
    from qtpy.QtWidgets import (
        QApplication,
    )

    if "QT_API" not in os.environ:
        os.environ["QT_API"] = "pyside6"

    from blinkview import __version__ as blinkview_version

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

            # Use the 'getattr' or 'hasattr' approach to satisfy both Qt 5 and 6
            # because qtpy cannot export what the underlying C++ library removed.

            if hasattr(Qt, "AA_EnableHighDpiScaling"):
                QApplication.setAttribute(Qt.AA_EnableHighDpiScaling)

            if hasattr(Qt, "AA_UseHighDpiPixmaps"):
                QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps)

            # For Qt 6 / PySide6, this is the modern way to handle fractional scaling
            # (like 125% or 175% windows scaling)
            if hasattr(Qt, "HighDpiScaleFactorRoundingPolicy"):
                QApplication.setHighDpiScaleFactorRoundingPolicy(Qt.HighDpiScaleFactorRoundingPolicy.PassThrough)

        app = QApplication(sys.argv)

        app.setStyle("Fusion")
        use_qdarktheme = True
        if use_qdarktheme:
            print("DEBUG: About to import qdarktheme...")
            import qdarktheme

            print("DEBUG: qdarktheme imported!")

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

        # Set the global application icon
        app.setWindowIcon(QIcon(str(Path(__file__).parent.parent / "assets" / "icon.png")))

        from blinkview.core.settings_manager import SettingsManager

        settings = SettingsManager()

        from blinkview.ui.widgets.update_widget import UpdateWidget

        if not UpdateWidget.ensure_update_path(settings):
            print("Update path setup aborted by user. Exiting.")
            sys.exit(0)

        # 2. Export Numba Cache BEFORE importing Registry/Kernels
        from blinkview.core.numba_setup import export_numba_cache

        cache_path = export_numba_cache(settings)
        print(f"Numba cache exported to: {cache_path}")

        from blinkview.core.registry import Registry

        registry = Registry(
            session_name=args.session,
            profile_name=args.profile,
            log_dir=args.logdir,
            config_path=args.config,
            settings=settings,
        )

        from blinkview.ui.main_window import BlinkMainWindow

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

    from blinkview.ui.cli_args import setup_gui_parser

    parser = argparse.ArgumentParser(description="BlinkView - A Real-Time Telemetry Visualization Tool")
    setup_gui_parser(parser)
    args = parser.parse_args()
    run(args)


if __name__ == "__main__":
    main()

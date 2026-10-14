# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""`blink setup`: compiles every Numba kernel into the on-disk cache ahead of time, so the next
`blink` starts without the cold-cache compile (~75 s, plans/background-warmup.md). The updater
runs it after installing a new version; it can also be run by hand. A GUI start on a cold cache
compiles behind its startup splash instead (plans/startup-splash.md)."""

import os
import sys
from threading import Event
from time import perf_counter
from typing import TYPE_CHECKING, Callable, Optional

if TYPE_CHECKING:
    from blinkview.core.warmup import NumbaWarmupHelper


def setup_setup_parser(parser):
    parser.add_argument(
        "--no-gui",
        action="store_true",
        help="No progress window: print the progress instead (the default without a display).",
    )


def build_warmup_helper(settings) -> "NumbaWarmupHelper":
    """The warmup environment without a Registry - no project, profile or session is needed (or
    created) to compile kernels. Imports the same registering modules as the GUI: core/registry.py
    for the pipeline's, widget_registry for the widgets'."""
    import blinkview.core.registry  # noqa: F401 - registers the factories and the pipeline kernels' warmups
    from blinkview.core.array_pool import NumpyArrayPool
    from blinkview.core.factory_category_registry import build_system_factory_registry
    from blinkview.core.system_context import SystemContext
    from blinkview.core.task_manager import TaskManager
    from blinkview.core.warmup import NumbaWarmupHelper
    from blinkview.ui.widget_registry import import_registerable_widgets
    from blinkview.utils.time_utils import TimeUtils

    import_registerable_widgets()

    ctx = SystemContext(
        time_ns=TimeUtils().now_ns,
        registry=None,
        id_registry=None,
        factories=build_system_factory_registry(),
        tasks=TaskManager(),
        settings=settings,
        array_pool=NumpyArrayPool(max_bytes=64 * 1024 * 1024),
        pid_history=None,
    )
    return NumbaWarmupHelper(ctx)


def compile_kernels(
    helper: "NumbaWarmupHelper",
    on_progress: Optional[Callable[[float, str], None]] = None,
    cancel: Optional[Event] = None,
) -> Optional[str]:
    """Runs the whole warmup. Returns None when every kernel compiled, else what went wrong."""
    from blinkview.core.warmup import WarmupCancelled

    try:
        helper.run_all(on_progress=on_progress, cancel=cancel)
    except WarmupCancelled:
        return "cancelled"
    except Exception as e:
        import traceback

        traceback.print_exc()
        return str(e) or type(e).__name__
    finally:
        helper.shared.tasks.shutdown()

    return None


def _has_display() -> bool:
    if sys.platform != "linux":
        return True
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


def ensure_source_repo(settings, headless: bool) -> bool:
    """A source checkout keeps its cache next to the repo (update.path). When that isn't set yet,
    asks for it with the same dialog as the GUI's first start (ui/run.py); without a window
    there's nobody to ask, so that's an error. False = no repo, don't compile."""
    from blinkview.utils.updater import GitUpdater

    # An unset path must not reach is_valid_repo(): Path("") is the current directory.
    path = settings.get("update.path")
    if path and GitUpdater.is_valid_repo(path):
        return True

    if headless:
        print(
            "Error: update.path is not set. Run: blink config --global update.path /path/to/repo",
            file=sys.stderr,
        )
        return False

    from blinkview.ui.widgets.update_widget import UpdateWidget

    if not UpdateWidget.ensure_update_path(settings):
        print("Update path setup aborted by user. Exiting.", file=sys.stderr)
        return False
    return True


def _print_progress():
    """on_progress for --no-gui: one line per kernel group, not one per sub-step."""
    last_label = None

    def on_progress(fraction: float, label: str):
        nonlocal last_label
        if label and label != last_label:
            last_label = label
            print(f"[{int(fraction * 100):3d}%] {label}", flush=True)

    return on_progress


def run_setup_window(app, settings) -> Optional[str]:
    """`blink setup`'s progress window over a whole warmup. Runs the event loop until the window
    has closed. Returns None when every kernel compiled, "cancelled", or what went wrong. Needs
    export_numba_cache() first."""
    from qtpy.QtCore import QTimer

    from blinkview.ui.windows.setup_window import SetupWindow

    helper = None

    def compile_fn(on_progress, cancel):
        return compile_kernels(helper, on_progress=on_progress, cancel=cancel)

    def begin():
        # Importing every kernel and widget module takes a few seconds by itself, so the
        # window is shown first. On this thread, not the compile thread: some of those
        # modules create QObjects on import.
        nonlocal helper
        try:
            helper = build_warmup_helper(settings)
        except Exception as e:
            import traceback

            traceback.print_exc()
            window.fail(str(e) or type(e).__name__)
            return
        window.start()

    window = SetupWindow(compile_fn)
    window.center_on_screen()
    window.show()
    QTimer.singleShot(50, begin)
    app.exec()
    return window.error or ("cancelled" if window.exit_code != 0 else None)


def run_setup(args):
    from blinkview import ICON_PATH, __app_name__, __version__

    headless = args.no_gui or not _has_display()

    if "QT_API" not in os.environ:
        os.environ["QT_API"] = "pyside6"
    if headless:
        # The widget modules are still imported (their kernels are compiled too), so Qt has to
        # load - just without a screen.
        os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")
    elif sys.platform == "linux":
        os.environ.setdefault("QT_QPA_PLATFORM", "xcb")

    from qtpy.QtCore import QLoggingCategory
    from qtpy.QtGui import QIcon
    from qtpy.QtWidgets import QApplication

    from blinkview.core.settings_manager import SettingsManager
    from blinkview.utils.install_info import UpdateSource, resolve_update_source

    settings = SettingsManager()
    update_source = resolve_update_source(settings)

    QLoggingCategory.setFilterRules("qt.gui.icc=false")  # unparseable monitor profiles; see ui/run.py
    app = QApplication.instance() or QApplication(sys.argv)
    app.setApplicationName(__app_name__)
    if not headless:
        import qdarktheme

        app.setStyle("Fusion")
        qdarktheme.setup_theme("dark", corner_shape="sharp")
        app.setWindowIcon(QIcon(str(ICON_PATH)))

    if update_source == UpdateSource.GIT and not ensure_source_repo(settings, headless):
        sys.exit(2)

    # Before anything imports a kernel, like ui/run.py.
    from blinkview.core.numba_setup import export_numba_cache

    cache_path = export_numba_cache(settings, update_source)

    started_at = perf_counter()

    if headless:
        print(f"Compiling {__app_name__} v{__version__} kernels into {cache_path} ...")
        error = compile_kernels(build_warmup_helper(settings), on_progress=_print_progress())
    else:
        error = run_setup_window(app, settings)

    if error:
        print(f"Setup did not finish: {error}", file=sys.stderr)
        sys.exit(1)
    print(f"Kernels compiled in {perf_counter() - started_at:.0f} s. Cache: {cache_path}")

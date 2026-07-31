# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.module_snapshot import LatestModuleValueTracker
from blinkview.core.registry import Registry
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.config.style_config import StyleConfig


def make_real_registry(tmp_path, session_name, *, profile_name=None, with_value_tracker=False, start=False):
    """Builds and configures a real Registry against a tmp_path log_dir - the common setup
    shared by the widget-playback end-to-end tests. Caller is responsible for reg.stop().

    config_path is pinned under tmp_path so ConfigManager reads/writes an isolated, throwaway
    file instead of falling back to FileManager's default profile location - which resolves to
    the real project's actual `.blinkview/profiles/<profile>/<profile>.json` on disk. Without
    this, any test that calls send_config/save_config on a real Registry (e.g. anything touching
    TelemetryWatch's config node) silently persists test data into that real file, corrupting the
    developer's actual local app state across test runs."""
    kwargs = {"session_name": session_name, "log_dir": tmp_path, "config_path": tmp_path / "test_config.json"}
    if profile_name is not None:
        kwargs["profile_name"] = profile_name
    reg = Registry(**kwargs)
    reg.configure_system()
    if with_value_tracker:
        reg.module_value_tracker = LatestModuleValueTracker(
            reg.central.log_pool, reg.id_registry.modules_table, reg.system_ctx.array_pool, reg.now_ns
        )
    if start:
        reg.start()
    return reg


def make_real_gui_context(registry, *, logger_name="gui"):
    """Builds a real GUIContext wired to `registry` - the common setup shared by the
    widget-playback end-to-end tests."""
    gui_context = GUIContext()
    gui_context.set_registry(registry)
    gui_context.set_theme(StyleConfig())
    gui_context.logger = registry.logger_creator(logger_name)()
    # BlinkMainWindow always wires these in the real app (see ui/main_window.py); several
    # widgets' closeEvent (e.g. LogViewerWidget, TelemetryPlotter) call
    # gui_context.deregister_log_target unconditionally, which otherwise stays None here and
    # crashes the moment something actually closes the widget (e.g. qtbot.addWidget's automatic
    # close() at test teardown - deleteLater() alone never triggered closeEvent, which is why
    # this went unnoticed before).
    gui_context.set_register_log_target(lambda target: None)
    gui_context.set_deregister_log_target(lambda target: None)
    return gui_context

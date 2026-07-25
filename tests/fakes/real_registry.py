from blinkview.core.module_snapshot import LatestModuleValueTracker
from blinkview.core.registry import Registry
from blinkview.ui.gui_context import GUIContext
from blinkview.ui.widgets.config.style_config import StyleConfig


def make_real_registry(tmp_path, session_name, *, profile_name=None, with_value_tracker=False, start=False):
    """Builds and configures a real Registry against a tmp_path log_dir - the common setup
    shared by the widget-playback end-to-end tests. Caller is responsible for reg.stop()."""
    kwargs = {"session_name": session_name, "log_dir": tmp_path}
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
    return gui_context

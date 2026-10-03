# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Apply in the Settings dialog must not delete profile keys the dialog doesn't show.

The dialog (main_window's "Settings" menu entry) opens the profile root `/`; its schema only
describes `central` and `reorder`. Apply used to diff the full root config against the form's
fields, so every other top-level key - the profile's `parameters` block, and anything else -
became a `remove` op and was gone from the profile file. Real Registry, real ConfigNodeManager,
real DynamicConfigWidget, checked on the file on disk."""

import json

from fakes.real_registry import make_real_gui_context, make_real_registry

from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.widgets.config.dynamic_config import DynamicConfigWidget

SETTINGS_DROP_KEYS = ["plugins", "version", "pipelines", "sources"]  # as main_window's Settings entry


def _find_spin_or_line_edit(registry: dict):
    """Any primitive field the test can change, from the dialog's widget registry."""
    for node in registry.values():
        if node.get("type") == "object":
            found = _find_spin_or_line_edit(node.get("registry", {}))
            if found:
                return found
        widget = node.get("widget")
        if widget is not None and hasattr(widget, "setValue") and hasattr(widget, "value"):
            return widget
    return None


def test_settings_apply_keeps_parameters_and_other_unknown_profile_keys(tmp_path, qapp, qtbot):
    config_path = tmp_path / "test_config.json"
    # A full default profile first: a sparse hand-written file fails configure_system().
    reg = make_real_registry(tmp_path, "settings_dialog")
    try:
        reg.config.apply_patch(
            "/",
            [
                {"op": "add", "path": "/parameters", "value": {"serial": {"paths": [], "description": "probe"}}},
                {"op": "add", "path": "/some_future_section", "value": {"keep": True}},
            ],
        )
        before = json.loads(config_path.read_text())
        assert "parameters" in before and "some_future_section" in before

        ctx = make_real_gui_context(reg)
        ctx.set_config_manager(ConfigNodeManager(ctx))
        dialog = DynamicConfigWidget(
            ctx, state={"path": "/", "child_name": "System", "drop_keys": SETTINGS_DROP_KEYS, "editable": True}
        )
        qtbot.addWidget(dialog)
        qtbot.waitUntil(lambda: "central" in dialog.current_config, timeout=10_000)
        assert "parameters" in dialog.current_config  # the dialog does load them

        field = _find_spin_or_line_edit(dialog._widget_registry)
        assert field is not None, "no numeric field found in the Settings dialog"
        field.setValue(field.value() + 1)
        dialog._on_apply_clicked()

        qtbot.waitUntil(lambda: json.loads(config_path.read_text()) != before, timeout=10_000)
        after = json.loads(config_path.read_text())
        assert after["parameters"] == before["parameters"]
        assert after["some_future_section"] == {"keep": True}
    finally:
        reg.stop()

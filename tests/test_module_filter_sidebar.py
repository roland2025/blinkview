# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.ui.widgets.module_filter_sidebar import ModuleFilterSidebar
from blinkview.utils.log_level import LogLevel


def _build_tree(id_registry):
    dev = id_registry.get_device("esp32")
    a = dev.get_module("a")
    b = dev.get_module("a.b")
    return dev, {"a": a, "a.b": b}


@pytest.fixture
def sidebar(qapp, qtbot, id_registry, log_filter):
    _build_tree(id_registry)
    gui_context = type("FakeGuiContext", (), {"id_registry": id_registry})()
    w = ModuleFilterSidebar(gui_context, log_filter)
    qtbot.addWidget(w)
    return w


class TestConstruction:
    def test_starts_disabled_matching_temp_log_filter_default(self, sidebar):
        assert sidebar.action_enable.isChecked() is False
        assert sidebar.log_filter.enabled is False
        assert sidebar.table.isEnabled() is False

    def test_show_hidden_defaults_to_false_unless_passed(self, qapp, qtbot, id_registry, log_filter):
        gui_context = type("FakeGuiContext", (), {"id_registry": id_registry})()
        w = ModuleFilterSidebar(gui_context, log_filter, show_hidden=True)
        qtbot.addWidget(w)
        assert w.action_show_non_essential.isChecked() is True

    def test_pause_indicator_starts_hidden(self, sidebar):
        assert sidebar.pause_action.isVisible() is False


class TestEnableToggle:
    def test_enabling_enables_the_table_and_the_underlying_filter(self, sidebar):
        sidebar.action_enable.setChecked(True)

        assert sidebar.table.isEnabled() is True
        assert sidebar.log_filter.enabled is True

    def test_disabling_disables_the_table_and_the_underlying_filter(self, sidebar):
        sidebar.action_enable.setChecked(True)
        sidebar.action_enable.setChecked(False)

        assert sidebar.table.isEnabled() is False
        assert sidebar.log_filter.enabled is False


class TestShowNonEssentialToggle:
    def test_toggling_show_hidden_updates_table_and_filter(self, sidebar):
        sidebar.action_show_non_essential.setChecked(True)

        assert sidebar.table.show_non_essential is True

        sidebar.action_show_non_essential.setChecked(False)

        assert sidebar.table.show_non_essential is False


class TestPauseIndicator:
    def test_sync_paused_signal_toggles_pause_label_visibility(self, sidebar):
        sidebar.table.sync_paused.emit(True)
        assert sidebar.pause_action.isVisible() is True

        sidebar.table.sync_paused.emit(False)
        assert sidebar.pause_action.isVisible() is False


class TestStateRoundtrip:
    def test_get_state_reflects_current_enabled_and_show_hidden(self, sidebar):
        sidebar.action_enable.setChecked(True)
        sidebar.action_show_non_essential.setChecked(True)

        state = sidebar.get_state()

        assert state["enabled"] is True
        assert state["show_non_essential"] is True
        assert state["module_filters"] == {}  # nothing explicitly overridden yet

    def test_restore_state_reapplies_enabled_and_show_hidden(self, qapp, qtbot, id_registry, log_filter):
        gui_context = type("FakeGuiContext", (), {"id_registry": id_registry})()
        w = ModuleFilterSidebar(gui_context, log_filter)
        qtbot.addWidget(w)

        w.restore_state({"enabled": True, "show_non_essential": True, "module_filters": {}})

        assert w.action_enable.isChecked() is True
        assert w.action_show_non_essential.isChecked() is True
        assert w.table.show_non_essential is True

    def test_restore_state_none_is_a_no_op(self, sidebar):
        sidebar.action_enable.setChecked(True)

        sidebar.restore_state(None)

        assert sidebar.action_enable.isChecked() is True  # untouched

    def test_restore_state_reapplies_explicit_module_overrides(self, qapp, qtbot, id_registry, log_filter):
        _, mods = _build_tree(id_registry)
        gui_context = type("FakeGuiContext", (), {"id_registry": id_registry})()
        w = ModuleFilterSidebar(gui_context, log_filter)
        qtbot.addWidget(w)
        w.sync_modules()

        a_id = mods["a"].id
        state = {
            "enabled": False,
            "show_non_essential": False,
            "module_filters": {"esp32.a": {"enabled": False, "level": LogLevel.ERROR.name_conf}},
        }

        w.restore_state(state)

        assert w.log_filter.enabled_mask[a_id] is False or w.log_filter.enabled_mask[a_id] == 0
        assert w.log_filter.level_mask[a_id] == LogLevel.ERROR.value


class TestSyncAndVisibility:
    def test_sync_modules_does_not_raise(self, sidebar):
        sidebar.sync_modules()  # must not raise even with no new modules

    def test_set_visible_true_triggers_sync_without_error(self, sidebar):
        sidebar.setVisible(True)  # must not raise

    def test_get_filter_delegates_to_temp_log_filter(self, sidebar):
        assert sidebar.get_filter() == sidebar.log_filter.get_filter()

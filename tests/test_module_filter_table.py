# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import pytest

from blinkview.ui.widgets.module_filter_table import TempLogFilter
from blinkview.utils.log_filter import LogFilter
from blinkview.utils.log_level import LogLevel


def _gui_context(id_registry):
    return SimpleNamespace(id_registry=id_registry)


def _build_tree(id_registry, essential=True):
    """esp32.a, esp32.a.b, esp32.c - a small two-branch tree under one device."""
    dev = id_registry.get_device("esp32", essential=essential)
    a = dev.get_module("a")
    b = dev.get_module("a.b")
    c = dev.get_module("c")
    return dev, {"a": a, "b": b, "c": c}


@pytest.fixture
def unconstrained_filter(qapp, id_registry, log_filter):
    _build_tree(id_registry)
    tlf = TempLogFilter(_gui_context(id_registry), log_filter)
    tlf.sync_modules()
    return tlf, id_registry


class TestConstruction:
    def test_unconstrained_starts_fully_enabled(self, qapp, id_registry, log_filter):
        _build_tree(id_registry)
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        assert tlf.enabled_mask[:5].all()

    def test_constrained_to_single_module_starts_disabled_except_target(self, qapp, id_registry):
        dev, mods = _build_tree(id_registry)
        constrained_filter = LogFilter(id_registry, filtered_module="esp32.a", filtered_module_children=False)

        tlf = TempLogFilter(_gui_context(id_registry), constrained_filter)

        assert tlf.enabled_mask[mods["a"].id]
        assert not tlf.enabled_mask[mods["c"].id]

    def test_constrained_to_subtree_enables_children_too(self, qapp, id_registry):
        dev, mods = _build_tree(id_registry)
        constrained_filter = LogFilter(id_registry, filtered_module="esp32.a", filtered_module_children=True)

        tlf = TempLogFilter(_gui_context(id_registry), constrained_filter)
        # Descendant states are only baked lazily as ensure_capacity/sync_modules touches
        # them (mirroring how the real table view initializes rows as they're painted) -
        # b sits beyond the tab-constraint's own ensure_capacity(a.id + 1) call.
        tlf.sync_modules()

        assert tlf.enabled_mask[mods["a"].id]
        assert tlf.enabled_mask[mods["b"].id]  # child of a
        assert not tlf.enabled_mask[mods["c"].id]


class TestSetModuleEnabled:
    def test_disabling_a_module_bakes_filter_to_off(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id = mods["a"].id

        tlf.set_module_enabled(a_id, False)

        assert not tlf.enabled_mask[a_id]
        assert tlf.filter_mask[a_id] == LogLevel.OFF.value

    def test_setting_same_value_does_not_emit_filter_changed(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id = mods["a"].id

        received = []
        tlf.filter_changed.connect(lambda: received.append(True))

        tlf.set_module_enabled(a_id, True)  # already enabled, no state change

        assert received == []


class TestSetModuleLevel:
    def test_changing_level_updates_filter_mask(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id = mods["a"].id

        tlf.set_module_level(a_id, LogLevel.WARN)

        assert tlf.level_mask[a_id] == LogLevel.WARN.value
        assert tlf.filter_mask[a_id] == LogLevel.WARN.value  # enabled + essential -> baked to new level


class TestSubtreeOperations:
    def test_set_subtree_enabled_propagates_to_descendants_only(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id, b_id, c_id = mods["a"].id, mods["a.b"].id, mods["c"].id

        tlf.set_subtree_enabled(a_id, False)

        assert not tlf.enabled_mask[a_id]
        assert not tlf.enabled_mask[b_id]  # descendant of a
        assert tlf.enabled_mask[c_id]  # unrelated sibling untouched

    def test_set_subtree_level_only_changes_level_not_enabled(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id, b_id = mods["a"].id, mods["a.b"].id

        tlf.set_subtree_level(a_id, LogLevel.ERROR)

        assert tlf.enabled_mask[a_id]  # untouched
        assert tlf.level_mask[a_id] == LogLevel.ERROR.value
        assert tlf.level_mask[b_id] == LogLevel.ERROR.value


class TestShowHidden:
    def test_show_hidden_toggle_rebakes_non_essential_modules_visible(self, qapp, id_registry, log_filter):
        _build_tree(id_registry, essential=False)  # esp32.a/b/c all non-essential by default
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        tlf.sync_modules()
        mods = id_registry.get_device("esp32").path_lookup
        a_id = mods["a"].id

        assert tlf.filter_mask[a_id] == LogLevel.OFF.value  # hidden by default (show_hidden=False)

        tlf.set_show_hidden(True)

        assert tlf.filter_mask[a_id] == tlf.level_mask[a_id]  # now visible


class TestStateRoundtrip:
    def test_get_state_then_restore_state_reproduces_explicit_overrides(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id, c_id = mods["a"].id, mods["c"].id

        tlf.set_module_enabled(a_id, False)
        tlf.set_module_level(c_id, LogLevel.ERROR)

        state = tlf.get_state()
        assert "esp32.a" in state
        assert state["esp32.a"]["enabled"] is False
        assert "esp32.c" in state
        assert state["esp32.c"]["level"] == LogLevel.ERROR.name_conf

        # Build a fresh filter and restore into it.
        fresh_log_filter = LogFilter(id_registry, log_level=LogLevel.ALL.name_conf)
        fresh = TempLogFilter(_gui_context(id_registry), fresh_log_filter)
        fresh.sync_modules()
        fresh.restore_state(state)

        assert not fresh.enabled_mask[a_id]
        assert fresh.level_mask[c_id] == LogLevel.ERROR.value

    def test_get_state_omits_modules_matching_inherited_default(self, unconstrained_filter):
        """Only modules that deviate from what they'd naturally inherit should be serialized -
        this keeps saved filter state small and forward-compatible with tree changes."""
        tlf, id_registry = unconstrained_filter

        state = tlf.get_state()

        assert state == {}  # nothing overridden yet, everything matches its default


class TestResetAll:
    def test_reset_all_clears_explicit_overrides(self, unconstrained_filter):
        tlf, id_registry = unconstrained_filter
        mods = id_registry.get_device("esp32").path_lookup
        a_id = mods["a"].id

        tlf.set_module_enabled(a_id, False)
        assert not tlf.enabled_mask[a_id]

        tlf.reset_all()

        assert tlf.enabled_mask[a_id]
        assert tlf.level_mask[a_id] == LogLevel.ALL.value

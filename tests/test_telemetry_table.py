# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import pytest
from qtpy.QtCore import QModelIndex, Qt
from qtpy.QtWidgets import QMenu

from blinkview.core.numpy_batch_manager import PooledLogBatch
from blinkview.ui.widgets.action_button_delegate import TelemetryCol
from blinkview.ui.widgets.telemetry_table import TelemetryTable, TelemetryTableModel
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "telemetry_table_test", with_value_tracker=True)
    yield reg
    reg.stop()


def _emit(registry, device, module, msg, ts=None):
    ts = ts if ts is not None else registry.now_ns()
    array_pool = registry.system_ctx.array_pool
    log_pool = registry.central.log_pool
    src = array_pool.create(PooledLogBatch, 4, 4096, has_levels=True, has_modules=True, has_devices=True)
    with src:
        src.insert_any(ts, ts, msg.encode("ascii"), level=0, module=module.id, device=device.id)
        log_pool.batch_append(src)
    registry.module_value_tracker.update()


@pytest.fixture
def table(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)
    w = TelemetryTable(gui_context)
    qtbot.addWidget(w)
    return w


def _msg_for(table, module):
    for row, mod_id in enumerate(table.model.visible_mod_ids):
        if table.model.modules[mod_id] == module:
            return table.model.index(row, TelemetryCol.VALUE).data()
    return None


class TestModelFilters:
    def test_hide_empty_excludes_modules_with_no_data(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        device = registry.id_registry.get_device("filter_dev")
        empty_mod = device.get_module("empty")
        model._sync_registry(registry.id_registry.module_list)

        model.set_hide_empty(True)
        assert empty_mod.id not in model.visible_mod_ids.tolist()

        model.set_hide_empty(False)
        assert empty_mod.id in model.visible_mod_ids.tolist()

    def test_allowed_device_filters_other_devices_out(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev_a = registry.id_registry.get_device("dev_a")
        mod_a = dev_a.get_module("m")
        dev_b = registry.id_registry.get_device("dev_b")
        mod_b = dev_b.get_module("m")
        model._sync_registry(registry.id_registry.module_list)

        model.set_allowed_device(dev_a)

        ids = model.visible_mod_ids.tolist()
        assert mod_a.id in ids
        assert mod_b.id not in ids

    def test_allowed_module_without_children_scopes_to_the_same_parent_subtree(self, qapp, registry):
        # "without children" checks the ancestor chain against allowed_module's *parent*, not
        # allowed_module itself - so it includes target's own descendants too (their ancestor
        # chain still passes through that same parent), just not modules under a different
        # branch of the tree entirely.
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("hier_dev")
        parent = dev.get_module("parent")
        other_parent = dev.get_module("other_parent")
        target = dev.get_module("parent.target")
        sibling = dev.get_module("parent.sibling")
        grandchild = dev.get_module("parent.target.grandchild")
        unrelated = dev.get_module("other_parent.unrelated")
        model._sync_registry(registry.id_registry.module_list)

        model.set_allowed_module(target)
        model.set_allowed_module_children(False)

        ids = model.visible_mod_ids.tolist()
        assert target.id in ids
        assert sibling.id in ids  # same parent as target
        assert grandchild.id in ids  # descendant of target, still under target's parent
        assert unrelated.id not in ids  # under a different parent entirely

    def test_allowed_module_with_children_shows_the_subtree(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("hier_dev2")
        target = dev.get_module("target")
        grandchild = dev.get_module("target.child.grandchild")
        unrelated = dev.get_module("unrelated")
        model._sync_registry(registry.id_registry.module_list)

        model.set_allowed_module(target)
        model.set_allowed_module_children(True)

        ids = model.visible_mod_ids.tolist()
        assert target.id in ids
        assert grandchild.id in ids
        assert unrelated.id not in ids

    def test_text_filter_positive_and_negative_groups(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("textdev")
        wifi = dev.get_module("wifi")
        ble = dev.get_module("ble")
        model._sync_registry(registry.id_registry.module_list)

        model.set_filter_text("wifi")
        ids = model.visible_mod_ids.tolist()
        assert wifi.id in ids
        assert ble.id not in ids

        model.set_filter_text("-wifi")
        ids = model.visible_mod_ids.tolist()
        assert wifi.id not in ids
        assert ble.id in ids

    def test_sort_by_name_vs_device(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("sortdev")
        dev.get_module("zeta")
        dev.get_module("alpha")
        model._sync_registry(registry.id_registry.module_list)

        model.sort(TelemetryCol.NAME, Qt.AscendingOrder)
        names = [model.modules[mid].name for mid in model.visible_mod_ids]
        assert names == sorted(names)


class TestModelHeaderAndData:
    def test_header_data_labels(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)

        assert model.headerData(TelemetryCol.NAME, Qt.Horizontal) == "Module"
        assert model.headerData(TelemetryCol.VALUE, Qt.Horizontal) == "Value"
        assert model.headerData(TelemetryCol.DEVICE, Qt.Horizontal) == "Device"
        assert model.headerData(TelemetryCol.ACTIONS, Qt.Horizontal) == "Actions"

    def test_row_and_column_count_are_zero_for_a_valid_parent(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("countdev")
        dev.get_module("m")
        model._sync_registry(registry.id_registry.module_list)

        real_index = model.index(0, 0)
        assert model.rowCount(real_index) == 0
        assert model.columnCount(real_index) == 0
        assert model.rowCount() == len(model.visible_mod_ids)

    def test_data_invalid_index_returns_none(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        assert model.data(QModelIndex()) is None

    def test_data_out_of_range_row_returns_none(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("oor_dev")
        dev.get_module("m")
        model._sync_registry(registry.id_registry.module_list)

        idx = model.index(len(model.visible_mod_ids) + 5, TelemetryCol.NAME)
        assert model.data(idx) is None

    def test_data_shows_device_and_name(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("displaydev")
        mod = dev.get_module("mymodule")
        model._sync_registry(registry.id_registry.module_list)

        row = model.visible_mod_ids.tolist().index(mod.id)
        assert model.data(model.index(row, TelemetryCol.DEVICE)) == "displaydev"
        assert model.data(model.index(row, TelemetryCol.NAME)) == "mymodule"

    def test_data_value_column_placeholder_before_any_message(self, qapp, registry):
        gui_context = make_real_gui_context(registry)
        model = TelemetryTableModel(gui_context)
        dev = registry.id_registry.get_device("valdev")
        mod = dev.get_module("mymodule")
        model._sync_registry(registry.id_registry.module_list)

        row = model.visible_mod_ids.tolist().index(mod.id)
        assert model.data(model.index(row, TelemetryCol.VALUE)) == "---"


class TestWidgetStateRoundTrip:
    def test_get_state_and_restore(self, qapp, qtbot, registry):
        gui_context = make_real_gui_context(registry)
        dev = registry.id_registry.get_device("statedev")
        mod = dev.get_module("statemod")

        w = TelemetryTable(gui_context)
        qtbot.addWidget(w)
        w.model.set_allowed_device(dev)
        w.filtered_device = dev
        w.search_box.setText("statemod")
        w.hide_empty = False
        w.action_hide_empty.setChecked(False)
        w.model.set_hide_empty(False)

        state = w.get_state()
        assert state["filtered_device"] == "statedev"
        assert state["filter_pattern"] == "statemod"
        assert state["hide_empty"] is False

        w2 = TelemetryTable(gui_context, state=state)
        qtbot.addWidget(w2)

        assert w2.filtered_device == dev
        assert w2.hide_empty is False
        assert w2.search_box.text() == "statemod"


class TestToggles:
    def test_toggle_device_column_hides_and_shows(self, table):
        table._toggle_device_column(False)
        assert table.view.isColumnHidden(TelemetryCol.DEVICE) is True

        table._toggle_device_column(True)
        assert table.view.isColumnHidden(TelemetryCol.DEVICE) is False

    def test_toggle_hide_empty_updates_model(self, table):
        table._toggle_hide_empty(False)
        assert table.model.hide_empty is False

    def test_toggle_show_non_essential_updates_model(self, table):
        table._toggle_show_non_essential(True)
        assert table.model.show_non_essential is True

    def test_on_search_changed_filters_the_model(self, table, registry):
        dev = registry.id_registry.get_device("searchdev")
        mod = dev.get_module("findme")
        table.model._sync_registry(registry.id_registry.module_list)

        table._on_search_changed("findme")

        assert mod.id in table.model.visible_mod_ids.tolist()


class TestGetModuleAtIndex:
    def test_returns_module_for_name_column(self, table, registry):
        dev = registry.id_registry.get_device("idxdev")
        mod = dev.get_module("idxmod")
        table.model._sync_registry(registry.id_registry.module_list)

        row = table.model.visible_mod_ids.tolist().index(mod.id)
        index = table.model.index(row, TelemetryCol.NAME)

        assert table._get_module_at_index(index) == mod

    def test_returns_none_for_other_columns(self, table, registry):
        dev = registry.id_registry.get_device("idxdev2")
        mod = dev.get_module("idxmod2")
        table.model._sync_registry(registry.id_registry.module_list)

        row = table.model.visible_mod_ids.tolist().index(mod.id)
        index = table.model.index(row, TelemetryCol.DEVICE)

        assert table._get_module_at_index(index) is None

    def test_returns_none_for_invalid_index(self, table):
        assert table._get_module_at_index(QModelIndex()) is None


class TestSortHandlers:
    def test_sort_by_device_toggles_order(self, table):
        header = table.view.horizontalHeader()
        table.sort_by_device()
        first_order = header.sortIndicatorOrder()
        table.sort_by_device()
        second_order = header.sortIndicatorOrder()
        assert first_order != second_order

    def test_sort_by_module_sorts_ascending_by_name(self, table):
        table.sort_by_module()
        assert table.view.horizontalHeader().sortIndicatorSection() == TelemetryCol.NAME

    def test_on_sort_indicator_changed_updates_state(self, table):
        table._on_sort_indicator_changed(TelemetryCol.NAME, Qt.DescendingOrder)
        assert table.sort_column == TelemetryCol.NAME
        assert table.sort_order == Qt.DescendingOrder.value


class TestTriggerModuleAction:
    def test_copy_name_sets_clipboard(self, table, registry, qapp):
        dev = registry.id_registry.get_device("clipdev")
        mod = dev.get_module("clipmod")

        table._trigger_module_action("copy_name", mod)

        assert qapp.clipboard().text() == "clipmod"

    def test_copy_value_sets_clipboard_when_present(self, table, registry, qapp):
        dev = registry.id_registry.get_device("clipvaldev")
        mod = dev.get_module("clipvalmod")
        _emit(registry, dev, mod, "hello-value")
        table.model.apply_updates(force=True)

        table._trigger_module_action("copy_value", mod)

        assert qapp.clipboard().text() == "hello-value"

    def test_view_logs_creates_log_viewer_widget(self, table, registry):
        dev = registry.id_registry.get_device("viewlogsdev")
        mod = dev.get_module("viewlogsmod")
        created = []
        table.gui_context.create_widget = lambda cls_name, title, **kw: created.append((cls_name, title, kw))

        table._trigger_module_action("view_logs", mod)

        assert created[0][0] == "LogViewerWidget"
        assert created[0][2]["params"]["filtered_module"] == mod

    def test_view_logs_table_creates_log_table_viewer_widget(self, table, registry):
        dev = registry.id_registry.get_device("viewtabledev")
        mod = dev.get_module("viewtablemod")
        created = []
        table.gui_context.create_widget = lambda cls_name, title, **kw: created.append((cls_name, title, kw))

        table._trigger_module_action("view_logs_table", mod)

        assert created[0][0] == "LogTableViewerWidget"

    def test_view_graph_creates_telemetry_plotter_with_single_module(self, table, registry):
        dev = registry.id_registry.get_device("viewgraphdev")
        mod = dev.get_module("viewgraphmod")
        created = []
        table.gui_context.create_widget = lambda cls_name, title, **kw: created.append((cls_name, title, kw))

        table._trigger_module_action("view_graph", mod)

        assert created[0][0] == "TelemetryPlotter"
        assert created[0][2]["params"]["modules"] == [mod]

    def test_no_module_is_a_noop(self, table):
        table._trigger_module_action("copy_name", None)  # must not raise


class TestDoubleClick:
    def test_double_click_on_value_column_copies_to_clipboard(self, table, registry, qapp):
        dev = registry.id_registry.get_device("dblclickdev")
        mod = dev.get_module("dblclickmod")
        _emit(registry, dev, mod, "dbl-value")
        table.model.apply_updates(force=True)

        row = table.model.visible_mod_ids.tolist().index(mod.id)
        index = table.model.index(row, TelemetryCol.VALUE)

        table._on_double_clicked(index)

        assert qapp.clipboard().text() == "dbl-value"


class TestHoveredRow:
    def test_set_hovered_row_is_a_noop_for_the_same_value(self, table):
        table.hovered_row = 3
        table._set_hovered_row(3)
        assert table.hovered_row == 3

    def test_set_hovered_row_updates_to_a_new_value(self, table):
        table._set_hovered_row(2)
        assert table.hovered_row == 2


class TestShowContextMenu:
    def test_builds_a_menu_without_blocking_and_without_raising(self, table, registry, monkeypatch):
        dev = registry.id_registry.get_device("ctxdev")
        mod = dev.get_module("ctxmod")
        table.model._sync_registry(registry.id_registry.module_list)

        captured = []
        monkeypatch.setattr(QMenu, "exec_", lambda self, *a, **kw: captured.append(self))

        row = table.model.visible_mod_ids.tolist().index(mod.id)
        index = table.model.index(row, TelemetryCol.NAME)
        pos = table.view.visualRect(index).center()

        table._show_context_menu(pos)

        assert len(captured) == 1
        assert any("ctxmod" in a.text() for a in captured[0].actions() if a.text())

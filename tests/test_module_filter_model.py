# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import pytest
from qtpy.QtCore import Qt

from blinkview.ui.widgets.module_filter_model import FastModuleFilterModel
from blinkview.ui.widgets.module_filter_table import TempLogFilter
from blinkview.utils.log_level import LogLevel


def _gui_context(id_registry):
    return SimpleNamespace(id_registry=id_registry)


def _build_tree(id_registry, device_name="esp32", essential=True):
    """<device>.a (essential), <device>.a.b (essential), <device>.c (non-essential)."""
    dev = id_registry.get_device(device_name, essential=essential)
    a = dev.get_module("a")
    b = dev.get_module("a.b")
    c = dev.get_module("c")
    c.set_essential(False)
    return dev, {"a": a, "b": b, "c": c}


@pytest.fixture
def model_and_modules(qapp, id_registry, log_filter):
    dev, mods = _build_tree(id_registry)
    tlf = TempLogFilter(_gui_context(id_registry), log_filter)
    model = FastModuleFilterModel(id_registry, tlf)
    model.sync_registry()
    return model, dev, mods, tlf


class TestSyncRegistry:
    def test_default_view_includes_only_essential_modules(self, qapp, id_registry, log_filter):
        dev, mods = _build_tree(id_registry)
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        model = FastModuleFilterModel(id_registry, tlf)

        model.sync_registry()

        ids = set(model.row_to_id.tolist())
        assert mods["a"].id in ids
        assert mods["b"].id in ids
        assert mods["c"].id not in ids  # non-essential, excluded by default

    def test_show_non_essential_includes_everything(self, qapp, id_registry, log_filter):
        dev, mods = _build_tree(id_registry)
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        model = FastModuleFilterModel(id_registry, tlf)

        model.sync_registry(show_non_essential=True)

        assert mods["c"].id in model.row_to_id.tolist()

    def test_allowed_device_filters_to_that_device_only(self, qapp, id_registry, log_filter):
        dev_a, mods_a = _build_tree(id_registry, device_name="esp32")
        dev_b, mods_b = _build_tree(id_registry, device_name="stm32")
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        model = FastModuleFilterModel(id_registry, tlf)

        model.sync_registry(allowed_device=dev_a)

        ids = set(model.row_to_id.tolist())
        assert mods_a["a"].id in ids
        assert mods_b["a"].id not in ids

    def test_root_module_without_children_shows_only_that_module(self, qapp, id_registry, log_filter):
        dev, mods = _build_tree(id_registry)
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        model = FastModuleFilterModel(id_registry, tlf)

        model.sync_registry(root_module=mods["a"], include_children=False)

        assert model.row_to_id.tolist() == [mods["a"].id]

    def test_root_module_with_children_includes_descendants(self, qapp, id_registry, log_filter):
        dev, mods = _build_tree(id_registry)
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        tlf.sync_modules()
        model = FastModuleFilterModel(id_registry, tlf)

        model.sync_registry(root_module=mods["a"], include_children=True)

        ids = set(model.row_to_id.tolist())
        assert mods["a"].id in ids
        assert mods["b"].id in ids

    def test_rows_are_sorted_by_device_then_module_name(self, qapp, id_registry, log_filter):
        dev, mods = _build_tree(id_registry, device_name="esp32")
        tlf = TempLogFilter(_gui_context(id_registry), log_filter)
        model = FastModuleFilterModel(id_registry, tlf)

        model.sync_registry(root_module=mods["a"], include_children=True)

        names = [id_registry.module_from_int(int(mid)).name for mid in model.row_to_id]
        assert names == sorted(names)


class TestRowColumnCounts:
    def test_row_count_matches_row_to_id_length(self, model_and_modules):
        model, _dev, _mods, _tlf = model_and_modules
        assert model.rowCount() == len(model.row_to_id)

    def test_column_count_is_two(self, model_and_modules):
        model, *_ = model_and_modules
        assert model.columnCount() == 2


class TestData:
    def test_invalid_index_returns_none(self, model_and_modules):
        model, *_ = model_and_modules
        assert model.data(model.index(-1, 0)) is None

    def test_user_role_returns_the_module_id(self, model_and_modules):
        model, _dev, mods, _tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        index = model.index(row, 0)
        assert model.data(index, Qt.UserRole) == mods["a"].id

    def test_column_zero_check_state_reflects_filter_enabled_mask(self, model_and_modules):
        model, _dev, mods, tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        index = model.index(row, 0)

        tlf.set_module_enabled(mods["a"].id, False)
        assert model.data(index, Qt.CheckStateRole) == Qt.Unchecked

        tlf.set_module_enabled(mods["a"].id, True)
        assert model.data(index, Qt.CheckStateRole) == Qt.Checked

    def test_column_zero_display_shows_device_name_at_depth_zero(self, model_and_modules):
        model, dev, mods, _tlf = model_and_modules
        # The device's root module (depth 0) row - find it via row_to_id containing the root id.
        root_row = model.row_to_id.tolist().index(dev.root.id) if dev.root.id in model.row_to_id.tolist() else None
        if root_row is not None:
            assert model.data(model.index(root_row, 0), Qt.DisplayRole) == dev.name

    def test_column_zero_display_indents_nested_modules(self, model_and_modules):
        model, _dev, mods, _tlf = model_and_modules
        # "a.b" is one level deeper than "a" - not the global root, so it should be indented.
        row = model.row_to_id.tolist().index(mods["b"].id)
        text = model.data(model.index(row, 0), Qt.DisplayRole)
        assert "b" in text
        assert "└──" in text

    def test_column_one_display_shows_level_name(self, model_and_modules):
        model, _dev, mods, tlf = model_and_modules
        tlf.set_module_level(mods["a"].id, LogLevel.WARN)
        row = model.row_to_id.tolist().index(mods["a"].id)

        assert model.data(model.index(row, 1), Qt.DisplayRole) == LogLevel.WARN.name_conf

    def test_column_one_alignment_is_centered(self, model_and_modules):
        model, _dev, mods, _tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        assert model.data(model.index(row, 1), Qt.TextAlignmentRole) == Qt.AlignCenter


class TestSetData:
    def test_toggling_checkbox_updates_filter_and_emits_data_changed(self, model_and_modules):
        model, _dev, mods, tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        index = model.index(row, 0)

        received = []
        model.dataChanged.connect(lambda *_a: received.append(True))

        result = model.setData(index, Qt.Unchecked, Qt.CheckStateRole)

        assert result is True
        assert tlf.enabled_mask[mods["a"].id] == False  # noqa: E712
        assert received == [True]

    def test_setting_same_check_state_is_a_noop(self, model_and_modules):
        model, _dev, mods, tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        index = model.index(row, 0)

        assert tlf.enabled_mask[mods["a"].id]  # already enabled
        result = model.setData(index, Qt.Checked, Qt.CheckStateRole)

        assert result is False

    def test_invalid_index_returns_false(self, model_and_modules):
        model, *_ = model_and_modules
        assert model.setData(model.index(-1, 0), Qt.Checked, Qt.CheckStateRole) is False

    def test_edit_role_column_one_updates_level(self, model_and_modules):
        model, _dev, mods, tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        index = model.index(row, 1)

        result = model.setData(index, LogLevel.ERROR, Qt.EditRole)

        assert result is True
        assert tlf.level_mask[mods["a"].id] == LogLevel.ERROR.value


class TestFlags:
    def test_invalid_index_has_no_flags(self, model_and_modules):
        model, *_ = model_and_modules
        assert model.flags(model.index(-1, 0)) == Qt.NoItemFlags

    def test_column_zero_is_checkable(self, model_and_modules):
        model, _dev, mods, _tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        assert model.flags(model.index(row, 0)) & Qt.ItemIsUserCheckable

    def test_column_one_is_editable(self, model_and_modules):
        model, _dev, mods, _tlf = model_and_modules
        row = model.row_to_id.tolist().index(mods["a"].id)
        assert model.flags(model.index(row, 1)) & Qt.ItemIsEditable

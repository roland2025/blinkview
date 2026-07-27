# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Coverage for TelemetryWatch's drag-and-drop entry reordering: _handle_internal_move (single
row + section/group block moves), _handle_external_drop (new row vs. add-to-existing-row), and
the drag*Event/dropEvent dispatch wiring. Uses fake event objects (mimeData/pos/
acceptProposedAction) rather than real Qt drag machinery (QDrag.exec_() blocks), same approach as
test_plotter.py's TestDragAndDrop."""

import pytest

from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.widgets.TelemetryWatch import (
    ButtonEntry,
    GroupEndEntry,
    GroupStartEntry,
    RowEntry,
    SectionEntry,
    TelemetryWatch,
)
from tests.fakes.real_registry import make_real_gui_context, make_real_registry


def _seed_watches_section(registry):
    registry.config.apply_patch("/", [{"op": "add", "path": "/watches", "value": {}}])


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "telemetry_watch_dragdrop_test")
    yield reg
    reg.stop()


@pytest.fixture
def watch(qapp, qtbot, registry):
    gui_context = make_real_gui_context(registry)
    gui_context.set_gui_config_manager(ConfigNodeManager(gui_context))
    gui_context.logger = registry.logger_creator("gui")()

    device = registry.id_registry.get_device("dragdroptest")
    module = device.get_module("status")

    _seed_watches_section(registry)
    watches_node = gui_context.gui_config_manager.create_node("/watches")
    watch_id, conf = TelemetryWatch.new_watch("Drag Watch")
    watches_node.send_config({watch_id: conf})

    w = TelemetryWatch(gui_context, state={"id": watch_id})
    qtbot.addWidget(w)
    w.module = module
    w.device = device
    yield w


class FakeMimeData:
    def __init__(self, text, has_text=True):
        self._text = text
        self._has_text = has_text

    def hasText(self):
        return self._has_text

    def text(self):
        return self._text


class FakeEvent:
    def __init__(self, text, pos=None):
        self._mime = FakeMimeData(text)
        self._pos = pos
        self.accepted = False
        self.ignored = False

    def mimeData(self):
        return self._mime

    def pos(self):
        return self._pos

    def acceptProposedAction(self):
        self.accepted = True

    def ignore(self):
        self.ignored = True


class TestDragEnterEvent:
    def test_accepts_text_mime_data(self, watch):
        event = FakeEvent("some text")
        watch.dragEnterEvent(event)
        assert event.accepted is True


class TestCalculateDropState:
    def test_empty_entries_returns_zero_and_below(self, watch):
        watch.entries = []
        watch.rebuild_ui()

        from qtpy.QtCore import QPoint

        index, action = watch._calculate_drop_state(QPoint(0, 0), is_internal=False)

        assert (index, action) == (0, "below")


class TestHandleExternalDrop:
    def test_above_action_inserts_a_new_row_before_target(self, watch):
        existing = RowEntry(label="Existing", modules=[watch.module])
        watch.entries = [existing]
        watch.rebuild_ui()

        watch._handle_external_drop(watch.module.name_with_device(), target_index=0, action="above")

        assert len(watch.entries) == 2
        assert watch.entries[0].label == watch.module.name
        assert watch.entries[1] is existing

    def test_below_action_inserts_a_new_row_after_target(self, watch):
        existing = RowEntry(label="Existing", modules=[watch.module])
        watch.entries = [existing]
        watch.rebuild_ui()

        watch._handle_external_drop(watch.module.name_with_device(), target_index=0, action="below")

        assert len(watch.entries) == 2
        assert watch.entries[0] is existing
        assert watch.entries[1].label == watch.module.name

    def test_into_action_adds_module_to_an_existing_row(self, watch):
        device2 = watch.gui_context.id_registry.get_device("dragdroptest2")
        module2 = device2.get_module("status2")
        existing = RowEntry(label="Existing", modules=[watch.module])
        watch.entries = [existing]
        watch.rebuild_ui()

        watch._handle_external_drop(module2.name_with_device(), target_index=0, action="into")

        assert len(watch.entries) == 1
        assert module2 in existing.modules

    def test_into_action_does_not_duplicate_an_already_present_module(self, watch):
        existing = RowEntry(label="Existing", modules=[watch.module])
        watch.entries = [existing]
        watch.rebuild_ui()

        watch._handle_external_drop(watch.module.name_with_device(), target_index=0, action="into")

        assert existing.modules == [watch.module]


class TestHandleInternalMoveSingleRow:
    def test_moves_a_row_below_another(self, watch):
        row_a = RowEntry(label="A", modules=[watch.module])
        row_b = RowEntry(label="B", modules=[watch.module])
        watch.entries = [row_a, row_b]
        watch.rebuild_ui()

        watch._handle_internal_move(old_index=0, target_index=1, action="below")

        assert watch.entries == [row_b, row_a]

    def test_moves_a_row_above_another(self, watch):
        row_a = RowEntry(label="A", modules=[watch.module])
        row_b = RowEntry(label="B", modules=[watch.module])
        watch.entries = [row_a, row_b]
        watch.rebuild_ui()

        watch._handle_internal_move(old_index=1, target_index=0, action="above")

        assert watch.entries == [row_b, row_a]

    def test_dropping_in_place_is_a_noop(self, watch):
        row_a = RowEntry(label="A", modules=[watch.module])
        row_b = RowEntry(label="B", modules=[watch.module])
        watch.entries = [row_a, row_b]
        watch.rebuild_ui()

        watch._handle_internal_move(old_index=0, target_index=0, action="above")

        assert watch.entries == [row_a, row_b]


class TestHandleInternalMoveSectionBlock:
    def test_moves_a_whole_section_with_its_children(self, watch):
        # A section's "block" is everything up to the *next* SectionEntry (sections have no
        # explicit end marker, unlike button groups) - a second section is needed to bound it,
        # or a trailing row would be swept into the first section's block too.
        section1 = SectionEntry(label="Sec1")
        child1 = RowEntry(label="Child1", modules=[watch.module])
        section2 = SectionEntry(label="Sec2")
        child2 = RowEntry(label="Child2", modules=[watch.module])
        watch.entries = [section1, child1, section2, child2]
        watch.rebuild_ui()

        watch._handle_internal_move(old_index=0, target_index=3, action="below")

        assert watch.entries == [section2, child2, section1, child1]


class TestHandleInternalMoveGroupBlock:
    def test_moves_a_whole_button_group_with_its_members(self, watch):
        group_start = GroupStartEntry(label="Group")
        btn = ButtonEntry(label="Btn")
        group_end = GroupEndEntry()
        trailer = RowEntry(label="Trailer", modules=[watch.module])
        watch.entries = [group_start, btn, group_end, trailer]
        watch.rebuild_ui()

        watch._handle_internal_move(old_index=0, target_index=3, action="below")

        assert watch.entries == [trailer, group_start, btn, group_end]


class TestDropEvent:
    def test_internal_drop_dispatches_to_handle_internal_move(self, watch):
        row_a = RowEntry(label="A", modules=[watch.module])
        row_b = RowEntry(label="B", modules=[watch.module])
        watch.entries = [row_a, row_b]
        watch.rebuild_ui()

        calls = []
        watch._handle_internal_move = lambda old_index, target_index, action: calls.append(
            (old_index, target_index, action)
        )
        watch._calculate_drop_state = lambda pos, is_internal: (1, "below")

        from qtpy.QtCore import QPoint

        event = FakeEvent("0", pos=QPoint(0, 0))
        watch.dropEvent(event)

        assert calls == [(0, 1, "below")]
        assert event.accepted is True

    def test_external_drop_dispatches_to_handle_external_drop(self, watch):
        watch.entries = []
        watch.rebuild_ui()

        calls = []
        watch._handle_external_drop = lambda module_id, target_index, action: calls.append(
            (module_id, target_index, action)
        )
        watch._calculate_drop_state = lambda pos, is_internal: (0, "below")

        from qtpy.QtCore import QPoint

        event = FakeEvent("some.module.id", pos=QPoint(0, 0))
        watch.dropEvent(event)

        assert calls == [("some.module.id", 0, "below")]

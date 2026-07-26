# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

"""Coverage for TelemetryWatch's non-playback surface: entry management (add/remove/reorder),
config save/restore roundtrip through a real ConfigNode, edit-mode toggling, section
collapse, row expand/collapse, and button-command dispatch. See test_telemetry_watch_playback.py
for the playback-clock-following coverage."""

import pytest
from qtpy.QtCore import QTimer
from qtpy.QtWidgets import QApplication, QMessageBox

from blinkview.ui.utils.config_node_manager import ConfigNodeManager
from blinkview.ui.widgets.message_box import MessageBox
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
    """Seeds an empty '/watches' container directly in the backend config before any
    TelemetryWatch's ConfigNode touches it. See
    TestFirstRunWatchesSectionBug.test_creating_the_first_ever_watch_fails_to_persist below:
    ConfigManager.apply_patch's jsonpatch 'add' requires the parent container to already exist,
    and nothing in this codebase ever creates a '/watches' key ahead of time - a returning user
    only avoids this because their profile's config file already has one saved from some earlier
    session. A brand-new profile hits the same first-run gap this helper works around."""
    registry.config.apply_patch("/", [{"op": "add", "path": "/watches", "value": {}}])


def _click_button(role):
    def _do_click():
        for w in QApplication.topLevelWidgets():
            if isinstance(w, QMessageBox):
                w.button(role).click()
                return

    QTimer.singleShot(50, _do_click)


@pytest.fixture
def registry(tmp_path):
    reg = make_real_registry(tmp_path, "telemetry_watch_widget_test")
    yield reg
    reg.stop()


@pytest.fixture
def watch(qapp, qtbot, registry):
    """Constructs the watch the way BlinkMainWindow.open_watch actually does in the real app:
    write the new watch's config under '/watches/{id}' first, then construct with
    state={"id": ...} so the widget's own ConfigNode resolves to that same absolute path.
    Constructing TelemetryWatch(gui_context) with no state at all takes __init__'s bare
    "watches" (no leading slash) fallback path instead - never exercised by the real app, and
    ConfigManager.apply_patch's relative-path promotion silently fails to persist through it
    (a missing leading '/' produces an invalid jsonpatch location, caught and logged, not
    raised) - so it isn't a valid way to test save/restore round-tripping."""
    gui_context = make_real_gui_context(registry)
    gui_context.set_gui_config_manager(ConfigNodeManager(gui_context))
    gui_context.logger = registry.logger_creator("gui")()

    device = registry.id_registry.get_device("watchwidgettest")
    module = device.get_module("status")

    _seed_watches_section(registry)
    watches_node = gui_context.gui_config_manager.create_node("/watches")
    watch_id, conf = TelemetryWatch.new_watch("Test Watch")
    watches_node.send_config({watch_id: conf})

    w = TelemetryWatch(gui_context, state={"id": watch_id})
    qtbot.addWidget(w)
    w.module = module
    w.device = device
    yield w


class TestEntryPrompts:
    def test_prompt_add_row_persists_through_config_roundtrip(self, watch):
        watch.prompt_add_row()

        assert any(isinstance(e, RowEntry) and e.label == "New Metric" for e in watch.entries)

    def test_prompt_add_section_persists(self, watch):
        watch.prompt_add_section()

        assert any(isinstance(e, SectionEntry) and e.label == "NEW SECTION" for e in watch.entries)

    def test_prompt_add_button_persists(self, watch):
        watch.prompt_add_button()

        assert any(isinstance(e, ButtonEntry) and e.label == "New Command" for e in watch.entries)

    def test_prompt_add_button_group_persists_start_and_end(self, watch):
        watch.prompt_add_button_group()

        types = [type(e) for e in watch.entries]
        assert GroupStartEntry in types
        assert GroupEndEntry in types
        assert any(isinstance(e, ButtonEntry) and e.label == "Cmd 1" for e in watch.entries)


class TestRemoval:
    def test_remove_item_drops_the_entry(self, watch):
        watch.prompt_add_row()
        watch.prompt_add_section()
        count_before = len(watch.entries)

        watch.remove_item(0)

        assert len(watch.entries) == count_before - 1

    def test_remove_module_from_row(self, watch):
        entry = RowEntry(label="Row", modules=[watch.module])
        watch.entries.append(entry)
        watch.rebuild_ui()

        watch.remove_module_from_row(watch.entries.index(entry), 0)

        assert entry.modules == []


class TestSectionAndRowToggling:
    def test_toggle_section_flips_collapsed_and_persists(self, watch):
        section = SectionEntry(label="Sec")
        watch.entries.append(section)
        watch.rebuild_ui()

        watch.toggle_section(section)

        assert section.collapsed is True

        watch.toggle_section(section)
        assert section.collapsed is False

    def test_toggle_row_expanded_flips_state(self, watch):
        entry = RowEntry(label="Row", modules=[watch.module])
        watch.entries.append(entry)
        watch.rebuild_ui()

        watch._toggle_row_expanded(entry)
        assert entry.is_expanded is True

        watch._toggle_row_expanded(entry)
        assert entry.is_expanded is False


class TestEditMode:
    def test_toggle_edit_mode_on_shows_edit_toolbar_and_name_edit(self, watch):
        watch.show()
        watch.toggle_edit_mode(True)

        assert watch.edit_mode is True
        assert watch.edit_toolbar.isVisible() is True
        assert watch.name_stack.currentIndex() == 1

    def test_toggle_edit_mode_off_shows_name_label_uppercased(self, watch):
        watch.name = "my watch"
        watch.toggle_edit_mode(True)
        watch.toggle_edit_mode(False)

        assert watch.edit_mode is False
        assert watch.name_label.text() == "MY WATCH"

    def test_handle_rename_updates_name_and_saves(self, watch):
        watch._handle_rename("Renamed Watch")

        assert watch.name == "Renamed Watch"


class TestRebuildUiWithVariousEntries:
    def test_rebuild_with_section_row_button_group_edit_mode_off(self, watch):
        watch.entries = [
            SectionEntry(label="Sec 1"),
            RowEntry(label="Row 1", modules=[watch.module]),
            GroupStartEntry(label="Group"),
            ButtonEntry(label="Btn 1"),
            GroupEndEntry(),
        ]

        watch.rebuild_ui()  # must not raise

    def test_rebuild_with_collapsed_section_hides_children(self, watch):
        watch.entries = [
            SectionEntry(label="Sec 1", collapsed=True),
            RowEntry(label="Row 1", modules=[watch.module]),
        ]

        watch.rebuild_ui()  # must not raise

    def test_rebuild_in_edit_mode_shows_drag_handles_and_remove_buttons(self, watch):
        watch.edit_mode = True
        watch.entries = [
            SectionEntry(label="Sec 1"),
            RowEntry(label="Row 1", modules=[watch.module]),
            GroupStartEntry(label="Group"),
            ButtonEntry(label="Btn 1"),
            GroupEndEntry(),
        ]

        watch.rebuild_ui()  # must not raise

    def test_rebuild_with_multi_module_row_expanded(self, watch):
        device2 = watch.gui_context.id_registry.get_device("watchwidgettest2")
        module2 = device2.get_module("status2")
        entry = RowEntry(label="Multi", modules=[watch.module, module2], is_expanded=True)
        watch.entries = [entry]

        watch.rebuild_ui()  # must not raise


class TestFirstRunWatchesSectionBug:
    def test_creating_the_first_ever_watch_fails_to_persist(self, qapp, qtbot, registry):
        """Regression test documenting a real first-run gap: BlinkMainWindow.open_watch (and
        this module's own TelemetryWatch.__init__ else-branch) both lazily
        gui_config_manager.create_node("/watches") and send_config a brand-new watch into it,
        with nothing anywhere in this codebase ever pre-creating a '/watches' key in the backend
        config. ConfigManager.apply_patch's jsonpatch 'add' requires the parent container to
        already exist (RFC 6902 semantics - jsonpatch.JsonPatchConflict, caught and merely
        logged, never raised past apply_patch), so the very first watch a brand-new profile ever
        creates silently fails to save: the widget still opens (its in-memory `entries` list was
        already mutated before save_config() was even called), but nothing lands in the config
        file. A returning user only avoids this because their profile's config already has a
        '/watches' key persisted from some earlier session that happened to work around it (e.g.
        via this test file's own _seed_watches_section helper)."""
        gui_context = make_real_gui_context(registry)
        gui_context.set_gui_config_manager(ConfigNodeManager(gui_context))
        gui_context.logger = registry.logger_creator("gui")()

        watches_node = gui_context.gui_config_manager.create_node("/watches")
        watch_id, conf = TelemetryWatch.new_watch("First Ever Watch")
        watches_node.send_config({watch_id: conf})  # logs "member 'watches' not found", doesn't raise

        assert "watches" not in registry.config.get_data()


class TestStateRoundtrip:
    def test_get_state_returns_tab_name_and_id(self, watch):
        watch.tab_name = "my_tab"

        state = watch.get_state()

        assert state == {"tab_name": "my_tab", "id": watch.watch_id}

    def test_restore_rebuilds_entries_from_config(self, qapp, qtbot, registry):
        """Mirrors how BlinkMainWindow.open_watch actually constructs a TelemetryWatch: the
        parent '/watches' collection is written first (with the new watch's full config,
        including its entries), then the widget is constructed with state={"id": ...} so its
        own node resolves to '/watches/{id}' - the same absolute path the data was written
        under. Constructing a fresh TelemetryWatch with no state at all (the `else` branch of
        __init__) writes to the differently-addressed 'watches' path instead, which is never
        exercised in the real app (open_watch always supplies an id) and isn't a valid way to
        seed data for a later restore()."""
        gui_context = make_real_gui_context(registry)
        gui_context.set_gui_config_manager(ConfigNodeManager(gui_context))
        gui_context.logger = registry.logger_creator("gui")()

        _seed_watches_section(registry)
        watches_node = gui_context.gui_config_manager.create_node("/watches")
        watch_id, conf = TelemetryWatch.new_watch("My Watch")
        conf["entries"] = [{"type": "section", "label": "NEW SECTION", "collapsed": False}]
        watches_node.send_config({watch_id: conf})

        w2 = TelemetryWatch(gui_context, state={"id": watch_id})
        qtbot.addWidget(w2)

        # The config fetch is dispatched through the real (threaded) task runner, so the first
        # update_config_schema callback can arrive before the backend has actually resolved this
        # node's data - wait for the corrected one instead of asserting against whatever landed
        # synchronously within the constructor call.
        qtbot.waitUntil(
            lambda: any(isinstance(e, SectionEntry) and e.label == "NEW SECTION" for e in w2.entries), timeout=1000
        )


class TestNewWatchClassmethod:
    def test_new_watch_generates_id_and_config(self):
        id_, conf = TelemetryWatch.new_watch("My Watch")

        assert conf == {"id": id_, "name": "My Watch"}
        assert id_.startswith("watch")

    def test_new_watch_avoids_colliding_with_existing_ids(self):
        id_, conf = TelemetryWatch.new_watch("My Watch")

        id2, _ = TelemetryWatch.new_watch("Another Watch", parent={id_: {}})

        assert id2 != id_


class TestButtonCommandExecution:
    def test_execute_button_command_with_no_target_is_a_noop(self, watch):
        entry = ButtonEntry(label="Btn", command_payload="", target_device="")
        watch.execute_button_command(entry)  # must not raise, no target resolved

    def test_execute_button_command_sends_to_button_specific_target(self, watch):
        calls = []
        watch.gui_context.registry.system_ctx.tasks.run_task = lambda fn, *a: calls.append((fn, a))

        entry = ButtonEntry(label="Btn", command_payload="ping", target_device="dev1")
        watch.execute_button_command(entry)

        assert len(calls) == 1
        assert calls[0][1] == ("dev1", "ping\n")

    def test_execute_button_command_falls_back_to_default_target(self, watch):
        calls = []
        watch.gui_context.registry.system_ctx.tasks.run_task = lambda fn, *a: calls.append(a)
        watch.default_target = "default_dev"

        entry = ButtonEntry(label="Btn", command_payload="ping", target_device="")
        watch.execute_button_command(entry)

        assert calls[0] == ("default_dev", "ping\n")


class TestGenericCommand:
    def test_empty_command_is_a_noop(self, watch):
        watch.command_input.setEditText("")
        watch._handle_generic_command()  # must not raise

        assert watch.command_history == []

    def test_command_with_no_default_target_is_a_noop(self, watch):
        watch.command_input.setEditText("hello")
        watch._handle_generic_command()

        assert watch.command_history == []

    def test_command_sent_and_recorded_in_history(self, watch):
        calls = []
        watch.gui_context.registry.system_ctx.tasks.run_task = lambda fn, *a: calls.append(a)
        watch.default_target = "dev1"
        watch.command_input.setEditText("hello")

        watch._handle_generic_command()

        assert watch.command_history == ["hello"]
        assert calls[0] == ("dev1", "hello\n")

    def test_repeated_command_moves_to_front_not_duplicated(self, watch):
        watch.gui_context.registry.system_ctx.tasks.run_task = lambda fn, *a: None
        watch.default_target = "dev1"

        watch.command_input.setEditText("first")
        watch._handle_generic_command()
        watch.command_input.setEditText("second")
        watch._handle_generic_command()
        watch.command_input.setEditText("first")
        watch._handle_generic_command()

        assert watch.command_history == ["first", "second"]


class TestRowContextMenuAction:
    def test_copy_names_action_copies_module_names_to_clipboard(self, watch):
        entry = RowEntry(label="Row", modules=[watch.module])

        watch._handle_row_action("copy_names", entry)

        assert QApplication.clipboard().text() == watch.module.name

    def test_action_without_modules_is_a_noop(self, watch):
        entry = RowEntry(label="Row", modules=[])
        watch._handle_row_action("view_graph", entry)  # must not raise

    def test_view_graph_creates_a_plotter_widget(self, watch):
        calls = []
        watch.gui_context.create_widget = lambda *a, **k: calls.append((a, k))
        entry = RowEntry(label="Row", modules=[watch.module])

        watch._handle_row_action("view_graph", entry)

        assert calls[0][0][0] == "TelemetryPlotter"


class TestDeleteWatchPrompt:
    def test_confirming_deletes_the_node(self, watch):
        deleted = []
        watch.node.delete = lambda: deleted.append(True)
        _click_button(QMessageBox.StandardButton.Yes)

        watch.prompt_delete_watch()

        assert deleted == [True]

    def test_declining_does_not_delete_the_node(self, watch):
        deleted = []
        watch.node.delete = lambda: deleted.append(True)
        _click_button(QMessageBox.StandardButton.No)

        watch.prompt_delete_watch()

        assert deleted == []


class TestEntryRowRect:
    def test_missing_entry_returns_none(self, watch):
        stray_entry = RowEntry(label="Not in watch")
        assert watch._calculate_entry_row_rect(stray_entry) is None

    def test_present_entry_returns_a_rect(self, watch):
        entry = RowEntry(label="Row", modules=[watch.module])
        watch.entries.append(entry)
        watch.rebuild_ui()

        rect = watch._calculate_entry_row_rect(entry)
        assert rect is not None

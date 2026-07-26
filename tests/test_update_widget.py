# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview import __version__
from blinkview.ui.widgets import update_widget as module
from blinkview.ui.widgets.update_widget import UpdateWidget
from blinkview.utils.updater import UpdateError


class FakeSettings:
    def __init__(self, values=None):
        self.values = values or {}
        self.sets = []

    def get(self, key, default=None):
        return self.values.get(key, default)

    def set(self, key, value, scope=None):
        self.values[key] = value
        self.sets.append((key, value, scope))


class FakeTaskManager:
    """Runs tasks synchronously so tests don't need real threading/waiting."""

    def run_task(self, fn, *args):
        fn(*args)


class FakeGuiContext:
    def __init__(self, settings=None):
        self.settings = settings or FakeSettings()
        self.registry = type("Registry", (), {"system_ctx": type("SystemCtx", (), {"tasks": FakeTaskManager()})()})()
        self.set_update_version = None


class FakeUpdater:
    """Stands in for blinkview.utils.updater.Updater - never touches git/filesystem."""

    fail_construction = False
    versions_local = ["v1.0.0"]
    versions_remote = ["v1.1.0", "v1.0.0"]
    fetch_should_raise = None

    def __init__(self, settings):
        if FakeUpdater.fail_construction:
            raise UpdateError("no path configured")
        self.settings = settings
        self.channel = str(settings.get("update.channel", "stable")).lower()

    def get_versions(self, remote=False):
        return FakeUpdater.versions_remote if remote else FakeUpdater.versions_local

    def fetch(self, force=False):
        if FakeUpdater.fetch_should_raise:
            raise FakeUpdater.fetch_should_raise
        return True


@pytest.fixture(autouse=True)
def _reset_fake_updater():
    FakeUpdater.fail_construction = False
    FakeUpdater.versions_local = ["v1.0.0"]
    FakeUpdater.versions_remote = ["v1.1.0", "v1.0.0"]
    FakeUpdater.fetch_should_raise = None
    yield


@pytest.fixture(autouse=True)
def _patch_module(monkeypatch):
    monkeypatch.setattr(module, "Updater", FakeUpdater)
    monkeypatch.setattr(module, "check_post_update", lambda updater, parent=None: None)


@pytest.fixture
def settings():
    return FakeSettings({"update.path": "/fake/repo"})


@pytest.fixture
def gui_context(settings):
    return FakeGuiContext(settings)


@pytest.fixture
def widget(qapp, qtbot, gui_context):
    w = UpdateWidget(gui_context)
    qtbot.addWidget(w)
    w.show()  # isVisible() (config_btn, progress bar) requires the whole ancestor chain shown
    return w


class TestConstruction:
    def test_successful_init_shows_current_version_and_enables_fetch(self, widget):
        assert f"v{__version__}" in widget.status_label.text()
        assert widget.fetch_btn.isEnabled() is True
        assert widget.config_btn.isVisible() is False

    def test_channel_combo_reflects_saved_setting(self, qapp, qtbot, settings):
        settings.values["update.channel"] = "rc"
        gui_context = FakeGuiContext(settings)
        w = UpdateWidget(gui_context)
        qtbot.addWidget(w)

        assert w.channel_combo.currentData() == "rc"

    def test_construction_failure_shows_path_not_configured(self, qapp, qtbot, gui_context, monkeypatch):
        FakeUpdater.fail_construction = True
        # ensure_updater() falls back to ensure_update_path(), which would otherwise open a real
        # blocking QFileDialog - simulate the user aborting that prompt.
        monkeypatch.setattr(UpdateWidget, "ensure_update_path", staticmethod(lambda settings: False))
        w = UpdateWidget(gui_context)
        qtbot.addWidget(w)
        w.show()

        assert "Path not configured" in w.status_label.text()
        assert w.fetch_btn.isEnabled() is False
        assert w.config_btn.isVisible() is True


class TestChannelChanged:
    def test_changing_channel_saves_setting_and_updates_updater(self, widget, gui_context):
        idx = widget.channel_combo.findData("dev")
        widget.channel_combo.setCurrentIndex(idx)

        assert gui_context.settings.get("update.channel") == "dev"
        assert widget.updater.channel == "dev"


class TestListLocalVersions:
    def test_populates_list_marking_first_as_local(self, widget):
        widget.list_local_versions()
        assert widget.version_list.count() == 1
        assert "(Local)" in widget.version_list.item(0).text()

    def test_empty_versions_shows_placeholder_and_disables_install(self, widget):
        FakeUpdater.versions_local = []
        widget.list_local_versions()

        assert widget.version_list.item(0).text() == "No local versions found. Please 'Fetch'."
        assert widget.install_btn.isEnabled() is False

    def test_install_button_enabled_when_set_update_version_present(self, widget, gui_context):
        gui_context.set_update_version = lambda v: None
        widget.list_local_versions()
        assert widget.install_btn.isEnabled() is True

    def test_no_updater_is_a_no_op(self, qapp, qtbot, gui_context, monkeypatch):
        FakeUpdater.fail_construction = True
        monkeypatch.setattr(UpdateWidget, "ensure_update_path", staticmethod(lambda settings: False))
        w = UpdateWidget(gui_context)
        qtbot.addWidget(w)
        w.list_local_versions()  # must not raise despite no updater

    def test_exception_reading_versions_is_caught(self, widget, monkeypatch):
        def _raise(remote=False):
            raise RuntimeError("disk error")

        monkeypatch.setattr(widget.updater, "get_versions", _raise)
        widget.list_local_versions()

        assert "Error reading local tags" in widget.version_list.item(0).text()


class TestRequestFetch:
    def test_fetch_completes_and_populates_versions(self, widget):
        # request_fetch's _fetch_logic calls self.updater.get_versions() with no args, i.e.
        # remote=False - it re-lists the (possibly freshly-fetched) local tags, not a separate
        # remote list.
        FakeUpdater.versions_local = ["v2.0.0", "v1.0.0"]

        widget.request_fetch()

        assert widget.version_list.count() == 2
        assert "(Latest)" in widget.version_list.item(0).text()
        assert widget.fetch_btn.isEnabled() is True  # loading finished

    def test_fetch_error_shows_critical_message(self, widget, monkeypatch):
        calls = []
        monkeypatch.setattr(module.MessageBox, "critical", staticmethod(lambda *a, **kw: calls.append(a)))
        FakeUpdater.fetch_should_raise = UpdateError("network down")

        widget.request_fetch()

        assert len(calls) == 1
        assert "network down" in calls[0][2]

    def test_no_updater_attempts_to_ensure_one(self, qapp, qtbot, gui_context, monkeypatch):
        FakeUpdater.fail_construction = True
        monkeypatch.setattr(UpdateWidget, "ensure_update_path", staticmethod(lambda settings: False))
        w = UpdateWidget(gui_context)
        qtbot.addWidget(w)

        w.request_fetch()  # ensure_updater() fails again, must not raise


class TestSetLoading:
    def test_loading_true_disables_fetch_and_shows_progress(self, widget):
        widget._set_loading(True)
        assert widget.fetch_btn.isEnabled() is False
        assert widget.progress.isVisible() is True

    def test_loading_false_re_enables_fetch_and_hides_progress(self, widget):
        widget._set_loading(True)
        widget._set_loading(False)
        assert widget.fetch_btn.isEnabled() is True
        assert widget.progress.isVisible() is False


class TestHandleInstallRequest:
    def test_no_selection_is_a_no_op(self, widget):
        # list_local_versions() (run during __init__) auto-selects row 0 - explicitly clear it
        # so this test genuinely exercises the "nothing selected" early-return, not a real
        # (unmocked, blocking) MessageBox.question confirmation dialog.
        widget.version_list.setCurrentRow(-1)
        widget.handle_install_request()  # must not raise

    def test_confirmed_install_calls_set_update_version_with_clean_version(self, widget, gui_context, monkeypatch):
        from blinkview.ui.widgets.message_box import MessageBox

        widget.list_local_versions()
        widget.version_list.setCurrentRow(0)  # "v1.0.0 (Local)"

        monkeypatch.setattr(MessageBox, "question", staticmethod(lambda *a, **kw: MessageBox.Btn.Yes))
        received = []
        gui_context.set_update_version = lambda v: received.append(v)

        widget.handle_install_request()

        assert received == ["v1.0.0"]  # " (Local)" suffix stripped

    def test_declined_confirmation_does_not_install(self, widget, gui_context, monkeypatch):
        from blinkview.ui.widgets.message_box import MessageBox

        widget.list_local_versions()
        widget.version_list.setCurrentRow(0)

        monkeypatch.setattr(MessageBox, "question", staticmethod(lambda *a, **kw: MessageBox.Btn.No))
        received = []
        gui_context.set_update_version = lambda v: received.append(v)

        widget.handle_install_request()

        assert received == []

    def test_set_update_version_exception_shows_critical_message(self, widget, gui_context, monkeypatch):
        from blinkview.ui.widgets.message_box import MessageBox

        widget.list_local_versions()
        widget.version_list.setCurrentRow(0)
        monkeypatch.setattr(MessageBox, "question", staticmethod(lambda *a, **kw: MessageBox.Btn.Yes))

        def _raise(v):
            raise RuntimeError("registration failed")

        gui_context.set_update_version = _raise
        calls = []
        monkeypatch.setattr(MessageBox, "critical", staticmethod(lambda *a, **kw: calls.append(a)))

        widget.handle_install_request()

        assert len(calls) == 1


class TestUpdateStatus:
    def test_never_fetched_shows_never(self, widget, gui_context):
        gui_context.settings.values["update.last_fetch_time"] = 0
        widget.update_status()
        assert "Never" in widget.status_label.text()

    def test_recent_fetch_shows_a_timestamp(self, widget, gui_context):
        from time import time

        gui_context.settings.values["update.last_fetch_time"] = time()
        widget.update_status()
        assert "Checked" in widget.status_label.text()


class TestOnFetchFinished:
    def test_empty_versions_shows_no_versions_found(self, widget):
        widget._on_fetch_finished([])
        assert widget.version_list.item(0).text() == "No versions found."

    def test_populates_and_marks_first_as_latest(self, widget):
        widget._on_fetch_finished(["v2.0.0", "v1.0.0"])
        assert widget.version_list.count() == 2
        assert widget.version_list.item(0).text() == "v2.0.0 (Latest)"
        assert widget.version_list.item(1).text() == "v1.0.0"


class TestOnError:
    def test_shows_critical_message_and_stops_loading(self, widget, monkeypatch):
        calls = []
        monkeypatch.setattr(module.MessageBox, "critical", staticmethod(lambda *a, **kw: calls.append(a)))
        widget._set_loading(True)

        widget._on_error("boom")

        assert widget.fetch_btn.isEnabled() is True
        assert len(calls) == 1
        assert calls[0][2] == "boom"


class TestEnsureUpdatePath:
    def test_already_valid_path_returns_true_without_prompting(self, monkeypatch):
        from blinkview.utils.updater import Updater as RealUpdater

        monkeypatch.setattr(RealUpdater, "is_valid_repo", staticmethod(lambda p: True))
        settings = FakeSettings({"update.path": "/already/valid"})

        assert UpdateWidget.ensure_update_path(settings) is True

    def test_user_cancels_dialog_returns_false(self, qapp, monkeypatch):
        from blinkview.utils.updater import Updater as RealUpdater
        from qtpy.QtWidgets import QFileDialog

        monkeypatch.setattr(RealUpdater, "is_valid_repo", staticmethod(lambda p: False))
        monkeypatch.setattr(module.MessageBox, "info", staticmethod(lambda *a, **kw: None))
        monkeypatch.setattr(QFileDialog, "getExistingDirectory", staticmethod(lambda *a, **kw: ""))

        settings = FakeSettings({"update.path": ""})
        assert UpdateWidget.ensure_update_path(settings) is False

    def test_valid_selection_saves_path_and_returns_true(self, qapp, monkeypatch, tmp_path):
        from blinkview.utils.updater import Updater as RealUpdater
        from qtpy.QtWidgets import QFileDialog

        monkeypatch.setattr(RealUpdater, "is_valid_repo", staticmethod(lambda p: True))
        monkeypatch.setattr(module.MessageBox, "info", staticmethod(lambda *a, **kw: None))
        monkeypatch.setattr(QFileDialog, "getExistingDirectory", staticmethod(lambda *a, **kw: str(tmp_path)))

        settings = FakeSettings({"update.path": ""})
        result = UpdateWidget.ensure_update_path(settings)

        assert result is True
        assert settings.get("update.path") == str(tmp_path.resolve())

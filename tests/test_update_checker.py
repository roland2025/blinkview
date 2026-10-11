# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

import blinkview.utils.updater as updater_module
import blinkview.ui.widgets.toast_dispatcher as toast_dispatcher_module
from blinkview import __version__
from blinkview.ui.utils import update_checker
from blinkview.ui.widgets.toast import ToastType


class FakeToastDispatcher:
    def __init__(self):
        self.calls = []

    def notify(self, message, toast_type=None, duration=5.0, **kwargs):
        self.calls.append(SimpleNamespace(message=message, toast_type=toast_type, duration=duration, kwargs=kwargs))


class FakeUpdater:
    def __init__(self, settings=None, latest_version=None, check_version_status_result=(None, None)):
        self.settings = settings
        self._latest_version = latest_version
        self._check_version_status_result = check_version_status_result
        self.fetch_calls = []

    def fetch(self, force=False):
        self.fetch_calls.append(force)

    def get_latest_version(self):
        return self._latest_version

    def check_version_status(self, current_version):
        return self._check_version_status_result


def make_fake_dispatcher(monkeypatch):
    fake = FakeToastDispatcher()
    monkeypatch.setattr(toast_dispatcher_module, "toast_dispatcher", fake)
    return fake


class TestCheckPostUpdate:
    def test_success_notifies_success_toast(self, qapp, monkeypatch):
        dispatcher = make_fake_dispatcher(monkeypatch)
        fake_updater = FakeUpdater(check_version_status_result=(True, __version__))

        update_checker.check_post_update(fake_updater, parent="the-parent")

        assert len(dispatcher.calls) == 1
        call = dispatcher.calls[0]
        assert __version__ in call.message
        assert call.toast_type == ToastType.SUCCESS
        assert call.kwargs["parent"] == "the-parent"

    def test_failure_notifies_error_toast_with_target_version(self, qapp, monkeypatch):
        dispatcher = make_fake_dispatcher(monkeypatch)
        fake_updater = FakeUpdater(check_version_status_result=(False, "9.9.9"))

        update_checker.check_post_update(fake_updater)

        assert len(dispatcher.calls) == 1
        call = dispatcher.calls[0]
        assert "9.9.9" in call.message
        assert call.toast_type == ToastType.ERROR

    def test_no_pending_update_does_not_notify(self, qapp, monkeypatch):
        dispatcher = make_fake_dispatcher(monkeypatch)
        fake_updater = FakeUpdater(check_version_status_result=(None, None))

        update_checker.check_post_update(fake_updater)

        assert dispatcher.calls == []


class TestCheckForUpdatesSilently:
    def make_gui_context(self, latest_version=None):
        run_task_calls = []

        def run_task(fn, *args):
            run_task_calls.append((fn, args))
            fn(*args)  # run synchronously so the test can observe effects immediately

        gui_context = SimpleNamespace(
            settings="the-settings",
            registry=SimpleNamespace(system_ctx=SimpleNamespace(tasks=SimpleNamespace(run_task=run_task))),
            set_update_version=lambda v: None,
            create_widget=lambda *a, **k: None,
        )
        return gui_context, run_task_calls

    def test_constructs_updater_with_gui_context_settings_and_runs_post_update_check(self, qapp, monkeypatch):
        make_fake_dispatcher(monkeypatch)
        constructed = []

        def fake_updater_factory(settings):
            constructed.append(settings)
            return FakeUpdater(settings, check_version_status_result=(None, None))

        monkeypatch.setattr(updater_module, "make_updater", fake_updater_factory)

        gui_context, _run_task_calls = self.make_gui_context()
        update_checker.check_for_updates_silently(gui_context)

        assert constructed == ["the-settings"]

    def test_background_worker_notifies_when_a_newer_version_is_available(self, qapp, monkeypatch):
        dispatcher = make_fake_dispatcher(monkeypatch)

        def fake_updater_factory(settings):
            return FakeUpdater(settings, latest_version="99.0.0", check_version_status_result=(None, None))

        monkeypatch.setattr(updater_module, "make_updater", fake_updater_factory)

        gui_context, run_task_calls = self.make_gui_context()
        update_checker.check_for_updates_silently(gui_context)

        assert len(run_task_calls) == 1  # the background worker ran
        assert len(dispatcher.calls) == 1
        call = dispatcher.calls[0]
        assert "99.0.0" in call.message
        assert call.toast_type == ToastType.INFO
        assert call.kwargs["action_text"] == "INSTALL"

    def _toast_install(self, monkeypatch, closed_others):
        """Clicks the toast's INSTALL with close_other_instances_for_update() answering
        closed_others. Returns the versions registered for install."""
        from blinkview.ui.widgets import update_widget

        dispatcher = make_fake_dispatcher(monkeypatch)
        monkeypatch.setattr(
            updater_module,
            "make_updater",
            lambda settings: FakeUpdater(settings, latest_version="99.0.0", check_version_status_result=(None, None)),
        )
        asked = []
        monkeypatch.setattr(
            update_widget,
            "close_other_instances_for_update",
            lambda parent, updater, version, busy=None: asked.append(version) or closed_others,
        )
        gui_context, _run_task_calls = self.make_gui_context()
        registered = []
        gui_context.set_update_version = registered.append
        update_checker.check_for_updates_silently(gui_context)

        dispatcher.calls[0].kwargs["action_callback"]()

        assert asked == ["99.0.0"]
        return registered

    def test_toast_install_registers_the_version_when_nothing_else_runs(self, qapp, monkeypatch):
        assert self._toast_install(monkeypatch, closed_others=None) == ["99.0.0"]

    def test_toast_install_continues_once_the_other_instances_are_closed(self, qapp, monkeypatch):
        assert self._toast_install(monkeypatch, closed_others=True) == ["99.0.0"]

    def test_toast_install_stops_when_another_instance_stays(self, qapp, monkeypatch):
        """Cancelled, or one would not close: the window must stay open instead of exiting into
        an install that is refused with nobody watching."""
        assert self._toast_install(monkeypatch, closed_others=False) == []

    def test_background_worker_does_not_notify_when_already_up_to_date(self, qapp, monkeypatch):
        dispatcher = make_fake_dispatcher(monkeypatch)

        def fake_updater_factory(settings):
            return FakeUpdater(settings, latest_version=__version__, check_version_status_result=(None, None))

        monkeypatch.setattr(updater_module, "make_updater", fake_updater_factory)

        gui_context, _run_task_calls = self.make_gui_context()
        update_checker.check_for_updates_silently(gui_context)

        assert dispatcher.calls == []

    def test_background_worker_swallows_exceptions(self, qapp, monkeypatch):
        make_fake_dispatcher(monkeypatch)

        class ExplodingUpdater(FakeUpdater):
            def fetch(self, force=False):
                raise RuntimeError("network down")

        monkeypatch.setattr(updater_module, "make_updater", lambda settings: ExplodingUpdater(settings))

        gui_context, _run_task_calls = self.make_gui_context()
        update_checker.check_for_updates_silently(gui_context)  # must not raise

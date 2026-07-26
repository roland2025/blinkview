# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

import blinkview.ui.widgets.toast as toast_mod
from blinkview.ui.widgets.toast_dispatcher import ToastDispatcher, toast_dispatcher


@pytest.fixture
def fake_show(qapp, monkeypatch):
    """Intercepts ToastManager.show (the eventual target of notify()) instead of letting a
    real ToastWidget get constructed - _handle_request's local import binds to the same class
    object toast_mod.ToastManager, so patching it here is visible from there too."""
    calls = []

    def _fake(*args, **kwargs):
        calls.append((args, kwargs))

    monkeypatch.setattr(toast_mod.ToastManager, "show", staticmethod(_fake))
    return calls


class TestSingleton:
    def test_constructor_always_returns_the_same_instance(self):
        assert ToastDispatcher() is ToastDispatcher()

    def test_module_level_instance_is_the_same_singleton(self):
        assert ToastDispatcher() is toast_dispatcher


class TestNotify:
    def test_notify_forwards_message_and_defaults_to_info_type(self, fake_show):
        from blinkview.ui.widgets.toast import ToastType

        toast_dispatcher.notify("hello world")

        assert len(fake_show) == 1
        _, kwargs = fake_show[0]
        assert kwargs["message"] == "hello world"
        assert kwargs["toast_type"] == ToastType.INFO
        assert kwargs["duration"] == 5.0

    def test_notify_forwards_an_explicit_toast_type(self, fake_show):
        from blinkview.ui.widgets.toast import ToastType

        toast_dispatcher.notify("uh oh", toast_type=ToastType.ERROR)

        _, kwargs = fake_show[0]
        assert kwargs["toast_type"] == ToastType.ERROR

    def test_notify_forwards_custom_duration(self, fake_show):
        toast_dispatcher.notify("brief", duration=1.5)

        _, kwargs = fake_show[0]
        assert kwargs["duration"] == 1.5

    def test_notify_forwards_action_and_click_callbacks(self, fake_show):
        action_cb = lambda: None  # noqa: E731
        click_cb = lambda: None  # noqa: E731

        toast_dispatcher.notify("msg", action_text="Undo", action_callback=action_cb, click_callback=click_cb)

        _, kwargs = fake_show[0]
        assert kwargs["action_text"] == "Undo"
        assert kwargs["action_callback"] is action_cb
        assert kwargs["click_callback"] is click_cb

    def test_notify_forwards_explicit_parent(self, fake_show, qtbot):
        from qtpy.QtWidgets import QWidget

        parent = QWidget()
        qtbot.addWidget(parent)

        toast_dispatcher.notify("msg", parent=parent)

        _, kwargs = fake_show[0]
        assert kwargs["parent"] is parent

    def test_notify_without_kwargs_forwards_none_for_optional_fields(self, fake_show):
        toast_dispatcher.notify("plain message")

        _, kwargs = fake_show[0]
        assert kwargs["action_text"] is None
        assert kwargs["action_callback"] is None
        assert kwargs["click_callback"] is None
        assert kwargs["parent"] is None

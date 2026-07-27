# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.bindable import bindable


@bindable
class Plain:
    def __init__(self):
        self.own_attr = "set-by-original-init"


@bindable
class WithArgs:
    def __init__(self, value):
        self.value = value


def test_injects_default_bindable_attributes():
    obj = Plain()
    assert obj.shared is None
    assert obj.local is None
    assert obj.logger is None


def test_still_runs_the_original_init():
    obj = Plain()
    assert obj.own_attr == "set-by-original-init"


def test_preserves_args_and_kwargs_to_original_init():
    obj = WithArgs(42)
    assert obj.value == 42


def test_bind_system_sets_shared_and_local():
    obj = Plain()
    shared = object()
    local = object()

    obj.bind_system(shared, local)

    assert obj.shared is shared
    assert obj.local is local


def test_bind_system_pulls_logger_from_local_when_available():
    obj = Plain()

    class LocalWithLogger:
        def get_logger(self):
            return "the-logger"

    obj.bind_system(shared=None, local=LocalWithLogger())

    assert obj.logger == "the-logger"


def test_bind_system_leaves_logger_none_when_local_has_no_get_logger():
    obj = Plain()

    obj.bind_system(shared=None, local=object())

    assert obj.logger is None

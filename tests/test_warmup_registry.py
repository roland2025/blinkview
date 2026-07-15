# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core import warmup_registry
from blinkview.core.warmup_registry import DEFAULT_PRIORITY, register_warmup


@pytest.fixture(autouse=True)
def clean_registry():
    """Each test gets an empty _WARMUP_CALLBACKS list and leaves one behind, matching how
    NumbaWarmupHelper.run_all() clears it after a real run."""
    warmup_registry._WARMUP_CALLBACKS.clear()
    yield
    warmup_registry._WARMUP_CALLBACKS.clear()


def test_bare_decorator_registers_with_default_priority():
    @register_warmup
    def cb(helper):
        pass

    assert warmup_registry._WARMUP_CALLBACKS == [(DEFAULT_PRIORITY, cb)]


def test_decorator_with_explicit_priority():
    @register_warmup(priority=100)
    def cb(helper):
        pass

    assert warmup_registry._WARMUP_CALLBACKS == [(100, cb)]


def test_decorator_returns_the_original_function_unchanged():
    def cb(helper):
        return "result"

    wrapped_bare = register_warmup(cb)
    assert wrapped_bare is cb

    warmup_registry._WARMUP_CALLBACKS.clear()

    wrapped_priority = register_warmup(priority=5)(cb)
    assert wrapped_priority is cb


def test_run_all_executes_highest_priority_first():
    calls = []

    @register_warmup
    def default_cb(helper):
        calls.append("default")

    @register_warmup(priority=100)
    def high_cb(helper):
        calls.append("high")

    @register_warmup(priority=-5)
    def low_cb(helper):
        calls.append("low")

    from blinkview.core.warmup import NumbaWarmupHelper

    class FakeHelper:
        def __init__(self):
            self.log_pool = self

        def release_all(self):
            pass

    NumbaWarmupHelper.run_all(FakeHelper())

    assert calls == ["high", "default", "low"]


def test_run_all_preserves_registration_order_among_equal_priorities():
    calls = []

    @register_warmup
    def first(helper):
        calls.append("first")

    @register_warmup
    def second(helper):
        calls.append("second")

    @register_warmup
    def third(helper):
        calls.append("third")

    from blinkview.core.warmup import NumbaWarmupHelper

    class FakeHelper:
        def __init__(self):
            self.log_pool = self

        def release_all(self):
            pass

    NumbaWarmupHelper.run_all(FakeHelper())

    assert calls == ["first", "second", "third"]


def test_run_all_clears_the_registry_afterward():
    @register_warmup
    def cb(helper):
        pass

    from blinkview.core.warmup import NumbaWarmupHelper

    class FakeHelper:
        def __init__(self):
            self.log_pool = self

        def release_all(self):
            pass

    NumbaWarmupHelper.run_all(FakeHelper())

    assert warmup_registry._WARMUP_CALLBACKS == []

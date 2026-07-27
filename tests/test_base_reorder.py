# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.base_reorder import BaseReorder, ReorderFactory
from blinkview.core.constants import SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.logger import PrintLogger


def make_reorder(**config_overrides):
    reorder = BaseReorder()
    reorder.logger = PrintLogger("test.base_reorder")
    reorder.apply_config(config_overrides)
    return reorder


def test_default_delay_is_100ms():
    reorder = make_reorder()
    assert reorder.delay == 100


def test_delay_can_be_overridden_via_config():
    reorder = make_reorder(delay=250)
    assert reorder.delay == 250


def test_enabled_schema_default_is_true_overriding_base_daemon_default():
    """BaseReorder overrides BaseDaemon's 'enabled' default (False) to True via
    @override_property. The instance attribute stays False right after __init__ (BaseDaemon's
    raw __init__ unconditionally resets it) - the override only takes effect through the real
    construction path, i.e. hydrate_config() filling in the schema default before apply_config()
    is called, exactly as BaseFactory.build() does it."""
    reorder = BaseReorder()
    reorder.logger = PrintLogger("test.base_reorder")

    hydrated = reorder.hydrate_config({})
    reorder.apply_config(hydrated)

    assert reorder.enabled is True


def test_targets_storage():
    reorder = BaseReorder()
    assert reorder.targets == [SysCat.STORAGE]


def test_reorder_factory_is_a_base_factory_for_base_reorder():
    assert issubclass(ReorderFactory, BaseFactory)
    assert ReorderFactory.produces_type is BaseReorder

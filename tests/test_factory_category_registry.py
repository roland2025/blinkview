# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core import factory_category_registry
from blinkview.core.constants import FactoryCategory
from blinkview.core.factory_category_registry import (
    build_system_factory_registry,
    register_factory_category,
)
from tests.fakes.real_registry import make_real_registry


class FakeFactoryA:
    pass


class FakeFactoryB:
    pass


@pytest.fixture
def isolated_registrations(monkeypatch):
    """Swaps in an empty _FACTORY_REGISTRATIONS list so fake registrations in these tests don't
    leak into (or collide with) the real, module-import-time ones."""
    monkeypatch.setattr(factory_category_registry, "_FACTORY_REGISTRATIONS", [])


def test_register_factory_category_appends_without_raising(isolated_registrations):
    register_factory_category("widget")(FakeFactoryA)

    assert factory_category_registry._FACTORY_REGISTRATIONS == [("widget", FakeFactoryA)]


def test_register_factory_category_returns_the_class_unchanged(isolated_registrations):
    decorated = register_factory_category("widget")(FakeFactoryA)

    assert decorated is FakeFactoryA


def test_build_system_factory_registry_resolves_registrations(isolated_registrations):
    register_factory_category("widget")(FakeFactoryA)
    register_factory_category("gadget")(FakeFactoryB)

    registry = build_system_factory_registry()

    assert registry.get_factory("widget") is FakeFactoryA
    assert registry.get_factory("gadget") is FakeFactoryB


def test_build_system_factory_registry_raises_on_duplicate_category(isolated_registrations):
    register_factory_category("widget")(FakeFactoryA)
    register_factory_category("widget")(FakeFactoryB)

    with pytest.raises(ValueError):
        build_system_factory_registry()


def test_real_registrations_spot_check_across_subsystems():
    """Spot-checks the real, undisturbed _FACTORY_REGISTRATIONS list (populated by every
    @register_factory_category call fired at import time) resolves to the exact real Factory
    class across subsystems - not just that the helper works against an isolated fake list."""
    from blinkview.core.base_reorder import ReorderFactory
    from blinkview.core.central_storage import CentralFactory
    from blinkview.io.BaseReader import DeviceFactory
    from blinkview.parsers.frame_decoders import FrameDecoderFactory
    from blinkview.parsers.multi_rule_key_value import ExtractionRuleFactory

    registry = build_system_factory_registry()

    assert registry.get_factory(FactoryCategory.REORDER) is ReorderFactory
    assert registry.get_factory(FactoryCategory.CENTRAL) is CentralFactory
    assert registry.get_factory(FactoryCategory.SOURCE) is DeviceFactory
    assert registry.get_factory(FactoryCategory.FRAME_DECODER) is FrameDecoderFactory
    assert registry.get_factory(FactoryCategory.KEY_VALUE_RULE) is ExtractionRuleFactory


def test_real_registry_boots_with_decorator_registered_factories(tmp_path):
    """End-to-end check (per this session's established habit of verifying wiring changes through
    a real boot, not just per-piece unit tests): building a real Registry threads the
    decorator-based global registration all the way through, not just build_system_factory_registry()
    in isolation."""
    from blinkview.core.base_reorder import ReorderFactory
    from blinkview.core.central_storage import CentralFactory
    from blinkview.io.BaseReader import DeviceFactory

    reg = make_real_registry(tmp_path, "factory_category_e2e")
    try:
        factories = reg.system_ctx.factories
        assert factories.get_factory(FactoryCategory.REORDER) is ReorderFactory
        assert factories.get_factory(FactoryCategory.CENTRAL) is CentralFactory
        assert factories.get_factory(FactoryCategory.SOURCE) is DeviceFactory
    finally:
        reg.stop()

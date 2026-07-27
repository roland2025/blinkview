# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.factory_registry import FactoryRegistry


class DummyProducedType:
    @classmethod
    def get_config_schema(cls):
        return {"type": "object", "properties": {}}


class FakeFactory:
    produces_type = DummyProducedType
    build_calls = []

    @classmethod
    def build(cls, config, system_ctx, local_ctx, **kwargs):
        cls.build_calls.append((config, system_ctx, local_ctx, kwargs))
        return "built-instance"

    @classmethod
    def get_available_types(cls):
        return [("fake", "A fake type")]

    @classmethod
    def get_schema(cls, type_name):
        return {"type_name": type_name}


@pytest.fixture
def registry():
    return FactoryRegistry()


def test_register_and_get_factory(registry):
    registry.register("subscriber", FakeFactory)

    assert registry.get_factory("subscriber") is FakeFactory


def test_register_normalizes_category_case(registry):
    registry.register("Subscriber", FakeFactory)

    assert registry.get_factory("subscriber") is FakeFactory
    assert registry.get_factory("SUBSCRIBER") is FakeFactory


def test_register_duplicate_category_raises(registry):
    registry.register("subscriber", FakeFactory)

    with pytest.raises(ValueError):
        registry.register("subscriber", FakeFactory)


def test_get_factory_returns_none_for_unknown_category(registry):
    assert registry.get_factory("does-not-exist") is None


def test_build_delegates_to_the_registered_factory(registry):
    FakeFactory.build_calls.clear()
    registry.register("subscriber", FakeFactory)

    result = registry.build("subscriber", config={"type": "fake"}, system_ctx="sys", local_ctx="local", extra=1)

    assert result == "built-instance"
    assert FakeFactory.build_calls == [({"type": "fake"}, "sys", "local", {"extra": 1})]


def test_build_unknown_category_raises_keyerror(registry):
    with pytest.raises(KeyError):
        registry.build("does-not-exist")


def test_get_category_types_returns_available_types(registry):
    registry.register("subscriber", FakeFactory)

    assert registry.get_category_types("subscriber") == [("fake", "A fake type")]


def test_get_category_types_unknown_category_raises_keyerror(registry):
    with pytest.raises(KeyError):
        registry.get_category_types("does-not-exist")


def test_get_produced_type_returns_the_factorys_produced_type(registry):
    registry.register("subscriber", FakeFactory)

    assert registry.get_produced_type("subscriber") is DummyProducedType


def test_get_produced_type_unknown_category_raises_keyerror(registry):
    with pytest.raises(KeyError):
        registry.get_produced_type("does-not-exist")


def test_get_schema_delegates_to_the_factory(registry):
    registry.register("subscriber", FakeFactory)

    assert registry.get_schema("subscriber", "fake") == {"type_name": "fake"}


def test_get_schema_unknown_category_raises_keyerror(registry):
    with pytest.raises(KeyError):
        registry.get_schema("does-not-exist", "fake")


def test_get_base_schema_returns_produced_types_config_schema(registry):
    registry.register("subscriber", FakeFactory)

    assert registry.get_base_schema("subscriber") == {"type": "object", "properties": {}}


def test_get_base_schema_unknown_category_raises_keyerror(registry):
    with pytest.raises(KeyError):
        registry.get_base_schema("does-not-exist")

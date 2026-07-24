# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.configurable import (
    configurable,
    configuration_factory,
    configuration_property,
    on_config_change,
    override_property,
)


class TestConfigurableBasics:
    def test_adds_empty_config_properties_if_missing(self):
        @configurable
        class Foo:
            def __init__(self):
                pass

        assert Foo.CONFIG_PROPERTIES == {}

    def test_schema_shape_and_title(self):
        @configuration_property("name", type="string", default="bob")
        class Foo:
            def __init__(self):
                pass

        schema = Foo.get_config_schema()
        assert schema["type"] == "object"
        assert schema["title"] == "Foo Configuration"
        assert schema["properties"]["name"]["default"] == "bob"
        assert "required" not in schema  # nothing marked required

    def test_get_config_keys_preserves_declared_order(self):
        @configuration_property("a", type="integer", default=1)
        @configuration_property("b", type="integer", default=2)
        class Foo:
            def __init__(self):
                pass

        # Stacked decorators apply bottom-up; configuration_property compensates so the
        # schema/keys still read in top-to-bottom declaration order.
        assert Foo.get_config_keys() == ("a", "b")

    def test_init_hydrates_declared_defaults_onto_the_instance(self):
        @configuration_property("name", type="string", default="bob")
        @configuration_property("count", type="integer", default=5)
        class Foo:
            def __init__(self):
                pass

        f = Foo()
        assert f.name == "bob"
        assert f.count == 5

    def test_init_still_invokes_the_original_constructor(self):
        calls = []

        @configurable
        class Foo:
            def __init__(self, x):
                calls.append(x)

        Foo(42)
        assert calls == [42]

    def test_apply_config_defaults_to_apply_base_config(self):
        @configuration_property("name", type="string", default="bob")
        class Foo:
            def __init__(self):
                pass

        f = Foo()
        assert f.apply_config({"name": "alice"}) is True
        assert f.name == "alice"

    def test_apply_config_is_not_overwritten_when_class_defines_its_own(self):
        @configuration_property("name", type="string", default="bob")
        class Foo:
            def __init__(self):
                pass

            def apply_config(self, config):
                return "custom"

        assert Foo().apply_config({}) == "custom"


class TestApplyBaseConfig:
    def _make_class(self):
        @configuration_property("name", type="string", default="bob")
        @configuration_property("count", type="integer", default=5)
        class Foo:
            def __init__(self):
                pass

        return Foo

    def test_ignores_keys_outside_the_schema(self):
        f = self._make_class()()
        changed = f.apply_base_config({"unknown": 1})
        assert changed is False
        assert not hasattr(f, "unknown")

    def test_applies_known_keys_and_reports_changed(self):
        f = self._make_class()()
        changed = f.apply_base_config({"count": 10})
        assert changed is True
        assert f.count == 10

    def test_reports_unchanged_when_value_is_identical(self):
        f = self._make_class()()
        changed = f.apply_base_config({"count": 5})  # same as the hydrated default
        assert changed is False

    def test_triggers_on_config_change_callback_with_new_and_old_value(self):
        calls = []

        @configuration_property("count", type="integer", default=5)
        class Foo:
            def __init__(self):
                pass

            @on_config_change("count")
            def on_count_changed(self, new_value, old_value):
                calls.append((new_value, old_value))

        f = Foo()
        f.apply_base_config({"count": 10})
        assert calls == [(10, 5)]

    def test_callback_not_triggered_when_value_is_unchanged(self):
        calls = []

        @configuration_property("count", type="integer", default=5)
        class Foo:
            def __init__(self):
                pass

            @on_config_change("count")
            def on_count_changed(self, new_value, old_value):
                calls.append((new_value, old_value))

        f = Foo()
        f.apply_base_config({"count": 5})
        assert calls == []


class TestRequiredFields:
    def test_required_true_is_listed_and_stripped_from_the_property_schema(self):
        @configuration_property("token", type="string", required=True)
        class Foo:
            def __init__(self):
                pass

        schema = Foo.get_config_schema()
        assert schema["required"] == ["token"]
        assert "required" not in schema["properties"]["token"]


class TestConfigurationFactory:
    def test_marks_factory_category_in_the_schema(self):
        @configuration_factory("sources")
        @configuration_property("name", type="string", default="x")
        class Foo:
            def __init__(self):
                pass

        assert Foo.get_config_schema()["_factory"] == "sources"

    def test_absent_when_not_declared(self):
        @configuration_property("name", type="string", default="x")
        class Foo:
            def __init__(self):
                pass

        assert "_factory" not in Foo.get_config_schema()


class TestOverrideProperty:
    def test_child_overrides_parent_default_without_mutating_parent(self):
        @configuration_property("enabled", type="boolean", default=False)
        class Base:
            def __init__(self):
                pass

        @override_property("enabled", default=True, hidden=True)
        class Child(Base):
            def __init__(self):
                super().__init__()

        assert Base.get_config_schema()["properties"]["enabled"]["default"] is False
        child_schema = Child.get_config_schema()["properties"]["enabled"]
        assert child_schema["default"] is True
        assert child_schema["hidden"] is True

        assert Child().enabled is True

    def test_raises_if_property_is_not_found_in_the_parent_hierarchy(self):
        @configuration_property("name", type="string", default="x")
        class Base:
            def __init__(self):
                pass

        with pytest.raises(ValueError):

            @override_property("missing", default="y")
            class Child(Base):
                def __init__(self):
                    super().__init__()


class TestInheritanceMerging:
    def test_subclass_properties_are_added_alongside_parent_properties(self):
        @configuration_property("a", type="integer", default=1)
        class Base:
            def __init__(self):
                pass

        @configuration_property("b", type="integer", default=2)
        class Child(Base):
            def __init__(self):
                super().__init__()

        schema = Child.get_config_schema()
        assert set(schema["properties"].keys()) == {"a", "b"}

        c = Child()
        assert c.a == 1
        assert c.b == 2


class TestHydrateConfig:
    def test_fills_in_missing_keys_with_defaults(self):
        @configuration_property("name", type="string", default="bob")
        @configuration_property("count", type="integer", default=5)
        class Foo:
            def __init__(self):
                pass

        hydrated = Foo.hydrate_config({"count": 99})
        assert hydrated["count"] == 99
        assert hydrated["name"] == "bob"

    def test_does_not_mutate_the_input_dict(self):
        @configuration_property("name", type="string", default="bob")
        class Foo:
            def __init__(self):
                pass

        original = {}
        Foo.hydrate_config(original)
        assert original == {}


class TestNestedObjectHydration:
    def test_required_object_is_auto_instantiated_with_nested_defaults(self):
        @configuration_property(
            "connection",
            type="object",
            required=True,
            properties={
                "host": {"type": "string", "default": "localhost"},
                "port": {"type": "integer", "default": 9000},
            },
        )
        class Foo:
            def __init__(self):
                pass

        f = Foo()
        assert f.connection == {"host": "localhost", "port": 9000}

    def test_optional_object_without_a_default_stays_unset(self):
        @configuration_property(
            "connection",
            type="object",
            properties={"host": {"type": "string", "default": "localhost"}},
        )
        class Foo:
            def __init__(self):
                pass

        f = Foo()
        assert getattr(f, "connection", None) is None

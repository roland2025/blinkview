# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

import pytest

from blinkview.core.array_pool import NumpyArrayPool
from blinkview.core.id_registry.registry import IDRegistry


@pytest.fixture
def registry():
    return IDRegistry(NumpyArrayPool())


class TestDeviceCreation:
    def test_get_device_lowercases_name(self, registry):
        device = registry.get_device("ESP32")
        assert device.name == "esp32"

    def test_get_device_is_memoized_by_name(self, registry):
        d1 = registry.get_device("esp32")
        d2 = registry.get_device("esp32")
        assert d1 is d2

    def test_get_device_memoization_is_case_insensitive(self, registry):
        d1 = registry.get_device("esp32")
        d2 = registry.get_device("ESP32")
        assert d1 is d2

    def test_distinct_names_produce_distinct_devices_with_increasing_ids(self, registry):
        d1 = registry.get_device("esp32")
        d2 = registry.get_device("nrf52")
        assert d1.id != d2.id
        assert d2.id > d1.id

    def test_device_gets_a_root_module(self, registry):
        device = registry.get_device("esp32")
        assert device.root is not None
        assert device.root.name == ""
        assert device.root.depth == 0
        assert device.root.device is device

    def test_root_module_is_registered_in_the_registry(self, registry):
        device = registry.get_device("esp32")
        assert registry.modules[device.root.id] is device.root

    def test_default_essential_true_does_not_affect_root(self, registry):
        # Quirk: DeviceIdentity always constructs its root ModuleIdentity without passing
        # is_essential, so the root is always essential regardless of default_essential.
        device = registry.get_device("esp32", essential=False)
        assert device.root.is_essential is True

    def test_default_essential_propagates_to_new_modules(self, registry):
        device = registry.get_device("esp32", essential=False)
        module = device.get_module("wifi")
        assert module.is_essential is False

        device2 = registry.get_device("nrf52", essential=True)
        module2 = device2.get_module("ble")
        assert module2.is_essential is True


class TestGetModule:
    def test_creates_and_returns_module(self, registry):
        device = registry.get_device("esp32")
        module = device.get_module("wifi")
        assert module.short_name == "wifi"
        assert module.name == "wifi"
        assert module.device is device
        assert module.depth == 1

    def test_repeated_calls_return_the_same_object(self, registry):
        device = registry.get_device("esp32")
        m1 = device.get_module("wifi")
        m2 = device.get_module("wifi")
        assert m1 is m2

    def test_path_lookup_is_case_insensitive(self, registry):
        device = registry.get_device("esp32")
        m1 = device.get_module("wifi")
        m2 = device.get_module("WiFi")
        assert m1 is m2

    def test_nested_path_creates_intermediate_and_leaf_modules(self, registry):
        device = registry.get_device("esp32")
        leaf = device.get_module("wifi.tx")

        wifi = device.path_lookup["wifi"]
        assert leaf.name == "wifi.tx"
        assert leaf.short_name == "tx"
        assert leaf.depth == 2
        assert wifi.depth == 1
        assert wifi.submodules["tx"] is leaf
        assert leaf in wifi.submodule_list

    def test_nested_path_reuses_existing_intermediate_module(self, registry):
        device = registry.get_device("esp32")
        device.get_module("wifi.tx")
        wifi_before = device.path_lookup["wifi"]

        device.get_module("wifi.rx")
        wifi_after = device.path_lookup["wifi"]

        assert wifi_before is wifi_after
        assert set(wifi_before.submodules.keys()) == {"tx", "rx"}

    def test_invalid_path_raises_value_error(self, registry):
        device = registry.get_device("esp32")
        with pytest.raises(ValueError):
            device.get_module("wifi tx!")

    def test_module_ids_are_globally_unique_across_devices(self, registry):
        d1 = registry.get_device("esp32")
        d2 = registry.get_device("nrf52")
        m1 = d1.get_module("wifi")
        m2 = d2.get_module("wifi")
        assert m1.id != m2.id

    def test_new_module_is_registered_in_registry_modules_table(self, registry):
        device = registry.get_device("esp32")
        module = device.get_module("wifi")
        assert registry.modules[module.id] is module
        assert module in registry.module_list


class TestModuleIdentityHelpers:
    def test_name_with_device(self, registry):
        device = registry.get_device("esp32")
        module = device.get_module("wifi.tx")
        assert module.name_with_device() == "esp32.wifi.tx"

    def test_parent_resolves_via_registry(self, registry):
        device = registry.get_device("esp32")
        leaf = device.get_module("wifi.tx")
        wifi = device.path_lookup["wifi"]
        assert leaf.parent is wifi
        assert wifi.parent is device.root

    def test_root_has_no_parent(self, registry):
        device = registry.get_device("esp32")
        assert device.root.parent is None

    def test_get_all_descendants(self, registry):
        device = registry.get_device("esp32")
        wifi = device.get_module("wifi")
        tx = device.get_module("wifi.tx")
        rx = device.get_module("wifi.rx")
        device.get_module("ble")  # unrelated sibling, must not appear

        descendants = wifi.get_all_descendants()
        assert set(descendants) == {tx, rx}

    def test_set_essential_updates_module_and_registry(self, registry):
        device = registry.get_device("esp32")
        module = device.get_module("wifi")
        assert module.is_essential is True

        module.set_essential(False)

        assert module.is_essential is False
        assert registry.is_module_essential(module.id) is False

    def test_str_and_repr(self, registry):
        device = registry.get_device("esp32")
        module = device.get_module("wifi.tx")
        assert str(module) == "wifi.tx"
        assert repr(module) == f"ModuleIdentity({module.id}: 'esp32.wifi.tx')"
        assert str(device) == "esp32"
        assert repr(device) == f"DeviceIdentity({device.id}: 'esp32')"


class TestGetAllModuleIds:
    def test_includes_root_and_created_modules(self, registry):
        device = registry.get_device("esp32")
        wifi = device.get_module("wifi")
        tx = device.get_module("wifi.tx")

        ids = set(device.get_all_module_ids().tolist())
        assert device.root.id in ids
        assert wifi.id in ids
        assert tx.id in ids

    def test_does_not_include_other_devices_modules(self, registry):
        device = registry.get_device("esp32")
        other = registry.get_device("nrf52")
        other_module = other.get_module("ble")

        ids = set(device.get_all_module_ids().tolist())
        assert other_module.id not in ids

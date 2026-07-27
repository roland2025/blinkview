# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from blinkview.core.constants import SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.logger import PrintLogger
from blinkview.io.BaseReader import BaseReader, DeviceFactory


def make_reader(**config_overrides):
    reader = BaseReader()
    reader.logger = PrintLogger("test.base_reader")
    reader.apply_config(config_overrides)
    return reader


def test_default_name():
    reader = make_reader()
    assert reader.name == "default_source"


def test_name_can_be_overridden_via_config():
    reader = make_reader(name="my-device")
    assert reader.name == "my-device"


def test_targets_parser():
    reader = BaseReader()
    assert reader.targets == [SysCat.PARSER]


def test_get_commands_returns_empty_list_by_default():
    reader = BaseReader()
    assert reader.get_commands() == []


def test_apply_config_creates_state_open_and_link_child_loggers():
    reader = make_reader()

    assert reader.logger_state is not None
    assert reader.logger_state_open is not None
    assert reader.logger_link is not None


def test_apply_config_does_not_recreate_child_loggers_on_second_call():
    reader = make_reader()
    state, state_open, link = reader.logger_state, reader.logger_state_open, reader.logger_link

    reader.apply_config({})

    assert reader.logger_state is state
    assert reader.logger_state_open is state_open
    assert reader.logger_link is link


def test_device_factory_is_a_base_factory_for_base_reader():
    assert issubclass(DeviceFactory, BaseFactory)
    assert DeviceFactory.produces_type is BaseReader

# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2026 Roland Uuesoo

from types import SimpleNamespace

from blinkview.core.constants import SysCat
from blinkview.core.factory import BaseFactory
from blinkview.core.logger import PrintLogger
from blinkview.parsers.parser import BaseParser, ParserFactory


class FakeTimeSyncer:
    """Stand-in for a built time_sync component - tracks lifecycle calls instead of
    running a real syncer thread."""

    def __init__(self):
        self.started = False
        self.stopped = False

    def start(self):
        self.started = True

    def stop(self):
        self.stopped = True


class FakeFactories:
    def __init__(self):
        self.built = []

    def build(self, category, config, system_ctx=None, local_ctx=None, **kwargs):
        self.built.append((category, config, system_ctx, local_ctx))
        return FakeTimeSyncer()


def make_parser(**config_overrides):
    parser = BaseParser()
    parser.logger = PrintLogger("test.base_parser")
    parser.shared = SimpleNamespace(factories=FakeFactories(), time_ns=lambda: 1_000_000_000)
    parser.apply_config(config_overrides)
    return parser


def test_default_config_values():
    parser = make_parser()
    assert parser.name == "pipeline"
    assert parser.delay == 30
    assert parser.sources_ == []


def test_targets_reorder_and_storage():
    parser = BaseParser()
    assert parser.targets == [SysCat.REORDER, SysCat.STORAGE]


def test_apply_config_without_time_sync_leaves_syncer_unset():
    parser = make_parser()
    assert parser.time_syncer is None
    assert parser.sync_state is None


def test_apply_config_with_time_sync_builds_and_subscribes_a_syncer():
    parser = make_parser(time_sync={"type": "fake"})

    assert isinstance(parser.time_syncer, FakeTimeSyncer)
    assert parser.time_syncer in parser.subscribers
    assert parser.time_syncer in parser._children
    assert parser.sync_state is not None


def test_apply_config_with_time_sync_builds_with_expected_args():
    parser = make_parser(time_sync={"type": "fake"})

    category, config, system_ctx, local_ctx = parser.shared.factories.built[0]
    assert category == "time_sync"
    assert config == {"type": "fake"}
    assert system_ctx is parser.shared
    assert local_ctx.parser is parser


def test_reapplying_time_sync_config_stops_and_replaces_the_old_syncer():
    parser = make_parser(time_sync={"type": "fake"})
    old_syncer = parser.time_syncer

    parser.apply_config({"time_sync": {"type": "fake"}})

    assert old_syncer.stopped is True
    assert old_syncer not in parser.subscribers
    assert parser.time_syncer is not old_syncer
    assert parser.time_syncer in parser.subscribers


def test_parser_factory_is_a_base_factory_for_base_parser():
    assert issubclass(ParserFactory, BaseFactory)
    assert ParserFactory.produces_type is BaseParser
